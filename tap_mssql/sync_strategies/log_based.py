#!/usr/bin/env python3
# pylint: disable=duplicate-code,too-many-locals,simplifiable-if-expression

import copy

import singer
from singer.schema import Schema

import tap_mssql.sync_strategies.common as common
from tap_mssql.connection import MSSQLConnection, connect_with_backoff

LOGGER = singer.get_logger()


def py_bin_to_mssql(binary_value):
    return "CONVERT(BINARY(10),'0x" + binary_value + "',1)"


def verify_change_data_capture_table(connection, schema_name, table_name):
    cur = connection.cursor()
    cur.execute(
        """select s.name as schema_name, t.name as table_name, t.is_tracked_by_cdc, t.object_id
                   from sys.tables t
                   join sys.schemas s on (s.schema_id = t.schema_id)
                   and  t.name = '{}'
                   and  s.name = '{}'""".format(
            table_name, schema_name
        )
    )
    row = cur.fetchone()

    return row[2]


def verify_change_data_capture_databases(connection):
    cur = connection.cursor()
    cur.execute(
        """SELECT name, is_cdc_enabled
                   FROM sys.databases WHERE database_id = DB_ID()"""
    )
    row = cur.fetchone()

    LOGGER.info(
        "CDC Databases enable : Database %s, Enabled %s",
        *row,
    )
    return row


def verify_read_isolation_databases(connection):
    cur = connection.cursor()
    cur.execute(
        """SELECT DB_NAME(database_id),
                          is_read_committed_snapshot_on,
                          snapshot_isolation_state_desc
                   FROM sys.databases
                   WHERE database_id = DB_ID();"""
    )
    row = cur.fetchone()

    if row[1] is False and row[2] == "OFF":
        LOGGER.warning(
            (
                "CDC Databases may result in dirty reads. Consider enabling Read Committed"
                " or Snapshot isolation: Database %s, Is Read Committed Snapshot is %s,"
                " Snapshot Isolation is %s"
            ),
            *row,
        )
    return row


def get_lsn_available_range(connection, capture_instance_name):
    cur = connection.cursor()
    query = """SELECT sys.fn_cdc_get_min_lsn ( '{}' ) lsn_from
                    , sys.fn_cdc_get_max_lsn () lsn_to
               ;
            """.format(
        capture_instance_name
    )
    cur.execute(query)
    row = cur.fetchone()

    if row[0] is None:  # Test that the lsn_from is not NULL i.e. there is change data to process
        LOGGER.info("No data available to process in CDC table %s", capture_instance_name)
    else:
        LOGGER.info(
            "Data available in cdc table %s from lsn %s", capture_instance_name, row[0].hex()
        )

    return row


def get_to_lsn(connection):
    cur = connection.cursor()
    query = """select sys.fn_cdc_get_max_lsn () """

    cur.execute(query)
    row = cur.fetchone()

    LOGGER.info(
        "Max LSN ID : %s",
        row[0].hex(),
    )
    return row


def add_synthetic_keys_to_schema(catalog_entry):
    catalog_entry.schema.properties["_sdc_operation_type"] = Schema(
        description="Source operation I=Insert, D=Delete, U=Update",
        type=["null", "string"],
        format="string",
    )
    catalog_entry.schema.properties["_sdc_lsn_commit_timestamp"] = Schema(
        description="Source system commit timestamp", type=["null", "string"], format="date-time"
    )
    catalog_entry.schema.properties["_sdc_lsn_deleted_at"] = Schema(
        description="Source system delete timestamp", type=["null", "string"], format="date-time"
    )
    catalog_entry.schema.properties["_sdc_lsn_value"] = Schema(
        description="Source system log sequence number (LSN)",
        type=["null", "string"],
        format="string",
    )
    catalog_entry.schema.properties["_sdc_lsn_seq_value"] = Schema(
        description="Source sequence number within the system log sequence number (LSN)",
        type=["null", "string"],
        format="string",
    )
    catalog_entry.schema.properties["_sdc_lsn_operation"] = Schema(
        description=(
            "The operation that took place (1=Delete, 2=Insert, 3=Update (Before Image),"
            "4=Update (After Image) )"
        ),
        type=["null", "integer"],
        format="integer",
    )

    return catalog_entry


def generate_bookmark_keys(catalog_entry):

    # TO_DO:
    # 1. check the use of the top three values above and the parameter value, seem to not be required.
    # 2. check the base_bookmark_keys required
    base_bookmark_keys = {
        "last_lsn_fetched",
        "max_lsn_values",
        "lsn",
        "version",
        "initial_full_table_complete",
    }

    bookmark_keys = base_bookmark_keys

    return bookmark_keys


def sync_historic_table(mssql_conn, config, catalog_entry, state, columns, stream_version):
    mssql_conn = MSSQLConnection(config)
    common.whitelist_bookmark_keys(
        generate_bookmark_keys(catalog_entry), catalog_entry.tap_stream_id, state
    )

    # Add additional keys to the columns
    extended_columns = columns + [
        "_sdc_operation_type",
        "_sdc_lsn_commit_timestamp",
        "_sdc_lsn_deleted_at",
        "_sdc_lsn_value",
        "_sdc_lsn_seq_value",
        "_sdc_lsn_operation",
    ]

    bookmark = state.get("bookmarks", {}).get(catalog_entry.tap_stream_id, {})
    version_exists = True if "version" in bookmark else False

    initial_full_table_complete = singer.get_bookmark(
        state, catalog_entry.tap_stream_id, "initial_full_table_complete"
    )

    state_version = singer.get_bookmark(state, catalog_entry.tap_stream_id, "version")

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream, version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if not initial_full_table_complete and not (version_exists and state_version is None):
        singer.write_message(activate_version_message)

    with connect_with_backoff(mssql_conn) as open_conn:
        with open_conn.cursor() as cur:

            escaped_columns = map(lambda c: common.prepare_columns_sql(catalog_entry, c), columns)
            table_name = catalog_entry.table
            schema_name = common.get_database_name(catalog_entry)

            if not verify_change_data_capture_table(mssql_conn, schema_name, table_name):
                raise Exception(
                    (
                        "Error {}.{}: does not have change data capture enabled. Call EXEC"
                        " sys.sp_cdc_enable_table with relevant parameters to enable CDC."
                    ).format(schema_name, table_name)
                )

            verify_read_isolation_databases(mssql_conn)

            # Store the current database lsn number, will use this to store at the end of the initial load.
            # Note: Recommend no transactions loaded when the initial loads are performed.
            # Have captured the to_lsn before the initial load sync in-case records are added during the sync.
            lsn_to = str(get_to_lsn(mssql_conn)[0].hex())

            select_sql = """
                            SELECT {}
                                ,'I' _sdc_operation_type
                                , cast('1900-01-01' as datetime) _sdc_lsn_commit_timestamp
                                , null _sdc_lsn_deleted_at
                                , '00000000000000000000' _sdc_lsn_value
                                , '00000000000000000000' _sdc_lsn_seq_value
                                , 2 as _sdc_lsn_operation
                            FROM {}.{}
                            ;""".format(
                ",".join(escaped_columns), schema_name, table_name
            )
            params = {}

            common.sync_query(
                cur,
                catalog_entry,
                state,
                select_sql,
                extended_columns,
                stream_version,
                params,
                config,
            )
            state = singer.write_bookmark(state, catalog_entry.tap_stream_id, "lsn", lsn_to)

    # store the state of the table lsn's after the initial load ready for the next CDC run
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    # clear max pk value and last pk fetched upon successful sync
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, "max_pk_values")
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, "last_pk_fetched")

    singer.write_message(activate_version_message)


def sync_table(mssql_conn, config, catalog_entry, state, columns, stream_version):
    mssql_conn = MSSQLConnection(config)
    common.whitelist_bookmark_keys(
        generate_bookmark_keys(catalog_entry), catalog_entry.tap_stream_id, state
    )

    # Add additional keys to the columns
    extended_columns = columns + [
        "_sdc_operation_type",
        "_sdc_lsn_commit_timestamp",
        "_sdc_lsn_deleted_at",
        "_sdc_lsn_value",
        "_sdc_lsn_seq_value",
        "_sdc_lsn_operation",
    ]

    bookmark = state.get("bookmarks", {}).get(catalog_entry.tap_stream_id, {})
    version_exists = True if "version" in bookmark else False

    initial_full_table_complete = singer.get_bookmark(
        state, catalog_entry.tap_stream_id, "initial_full_table_complete"
    )

    state_version = singer.get_bookmark(state, catalog_entry.tap_stream_id, "version")

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream, version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if not initial_full_table_complete and not (version_exists and state_version is None):
        singer.write_message(activate_version_message)

    with connect_with_backoff(mssql_conn) as open_conn:
        with open_conn.cursor() as cur:

            state_last_lsn = singer.get_bookmark(state, catalog_entry.tap_stream_id, "lsn")

            escaped_columns = map(lambda c: common.prepare_columns_sql(catalog_entry, c), columns)
            table_name = catalog_entry.table
            schema_name = common.get_database_name(catalog_entry)
            schema_table = schema_name + "_" + table_name

            if not verify_change_data_capture_table(mssql_conn, schema_name, table_name):
                raise Exception(
                    (
                        "Error {}.{}: does not have change data capture enabled. "
                        "Call EXEC sys.sp_cdc_enable_table with relevant parameters to enable CDC."
                    ).format(schema_name, table_name)
                )

            lsn_range = get_lsn_available_range(mssql_conn, schema_table)

            if lsn_range[0] is not None:  # Test to see if there are any change records to process
                lsn_from = str(lsn_range[0].hex())
                lsn_to = str(lsn_range[1].hex())

                if lsn_from <= state_last_lsn:
                    LOGGER.info(
                        (
                            "The last lsn processed as per the state file %s, minimum available lsn"
                            " for extract table %s, and the maximum lsn is %s."
                        ),
                        state_last_lsn,
                        lsn_from,
                        lsn_to,
                    )
                    if lsn_to == state_last_lsn:
                        LOGGER.info(
                            (
                                "The last lsn processed as per the state file is equal to the max"
                                " lsn available - no changes expected - state lsn will not be incremented"
                            ),
                        )
                        from_lsn_expression = "{}".format(py_bin_to_mssql(state_last_lsn))
                    else:
                        from_lsn_expression = (
                            (
                                "sys.fn_cdc_increment_lsn({})"
                            ).format(py_bin_to_mssql(state_last_lsn))
                        )
                else:
                    raise Exception(
                        (
                            "Error {}.{}: CDC changes have expired, the minimum lsn is {}, the last"
                            " processed lsn is {}. Recommend a full load as there may be missing data."
                        ).format(schema_name, table_name, lsn_from, state_last_lsn)
                    )

                select_sql = """DECLARE @from_lsn binary (10), @to_lsn binary (10)

                                SET @from_lsn = {}
                                SET @to_lsn = {}

                                SELECT {}
                                    ,case __$operation
                                        when 2 then 'I'
                                        when 4 then 'U'
                                        when 1 then 'D'
                                    end _sdc_operation_type
                                    , sys.fn_cdc_map_lsn_to_time(__$start_lsn) _sdc_lsn_commit_timestamp
                                    , case __$operation
                                        when 1 then sys.fn_cdc_map_lsn_to_time(__$start_lsn)
                                        else null
                                        end _sdc_lsn_deleted_at
                                    , __$start_lsn _sdc_lsn_value
                                    , __$seqval _sdc_lsn_seq_value
                                    , __$operation _sdc_lsn_operation
                                FROM cdc.fn_cdc_get_all_changes_{}(@from_lsn, @to_lsn, 'all')
                                ORDER BY __$start_lsn, __$seqval, __$operation
                                ;""".format(
                    from_lsn_expression,
                    py_bin_to_mssql(lsn_to),
                    ",".join(escaped_columns),
                    schema_table,
                )

                params = {}

                common.sync_query(
                    cur,
                    catalog_entry,
                    state,
                    select_sql,
                    extended_columns,
                    stream_version,
                    params,
                    config,
                )

            else:
                # Store the current database lsn number, need to store the latest lsn checkpoint because the
                # CDC logs expire after a point in time. Therefore if there are no records read, then refresh
                # the max lsn_to to the latest LSN in the database.
                lsn_to = str(get_to_lsn(mssql_conn)[0].hex())

            state = singer.write_bookmark(state, catalog_entry.tap_stream_id, "lsn", lsn_to)
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    # clear max lsn value and last lsn fetched upon successful sync
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, "max_lsn_values")
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, "last_lsn_fetched")

    singer.write_message(activate_version_message)
