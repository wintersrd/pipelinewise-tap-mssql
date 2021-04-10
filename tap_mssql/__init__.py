#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,too-many-branches,invalid-name,duplicate-code,too-many-statements

import datetime
import collections
import itertools
from itertools import dropwhile
import json
import logging
import copy
import uuid

import pymssql

import singer
import singer.metrics as metrics
import singer.schema

from singer import bookmarks
from singer import metadata
from singer import utils
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

import tap_mssql.sync_strategies.common as common
import tap_mssql.sync_strategies.full_table as full_table
import tap_mssql.sync_strategies.incremental as incremental

from tap_mssql.connection import connect_with_backoff, MSSQLConnection


Column = collections.namedtuple(
    "Column",
    [
        "table_schema",
        "table_name",
        "column_name",
        "data_type",
        "character_maximum_length",
        "numeric_precision",
        "numeric_scale",
        "is_primary_key",
    ],
)

REQUIRED_CONFIG_KEYS = ["host", "database", "user", "password"]

LOGGER = singer.get_logger()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

STRING_TYPES = set(
    [
        "char",
        "enum",
        "longtext",
        "mediumtext",
        "text",
        "varchar",
        "uniqueidentifier",
        "nvarchar",
        "nchar",
    ]
)

BYTES_FOR_INTEGER_TYPE = {
    "tinyint": 1,
    "smallint": 2,
    "mediumint": 3,
    "int": 4,
    "real": 4,
    "bigint": 8,
}

FLOAT_TYPES = set(["float", "double", "money"])

DATETIME_TYPES = set(["datetime", "timestamp", "date", "time", "smalldatetime"])

VARIANT_TYPES = set(["json"])


def schema_for_column(c):
    """Returns the Schema object for the given Column."""
    data_type = c.data_type.lower()

    inclusion = "available"

    if c.is_primary_key == 1:
        inclusion = "automatic"

    result = Schema(inclusion=inclusion)

    if data_type == "bit":
        result.type = ["null", "boolean"]

    elif data_type in BYTES_FOR_INTEGER_TYPE:
        result.type = ["null", "integer"]
        bits = BYTES_FOR_INTEGER_TYPE[data_type] * 8
        result.minimum = 0 - 2 ** (bits - 1)
        result.maximum = 2 ** (bits - 1) - 1

    elif data_type in FLOAT_TYPES:
        result.type = ["null", "number"]
        result.multipleOf = 10 ** (0 - (c.numeric_scale or 6))

    elif data_type in ["decimal", "numeric"]:
        result.type = ["null", "number"]
        result.multipleOf = 10 ** (0 - c.numeric_scale)
        return result

    elif data_type in STRING_TYPES:
        result.type = ["null", "string"]
        result.maxLength = c.character_maximum_length

    elif data_type in DATETIME_TYPES:
        result.type = ["null", "string"]
        result.format = "date-time"

    elif data_type in VARIANT_TYPES:
        result.type = ["null", "object"]

    else:
        result = Schema(None, inclusion="unsupported", description="Unsupported column type",)
    return result


def create_column_metadata(cols):
    mdata = {}
    mdata = metadata.write(mdata, (), "selected-by-default", False)
    for c in cols:
        schema = schema_for_column(c)
        mdata = metadata.write(
            mdata,
            ("properties", c.column_name),
            "selected-by-default",
            schema.inclusion != "unsupported",
        )
        mdata = metadata.write(
            mdata, ("properties", c.column_name), "sql-datatype", c.data_type.lower()
        )

    return metadata.to_list(mdata)


def discover_catalog(mssql_conn, config):
    """Returns a Catalog describing the structure of the database."""
    LOGGER.info("Preparing Catalog")
    mssql_conn = MSSQLConnection(config)
    filter_dbs_config = config.get("filter_dbs")

    if filter_dbs_config:
        filter_dbs_clause = ",".join(["'{}'".format(db) for db in filter_dbs_config.split(",")])

        table_schema_clause = "WHERE c.table_schema IN ({})".format(filter_dbs_clause)
    else:
        table_schema_clause = """
        WHERE c.table_schema NOT IN (
        'information_schema',
        'performance_schema',
        'sys'
        )"""

    with connect_with_backoff(mssql_conn) as open_conn:
        cur = open_conn.cursor()
        LOGGER.info("Fetching tables")
        cur.execute(
            """SELECT table_schema,
                table_name,
                table_type
            FROM information_schema.tables c
            {}
        """.format(
                table_schema_clause
            )
        )
        table_info = {}

        for (db, table, table_type) in cur.fetchall():
            if db not in table_info:
                table_info[db] = {}

            table_info[db][table] = {"row_count": None, "is_view": table_type == "VIEW"}
        LOGGER.info("Tables fetched, fetching columns")
        cur.execute(
            """with constraint_columns as (
                select c.table_schema
                , c.table_name
                , c.column_name

                from information_schema.constraint_column_usage c

                join information_schema.table_constraints tc
                        on tc.table_schema = c.table_schema
                        and tc.table_name = c.table_name
                        and tc.constraint_name = c.constraint_name
                        and tc.constraint_type in ('PRIMARY KEY', 'UNIQUE'))
                SELECT c.table_schema,
                    c.table_name,
                    c.column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    case when cc.column_name is null then 0 else 1 end
                FROM information_schema.columns c

                left join constraint_columns cc
                    on cc.table_name = c.table_name
                    and cc.table_schema = c.table_schema
                    and cc.column_name = c.column_name

                {}
                ORDER BY c.table_schema, c.table_name
        """.format(
                table_schema_clause
            )
        )
        columns = []
        rec = cur.fetchone()
        while rec is not None:
            columns.append(Column(*rec))
            rec = cur.fetchone()
        LOGGER.info("Columns Fetched")
        entries = []
        for (k, cols) in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
            cols = list(cols)
            (table_schema, table_name) = k
            schema = Schema(
                type="object", properties={c.column_name: schema_for_column(c) for c in cols}
            )
            md = create_column_metadata(cols)
            md_map = metadata.to_map(md)

            md_map = metadata.write(md_map, (), "database-name", table_schema)

            is_view = table_info[table_schema][table_name]["is_view"]

            if table_schema in table_info and table_name in table_info[table_schema]:
                row_count = table_info[table_schema][table_name].get("row_count")

                if row_count is not None:
                    md_map = metadata.write(md_map, (), "row-count", row_count)

                md_map = metadata.write(md_map, (), "is-view", is_view)

            key_properties = [c.column_name for c in cols if c.is_primary_key == 1]

            md_map = metadata.write(md_map, (), "table-key-properties", key_properties)

            entry = CatalogEntry(
                table=table_name,
                stream=table_name,
                metadata=metadata.to_list(md_map),
                tap_stream_id=common.generate_tap_stream_id(table_schema, table_name),
                schema=schema,
            )

            entries.append(entry)
    LOGGER.info("Catalog ready")
    return Catalog(entries)


def do_discover(mssql_conn, config):
    discover_catalog(mssql_conn, config).dump()


# TODO: Maybe put in a singer-db-utils library.
def desired_columns(selected, table_schema):

    """Return the set of column names we need to include in the SELECT.

    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    """
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in table_schema.properties.items():
        all_columns.add(column)
        inclusion = column_schema.inclusion
        if inclusion == "automatic":
            automatic.add(column)
        elif inclusion == "available":
            available.add(column)
        elif inclusion == "unsupported":
            unsupported.add(column)
        else:
            raise Exception("Unknown inclusion " + inclusion)

    selected_but_unsupported = selected.intersection(unsupported)
    if selected_but_unsupported:
        LOGGER.warning(
            "Columns %s were selected but are not supported. Skipping them.",
            selected_but_unsupported,
        )

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning("Columns %s were selected but do not exist.", selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning(
            "Columns %s are primary keys but were not selected. Adding them.",
            not_selected_but_automatic,
        )

    return selected.intersection(available).union(automatic)


def is_valid_currently_syncing_stream(selected_stream, state):
    return True


def resolve_catalog(discovered_catalog, streams_to_sync):
    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams_to_sync:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_metadata.get((), {}).get("replication-key")

        discovered_table = discovered_catalog.get_stream(catalog_entry.tap_stream_id)
        database_name = common.get_database_name(catalog_entry)

        if not discovered_table:
            LOGGER.warning(
                "Database %s table %s was selected but does not exist",
                database_name,
                catalog_entry.table,
            )
            continue

        selected = {
            k
            for k, v in discovered_table.schema.properties.items()
            if common.property_is_selected(catalog_entry, k) or k == replication_key
        }

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema)
        result.streams.append(
            CatalogEntry(
                tap_stream_id=catalog_entry.tap_stream_id,
                metadata=catalog_entry.metadata,
                stream=catalog_entry.tap_stream_id,
                table=catalog_entry.table,
                schema=Schema(
                    type="object",
                    properties={col: discovered_table.schema.properties[col] for col in columns},
                ),
            )
        )

    return result


def get_non_binlog_streams(mssql_conn, catalog, config, state):
    """Returns the Catalog of data we're going to sync for all SELECT-based
    streams (i.e. INCREMENTAL, FULL_TABLE, and LOG_BASED that require a historical
    sync). LOG_BASED streams that require a historical sync are inferred from lack
    of any state.

    Using the Catalog provided from the input file, this function will return a
    Catalog representing exactly which tables and columns that will be emitted
    by SELECT-based syncs. This is achieved by comparing the input Catalog to a
    freshly discovered Catalog to determine the resulting Catalog.

    The resulting Catalog will include the following any streams marked as
    "selected" that currently exist in the database. Columns marked as "selected"
    and those labled "automatic" (e.g. primary keys and replication keys) will be
    included. Streams will be prioritized in the following order:
      1. currently_syncing if it is SELECT-based
      2. any streams that do not have state
      3. any streams that do not have a replication method of LOG_BASED

    """
    mssql_conn = MSSQLConnection(config)
    discovered = discover_catalog(mssql_conn, config)

    # Filter catalog to include only selected streams
    selected_streams = list(filter(lambda s: common.stream_is_selected(s), catalog.streams))
    streams_with_state = []
    streams_without_state = []

    for stream in selected_streams:
        stream_metadata = metadata.to_map(stream.metadata)
        # if stream_metadata.table in ["aagaggpercols", "aagaggdef"]:
        for k, v in stream_metadata.get((), {}).items():
            LOGGER.info(f"{k}: {v}")
            # LOGGER.info(stream_metadata.get((), {}).get("table-key-properties"))
        replication_method = stream_metadata.get((), {}).get("replication-method")
        stream_state = state.get("bookmarks", {}).get(stream.tap_stream_id)

        if not stream_state:
            streams_without_state.append(stream)
        else:
            streams_with_state.append(stream)

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)

    # prioritize streams that have not been processed
    ordered_streams = streams_without_state + streams_with_state

    if currently_syncing:
        currently_syncing_stream = list(
            filter(
                lambda s: s.tap_stream_id == currently_syncing
                and is_valid_currently_syncing_stream(s, state),
                streams_with_state,
            )
        )

        non_currently_syncing_streams = list(
            filter(lambda s: s.tap_stream_id != currently_syncing, ordered_streams)
        )

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        # prioritize streams that have not been processed
        streams_to_sync = ordered_streams

    return resolve_catalog(discovered, streams_to_sync)


def get_binlog_streams(mssql_conn, catalog, config, state):
    discovered = discover_catalog(mssql_conn, config)

    selected_streams = list(filter(lambda s: common.stream_is_selected(s), catalog.streams))
    binlog_streams = []

    for stream in selected_streams:
        stream_metadata = metadata.to_map(stream.metadata)
        replication_method = stream_metadata.get((), {}).get("replication-method")
        stream_state = state.get("bookmarks", {}).get(stream.tap_stream_id)

    return resolve_catalog(discovered, binlog_streams)


def write_schema_message(catalog_entry, bookmark_properties=[]):
    key_properties = common.get_key_properties(catalog_entry)

    singer.write_message(
        singer.SchemaMessage(
            stream=catalog_entry.stream,
            schema=catalog_entry.schema.to_dict(),
            key_properties=key_properties,
            bookmark_properties=bookmark_properties,
        )
    )


def do_sync_incremental(mssql_conn, config, catalog_entry, state, columns):
    mssql_conn = MSSQLConnection(config)
    md_map = metadata.to_map(catalog_entry.metadata)
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    replication_key = md_map.get((), {}).get("replication-key")
    write_schema_message(catalog_entry=catalog_entry, bookmark_properties=[replication_key])
    LOGGER.info("Schema written")
    incremental.sync_table(mssql_conn, config, catalog_entry, state, columns)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync_full_table(mssql_conn, config, catalog_entry, state, columns):
    key_properties = common.get_key_properties(catalog_entry)
    mssql_conn = MSSQLConnection(config)

    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

    full_table.sync_table(mssql_conn, config, catalog_entry, state, columns, stream_version)

    # Prefer initial_full_table_complete going forward
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, "version")

    state = singer.write_bookmark(
        state, catalog_entry.tap_stream_id, "initial_full_table_complete", True
    )

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def sync_non_binlog_streams(mssql_conn, non_binlog_catalog, config, state):
    mssql_conn = MSSQLConnection(config)

    for catalog_entry in non_binlog_catalog.streams:
        columns = list(catalog_entry.schema.properties.keys())

        if not columns:
            LOGGER.warning(
                "There are no columns selected for stream %s, skipping it.", catalog_entry.stream
            )
            continue

        state = singer.set_currently_syncing(state, catalog_entry.tap_stream_id)

        # Emit a state message to indicate that we've started this stream
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        md_map = metadata.to_map(catalog_entry.metadata)
        replication_method = md_map.get((), {}).get("replication-method")
        replication_key = md_map.get((), {}).get("replication-key")
        primary_keys = md_map.get((), {}).get("table-key-properties")
        LOGGER.info(f"Table {catalog_entry.table} proposes {replication_method} sync")
        if replication_method == "INCREMENTAL" and not replication_key:
            LOGGER.info(
                f"No replication key for {catalog_entry.table}, using full table replication"
            )
            replication_method = "FULL_TABLE"
        if replication_method == "INCREMENTAL" and not primary_keys:
            LOGGER.info(f"No primary key for {catalog_entry.table}, using full table replication")
            replication_method = "FULL_TABLE"
        LOGGER.info(f"Table {catalog_entry.table} will use {replication_method} sync")

        database_name = common.get_database_name(catalog_entry)

        with metrics.job_timer("sync_table") as timer:
            timer.tags["database"] = database_name
            timer.tags["table"] = catalog_entry.table

            if replication_method == "INCREMENTAL":
                LOGGER.info(f"syncing {catalog_entry.table} incrementally")
                do_sync_incremental(mssql_conn, config, catalog_entry, state, columns)
            elif replication_method == "FULL_TABLE":
                LOGGER.info(f"syncing {catalog_entry.table} full table")
                do_sync_full_table(mssql_conn, config, catalog_entry, state, columns)
            else:
                raise Exception("only INCREMENTAL and FULL TABLE replication methods are supported")

    state = singer.set_currently_syncing(state, None)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync(mssql_conn, config, catalog, state):
    LOGGER.info("Beginning sync")
    non_binlog_catalog = get_non_binlog_streams(mssql_conn, catalog, config, state)
    for entry in non_binlog_catalog.streams:
        LOGGER.info(f"Need to sync {entry.table}")
    sync_non_binlog_streams(mssql_conn, non_binlog_catalog, config, state)


def log_server_params(mssql_conn):
    with connect_with_backoff(mssql_conn) as open_conn:
        try:
            with open_conn.cursor() as cur:
                cur.execute("""SELECT @@VERSION as version, @@lock_timeout as lock_wait_timeout""")
                row = cur.fetchone()
                LOGGER.info(
                    "Server Parameters: " + "version: %s, " + "lock_timeout: %s, ", *row,
                )
        except:
            LOGGER.warning("Encountered error checking server params. Error: (%s) %s", *e.args)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    mssql_conn = MSSQLConnection(args.config)
    log_server_params(mssql_conn)

    if args.discover:
        do_discover(mssql_conn, args.config)
    elif args.catalog:
        state = args.state or {}
        do_sync(mssql_conn, args.config, args.catalog, state)
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = args.state or {}
        do_sync(mssql_conn, args.config, catalog, state)
    else:
        LOGGER.info("No properties were selected")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
