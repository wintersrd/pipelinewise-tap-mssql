import copy
import os
import unittest

import pymssql
import singer
import singer.metadata

import tap_mssql
from tap_mssql.connection import connect_with_backoff

try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils

# import tap_mssql.sync_strategies.binlog as binlog
from singer.schema import Schema

import tap_mssql.sync_strategies.common as common

# from pymssqlreplication import BinLogStreamReader
# from pymssqlreplication.event import RotateEvent
# from pymssqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent


LOGGER = singer.get_logger()

SINGER_MESSAGES = []


def accumulate_singer_messages(message):
    SINGER_MESSAGES.append(message)


singer.write_message = accumulate_singer_messages


class TestTypeMapping(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conn = test_utils.get_test_connection()

        with connect_with_backoff(conn) as open_conn:
            with open_conn.cursor() as cur:
                try:
                    cur.execute("drop table test_type_mapping")
                except:
                    pass
                cur.execute(
                    """
                CREATE TABLE test_type_mapping (
                c_pk INTEGER PRIMARY KEY,
                c_numeric NUMERIC,
                c_decimal DECIMAL,
                c_decimal_2_unsigned DECIMAL(5, 2),
                c_decimal_2 DECIMAL(11, 2),
                c_tinyint TINYINT,
                c_smallint SMALLINT,
                c_int INT,
                c_bigint BIGINT,
                c_money MONEY,
                c_smallmoney SMALLMONEY,
                c_float FLOAT,
                c_real REAL,
                c_bit BIT,
                c_date DATE,
                c_time TIME,
                c_datetime DATETIME,
                c_datetime2 DATETIME2,
                c_datetimeoffset DATETIMEOFFSET,
                c_smalldatetime SMALLDATETIME
                )"""
                )

        catalog = test_utils.discover_catalog(conn, {})
        cls.schema = catalog.streams[0].schema
        cls.metadata = catalog.streams[0].metadata

    def get_metadata_for_column(self, colName):
        return next(md for md in self.metadata if md["breadcrumb"] == ("properties", colName))[
            "metadata"
        ]

    def test_decimal(self):
        self.assertEqual(
            self.schema.properties["c_decimal"],
            Schema(["null", "number"], inclusion="available", multipleOf=1),
        )
        self.assertEqual(
            self.get_metadata_for_column("c_decimal"),
            # {"selected-by-default": True, "sql-datatype": "decimal(10,0)"},
            {"selected-by-default": True, "sql-datatype": "decimal"},
        )

    def test_decimal_unsigned(self):
        self.assertEqual(
            self.schema.properties["c_decimal_2_unsigned"],
            Schema(["null", "number"], inclusion="available", multipleOf=0.01),
        )
        self.assertEqual(
            self.get_metadata_for_column("c_decimal_2_unsigned"),
            # {"selected-by-default": True, "sql-datatype": "decimal(5,2) unsigned"},
            {"selected-by-default": True, "sql-datatype": "decimal"},
        )

    def test_decimal_with_defined_scale_and_precision(self):
        self.assertEqual(
            self.schema.properties["c_decimal_2"],
            Schema(["null", "number"], inclusion="available", multipleOf=0.01),
        )
        self.assertEqual(
            self.get_metadata_for_column("c_decimal_2"),
            {"selected-by-default": True, "sql-datatype": "decimal"},
        )

    def test_tinyint(self):
        self.assertEqual(
            self.schema.properties["c_tinyint"],
            Schema(["null", "integer"], inclusion="available", minimum=0, maximum=255),
        )
        # self.assertEqual(
        #     self.get_metadata_for_column("c_tinyint"),
        #     {"selected-by-default": True, "sql-datatype": "tinyint(4)"},
        # )

    # def test_tinyint_1(self):
    #     self.assertEqual(
    #         self.schema.properties["c_tinyint_1"],
    #         Schema(["null", "boolean"], inclusion="available"),
    #     )
    #     self.assertEqual(
    #         self.get_metadata_for_column("c_tinyint_1"),
    #         {"selected-by-default": True, "sql-datatype": "tinyint(1)"},
    #     )

    # def test_tinyint_1_unsigned(self):
    #     self.assertEqual(
    #         self.schema.properties["c_tinyint_1_unsigned"],
    #         Schema(["null", "boolean"], inclusion="available"),
    #     )
    #     self.assertEqual(
    #         self.get_metadata_for_column("c_tinyint_1_unsigned"),
    #         {"selected-by-default": True, "sql-datatype": "tinyint(1) unsigned"},
    #     )

    def test_smallint(self):
        self.assertEqual(
            self.schema.properties["c_smallint"],
            Schema(["null", "integer"], inclusion="available", minimum=-32768, maximum=32767),
        )
        # self.assertEqual(
        #     self.get_metadata_for_column("c_smallint"),
        #     {"selected-by-default": True, "sql-datatype": "smallint(6)"},
        # )

    # def test_mediumint(self):
    #     self.assertEqual(
    #         self.schema.properties["c_mediumint"],
    #         Schema(["null", "integer"], inclusion="available", minimum=-8388608, maximum=8388607),
    #     )
    #     self.assertEqual(
    #         self.get_metadata_for_column("c_mediumint"),
    #         {"selected-by-default": True, "sql-datatype": "mediumint(9)"},
    #     )

    def test_int(self):
        self.assertEqual(
            self.schema.properties["c_int"],
            Schema(
                ["null", "integer"], inclusion="available", minimum=-2147483648, maximum=2147483647
            ),
        )
        self.assertEqual(
            self.get_metadata_for_column("c_int"),
            {"selected-by-default": True, "sql-datatype": "int"},
        )

    def test_bigint(self):
        self.assertEqual(
            self.schema.properties["c_bigint"],
            Schema(
                ["null", "integer"],
                inclusion="available",
                minimum=-9223372036854775808,
                maximum=9223372036854775807,
            ),
        )
        self.assertEqual(
            self.get_metadata_for_column("c_bigint"),
            {"selected-by-default": True, "sql-datatype": "bigint"},
        )

    # def test_bigint_unsigned(self):
    #     self.assertEqual(
    #         self.schema.properties["c_bigint_unsigned"],
    #         Schema(
    #             ["null", "integer"], inclusion="available", minimum=0, maximum=18446744073709551615
    #         ),
    #     )

    #     self.assertEqual(
    #         self.get_metadata_for_column("c_bigint_unsigned"),
    #         {"selected-by-default": True, "sql-datatype": "bigint(20) unsigned"},
    #     )

    def test_float(self):
        # self.assertEqual(
        #     self.schema.properties["c_float"], Schema(["null", "number"], inclusion="available")
        # )
        self.assertEqual(
            self.get_metadata_for_column("c_float"),
            {"selected-by-default": True, "sql-datatype": "float"},
        )

    # def test_double(self):
    #     self.assertEqual(
    #         self.schema.properties["c_double"], Schema(["null", "number"], inclusion="available")
    #     )
    #     self.assertEqual(
    #         self.get_metadata_for_column("c_double"),
    #         {"selected-by-default": True, "sql-datatype": "double"},
    #     )

    def test_bit(self):
        self.assertEqual(
            self.schema.properties["c_bit"], Schema(["null", "boolean"], inclusion="available")
        )
        self.assertEqual(
            self.get_metadata_for_column("c_bit"),
            {"selected-by-default": True, "sql-datatype": "bit"},
        )

    def test_date(self):
        self.assertEqual(
            self.schema.properties["c_date"],
            Schema(["null", "string"], format="date-time", inclusion="available"),
        )
        self.assertEqual(
            self.get_metadata_for_column("c_date"),
            {"selected-by-default": True, "sql-datatype": "date"},
        )

    def test_time(self):
        self.assertEqual(
            self.schema.properties["c_time"],
            Schema(["null", "string"], format="date-time", inclusion="available"),
        )
        self.assertEqual(
            self.get_metadata_for_column("c_time"),
            {"selected-by-default": True, "sql-datatype": "time"},
        )

    # def test_year(self):
    #     self.assertEqual(self.schema.properties["c_year"].inclusion, "unsupported")
    #     self.assertEqual(
    #         self.get_metadata_for_column("c_year"),
    #         {"selected-by-default": False, "sql-datatype": "year(4)"},
    #     )

    def test_pk(self):
        self.assertEqual(self.schema.properties["c_pk"].inclusion, "automatic")


class TestSelectsAppropriateColumns(unittest.TestCase):
    def runTest(self):
        selected_cols = ["a", "a", "a1", "a2", "b", "d"]
        table_schema = Schema(
            type="object",
            properties={
                "a": Schema(None, inclusion="available"),
                "a1": Schema(None, inclusion="available"),
                "a2": Schema(None, inclusion="available"),
                "a3": Schema(None, inclusion="available"),
                "b": Schema(None, inclusion="unsupported"),
                "c": Schema(None, inclusion="automatic"),
            },
        )

        got_cols = tap_mssql.desired_columns(selected_cols, table_schema)

        self.assertEqual(
            got_cols, ["a", "a1", "a2", "c"], "Keep automatic as well as selected, available columns. Ordered correctly."
        )

class TestInvalidInclusion(unittest.TestCase):
    def runTest(self):
        selected_cols = ["a", "e"]
        table_schema = Schema(
            type="object",
            properties={
                "a": Schema(None, inclusion="available"),
                "e": Schema(None, inclusion="invalid"),
            },
        )

        self.assertRaises(Exception, tap_mssql.desired_columns, selected_cols, table_schema)

class TestSchemaMessages(unittest.TestCase):
    def runTest(self):
        conn = test_utils.get_test_connection()

        with connect_with_backoff(conn) as open_conn:
            with open_conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE tab (
                      id INTEGER PRIMARY KEY,
                      a INTEGER,
                      b INTEGER)
                """
                )

        catalog = test_utils.discover_catalog(conn, {})
        catalog.streams[0].stream = "tab"
        catalog.streams[0].metadata = [
            {"breadcrumb": (), "metadata": {"selected": True, "database-name": "dbo"}},
            {"breadcrumb": ("properties", "a"), "metadata": {"selected": True}},
        ]

        test_utils.set_replication_method_and_key(catalog.streams[0], "FULL_TABLE", None)

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        config = test_utils.get_db_config()
        tap_mssql.do_sync(conn, config, catalog, {})

        schema_message = list(
            filter(lambda m: isinstance(m, singer.SchemaMessage), SINGER_MESSAGES)
        )[0]
        self.assertTrue(isinstance(schema_message, singer.SchemaMessage))
        # tap-mssql selects new fields by default. If a field doesn't appear in the schema, then it should be
        # selected
        expectedKeys = ["id", "a", "b"]

        self.assertEqual(schema_message.schema["properties"].keys(), set(expectedKeys))


def currently_syncing_seq(messages):
    return "".join(
        [
            (m.value.get("currently_syncing", "_") or "_")[-1]
            for m in messages
            if isinstance(m, singer.StateMessage)
        ]
    )


class TestCurrentStream(unittest.TestCase):
    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                for t in ["a", "b", "c"]:
                    try:
                        cursor.execute(f"drop table {t}")
                    except:
                        pass
                cursor.execute("CREATE TABLE a (val int)")
                cursor.execute("CREATE TABLE b (val int)")
                cursor.execute("CREATE TABLE c (val int)")
                cursor.execute("INSERT INTO a (val) VALUES (1)")
                cursor.execute("INSERT INTO b (val) VALUES (1)")
                cursor.execute("INSERT INTO c (val) VALUES (1)")

        self.catalog = test_utils.discover_catalog(self.conn, {})

        for stream in self.catalog.streams:
            stream.key_properties = []

            stream.metadata = [
                {
                    "breadcrumb": (),
                    "metadata": {"selected": True, "database-name": "dbo"},
                },
                {"breadcrumb": ("properties", "val"), "metadata": {"selected": True}},
            ]

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, "FULL_TABLE", None)

    def test_emit_currently_syncing(self):
        state = {}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        tap_mssql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)
        self.assertRegex(currently_syncing_seq(SINGER_MESSAGES), "^a+b+c+_+")

    def test_start_at_currently_syncing(self):
        state = {
            "currently_syncing": "dbo-b",
            "bookmarks": {
                "dbo-a": {"version": 123},
                "dbo-b": {"version": 456},
            },
        }

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mssql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)

        self.assertRegex(currently_syncing_seq(SINGER_MESSAGES), "^b+c+a+_+")


def message_types_and_versions(messages):
    message_types = []
    versions = []
    for message in messages:
        t = type(message)
        if t in set([singer.RecordMessage, singer.ActivateVersionMessage]):
            message_types.append(t.__name__)
            versions.append(message.version)
    return (message_types, versions)


class TestStreamVersionFullTable(unittest.TestCase):
    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                try:
                    cursor.execute("drop table full_table")
                except:
                    pass
                cursor.execute("CREATE TABLE full_table (val int)")
                cursor.execute("INSERT INTO full_table (val) VALUES (1)")

        self.catalog = test_utils.discover_catalog(self.conn, {})
        for stream in self.catalog.streams:
            stream.key_properties = []

            stream.metadata = [
                {
                    "breadcrumb": (),
                    "metadata": {"selected": True, "database-name": "dbo"},
                },
                {"breadcrumb": ("properties", "val"), "metadata": {"selected": True}},
            ]

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, "FULL_TABLE", None)

    def test_with_no_state(self):
        state = {}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mssql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        self.assertEqual(
            ["ActivateVersionMessage", "RecordMessage"], sorted(list(set(message_types)))
        )
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])

    def test_with_no_initial_full_table_complete_in_state(self):
        common.get_stream_version = lambda a, b: 12345

        state = {"bookmarks": {"dbo-full_table": {"version": None}}}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mssql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        self.assertEqual(["RecordMessage", "ActivateVersionMessage"], message_types)
        self.assertEqual(versions, [12345, 12345])

        self.assertFalse("version" in state["bookmarks"]["dbo-full_table"].keys())
        self.assertTrue(state["bookmarks"]["dbo-full_table"]["initial_full_table_complete"])

    def test_with_initial_full_table_complete_in_state(self):
        common.get_stream_version = lambda a, b: 12345

        state = {"bookmarks": {"dbo-full_table": {"initial_full_table_complete": True}}}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mssql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        self.assertEqual(["RecordMessage", "ActivateVersionMessage"], message_types)
        self.assertEqual(versions, [12345, 12345])

    def test_version_cleared_from_state_after_full_table_success(self):
        common.get_stream_version = lambda a, b: 12345

        state = {
            "bookmarks": {"dbo-full_table": {"version": 1, "initial_full_table_complete": True}}
        }

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mssql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        self.assertEqual(["RecordMessage", "ActivateVersionMessage"], message_types)
        self.assertEqual(versions, [12345, 12345])

        self.assertFalse("version" in state["bookmarks"]["dbo-full_table"].keys())
        self.assertTrue(state["bookmarks"]["dbo-full_table"]["initial_full_table_complete"])


class TestIncrementalReplication(unittest.TestCase):
    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                try:
                    cursor.execute("drop table incremental")
                except:
                    pass
                cursor.execute("CREATE TABLE incremental (val int, updated datetime)")
                cursor.execute("INSERT INTO incremental (val, updated) VALUES (1, '2017-06-01')")
                cursor.execute("INSERT INTO incremental (val, updated) VALUES (2, '2017-06-20')")
                cursor.execute("INSERT INTO incremental (val, updated) VALUES (3, '2017-09-22')")
                try:
                    cursor.execute("drop table integer_incremental")
                except:
                    pass
                cursor.execute("CREATE TABLE integer_incremental (val int, updated int)")
                cursor.execute("INSERT INTO integer_incremental (val, updated) VALUES (1, 1)")
                cursor.execute("INSERT INTO integer_incremental (val, updated) VALUES (2, 2)")
                cursor.execute("INSERT INTO integer_incremental (val, updated) VALUES (3, 3)")

        self.catalog = test_utils.discover_catalog(self.conn, {})

        for stream in self.catalog.streams:
            stream.metadata = [
                {
                    "breadcrumb": (),
                    "metadata": {
                        "selected": True,
                        "table-key-properties": [],
                        "database-name": "dbo",
                    },
                },
                {"breadcrumb": ("properties", "val"), "metadata": {"selected": True}},
            ]

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, "INCREMENTAL", "updated")

    def test_with_no_state(self):
        state = {}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        tap_mssql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)


        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])
        record_messages = [message for message in SINGER_MESSAGES if isinstance(message,singer.RecordMessage)]
        incremental_record_messages = [m for m in record_messages if m.stream == 'dbo-incremental']
        integer_incremental_record_messages = [m for m in record_messages if m.stream == 'dbo-integer_incremental']
        
        self.assertEqual(len(incremental_record_messages),3)
        self.assertEqual(len(integer_incremental_record_messages),3)

    def test_with_state(self):
        state = {
            "bookmarks": {
                "dbo-incremental": {
                    "version": 1,
                    "replication_key_value": "2017-06-20",
                    "replication_key": "updated",
                },
                "dbo-integer_incremental": {
                    "version": 1,
                    "replication_key_value": 3,
                    "replication_key": "updated",
                },
            }
        }

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mssql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        self.assertEqual(
            [
                "ActivateVersionMessage",
                "RecordMessage",
            ],
            sorted(list(set(message_types))),
        )
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])
        
        # Based on state values provided check the number of record messages emitted
        record_messages = [message for message in SINGER_MESSAGES if isinstance(message,singer.RecordMessage)]
        incremental_record_messages = [m for m in record_messages if m.stream == 'dbo-incremental']
        integer_incremental_record_messages = [m for m in record_messages if m.stream == 'dbo-integer_incremental']
        
        self.assertEqual(len(incremental_record_messages),2)
        self.assertEqual(len(integer_incremental_record_messages),1)


class TestViews(unittest.TestCase):
    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                try:
                    cursor.execute("drop table a_table")
                except:
                    pass
                cursor.execute(
                    """
                    CREATE TABLE a_table (
                      id int primary key,
                      a int,
                      b int)
                    """
                )

                cursor.execute(
                    """
                    CREATE OR ALTER VIEW a_view AS SELECT id, a FROM a_table
                    """
                )

    def test_discovery_sets_is_view(self):
        catalog = test_utils.discover_catalog(self.conn, {})
        is_view = {}

        for stream in catalog.streams:
            md_map = singer.metadata.to_map(stream.metadata)
            is_view[stream.table] = md_map.get((), {}).get("is-view")

        self.assertEqual(is_view, {"a_table": False, "a_view": True})

    def test_do_not_discover_key_properties_for_view(self):
        catalog = test_utils.discover_catalog(self.conn, {})
        primary_keys = {}
        for c in catalog.streams:
            primary_keys[c.table] = (
                singer.metadata.to_map(c.metadata).get((), {}).get("table-key-properties")
            )

        self.assertEqual(primary_keys, {"a_table": ["id"], "a_view": []})

class TestTimestampIncrementalReplication(unittest.TestCase):
    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                try:
                    cursor.execute("drop table incremental")
                except:
                    pass
                cursor.execute("CREATE TABLE incremental (val int, updated timestamp)")
                cursor.execute("INSERT INTO incremental (val) VALUES (1)") #00000000000007d1
                cursor.execute("INSERT INTO incremental (val) VALUES (2)") #00000000000007d2
                cursor.execute("INSERT INTO incremental (val) VALUES (3)") #00000000000007d3

        self.catalog = test_utils.discover_catalog(self.conn, {})

        for stream in self.catalog.streams:
            stream.metadata = [
                {
                    "breadcrumb": (),
                    "metadata": {
                        "selected": True,
                        "table-key-properties": [],
                        "database-name": "dbo",
                    },
                },
                {"breadcrumb": ("properties", "val"), "metadata": {"selected": True}},
            ]

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, "INCREMENTAL", "updated")

    def test_with_no_state(self):
        state = {}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        tap_mssql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        record_messages = [message for message in SINGER_MESSAGES if isinstance(message,singer.RecordMessage)]
        
        self.assertEqual(len(record_messages),3)


    def test_with_state(self):
        state = {
            "bookmarks": {
                "dbo-incremental": {
                    "version": 1,
                    "replication_key_value": '00000000000007d2',
                    "replication_key": "updated",
                },
            }
        }

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mssql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        # Given the state value supplied, there should only be two RECORD messages
        record_messages = [message for message in SINGER_MESSAGES if isinstance(message,singer.RecordMessage)]
        
        self.assertEqual(len(record_messages),2)


if __name__ == "__main__":
    # test1 = TestBinlogReplication()
    # test1.setUp()
    # test1.test_binlog_stream()
    test1 = TestTypeMapping()
    test1.setUpClass()
    test1.test_decimal()
