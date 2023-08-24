# pipelinewise-tap-mssql
![singer_sqlserver_tap](https://user-images.githubusercontent.com/84364906/220873968-8b2e7357-8539-421f-888f-97fb3a17975e.png)

[Singer](https://www.singer.io/) tap that extracts data from a [mssql](https://www.mssql.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

## How to use it

The recommended method of running this tap is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Tap mssql](https://transferwise.github.io/pipelinewise/connectors/taps/mssql.html)

If you want to run this [Singer Tap](https://singer.io) independently please read further.

## Usage

This section dives into basic usage of `tap-mssql` by walking through extracting
data from a table. It assumes that you can connect to and read from a mssql
database.

### Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install tap-mssql
```

or

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install tap-mssql
```

#### Additional OS X Requirements

In the event you encounter install issues on OS X stating `fatal error: 'sqlfront.h' file not found`, install FreeTDS with SQL Server compatibility:

1. Download the latest version from [FreeTDS](https://www.freetds.org/)
2. Extract and run `./configure --enable-msdblib`
3. Run `make && make install`

### Have a source database

There's some important business data siloed in this mssql database -- we need to
extract it. Here's the table we'd like to sync:

```
mssql> select * from example_db.animals;
+----|----------|----------------------+
| id | name     | likes_getting_petted |
+----|----------|----------------------+
|  1 | aardvark |                    0 |
|  2 | bear     |                    0 |
|  3 | cow      |                    1 |
+----|----------|----------------------+
3 rows in set (0.00 sec)
```

### Create the configuration file

Create a config file containing the database connection credentials, e.g.:

```json
{
  "host": "localhost",
  "database": "db",
  "port": "3306",
  "user": "root",
  "password": "password"
}
```

Recommended optional settings

* `"use_date_datatype": true`   - This will emit true timestamps and handle the time datatype.
* `"use_singer_decimal": true`        - This will help avoid numeric rounding issues emitting as a string with a format of singer.decimal.
* `"cursor_array_size": 10000` - This will help speed up extracts over a WAN or low latency network. The default is 1.

Windows Authentication is available! Don't provide a user or password and pymssql will use the user that is running the process on windows to login.
e.g.:

```json
{
  "host": "localhost",
  "database": "db"
}
```

Optional:

To filter the discovery to a particular schema within a database. This is useful if you have a large number of schemas and wish to speed up the discovery.

```json
{
  "filter_dbs": "your database schema name"
}
```

Optional:

To emit a date as a date without a time component or time without an UTC offset. This is helpful to avoid time conversions or to just work with a date datetype in the target database. If this boolean config item is not set, the default behaviour is `false` i.e. emit date datatypes as a datetime. It is recommended to set this on if you have time datetypes and are having issues uploading into into a target database.

```json
{
  "use_date_datatype": true
}
```

Optional:

Set the version of TDS to use when communicating with MS SQL Server (the default is 7.3). This is used by pymssql with connecting and fetching data from SQL Server databases. See the [pymssql](https://pymssql.readthedocs.io/en/stable/index.html) documentation and [FreeTDS](https://www.freetds.org/) documentation for more details.

```json
{
  "tds_version": "7.3"
}
```

Optional:

The characterset for the database / source system. The default is `utf8`, however older databases might use a charactersets like [cp1252](https://en.wikipedia.org/wiki/Windows-1252) for the encoding. If you have errors with a `UnicodeDecodeError: 'utf-8' codec can't decode byte ....` then a solution is examine the characterset of the source database / system and make an appropriate substitution for utf8 like cp1252.

```json
{
  "characterset": "utf8"
}
```

These are the same basic configuration properties used by the mssql command-line
client (`mssql`).

Optional:

To emit all numeric values as strings and treat floats as string data types for the target, set use_singer_decimal to true. The resulting SCHEMA message will contain an attribute in additionalProperties containing the scale and precision of the discovered property:

```json
"property": {
            "inclusion": "available",
            "format": "singer.decimal",
            "type": [
              "null",
              "number"
            ],
            "additionalProperties": {
              "scale_precision": "(12,0)"
            }
```

Usage:
```json
{
  "use_singer_decimal": true
}
```

Optional:

A numeric setting adjusting the internal buffersize. The common query tuning scenario is for SELECT statements that return a large number of rows over a slow network. Increasing arraysize can improve performance by reducing the number of round-trips to the database. However increasing this value increases the amount of memory required.

```json
{
  "cursor_array_size": 10000,
}
```

### Discovery mode

The tap can be invoked in discovery mode to find the available tables and
columns in the database:

```bash
$ tap-mssql --config config.json --discover

```

A discovered catalog is output, with a JSON-schema description of each table. A
source table directly corresponds to a Singer stream.

```json
{
  "streams": [
    {
      "tap_stream_id": "example_db-animals",
      "table_name": "animals",
      "schema": {
        "type": "object",
        "properties": {
          "name": {
            "inclusion": "available",
            "type": ["null", "string"],
            "maxLength": 255
          },
          "id": {
            "inclusion": "automatic",
            "minimum": -2147483648,
            "maximum": 2147483647,
            "type": ["null", "integer"]
          },
          "likes_getting_petted": {
            "inclusion": "available",
            "type": ["null", "boolean"]
          }
        }
      },
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "row-count": 3,
            "table-key-properties": ["id"],
            "database-name": "example_db",
            "selected-by-default": false,
            "is-view": false
          }
        },
        {
          "breadcrumb": ["properties", "id"],
          "metadata": {
            "sql-datatype": "int(11)",
            "selected-by-default": true
          }
        },
        {
          "breadcrumb": ["properties", "name"],
          "metadata": {
            "sql-datatype": "varchar(255)",
            "selected-by-default": true
          }
        },
        {
          "breadcrumb": ["properties", "likes_getting_petted"],
          "metadata": {
            "sql-datatype": "tinyint(1)",
            "selected-by-default": true
          }
        }
      ],
      "stream": "animals"
    }
  ]
}
```

### Field selection

In sync mode, `tap-mssql` consumes the catalog and looks for tables and fields
have been marked as _selected_ in their associated metadata entries.

Redirect output from the tap's discovery mode to a file so that it can be
modified:

```bash
$ tap-mssql -c config.json --discover > properties.json
```

Then edit `properties.json` to make selections. In this example we want the
`animals` table. The stream's metadata entry (associated with `"breadcrumb": []`)
gets a top-level `selected` flag, as does its columns' metadata entries. Additionally,
we will mark the `animals` table to replicate using a `FULL_TABLE` strategy. For more,
information, see [Replication methods and state file](#replication-methods-and-state-file).

```json
[
  {
    "breadcrumb": [],
    "metadata": {
      "row-count": 3,
      "table-key-properties": ["id"],
      "database-name": "example_db",
      "selected-by-default": false,
      "is-view": false,
      "selected": true,
      "replication-method": "FULL_TABLE"
    }
  },
  {
    "breadcrumb": ["properties", "id"],
    "metadata": {
      "sql-datatype": "int(11)",
      "selected-by-default": true,
      "selected": true
    }
  },
  {
    "breadcrumb": ["properties", "name"],
    "metadata": {
      "sql-datatype": "varchar(255)",
      "selected-by-default": true,
      "selected": true
    }
  },
  {
    "breadcrumb": ["properties", "likes_getting_petted"],
    "metadata": {
      "sql-datatype": "tinyint(1)",
      "selected-by-default": true,
      "selected": true
    }
  }
]
```

### Sync mode

With a properties catalog that describes field and table selections, the tap can be invoked in sync mode:

```bash
$ tap-mssql -c config.json --properties properties.json
```

Messages are written to standard output following the Singer specification. The
resultant stream of JSON data can be consumed by a Singer target.

```json
{"value": {"currently_syncing": "example_db-animals"}, "type": "STATE"}

{"key_properties": ["id"], "stream": "animals", "schema": {"properties": {"name": {"inclusion": "available", "maxLength": 255, "type": ["null", "string"]}, "likes_getting_petted": {"inclusion": "available", "type": ["null", "boolean"]}, "id": {"inclusion": "automatic", "minimum": -2147483648, "type": ["null", "integer"], "maximum": 2147483647}}, "type": "object"}, "type": "SCHEMA"}

{"stream": "animals", "version": 1509133344771, "type": "ACTIVATE_VERSION"}

{"record": {"name": "aardvark", "likes_getting_petted": false, "id": 1}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"record": {"name": "bear", "likes_getting_petted": false, "id": 2}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"record": {"name": "cow", "likes_getting_petted": true, "id": 3}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"stream": "animals", "version": 1509133344771, "type": "ACTIVATE_VERSION"}

{"value": {"currently_syncing": "example_db-animals", "bookmarks": {"example_db-animals": {"initial_full_table_complete": true}}}, "type": "STATE"}

{"value": {"currently_syncing": null, "bookmarks": {"example_db-animals": {"initial_full_table_complete": true}}}, "type": "STATE"}
```

## Replication methods and state file

In the above example, we invoked `tap-mssql` without providing a _state_ file
and without specifying a replication method. The three ways to replicate a given
table are `FULL_TABLE`, `LOG_BASED`, and `INCREMENTAL`.

### Full Table

Full-table replication extracts all data from the source table each time the tap
is invoked.

### Log Based

Log_Based replication extracts change data from the MS SQL Server Change Data Capture (CDC) tables you have enrolled.

This method allows you to replicate just the changes to a table e.g. the Inserts, Deletes, and Updates. For this method to work you
must enrol the database in question and tables that you wish to replicate.

See : https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server for
more details.

Please Note: CDC is different to Change Tracking which is a older approach for tracking change. Log Based only works with CDC, it does
not work with Change Tracking!

To find out more about setting up CDC, refer to this page [MSSQL CDC Setup](MS_CDC_SETUP.md)

### Incremental

Incremental replication works in conjunction with a state file to only extract
new records each time the tap is invoked. This requires a replication key to be
specified in the table's metadata as well.

#### Example

Let's sync the `animals` table again, but this time using incremental
replication. The replication method and replication key are set in the
table's metadata entry in properties file:

```json
{
  "streams": [
    {
      "tap_stream_id": "example_db-animals",
      "table_name": "animals",
      "schema": { ... },
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "row-count": 3,
            "table-key-properties": [
              "id"
            ],
            "database-name": "example_db",
            "selected-by-default": false,
            "is-view": false,
            "replication-method": "INCREMENTAL",
            "replication-key": "id"
          }
        },
        ...
      ],
      "stream": "animals"
    }
  ]
}
```

We have no meaningful state so far, so just invoke the tap in sync mode again
without a state file:

```bash
$ tap-mssql -c config.json --properties properties.json
```

The output messages look very similar to when the table was replicated using the
default `FULL_TABLE` replication method. One important difference is that the
`STATE` messages now contain a `replication_key_value` -- a bookmark or
high-water mark -- for data that was extracted:

```json
{"type": "STATE", "value": {"currently_syncing": "example_db-animals"}}

{"stream": "animals", "type": "SCHEMA", "schema": {"type": "object", "properties": {"id": {"type": ["null", "integer"], "minimum": -2147483648, "maximum": 2147483647, "inclusion": "automatic"}, "name": {"type": ["null", "string"], "inclusion": "available", "maxLength": 255}, "likes_getting_petted": {"type": ["null", "boolean"], "inclusion": "available"}}}, "key_properties": ["id"]}

{"stream": "animals", "type": "ACTIVATE_VERSION", "version": 1509135204169}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 1, "name": "aardvark", "likes_getting_petted": false}}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 2, "name": "bear", "likes_getting_petted": false}}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 3, "name": "cow", "likes_getting_petted": true}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"version": 1509135204169, "replication_key_value": 3, "replication_key": "id"}}, "currently_syncing": "example_db-animals"}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"version": 1509135204169, "replication_key_value": 3, "replication_key": "id"}}, "currently_syncing": null}}
```

Note that the final `STATE` message has a `replication_key_value` of `3`,
reflecting that the extraction ended on a record that had an `id` of `3`.
Subsequent invocations of the tap will pick up from this bookmark.

Normally, the target will echo the last `STATE` after it's finished processing
data. For this example, let's manually write a `state.json` file using the
`STATE` message:

```json
{
  "bookmarks": {
    "example_db-animals": {
      "version": 1509135204169,
      "replication_key_value": 3,
      "replication_key": "id"
    }
  },
  "currently_syncing": null
}
```

Let's add some more animals to our farm:

```
mssql> insert into animals (name, likes_getting_petted) values ('dog', true), ('elephant', true), ('frog', false);
```

```bash
$ tap-mssql -c config.json --properties properties.json --state state.json
```

This invocation extracts any data since (and including) the
`replication_key_value`:

```json
{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 3}}, "currently_syncing": "example_db-animals"}}

{"key_properties": ["id"], "schema": {"properties": {"name": {"maxLength": 255, "inclusion": "available", "type": ["null", "string"]}, "id": {"maximum": 2147483647, "minimum": -2147483648, "inclusion": "automatic", "type": ["null", "integer"]}, "likes_getting_petted": {"inclusion": "available", "type": ["null", "boolean"]}}, "type": "object"}, "type": "SCHEMA", "stream": "animals"}

{"type": "ACTIVATE_VERSION", "version": 1509135204169, "stream": "animals"}

{"record": {"name": "cow", "id": 3, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "dog", "id": 4, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "elephant", "id": 5, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "frog", "id": 6, "likes_getting_petted": false}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 6}}, "currently_syncing": "example_db-animals"}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 6}}, "currently_syncing": null}}
```

---

Based on Stitch documentation

## Data Types

The json output by this tap should be JSON schema compliant and also conform to the Singer Framework standard.

Noted: Below is some examples of the data transformation to present data in a standard manner for Singer Targets.

A special note for `datetime2` and `datetimeoffsets` MS SQL Server datatypes.

For `datetime2`, it as a naïve datetime timestamp with no concept of an offset, `datetime2` supports nanoseconds with up to seven decimal places. With `datetimeoffset` datatypes, the data is normalized to UTC i.e. taking the offset as it is in MSSQL Server and converting it to a UTC so it is agnostic to the target platform - anything with a datetimeoffset can be cast into local time in the target.

To work-around issues with the PYMSSQL driver, the conversion takes place on the SQL Server source to ensure there is no loss of precision leading to errors as a result of rounding.

|SQL DataType|Example Value|Min|Max| JSON Format| JSON Type |Example Result|
|-----------------|------------------------|-------------------------|-----------------------------|------------------------|------------------------|-----------------------|
| tinyint  | 254  | 0  | 255   | | integer   | 254  |
| smallint  | -32768  | -32768  | 32767   | | integer   | -32768  |
| int  | -2147483648  | -2147483648  | 2147483647   | | integer   | -2147483648  |
| bigint  | -9223372036854775808  | -9223372036854775808  | 9223372036854775807   | | integer   | -9223372036854775808  |
| datetime2 | 9999-12-31 23:59:59.9999999 | | | date-time |  string | 9999-12-31 23:59:59.9999999 |
| datetimeoffset | 2023-08-22 08:24:32.1277000 +10:00 | | | date-time |  string | 2023-08-21T22:24:32.1277000Z |

## Build Instructions

This section dives into basic commands to build `tap-mssql` if an alteration is made to the code.

### To build the tap

Run the following command each time you need to rebuild the tap.

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
```

### Debugging in Visual Studio Code

To run the **init**.py python program in debug mode, you need to do the following two steps. Note: This was run within a Docker Container in Visual Studio Code.

1. Create a .vscode/launch.json file. Note: The parameters config.json and properties.json should point to the files you have generated in previous steps above.
   If you want to test state, include the state parameter as shown below and prepare an appropriate state file as per the instructions in an earlier section.

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Program",
            "type": "python",
            "request": "launch",
            "program": "${workspaceRoot}/tap_mssql/__init__.py",
            "args": [
                "-c", "config.json",
                "--properties", "properties.json"
                "--state", "state.json"
            ]
        }
    ]
}
```

2. Add a main entry to the **init**.py file to run interactively

Add the following lines to the end of the **init**.py in the tap_mssql directory.

```python

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
```
