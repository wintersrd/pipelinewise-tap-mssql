# tap-mssql 2.6.4 2024-10-24
* Update to handle `timestamp` (not a datetime value, a [deprecated](https://learn.microsoft.com/en-us/sql/t-sql/data-types/rowversion-transact-sql?view=sql-server-ver16#remarks) synonym of internal `rowversion`) as string
* Add tests for incremental syncing using a `timestamp` column as `replication-key`

# tap-mssql 2.6.3 2024-10-17
* Updating CDC documentation with a packaged method to maintain CDC tables.

# tap-mssql 2.6.2 2024-10-09
* Resolving issue when a table has a primary key and unique key. Both unique and primary key
  columns were being identified as the primary key for the target table. Prioritising the
  primary key first, and unique key secondary if there is no primary key.

# tap-mssql 2.6.1 2024-10-09
* Resolving issue with call get the prior LSN number (passing in unescaped table).

# tap-mssql 2.6.0 2024-09-02
* Resolving issue LOG_BASED tables with special characters. Escaping the object
  names with double quotes. Issue #74
* Providing more flexibility with database connections to support newer
  SQL Server instances like PDW via option conn_properties config. Issue #28
* Provides the ability to dump TDS logs via new enable_tds_logging config.

# tap-mssql 2.5.0 2024-08-26

* removing tox-poetry-install - Not well supported.
* Updating integration tests vars to CAPS to support passing MSSQL Port
* tox-docker requires expose rather than ports syntax in tox.ini
* Bumping docker = "^6.1.3" -> "^7.1.0"
* Bumping pytest = "^7.0.0" -> "^8.3.2"
* Bumping pytest-cov = "^4.1.0" -> = "^5.0.0"
* Bumping mypy = "^1.10.1" -> = "^1.11.1"
* Bumping pytest-sugar = "^0.9.7" -> = "^1.0.0"
* Bumping pytest-datafiles = "^2.0" ->  = "^3.0"
* Bumping tox-docker = "4.1.0" ->  = "5.0.0"
* Bumping requests = "2.31.0" -> "2.32.3"
* Bumping tox = "^3.28.0" -> "^4.18.0"

# tap-mssql 2.4.0 2024-08-08

* Moving to patched version of Singer Framework plus using msgspec rather than orjson for JSON serialization speed.
* Adjusting tox file for test pipeline to point to correct location of the sqlcmd in the latest MSSQL docker image.
* Adjusting tox file. Added -No switch calling sqlcmd to allow user/password connection to MSSQL.
* Patching dependencies.
* Pinned Requests to "2.31.0". This is required as the tox pipeline fails with a Docker API error with requests "2.32.x".
* Explicitly setting python version 3.8 -> 3.12
* Moving to using recommended poetry-core for build. https://python-poetry.org/docs/pyproject/#poetry-and-pep-517
* NOTE: Recommend refactoring to avoid using tox-poetry-installer as restrictive dependencies are stopping tox and docker updates. This may resolve the requests/docker api issue.

# tap-mssql 2.3.1 2024-07-22

* Bug Fix. Issue #62 - change to column selection using lists instead of sets to preserve column ordering

# tap-mssql 2.3.0 2024-04-18

* Bug Fix. Change pendulum DateTime type to datetime.datetime as pymssql 2.3.0 is no longer compatible with query parameters as pendulum DateTime (https://github.com/pymssql/pymssql/issues/889)

# tap-mssql 2.2.3 2024-02-26

* Bug Fix. Enhancing singer-decimal output for numeric data to correctly output the correct datatype - string and precision.

# tap-mssql 2.2.1 2023-10-30

* Bug Fix. Removing test code which slipped into the release, and adjusting offending code with correct dynamic column name.

# tap-mssql 2.2.0 2023-08-23

This feature increases support for SQL Datatypes.

* Adds support for datetimeoffset - https://github.com/wintersrd/pipelinewise-tap-mssql/issues/50
* Resolves issue with datetime2 rounding with high precision timestamps https://github.com/wintersrd/pipelinewise-tap-mssql/issues/51
* Resolves the max and min range for tinyints. The current min and max are correct for MariaDB and MySQL only. MSSQL Server only supports
positive integers (unsigned tinyint). https://github.com/wintersrd/pipelinewise-tap-mssql/issues/2

# tap-mssql 2.1.0 2023-08-01

This is a number of new enhancements to extend capability and resolving a few bugs.

## Features
* Adds `use_singer_decimal` setting, capability i.e. the ability to output decimal, float, and integer data as strings to avoid loss of precision. A target like this custom [target-snowflake](https://github.com/mjsqu/pipelinewise-target-snowflake) can decode this back into numeric data.
* Ordering columns by their ordinal position so the schema matches the database definition
* Support for new `cursor_array_size` parameter. This allows fetching records in batches
speeding up network traffic. Recommend a setting of `10000` records.
* Fixing bug. We have encountered a scenario where a stream starts. It failed and there
 was no bookmark written.
When the job is re-run the state file had currently_syncing for the given stream but with
no bookmark written it didn't appear in the streams_with_state list. This small change will
ensure that the currently_syncing_list will be populated whether there is a bookmark or not
by filtering against the ordered_streams list which includes both streams_without_state and
streams_with_state.
* Removed test for "INCREMENTAL" and not primary_keys.
INCREMENTAL loads can be performed without primary keys as long as there
is a replication key.
* Updating the CDC documentation
* Fixing bug. Where encountered a scenario where state lsn == max lsn.
If the lsn coming from state is equal to the maximum lsn, then we should not be incrementing
this lsn as it causes errors in the `fn_cdc_get_all_changes` function.
* Fixing bug. The _sdc_lsn_operation should be 2 for Inserts not Deletes.
* Fixing bug. pymssql 2.2.8 does not work. Excluding this version from pyproject.toml.
* Feature. Adding a logo to the tap.
* Resolving PR: https://github.com/wintersrd/pipelinewise-tap-mssql/pull/36
* Resolving PR: https://github.com/wintersrd/pipelinewise-tap-mssql/pull/16

# tap-mssql 2.0.0 2022-08-15

This is the first properly productionalized release of tap-mssql and brings a number of significant enhancements to both extend capability and improve future maintainability

## Features
* Adds capability to use log based CDC
* Adds a number of new data types:
  * Additional datetime options
  * Small money
  * Numeric
* Allows configuration on how to use datetime 

## Development enhancements
* Adds a tox test environment plus dockerized MSSQL server to allow proper testing
* Rebuilds all tests to match SQL server
* Replaces setup.py with pyproject.toml + poetry.lock and extends requirements to include full testing suite
* Adds a robust pre-commit for development purposes
* Configures bumpversion

## Other under the hood
* Reformats all code to black (100 character lines)
* Corrects all open flake8 errors
* Removes unused packages
