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