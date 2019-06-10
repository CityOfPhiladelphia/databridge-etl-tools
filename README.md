[![Build Status](https://travis-ci.com/CityOfPhiladelphia/databridge-etl-tools.svg?branch=master)](https://travis-ci.com/CityOfPhiladelphia/databridge-etl-tools)

# databridge-etl-tools

Command line tools to extract and load SQL and Carto tables using [JSON Table Schema](http://frictionlessdata.io/guides/json-table-schema/).

## Overview
Use this tool to do the following things:
- extract data from an Oracle SDE database and load it to S3
- Load data from S3 to Carto
- Load data from S3 to PostGIS

## Requirements (w/o Docker)
- Python 3.5 +
- Pip
- Oracle 11g Client
- Postgres

## Requirements (w/ Docker)
- Docker
- Access to citygeo-oracle-instant-client S3 bucket

## Usage
```bash
# Extract a table from Oracle SDE to S3
databridge_etl_tools extract \
    --table_name li_appeals_type \
    --table_schema gis_lni \
    --connection_string user/password@db_alias \
    --s3_bucket s3_bucket \
    --s3_key s3_key

# Load a table from S3 to Carto
databridge_etl_tools cartoupdate \
    --table_name li_appeals_type \
    --connection_string carto://user:apikey \
    --s3_bucket s3_bucket \
    --json_schema_s3_key json_schema_s3_key\
    --csv_s3_key csv_s3_key \
    --select_users select_users \
    --index_fields index_fields

# Load a table from S3 to Postgres
databridge_etl_tools load \
    --table_name li_appeals_type \
    --table_schema lni \
    --connection-string postgresql://user:password@host:port/db_name \
    --s3_bucket s3_bucket \
    --json_schema_s3_key json_schema_s3_key \
    --csv_s3_key csv_s3_key
```

| Flag                 | Help                                                                    |
|----------------------|-------------------------------------------------------------------------|
| --table_name         | The name of the table to extract or load to/from in a database or Carto |
| --table_schema       | The name of the schema (user) to extract or load to/from in a database  |
| --connection_string  | The connection string to a database or Carto                            |
| --s3_bucket          | The S3 bucket to fetch or load to                                       |
| --s3_key             | The S3 key to dump an extract to                                        |
| --json_schema_s3_key | The S3 key to fetch a JSON schema file                                  |
| --csv_s3_key         | The S3 key to fetch a CSV file                                          |
| --select_users       | The Carto users to grant select access to (comma separated ie. public,tileuser)                              |
| --index_fields       | The fields to index in the created table                                |

## Installation
```bash
pip install git+https://github.com/CityOfPhiladelphia/databridge-etl-tools#egg=databridge_etl_tools[carto,oracle,postgres] --process-dependency-links
```

## Development
To manually test while developing, the package can be entered using the -m module flag (due to the presence of the `__main__.py` file)
```bash
python -m databridge_etl_tools load \
    --table_name li_appeals_type \
    --table_schema lni \
    --connection-string postgresql://user:password@host:port/db_name \
    --s3_bucket s3_bucket \
    --json_schema_s3_key json_schema_s3_key \
    --csv_s3_key csv_s3_key
```

## Run tests
```bash
source scripts/run_tests.sh
```

## Deployment
When a commit is made to master, Travis CI bundles the code and its dependencies into a zip file, loads it to S3, and then publishes a new version of a lambda function using that updated zip file in S3. Additionally, Travis CI builds a docker image with an installed version of this repo and pushes it to ECR.

For this reason you should make changes to the test branch, make sure they pass automated tests and manual QA testing before making any changes to master.