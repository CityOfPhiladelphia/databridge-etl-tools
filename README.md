# databridge-etl-tools

Command line tools to extract and load SQL and Carto tables.

## Usage
```bash
# Extract a table from Oracle SDE to S3
databridge_etl_tools extract \
    --table_name li_appeals_type \
    --table_schema gis_lni \
    --connection_string user/password@db_alias \
    --s3_bucket s3_bucket
    --s3_key s3_key

# Load a table from S3 to Carto
databridge_etl_tools cartoupdate \
    --table_name li_appeals_type \
    --connection_string carto://user:apikey \
    --s3_bucket s3_bucket \
    --json_schema_s3_key json_schema_s3_key\
    --csv_s3_key csv_s3_key\
    --select_users select_users

# Load a table from S3 to Postgres
databridge_etl_tools load \
    --table_name li_appeals_type \
    --table_schema gis_lni \
    --connection-string postgresql://user:password@host:port/db_name \
    --s3_bucket s3_bucket \
    --json_schema_s3_key json_schema_s3_key\
    --csv_s3_key csv_s3_key
```

| Flag                 | Help                                                                    |
|----------------------|-------------------------------------------------------------------------|
| --table_name         | The name of the table to extract or load to/from in a database or Carto |
| --table_schema       | The name of the schema (user) to extract or load to/from in a database  |
| --connection_string  | The connection string to a database or Carto                            |
| --s3_bucket          | The S3 bucket to fetch or load to                                       |
| --json_schema_s3_key | The S3 key for the JSON schema file                                     |
| --csv_s3_key         | The S3 key for the CSV file                                             |
| --select_users       | The Carto users to grant select access to                               |

## Installation
```bash
pip install git+https://github.com/CityOfPhiladelphia/databridge-etl-tools#egg=databridge_etl_tools
```