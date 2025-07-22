# databridge-etl-tools

Command line tools to extract and load SQL and Carto tables using [JSON Table Schema](http://frictionlessdata.io/guides/json-table-schema/).

## Overview
Use this tool to extract data from a source such as an Oracle SDE database, load it to S3, and then load it to a destination such as PostGIS or Carto from S3. In order for this to work, a JSON table schema for the table you are working with needs to reside in S3, in the bucket _citygeo-airflow-databridge2_ in the _schemas/_ folder. 

The tool can be used either with Docker or as a standalone Python package. 

## Requirements 

### (w/o Docker)
- Python `>=3.7, <3.10` due to dependencies in the `arcgis` package.
- Pip
- AWS CLI
- Oracle 11g Client
- Postgres
- Access to _citygeo-oracle-instant-client_ S3 bucket

### (w/ Docker)
- Docker
- Access to _citygeo-oracle-instant-client_ S3 bucket

## Installation
* Perform the following if installing this package onto a server that has not already had these steps completed
    * Install aws CLI if you don't have it `sudo apt-get install awscli`
    * Install alien if don't already have it `sudo apt-get install alien`
    * Run `bash ./scripts/pull-oracle-rpm.sh`
        * You can then verify with `ls` that the oracle instant client was downloaded into the project folder
    * Run `alien -i oracle-instantclient18.5-basiclite-18.5.0.0.0-3.x86_64.rpm \
    && rm oracle-instantclient18.5-basiclite-18.5.0.0.0-3.x86_64.rpm`

### (with pip)
* Source a virtual environment, then
```bash
pip install git+https://github.com/CityOfPhiladelphia/databridge-etl-tools.git
```

### (w/o Docker)
* Set environment variables for Oracle, if the below are not already in your `~/.bashrc` file then add them there and run them in the terminal as well: 
    ```
    export ORACLE_HOME=/usr/lib/oracle/18.5/client64
    export LD_LIBRARY_PATH=$ORACLE_HOME/lib
    export PATH="$PATH:$ORACLE_HOME"
    ```
* Create a virtual environment if one does not already exist
* Source your virtual environment 
* On Python3.9 (and maybe other versions) remove the version dependencies on the following packages: 
    * `pyproj`
    * `arcgis`
* Install the following necessary packages (note that this is copied from the Dockerfile, which essentially does a similar process)
    ```bash
    sudo apt-get install --no-install-recommends
        python3-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
        netbase \
        apt-utils \
        unzip \
        curl \
        netcat \
        locales \
        git \
        alien \
        libgdal-dev \
        libgeos-dev \
        binutils \
        libproj-dev \
        gdal-bin \
        libspatialindex-dev \
        libaio1 \
        freetds-dev
    ```
* Attempt to install the python3-dev that matches your python minor version, so for example
    * `sudo apt-get install python3.9-dev`
* `pip install .` or `pip install -e ./` if you are contributing to the repository
	* _Note, you do not need the file_ `docker-fast-requirements.txt`

### (w/ Docker)
* Until the dockerfile is able to auto-pull AWS credentials from `~/.aws/credentials`, substitute the correct keys and run: 
    * `export aws_access_key_id=<aws_access_key_id>`
    * `export aws_secret_access_key=<aws_secret_access_key>`
* Ensure docker is installed on the system - it may be necessary to run `sudo apt install docker.io`
* Download the oracle-client (`oracle-instantclient18.5-basiclite-18.5.0.0.0-3.x86_64.rpm`) to the project folder from the _citygeo-oracle-instant-client_ bucket
* Run `docker build -f Dockerfile.fast -t dbtools --build-arg AWS_ACCESS_KEY_ID="$aws_access_key_id" --build-arg AWS_SECRET_ACCESS_KEY="$aws_secret_access_key" ./`

## Usage
This package uses a nested series of commands (via sub-modules) to implement _separation of concerns_. This makes it easier to isolate any bugs and offer additional functionality over time. At any time, add `--help` to the command to review the help guide for that command or sub-group. In `click`, commands and sub-groups are internally the same thing. 

All commands will take the form of 
```
databridge_etl_tools \
    GROUP \
    GROUP_ARGS \
    COMMAND or SUB_GROUP1 \
    [COMMAND ARGS or SUB_GROUP1 ARGS] \
    [COMMAND or SUB_GROUP2] \
    [COMMAND ARGS or SUB_GROUP2 ARGS] ...
```
See **GROUPS, ARGS, SUB-GROUPS and COMMANDS** at the end of this README for a full documentation of all possible methods. 

### Examples
```bash
# Remember to add --help after any command for an explanation and its parameters

# Upsert a record into AGO using a primary key
databridge_etl_tools \
    ago \
    --ago_org_url test \
    --ago_user test \
    --ago_pw test \
    --ago_item_name test \
    --s3_bucket test \
    --s3_key test \
    append-group \
    --in_srid 0 \
    --clean_columns test \
    --batch_size 0 \
    upsert \
    --primary_key test

# Extract a table from Oracle SDE to S3
databridge_etl_tools \
    oracle \
    --table_name li_appeals_type \
    --table_schema gis_lni \
    --connection_string <user>/<password>@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)      (HOST=<host_name>)(PORT=<port>))(CONNECT_DATA=(SID=<dbname>))) \
    --s3_bucket s3-bucket \
    --s3_key s3_key \
    extract

# Load a table from S3 to Carto
databridge_etl_tools \
    carto \
	--table_name test \
	--connection_string carto://user:apikey \
	--s3_bucket test \
	--s3_key test \
	--select_users optional \
	--index_fields optional 
    update

# Load a table from S3 to Postgres
databridge_etl_tools \
    postgres \
	--table_name li_appeals_type \
	--table_schema lni \
	--connection_string postgresql://user:password@host:port/db_name \
	--s3_bucket test \
	--s3_key test \
    load \
	--json_schema_s3_key test

# Extract a json schema from Oracle (right now needed for carto uploads)
databridge_etl_tools \
    oracle \
    --table_name li_appeals_type \
    --table_schema gis_lni \
    --connection_string <user>/<password>@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)      (HOST=<host_name>)(PORT=<port>))(CONNECT_DATA=(SID=<dbname>))) \
    --s3_bucket=test \
    --s3_key=schemas/lni/li_appeals_type.json \
    extract-json-schema

# Extract a json schema from DB2 (needed for carto uploads)
databridge_etl_tools \
    postgres \
    --table_name fema_floodplain_2023 \
    --table_schema viewer_planning \
    --connection_string postgresql://postgres:<password>@<hostname>:5432/<dbname> \
    --s3_bucket=test \
    --s3_key=schemas/planning/fema_floodplain_2023.json \
    extract-json-schema

# Extract from Airtable
databridge_etl_tools \
    airtable \
    --app_id <some_app_id> \
    --api_key <key_for_some_app> \
    --table_name immigrant_services \
    --s3_bucket test \
    --s3_key staging/test/immigrant_services.csv \
    extract

# Extract from Knack
databridge_etl_tools \
    knack \
    --app_id <some_app_id> \
    --api_key <key_for_some_app> \
    --knack_objectid <some_id_corresponding_to_table> \
    --s3_bucket test \
    --s3_key staging/test/candidates.csv \
    extract

# Extract from Sharepoint
databridge_etl_tools \
    sharepoint \
    --graphapi_tenant_id <Tenant ID from Keeper> \
    --graphapi_application_id <Application ID from Keeper> \
    --graphapi_secret_value' <Secret Value from Keeper> \
    --site_name ps360-metrics-share \
    --file_path etl_tools_test_workbook.xlsx \
    --s3_bucket test \
    --s3_key staging/test/sharepoint_xlsx_test.csv \
    --sheet_name Dataset \
    extract
```

## Development
To manually test while developing, the package can be entered using the -m module flag (due to the presence of the `__main__.py` file)
```bash
python -m databridge_etl_tools \
    oracle \
    --table_name li_appeals_type \
    --table_schema gis_lni \
    --connection_string <user>/<password>@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=<host_name>)(PORT=<port>))(CONNECT_DATA=(SID=<dbname>))) \
    --s3_bucket s3-bucket \
    --s3_key s3_key \
    extract
```



## Run tests
To one run suite of tests locally, from the root `databridge-etl-tools` folder, run
```bash
pytest tests/<test_database.py>
    --user $USER 
    --password $PASSWORD 
    --host $HOST 
    --database $DATABASE 
    -xv 
    --showlocals 
    --disable-warnings 
    --show-capture stdout
```
To run just one test, add `::test_name` to the end of `test_database.py`

## Deployment
When a commit is pushed to the _master_ branch, GitHub actions will automatically run the tests given in `.github/workflows/test_pr_build.yml` using the secrets located in the repository in Settings > Secrets and Variables. 

For this reason you should make changes to your test branch, make sure they pass automated tests and manual QA testing before merging to master.

## GROUPS, ARGS, SUB-GROUPS and COMMANDS:
* `ago`: Run ETL commands for AGO
    * Args: 
        * `--ago_org_url` TEXT    [required]
        * `--ago_user` TEXT       [required]
        * `--ago_pw` TEXT         [required]
        * `--ago_item_name` TEXT  [required]
        * `--s3_bucket` TEXT      [required]
        * `--s3_key` TEXT         [required]
    * Commands: 
        * `export`             Export from an AGO dataset into a csv file in S3
        * `post-index-fields`  Post index fields to AGO
    * Sub-Group: 
        * `ago-append`: Use this group for any commands that utilize append
            * Args: 
                * `--in_srid` INTEGER     The SRID of the source datasets geometry features.
                * `--clean_columns` TEXT  Column, or comma separated list of column names to clean of AGO invalid characters.
                * `--batch_size` INTEGER  Size of batch updates to send to AGO            
            * Commands: 
                * `append` Appends records to AGO without truncating. NOTE that this is NOT an upsert and will absolutely duplicate rows if you run this multiple times.
                * `truncate-append`  Truncates a dataset in AGO and appends to it from a CSV.
                * `upsert` Upserts records to AGO, requires a primary key. Upserts the entire CSV into AGO, it does not look for changes or differences.
                    * Args: 
                        `--primary_key` TEXT  [required]
* `carto`: Run ETL commands for Carto: 
    * Args: 
        * `--table_name` TEXT         [required]
        * `--connection_string` TEXT  [required]
        * `--s3_bucket` TEXT          [required]
        * `--s3_key` TEXT             [required]
        * `--select_users` TEXT
        * `--index_fields` TEXT    
    * Commands: 
        * `update`  Loads a datasets from S3 into carto
* `db2`: Run ETL commands for DB2
    * Args: 
        * `--table_name` TEXT    [required]
        * `--account_name` TEXT  [required]    
    * Commands: 
        * `copy-staging-to-enterprise` Copies from etl_staging to the specified enterprise authoritative dataset.
        * `update-oracle-scn` WRONG Creates a staging table in etl_staging from the specified enterprise authoritative dataset.
            * Args: 
                * `--oracle_conn_string` TEXT
    * Sub-Group: 
        * `libpq` Use this group for any commands that utilize libpq
            * Args: 
                * `--enterprise_schema` TEXT  [required]
                * `--libpq_conn_string` TEXT  [required]            
            * Commands: 
                * `copy-dept-to-enterprise` Copy from the dept table directly to an enterpise able in a single transaction that can roll back if it fails.
                * `create-staging-from-enterprise` Creates a staging table in etl_staging from the specified enterprise authoritative dataset.
* `opendata`: Run ETL commands for OpenData
    * Args: 
        * `--table_name` TEXT         [required]
        * `--table_schema` TEXT       [required]
        * `--s3_bucket` TEXT          [required]
        * `--s3_key` TEXT             [required]
        * `--libpq_conn_string` TEXT  [required]
        * `--opendata_bucket` TEXT    [required]    
    * Commands: 
        * `upload` Takes a CSV from S3, runs some transformations, and then uploads to the specified opendata bucket
* `oracle`: Run ETL commands for Oracle
    * Args: 
        * `--table_name` TEXT         [required]
        * `--table_schema` TEXT       [required]
        * `--connection_string` TEXT  [required]
        * `--s3_bucket` TEXT          [required]
        * `--s3_key` TEXT             [required]    
    * Commands: 
        * `extract` Extracts a dataset in Oracle into a CSV file in S3
        * `extract-json-schema` Extracts a dataset's schema in Oracle into a JSON file in S3
* `postgres`: Run ETL commands for Postgres
    * Args: 
        * `--table_name` TEXT [required]
        * `--table_schema` TEXT [required]
        * `--connection_string` TEXT [required]
        * `--s3_bucket` TEXT
        * `--s3_key` TEXT    
    * Commands: 
        * `extract` Extracts data from a postgres table into a CSV file in S3. Has spatial and SRID detection
    and will output it in a way that the ago append commands will recognize.  
            * Args: 
                * `--with_srid` BOOLEAN Likely only needed for certain views. This controls whether the geopetl frompostgis() function exports with geom_with_srid. That wont work for some views so just export without. [default: True]
        * `extract-json-schema` Extracts a dataset's schema in Postgres into a JSON file in S3  
        * `load` Prepare and COPY a CSV from S3 to a Postgres table. The keyword arguments "column_mappings" or "mappings_file" can be used to map data file columns to database table colums with different names. Only one of column_mappings or mappings_file should be provided. Note that only the columns whose headers differ between the data file and the database table need to be included. All column names must be quoted.  
            * Args: 
                * `--column_mappings` TEXT  A string that can be read as a dictionary using `ast.literal_eval()`. It should  take the form `"{'data_col': 'db_table_col', 'data_col2': 'db_table_col2', ...}"`  
                * `--mappings_file` TEXT    A text file that can be opened with `open()` and that contains one Python dict that can be read with `ast.literal_eval()`. The file should take the form `{"data_col": "db_table_col", "data_col2": "db_table_col2", ... }`. Note no quotes around the curly braces `{}`.
        * `upsert-csv` Upserts data from a CSV to a Postgres table, which must have at least one primary key. The keyword arguments "column_mappings" or "mappings_file" can be used to map data file columns to database table colums with different names. Only one of column_mappings or mappings_file should be provided. Note that only the columns whose headers differ between the data file and the database table need to be included. All column names must be quoted.  

            * Args: 
                * `--column_mappings` TEXT  A string that can be read as a dictionary using `ast.literal_eval()`. It should  take the form `"{'data_col': 'db_table_col', 'data_col2': 'db_table_col2', ...}"`  
                * `--mappings_file` TEXT    A text file that can be opened with `open()` and that contains one Python dict that can be read with `ast.literal_eval()`. The file should take the form `{"data_col": "db_table_col", "data_col2": "db_table_col2", ... }`. Note no quotes around the curly braces `{}`.  
        * `upsert_table` Upserts data from a Postgres table to a Postgres table in the same database, which must have at least one primary key. The keyword arguments  "column_mappings" or "mappings_file" can be used to map data file columns to database table colums with different names. Only one of column_mappings or mappings_file should be provided. Note that only the columns whose headers differ between the data file and the database table need to be included. All column names must be quoted.  
            * Args: 
                * `--column_mappings` TEXT  A string that can be read as a dictionary using `ast.literal_eval()`. It should  take the form `"{'data_col': 'db_table_col', 'data_col2': 'db_table_col2', ...}"`  
                * `--mappings_file` TEXT    A text file that can be opened with `open()` and that contains one Python dict that can be read with `ast.literal_eval()`. The file should take the form `{"data_col": "db_table_col", "data_col2": "db_table_col2", ... }`. Note no quotes around the curly braces `{}`.  
                * `--other_schema` TEXT     Schema of Postgres table  to upsert from. If None or absent, assume the same schema as the table being upserted to
                * `--other_table` TEXT      Name of Postgres table to upsert from   [required]
* `sharepoint`: Run ETL commands for Sharepoint
    * Args: 
        * `--graphapi_tenant_id` TEXT         [required]
        * `--graphapi_application_id` TEXT       [required]
        * `--graphapi_secret_value` TEXT        [required]
        * `--site_name` TEXT  [required]
        * `--file_path` TEXT [required]
        * `--s3_bucket` TEXT          [required]
        * `--s3_key` TEXT             [required]    
        * `--sheet_name ` TEXT
    * Commands: 
        * `extract` Extracts a dataset in Sharepoint into a CSV file in S3