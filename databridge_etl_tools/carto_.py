import logging
import csv
import sys
import os
import re
import json

from carto.sql import SQLClient
from carto.auth import APIKeyAuthClient
from carto.exceptions import CartoException
import boto3
import requests
import petl as etl


csv.field_size_limit(sys.maxsize)

USR_BASE_URL = "https://{user}.carto.com/"
CONNECTION_STRING_REGEX = r'^carto://(.+):(.+)'

DATA_TYPE_MAP = {
    'string':           'text',
    'number':           'numeric',
    'float':            'numeric',
    'double precision': 'numeric',
    'integer':          'integer',
    'boolean':          'boolean',
    'object':           'jsonb',
    'array':            'jsonb',
    'date':             'date',
    'time':             'time',
    'datetime':         'timestamp',
    'geom':             'geometry',
    'geometry':         'geometry'
}

GEOM_TYPE_MAP = {
    'point':           'Point',
    'line':            'Linestring',
    'linestring':      'Linestring',
    'polygon':         'MultiPolygon',
    'multipolygon':    'MultiPolygon',
    'multilinestring': 'MultiLineString',
    'geometry':        'Geometry',
}

class Carto():

    _conn = None
    _logger = None
    _user = None
    _api_key = None
    _schema = None
    _geom_field = None
    _geom_srid = None

    def __init__(self, 
                 connection_string, 
                 table_name, 
                 s3_bucket, 
                 json_schema_s3_key,
                 csv_s3_key,
                 select_users,
                 index_fields=None):
        self.connection_string = connection_string
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.json_schema_s3_key = json_schema_s3_key
        self.csv_s3_key = csv_s3_key
        self.select_users = select_users
        self.index_fields = index_fields

    @property
    def user(self):
        if self._user is None:
            creds = re.match(CONNECTION_STRING_REGEX, self.connection_string).groups()
            user = creds[0]
            self._user = user
        return self._user

    @property
    def api_key(self):
        if self._api_key is None:
            creds = re.match(CONNECTION_STRING_REGEX, self.connection_string).groups()
            api_key = creds[1]
            self._api_key = api_key
        return self._api_key

    @property
    def conn(self):
        if self._conn is None:
            self.logger.info('Making connection to Carto {} account...'.format(self.user))
            try:
                api_key = self.api_key
                base_url = USR_BASE_URL.format(user=self.user)
                auth_client = APIKeyAuthClient(api_key=api_key, base_url=base_url)
                conn = SQLClient(auth_client)
                self._conn = conn
                self.logger.info('Connected to Carto.\n')
            except CartoException as e:
                self.logger.error('Failed making connection to Carto {} account...'.format(self.user))
                raise e
        return self._conn

    @property
    def temp_table_name(self):
        if not self.table_name:
            self.logger.error("Can't get table name, exiting...")
            exit(1)
        return 't_' + self.table_name

    @property
    def csv_path(self):
        csv_file_name = self.table_name
        # On Windows, save to current directory
        if os.name == 'nt':
            csv_path = '{}.csv'.format(csv_file_name)
        # On Linux, save to tmp folder
        else:
            csv_path = '/tmp/{}.csv'.format(csv_file_name)
        return csv_path

    @property
    def temp_csv_path(self):
        temp_csv_path = self.csv_path.replace('.csv', '_t.csv')
        return temp_csv_path

    @property
    def logger(self):
       if self._logger is None:
           logger = logging.getLogger(__name__)
           logger.setLevel(logging.INFO)
           sh = logging.StreamHandler(sys.stdout)
           logger.addHandler(sh)
           self._logger = logger
       return self._logger

    @property
    def json_schema_file_name(self):
        # This expects the schema to be in a subfolder on S3
        if ('/') in self.json_schema_s3_key:
            json_schema_file_name = self.json_schema_s3_key.split('/')[1]
        else:
            json_schema_file_name = self.json_schema_s3_key
        return json_schema_file_name

    @property
    def json_schema_path(self):
        # On Windows, save to current directory
        if os.name == 'nt':
            json_schema_path = self.json_schema_file_name
        # On Linux, save to tmp folder
        else:
            json_schema_directory = '/tmp'
            json_schema_path = os.path.join(json_schema_directory, self.json_schema_file_name)
        return json_schema_path

    @property
    def schema(self):
        if self._schema is None:
            self.get_json_schema_from_s3()

            with open(self.json_schema_path) as json_file:
                schema = json.load(json_file).get('fields', '')
                if not schema:
                    logger.error('Json schema malformatted...')
                    raise
                num_fields = len(schema)
                schema_fmt = ''
                for i, scheme in enumerate(schema):
                    scheme_type = DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
                    if scheme_type == 'geometry':
                        scheme_srid = scheme.get('srid', '')
                        scheme_geometry_type = GEOM_TYPE_MAP.get(scheme.get('geometry_type', '').lower(), '')
                        if scheme_srid and scheme_geometry_type:
                            scheme_type = '''geometry ({}, {}) '''.format(scheme_geometry_type, scheme_srid)
                        else:
                            logger.error('srid and geometry_type must be provided with geometry field...')
                            raise

                    schema_fmt += ' {} {}'.format(scheme['name'], scheme_type)
                    if i < num_fields - 1:
                        schema_fmt += ','
            self._schema = schema_fmt
        return self._schema

    @property
    def geom_field(self):
        if self._geom_field is None:
            with open(self.json_schema_path) as json_file:
                schema = json.load(json_file).get('fields', None)
                if not schema:
                    self.logger.error('Json schema malformatted...')
                    raise
                for scheme in schema:
                    scheme_type = DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
                    if scheme_type == 'geometry':
                        geom_field = scheme.get('name', None)
                        self._geom_field = geom_field
        return self._geom_field

    @property
    def geom_srid(self):
        if self._geom_srid is None:
            with open(self.json_schema_path) as json_file:
                schema = json.load(json_file).get('fields', None)
                if not schema:
                    self.logger.error('Json schema malformatted...')
                    raise
                for scheme in schema:
                    scheme_type = DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
                    if scheme_type == 'geometry':
                        geom_srid = scheme.get('srid', None)
                        self._geom_srid = geom_srid
        return self._geom_srid

    def get_json_schema_from_s3(self):
        self.logger.info('Fetching json schema: s3://{}/{}'.format(self.s3_bucket, self.json_schema_s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.json_schema_s3_key).download_file(self.json_schema_path)

        self.logger.info('Json schema successfully downloaded.\n'.format(self.s3_bucket, self.json_schema_s3_key))

    def get_csv_from_s3(self):
        self.logger.info('Fetching csv s3://{}/{}'.format(self.s3_bucket, self.csv_s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.csv_s3_key).download_file(self.csv_path)

        self.logger.info('CSV successfully downloaded.\n'.format(self.s3_bucket, self.csv_s3_key))

    def execute_sql(self, stmt, fetch='many'):
        self.logger.info('Executing: {}'.format(stmt))
        response = self.conn.send(stmt)
        return response

    def create_table(self):
        self.logger.info('Creating temp table...')
        stmt = '''DROP TABLE IF EXISTS {table_name}; 
                    CREATE TABLE {table_name} ({schema});'''.format(table_name=self.temp_table_name,
                                                                    schema=self.schema)
        self.execute_sql(stmt)
        check_table_sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{}');".format(self.temp_table_name)
        response = self.execute_sql(check_table_sql, fetch='many')
        exists = response['rows'][0]['exists']

        if not exists:
            message = '{} - Could not create table'.format(self.temp_table_name)
            self.logger.error(message)
            raise Exception(message)

        if self.index_fields:
            self.logger.info("Indexing fields: {}".format(self.index_fields))
            self.create_indexes()

        self.logger.info('Temp table created successfully.\n')
        
    def create_indexes(self):
        self.logger.info('Creating indexes on {}: {}'.format(
            self.temp_table_name,
            self.index_fields)
        )
        
        indexes = self.index_fields.split(',')
        stmt = ''
        for indexes_field in indexes:
            stmt += 'CREATE INDEX {table}_{field} ON "{table}" ("{field}");\n'.format(table=self.temp_table_name,
                                                                                      field=indexes_field)
        self.execute_sql(stmt)
        self.logger.info('Indexes created successfully.\n')

    def extract(self):
        raise NotImplementedError

    def write(self):
        self.get_csv_from_s3()
        rows = etl.fromcsv(self.csv_path, encoding='latin-1')

        header = rows[0]
        str_header = ''
        num_fields = len(header)
        self._num_rows_in_upload_file = rows.nrows()
        for i, field in enumerate(header):
            if i < num_fields - 1:
                str_header += field + ', '
            else:
                str_header += field

        self.logger.info('Writing to temp table...')
        write_file = self.temp_csv_path
        rows.tocsv(write_file)
        q = "COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true)".format(
            table_name=self.temp_table_name, header=str_header)
        url = USR_BASE_URL.format(user=self.user) + 'api/v2/sql/copyfrom'
        with open(write_file, 'rb') as f:
            r = requests.post(url, params={'api_key': self.api_key, 'q': q}, data=f, stream=True)

            if r.status_code != 200:
                self.logger.error('Carto Write Error Response: {}'.format(r.text))
                self.logger.error('Exiting...')
                exit(1)
            else:
                status = r.json()
                self.logger.info('Carto Write Successful: {} rows imported.\n'.format(status['total_rows']))

    def verify_count(self):
        self.logger.info('Verifying row count...')

        data = self.execute_sql('SELECT count(*) FROM "{}";'.format(self.temp_table_name), fetch='many')
        num_rows_in_table = data['rows'][0]['count']
        num_rows_inserted = num_rows_in_table  # for now until inserts/upserts are implemented
        # Carto does count the header
        num_rows_expected = self._num_rows_in_upload_file
        message = '{} - expected rows: {} inserted rows: {}.'.format(
            self.temp_table_name,
            num_rows_expected,
            num_rows_inserted
        )
        self.logger.info(message)
        if num_rows_in_table != num_rows_expected:
            self.logger.error('Did not insert all rows, reverting...')
            stmt = 'BEGIN;' + \
                    'DROP TABLE if exists "{}" cascade;'.format(temp_table_name) + \
                    'COMMIT;'
            execute_sql(stmt)
            exit(1)
        self.logger.info('Row count verified.\n')

    def cartodbfytable(self):
        self.logger.info('Cartodbfytable\'ing table: {}'.format(self.temp_table_name))
        self.execute_sql("select cdb_cartodbfytable('{}', '{}');".format(self.user, self.temp_table_name))
        self.logger.info('Successfully Cartodbyfty\'d table.\n')

    def vacuum_analyze(self):
        self.logger.info('Vacuum analyzing table: {}'.format(self.temp_table_name))
        self.execute_sql('VACUUM ANALYZE "{}";'.format(self.temp_table_name))
        self.logger.info('Vacuum analyze complete.\n')

    def generate_select_grants(self):
        grants_sql = ''
        select_users = self.select_users.split(',')
        for user in select_users:
            self.logger.info('{} - Granting SELECT to {}'.format(self.table_name, user))
            grants_sql += 'GRANT SELECT ON "{}" TO "{}";'.format(self.table_name, user)
        return grants_sql

    def cleanup(self):
        self.logger.info('Attempting to drop any temporary tables: {}'.format(self.temp_table_name))
        stmt = '''DROP TABLE IF EXISTS {} cascade'''.format(self.temp_table_name)
        self.execute_sql(stmt)
        self.logger.info('Temporary tables dropped successfully.\n')

        self.logger.info('Attempting to drop temp files...')

        for f in [self.csv_path, self.temp_csv_path, self.json_schema_path]:
            if os.path.isfile(f):
                os.remove(f)

        self.logger.info('Successfully removed temp files.')

    def swap_table(self):
        stmt = 'BEGIN;' + \
                'ALTER TABLE IF EXISTS "{}" RENAME TO "{}_old";'.format(self.table_name, self.table_name) + \
                'ALTER TABLE "{}" RENAME TO "{}";'.format(self.temp_table_name, self.table_name) + \
                'DROP TABLE IF EXISTS "{}_old" cascade;'.format(self.table_name) + \
                self.generate_select_grants() + \
                'COMMIT;'
        self.logger.info('Swapping temporary and production tables...')
        self.logger.info(stmt)
        self.execute_sql(stmt)

    def run_workflow(self):
        try:
            self.create_table()
            self.write()
            self.verify_count()
            self.cartodbfytable()
            self.vacuum_analyze()
            self.swap_table()
            self.logger.info('Done!')
        except Exception as e:
            self.logger.error('Workflow failed, reverting...')
            raise e
        finally:
            self.cleanup()