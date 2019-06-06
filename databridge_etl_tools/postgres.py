import csv
import logging
import sys
import os
import json

import psycopg2
import boto3
import petl as etl


csv.field_size_limit(sys.maxsize)

DATA_TYPE_MAP = {
    'string':                      'text',
    'number':                      'numeric',
    'float':                       'numeric',
    'double precision':            'numeric',
    'integer':                     'integer',
    'boolean':                     'boolean',
    'object':                      'jsonb',
    'array':                       'jsonb',
    'date':                        'date',
    'time':                        'time',
    'datetime':                    'timestamp without time zone',
    'timestamp without time zone': 'timestamp without time zone',
    'timestamp with time zone':    'timestamp with time zone',
    'geom':                        'geometry',
    'geometry':                    'geometry'
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

class Postgres():

    _conn = None
    _logger = None
    _schema = None
    _geom_field = None
    _geom_srid = None

    def __init__(self, 
                 table_name, 
                 table_schema, 
                 connection_string, 
                 s3_bucket, 
                 json_schema_s3_key, 
                 csv_s3_key):
        self.table_name = table_name
        self.table_schema = table_schema
        self.connection_string = connection_string
        self.s3_bucket = s3_bucket
        self.json_schema_s3_key = json_schema_s3_key
        self.csv_s3_key = csv_s3_key

    @property
    def table_schema_name(self):
        # schema.table
        return '{}.{}'.format(self.table_schema, self.table_name)

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
            json_schema_directory = os.path.join('/tmp')
            json_schema_path = os.path.join(json_schema_directory, self.json_schema_file_name)
        return json_schema_path

    @property
    def conn(self):
        if self._conn is None:
            self.logger.info('Trying to connect to postgres...')
            conn = psycopg2.connect(self.connection_string)
            self._conn = conn
            self.logger.info('Connected to postgres.')
        return self._conn

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

    @property
    def schema(self):
        if self._schema is None:

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
    def logger(self):
       if self._logger is None:
           logger = logging.getLogger(__name__)
           logger.setLevel(logging.INFO)
           sh = logging.StreamHandler(sys.stdout)
           logger.addHandler(sh)
           self._logger = logger
       return self._logger

    def execute_sql(self, stmt, fetch=None):
        self.logger.info('Executing: {}'.format(stmt))

        with self.conn.cursor() as cursor:
            cursor.execute(stmt)

            if fetch == 'one':
                result = cursor.fetchone()
                return result

            elif fetch == 'many':
                result = cursor.fetchmany()
                return result

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

    def create_indexes(self, table_name):
        raise NotImplementedError

    def extract(self):
        raise NotImplementedError

    def write(self):
        self.get_csv_from_s3()
        self.get_json_schema_from_s3()
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

        self.logger.info('Writing to table: {}...'.format(self.table_schema_name))

        write_file = self.temp_csv_path
        rows.tocsv(write_file, write_header=False)

        with open(write_file, 'r') as f:
            with self.conn.cursor() as cursor:
                copy_stmt = "COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true)".format(
                            table_name=self.table_schema_name, header=str_header)
                cursor.copy_expert(copy_stmt, f)

        check_load_stmt = "SELECT COUNT(*) FROM {table_name}".format(table_name=self.table_schema_name)
        response = self.execute_sql(check_load_stmt, fetch='one')

        self.logger.info('Postgres Write Successful: {} rows imported.\n'.format(response[0]))

    def get_geom_field(self):
        with open(json_schema_path) as json_file:
            schema = json.load(json_file).get('fields', '')
            if not schema:
                self.logger.error('json schema malformatted...')
                raise
            for scheme in schema:
                scheme_type = mapping.get(scheme['type'].lower(), scheme['type'])
                if scheme_type == 'geometry':
                    geom_srid = scheme.get('srid', '')
                    geom_field = scheme.get('name', '')

    # def verify_count(self):
    #     self.logger.info('Verifying row count...')

    #     data = self.execute_sql('SELECT count(*) FROM "{}";'.format(self.table_schema_name), fetch='many')
    #     num_rows_in_table = data[0][0]
    #     num_rows_inserted = num_rows_in_table  # for now until inserts/upserts are implemented
    #     # Postgres doesn't count the header
    #     num_rows_expected = self._num_rows_in_upload_file - 1
    #     message = '{} - expected rows: {} inserted rows: {}.'.format(
    #         self.temp_table_name,
    #         num_rows_expected,
    #         num_rows_inserted
    #     )
    #     self.logger.info(message)
    #     if num_rows_in_table != num_rows_expected:
    #         self.logger.error('Did not insert all rows, reverting...')
    #         stmt = 'BEGIN;' + \
    #                 'DROP TABLE if exists "{}" cascade;'.format(self.temp_table_name) + \
    #                 'COMMIT;'
    #         self.execute_sql(stmt)
    #         exit(1)
    #     self.logger.info('Row count verified.\n')

    def vacuum_analyze(self):
        self.logger.info('Vacuum analyzing table: {}'.format(self.table_schema_name))

        # An autocommit connection is needed for vacuuming for psycopg2
        # https://stackoverflow.com/questions/1017463/postgresql-how-to-run-vacuum-from-code-outside-transaction-block
        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.execute_sql('VACUUM ANALYZE {};'.format(self.table_schema_name))
        self.conn.set_isolation_level(old_isolation_level)
        
        self.logger.info('Vacuum analyze complete.\n')

    def cleanup(self):
        self.logger.info('Attempting to drop temp files...')
        
        for f in [self.csv_path, self.temp_csv_path, self.json_schema_path]:
            if os.path.isfile(f):
                os.remove(f)

        self.logger.info('Successfully removed temp files.')

    def run_workflow(self):
        try:
            self.write()
            self.conn.commit()
            self.vacuum_analyze()
            self.logger.info('Done!')
        except Exception as e:
            self.logger.error('Workflow failed...')
            self.conn.rollback()
            raise e
        finally:
            self.cleanup()