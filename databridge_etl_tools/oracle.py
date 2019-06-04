import logging
import sys
import os

import boto3
import petl as etl


class Oracle():

    _conn = None
    _logger = None

    def __init__(self, connection_string, table_name, table_schema, s3_bucket, s3_key):
        self.connection_string = connection_string
        self.table_name = table_name
        self.table_schema = table_schema
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    @property
    def schema_table_name(self):
        schema_table_name = '{}.{}'.format(self.table_schema, self.table_name)
        return schema_table_name

    @property
    def conn(self):
        if self._conn is None:
            try:
                import cx_Oracle
            except ImportError:
                self.logger.error("cx_Oracle wasn't found... Did you install it as well as the oracle instant client?")
            self.logger.info('Trying to connect to Oracle database...')
            conn = cx_Oracle.connect(self.connection_string)
            self._conn = conn
            self.logger.info('Connected to database.')
        return self._conn

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
    def logger(self):
       if self._logger is None:
           logger = logging.getLogger(__name__)
           logger.setLevel(logging.INFO)
           sh = logging.StreamHandler(sys.stdout)
           logger.addHandler(sh)
           self._logger = logger
       return self._logger

    def load_csv_to_s3(self):
        self.logger.info('Starting load to s3: {}'.format(self.s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).put(Body=open(self.csv_path, 'rb'))
        
        self.logger.info('Successfully loaded to s3: {}'.format(self.s3_key))

    def extract(self):
        self.logger.info('Starting extract from {}'.format(self.schema_table_name))
        import geopetl

        etl.fromoraclesde(self.conn, self.schema_table_name, geom_with_srid=True) \
           .tocsv(self.csv_path, encoding='latin-1')

        self.load_csv_to_s3()
        os.remove(self.csv_path)

        self.logger.info('Successfully extracted from {}'.format(self.schema_table_name))

    def write(self):
        raise NotImplementedError