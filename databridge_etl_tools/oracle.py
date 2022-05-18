import logging
import sys
import os
import csv

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

    def check_remove_nulls(self):
        '''
        This function checks for null bytes ('\0'), and if exists replace with null string (''):
        Check only the first 500 lines to stay efficient, if there aren't
        any in the first 500, there likely(maybe?) aren't any.
        '''
        has_null_bytes = False
        with open(self.csv_path, 'r') as infile:
            for i, line in enumerate(infile):
                if i >= 500:
                    break
                for char in line:
                    if char == '\0':
                        has_null_bytes = True
                        break

        if has_null_bytes:
            self.logger.info("Dataset has null bytes, removing...")
            temp_file = self.csv_path.replace('.csv', '_fmt.csv')
            with open(self.csv_path, 'r') as infile:
                with open(temp_file, 'w') as outfile:
                    reader = csv.reader((line.replace('\0', '') for line in infile), delimiter=",")
                    writer = csv.writer(outfile)
                    writer.writerows(reader)
            os.replace(temp_file, self.csv_path)


    def extract(self):
        self.logger.info('Starting extract from {}'.format(self.schema_table_name))
        import geopetl

        try:
            etl.fromoraclesde(self.conn, self.schema_table_name, geom_with_srid=True) \
               .tocsv(self.csv_path, encoding='utf-8')
        except UnicodeError:
            self.logger.info("Exception encountered trying to extract to CSV with utf-8 encoding, trying latin-1...")
            etl.fromoraclesde(self.conn, self.schema_table_name, geom_with_srid=True) \
               .tocsv(self.csv_path, encoding='latin-1')

        # Confirm CSV isn't empty
        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')

        self.check_remove_nulls()

        num_rows_in_csv = rows.nrows()
        if num_rows_in_csv == 0:
            raise AssertionError('Error! Dataset is empty? Line count of CSV is 0.')

        self.load_csv_to_s3()
        os.remove(self.csv_path)

        self.logger.info('Successfully extracted from {}'.format(self.schema_table_name))

    def write(self):
        raise NotImplementedError

