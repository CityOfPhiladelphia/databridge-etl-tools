import logging
import sys
import os
import csv
import pytz
import boto3
import petl as etl
import geopetl
import json


class Oracle():

    _conn = None
    _logger = None
    _json_schema_path = None
    _fields = None
    _row_count = None

    def __init__(self, connection_string, table_name, table_schema, s3_bucket, s3_key):
        self.connection_string = connection_string
        self.table_name = table_name
        self.table_schema = table_schema
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.times_db_called = 0
        # just initialize this self variable here so we connect first
        self.conn

    @property
    def schema_table_name(self):
        schema_table_name = '{}.{}'.format(self.table_schema, self.table_name)
        return schema_table_name

    @property
    def fields(self):
        if self._fields:
            return self._fields
        stmt=f'''
        SELECT
            COLUMN_NAME,
            DATA_TYPE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = '{self.table_schema.upper()}'
        AND TABLE_NAME = '{self.table_name.upper()}'
        '''
        cursor = self.conn.cursor()
        cursor.execute(stmt)
        self._fields = cursor.fetchall()
        return self._fields

    @property
    def row_count(self):
        if self._row_count:
            return self._row_count
        if 'OBJECTID' in self.fields:
            stmt=f'''
            SELECT COUNT(OBJECTID) FROM {self.table_schema.upper()}.{self.table_name.upper()}
            '''
        else:
            stmt=f'''
            SELECT COUNT(*) FROM {self.table_schema.upper()}.{self.table_name.upper()}
            '''
        cursor = self.conn.cursor()
        cursor.execute(stmt)
        self._row_count = cursor.fetchone()[0]
        return self._row_count

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
    def json_schema_path(self):
        if self._json_schema_path:
            return self._json_schema_path
        self._json_schema_path = self.csv_path.replace('.csv','') + '.json'
        return self._json_schema_path

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

    def load_json_schema_to_s3(self):
        s3 = boto3.resource('s3')

        # load the schema into a tmp file in /tmp/
        etl.oracle_extract_table_schema(dbo=self.conn, table_name=self.schema_table_name, table_schema_output_path=self.json_schema_path)
        json_s3_key = self.s3_key.replace('staging', 'schemas').replace('.csv', '.json')
        s3.Object(self.s3_bucket, json_s3_key).put(Body=open(self.json_schema_path, 'rb'))
        self.logger.info('Successfully loaded to s3: {}'.format(json_s3_key))

    def check_remove_nulls(self):
        '''
        This function checks for null bytes ('\0'), and if exists replace with null string (''):
        Check only the first 500 lines to stay efficient, if there aren't
        any in the first 500, there likely(maybe?) aren't any.
        '''
        has_null_bytes = False
        with open(self.csv_path, 'r', encoding='utf-8') as infile:
            for i, line in enumerate(infile):
                if has_null_bytes:
                # Break the cycle if we already found them 
                    break
                if i >= 500:
                    break
                for char in line:
                    #if char == '\0' or char == u'\xa0' or char == b'\xc2\xa0':
                    if char == '\0' or char == u'\xa0':
                        has_null_bytes = True
                        break


        if has_null_bytes:
            self.logger.info("Dataset has null bytes, removing...")
            temp_file = self.csv_path.replace('.csv', '_fmt.csv')
            with open(self.csv_path, 'r', encoding='utf-8') as infile:
                with open(temp_file, 'w', encoding='utf-8') as outfile:
                    reader = csv.reader((line.replace('\0', '') \
                                            .replace(u'\xa0', '') \
                                            .replace(u'\x00', '') \
                            for line in infile), delimiter=",")
                    writer = csv.writer(outfile)
                    writer.writerows(reader)
            os.replace(temp_file, self.csv_path)

    def extract(self):
        '''
        Extract data from database and save as a CSV file. Any fields that contain 
        datetime information without a timezone offset will be converted to US/Eastern 
        time zone (with historical accuracy for Daylight Savings Time). Oracle also 
        stores DATE fields with a time component as well, so "DATE" fields that may appear 
        without time information will also have timezone niformation added.
        Append CSV file to S3 bucket.
        '''
        self.logger.info(f'Starting extract from {self.schema_table_name}')
        self.logger.info(f'Rows to extract: {self.row_count}')
        self.logger.info('Note: petl can cause log messages to seemingly come out of order.')
        import geopetl

        # Note: data isn't read just yet at this point
        self.logger.info('Initializing data var with etl.fromoraclesde()..')
        data = etl.fromoraclesde(self.conn, self.schema_table_name, geom_with_srid=True)
        self.logger.info('Initialized.')


        datetime_fields = []
        # Do not use etl.typeset to determine data types because otherwise it causes geopetl to
        # read the database multiple times
        for field in self.fields: 
            # Create list of datetime type fields that aren't timezone aware:
            if ('TIMESTAMP' in field[1].upper() or 'DATE' in field[1].upper()) and ('TZ' not in field[1].upper() and 'TIMEZONE' not in field[1].upper() and 'TIME ZONE' not in field[1].upper()):
                datetime_fields.append(field[0].lower())


        # Try to get an (arbitrary) sensible interval to print progress on by dividing by the row count
        if self.row_count < 10000:
            interval = int(self.row_count/3)
        if self.row_count > 10000:
            interval = int(self.row_count/15)
        if self.row_count == 1:
            interval = 1
        # If it rounded down to 0 with int(), that means we have a very small amount of rows
        if not interval:
            interval = 1

        if datetime_fields:
            self.logger.info(f'Converting {datetime_fields} fields to Eastern timezone datetime')
            #data = etl.convert(data, datetime_fields, pytz.timezone('US/Eastern').localize)
            # Reasign to new object, so below "times_db_called" works
            # data_conv unbecomes a geopetl object after a convert() and becomes a 'petl.transform.conversions.FieldConvertView' object
            data_conv = etl.convert(data, datetime_fields, pytz.timezone('US/Eastern').localize)
            # Write to a CSV
            try:
                self.logger.info(f'Writing to temporary local csv {self.csv_path}..')
                etl.tocsv(data_conv.progress(interval), self.csv_path, encoding='utf-8')
            except UnicodeError:
                self.logger.info("Exception encountered trying to extract to CSV with utf-8 encoding, trying latin-1...")
                self.logger.info(f'Writing to temporary local csv {self.csv_path}..')
                etl.tocsv(data_conv.progress(interval), self.csv_path, encoding='latin-1')
        else:
            # Write to a CSV
            try:
                self.logger.info(f'Writing to temporary local csv {self.csv_path}..')
                etl.tocsv(data.progress(interval), self.csv_path, encoding='utf-8')
            except UnicodeError:
                self.logger.info("Exception encountered trying to extract to CSV with utf-8 encoding, trying latin-1...")
                self.logger.info(f'Writing to temporary local csv {self.csv_path}..')
                etl.tocsv(data.progress(interval), self.csv_path, encoding='latin-1')

        # Used solely in pytest to ensure database is called only once.
        self.times_db_called = data.times_db_called
        self.logger.info(f'Times database queried: {self.times_db_called}')

        # Confirm CSV isn't empty
        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')

        # Remove bad null characters from the csv
        self.check_remove_nulls()

        num_rows_in_csv = rows.nrows()
        if num_rows_in_csv == 0:
            raise AssertionError('Error! Dataset is empty? Line count of CSV is 0.')

        self.logger.info(f'Asserting counts match between recorded count in db and extracted csv')
        self.logger.info(f'{self.row_count} == {num_rows_in_csv}')
        assert self.row_count == num_rows_in_csv

        self.logger.info(f'Checking row count again and comparing against csv count, this can catch large datasets that are actively updating..')

        if 'OBJECTID' in self.fields:
            stmt=f'''
            SELECT COUNT(OBJECTID) FROM {self.table_schema.upper()}.{self.table_name.upper()}
            '''
        else:
            stmt=f'''
            SELECT COUNT(*) FROM {self.table_schema.upper()}.{self.table_name.upper()}
            '''
        cursor = self.conn.cursor()
        cursor.execute(stmt)
        recent_row_count = cursor.fetchone()[0]
        self.logger.info(f'{recent_row_count} == {num_rows_in_csv}')
        assert recent_row_count == num_rows_in_csv

        self.load_csv_to_s3()
        os.remove(self.csv_path)
    
        self.logger.info('Successfully extracted from {}'.format(self.schema_table_name))

    def write(self):
        raise NotImplementedError
