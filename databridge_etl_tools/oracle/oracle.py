import logging
import sys
import os
import csv
import pytz
import boto3
import petl as etl
import geopetl
import json
import hashlib
import re



class Oracle():

    _conn = None
    _logger = None
    _json_schema_path = None
    _fields = None
    _row_count = None
    from ._s3 import (get_csv_from_s3)

    def __init__(self, connection_string, table_name, table_schema, s3_bucket, s3_key, **kwargs):
        self.connection_string = connection_string
        self.table_name = table_name
        self.table_schema = table_schema
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.times_db_called = 0
        self.nonoid_fields_w_objectid = ['addressobjectid', 'eclipse_addressobjectid']
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

    def get_interval(self, row_count):
        # Try to get an (arbitrary) sensible interval to print progress on by dividing by the row count
        if row_count < 10000:
            interval = int(row_count/3)
        if row_count > 10000:
            interval = int(row_count/15)
        if row_count == 1:
            interval = 1
        # If it rounded down to 0 with int(), that means we have a very small amount of rows
        if not interval:
            interval = 1
        return interval

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


        interval = self.get_interval(self.row_count)

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
        assert num_rows_in_csv != 0, 'Error! Dataset is empty? Line count of CSV is 0.'

        self.logger.info(f'{num_rows_in_csv} == {self.row_count}')
        assert self.row_count == num_rows_in_csv, f'Row counts dont match!! extracted csv: {num_rows_in_csv}, oracle table: {self.row_count}'

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
        assert recent_row_count == num_rows_in_csv, f'Row counts dont match!! recent row count: {recent_row_count}, csv : {self.num_rows_in_csv}'

        if self.s3_bucket and self.s3_key:
            self.load_csv_to_s3()
            os.remove(self.csv_path)
        else:
            self.logger.info('Saving CSV to working directory..')
            # Move CSV out of temp path in self.csv_path and place in the current working directory
            os.replace(self.csv_path, os.path.join(os.getcwd(), self.table_name + '.csv'))
            
    
        self.logger.info('Successfully extracted from {}'.format(self.schema_table_name))

    def append(self):
        '''append a csv into a table.'''
        self.get_csv_from_s3()
        print('loading CSV into geopetl..')
        rows = etl.fromcsv(self.csv_path)
        num_rows_in_csv = rows.nrows()
        assert num_rows_in_csv != 0, 'Error! Dataset is empty? Line count of CSV is 0.'
        print(f'Rows: {num_rows_in_csv}')

        interval = self.get_interval(num_rows_in_csv)
        
        print(f"Loading CSV into Oracle table '{self.table_schema.upper()}.{self.table_name.upper()}..")
        rows.progress(interval).appendoraclesde(self.conn, f'{self.table_schema.upper()}.{self.table_name.upper()}')

    def load(self):
        '''Copy CSV into table by first inserting into a temp table (_T affix) and then deleting and inserting into table in one transaction.'''
        self.get_csv_from_s3()
        print('loading CSV into geopetl..')
        rows = etl.fromcsv(self.csv_path)
        num_rows_in_csv = rows.nrows()
        assert num_rows_in_csv != 0, 'Error! Dataset is empty? Line count of CSV is 0.'
        print(f'Rows: {num_rows_in_csv}')
        # Interval to print progress
        interval = self.get_interval(num_rows_in_csv)

        # Get columns from prod oracle table
        cursor = self.conn.cursor()
        cols_stmt = f'''
		SELECT LISTAGG(column_name, ', ') 
		WITHIN GROUP (ORDER BY column_id)
        FROM all_tab_cols
        WHERE table_name = '{self.table_name.upper()}'
        AND owner  = '{self.table_schema.upper()}'
        AND column_name not like 'SYS\_%' ESCAPE '\\'
        AND (column_name not like '%OBJECTID%' or column_name in ({','.join(["'" + f.upper() + "'" for f in self.nonoid_fields_w_objectid])}))
            '''
        cursor.execute(cols_stmt)
        cols = cursor.fetchall()[0][0]
        assert cols, f'Could not fetch columns, does the table exist?\n Statement: {cols_stmt}'

        # Becaues we're inserting into a temp table first, we need to provide the SRID. Geopetl will try to determine it for the temp table
        # and it will fail because we just made a bare, non-SDE table.
        # So, get the final table's SRID first and use it for geopetl's "insert_srid" argument.
        srid_stmt = f'''select s.auth_srid
            from sde.layers l
            join sde.spatial_references s
            on l.srid = s.srid
            where l.owner = '{self.table_schema.upper()}'
            and l.table_name = '{self.table_name.upper()}'
            '''
        cursor.execute(srid_stmt)
        response = cursor.fetchone()
        srid = response[0] if response else None

        # Detect if registered through existence of objectid column
        oid_stmt = f'''SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY column_id)
                        FROM all_tab_cols
                        WHERE table_name = '{self.table_name.upper()}'
                        AND owner  = '{self.table_schema.upper()}'
                        AND column_name like '%OBJECTID%'
                        AND column_name not in ('{','.join([ f.upper() for f in self.nonoid_fields_w_objectid])}')
                        '''
        cursor.execute(oid_stmt)
        oids = cursor.fetchall()[0][0]
        sde_registered = False
        if 'OBJECTID_' in oids:
            raise AssertionError('Nonstandard OBJECTID columm detected! Please correct your objectid column to be named just "OBJECTID"!!')
        if 'OBJECTID' in oids:
            sde_registered = True
            print('objectid found, assuming sde registered.')

        # Create a temp table name exactly 30 characters in length so we don't go over oracle 11g's table name limit
        # and then hash it so that it's unique to our table name.
        hashed = hashlib.sha256(self.table_name.encode()).hexdigest()
        temp_table_name = self.table_schema.upper() + '.TMP_' + hashed[:26].upper()
        # Create as the user we're logged in as, so we have sufficient perms to make the table.
        cursor.execute('select user from dual')
        running_user = cursor.fetchone()[0]
        temp_table_name = running_user + '.TMP_' + hashed[:26].upper()

        if running_user != 'SDE' and self.table_schema.upper() != running_user:
            raise Exception(f'Must run this as schema owner or as SDE user, please adjust your connection string! Running user: {running_user}')

        try:
            # Create temp table to hold columns, minus any possible objectid name
            tmp_table_made = False
            tmp_tbl_stmt = f'''CREATE TABLE {temp_table_name} AS
                                SELECT {cols}
                                FROM {self.table_schema.upper()}.{self.table_name.upper()} 
                                WHERE 1=0
                            '''
            print(tmp_tbl_stmt)
            cursor.execute(tmp_tbl_stmt)
            cursor.execute('COMMIT')
            tmp_table_made = True
            
            # Replace empty strings with None (which corresponds to NULL in databases)
            #rows = etl.convert(rows, {field: lambda v: 'NULL' if v == '' else v for field in etl.header(rows)})
            
            print(f'Loading CSV into {temp_table_name} (note that first printed progress rows are just loading csv into petl object)..')
            # Remove objectid because we're loading into temp table first minus objectid
            if sde_registered:
                rows_mod = etl.cutout(rows, 'objectid')
                rows_mod.progress(interval).tooraclesde(self.conn, temp_table_name, table_srid=srid)
            else:
                rows.progress(interval).tooraclesde(self.conn, temp_table_name, table_srid=srid)

            if sde_registered:
                copy_into_cols = 'OBJECTID, ' + cols
                copy_stmt = f'''
                    INSERT INTO {self.table_schema.upper()}.{self.table_name.upper()} ({copy_into_cols})
                    SELECT SDE.GDB_UTIL.NEXT_ROWID('{self.table_schema.upper()}', '{self.table_name.upper()}'), {cols}
                    FROM {temp_table_name}
                    '''
            else:
                copy_into_cols = cols
                copy_stmt = f'''
                    INSERT INTO {self.table_schema.upper()}.{self.table_name.upper()} ({copy_into_cols})
                    SELECT {cols}
                    FROM {temp_table_name}
                    '''
            print('Begin copying from temp into final table..')
            print(copy_stmt)
            try:            
                cursor.execute(f'DELETE FROM {self.table_schema.upper()}.{self.table_name.upper()}')
                cursor.execute(copy_stmt)
                cursor.execute('COMMIT')
                cursor.execute(f'DROP TABLE {temp_table_name}')
                cursor.execute('COMMIT')
                cursor.execute(f'SELECT COUNT(*) FROM {self.table_schema.upper()}.{self.table_name.upper()}')
            except Exception as e:
                if 'ORA-01031: insufficient privileges' in str(e):
                    fix_sql_stmt = 'GRANT ALL PRIVILEGES ON ' + self.table_schema.upper() + '.' + self.table_name.upper() + ' TO SDE'
                    print(f'You need to grant the SDE user privileges over this table! Connect to the database as the {self.table_schema.upper()} user and run this: {fix_sql_stmt}')
                raise e
            oracle_rows = cursor.fetchone()[0]
            print(f'assert {num_rows_in_csv} == {oracle_rows}')
            assert num_rows_in_csv == oracle_rows, f'Row counts dont match!! csv: {num_rows_in_csv}, oracle table: {oracle_rows}'
            print('Done.')
        except (Exception, KeyboardInterrupt) as e:
            cursor.execute('ROLLBACK')
            if tmp_table_made or 'name is already used by an existing object' in str(e):
                cursor.execute(f'DROP TABLE {temp_table_name}')
                cursor.execute('COMMIT')
            raise e 
