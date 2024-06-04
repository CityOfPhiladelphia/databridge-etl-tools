import os
import csv
import psycopg2

def vacuum_analyze(self):
    self.logger.info('Vacuum analyzing table: {}'.format(self.fully_qualified_table_name))

    # An autocommit connection is needed for vacuuming for psycopg2
    # https://stackoverflow.com/questions/1017463/postgresql-how-to-run-vacuum-from-code-outside-transaction-block
    old_isolation_level = self.conn.isolation_level
    self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    self.execute_sql('VACUUM ANALYZE {};'.format(self.fully_qualified_table_name))
    self.conn.set_isolation_level(old_isolation_level)
    
    self.logger.info('Vacuum analyze complete.\n')

def cleanup(self):
    '''Remove local CSV, temp CSV, JSON schema'''
    self.logger.info('Attempting to drop temp files...')
    for f in [self.csv_path, self.temp_csv_path, self.json_schema_path]:
        if f is not None:
            if os.path.isfile(f):
                try:
                    os.remove(f)
                    self.logger.info(f'\tRemoved file {f}.')
                except Exception as e:
                    self.logger.info(f'Failed to remove file {f}.')
    print('\tRemoving temp files process completed.\n')

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
