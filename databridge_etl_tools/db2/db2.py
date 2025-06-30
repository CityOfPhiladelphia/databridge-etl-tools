import os, sys, signal, time
import logging
import click
import json
import psycopg2
import psycopg2.extras
import cx_Oracle
import re


class Db2():
    '''One-off functions for databridge v2 stuff'''
    _logger = None
    _staging_dataset_name = None
    _enterprise_dataset_name = None
    _pg_cursor = None
    _oracle_cursor = None

    def __init__(self,
                table_name,
                account_name,
                copy_from_source_schema = None,
                enterprise_schema = None,
                oracle_conn_string = None,
                libpq_conn_string = None,
                index_fields = None
                ):
        self.table_name = table_name
        self.account_name = account_name
        self.copy_from_source_schema = copy_from_source_schema
        self.enterprise_schema = enterprise_schema
        self.libpq_conn_string = libpq_conn_string
        self.index_fields = index_fields.split(',') if index_fields else None
        self.oracle_conn_string = oracle_conn_string
        self.staging_schema = 'etl_staging'
        # use this to transform specific to more general data types for staging table
        self.data_type_map = {'character varying': 'text'}
        self.ignore_field_name = []
        # placeholders vars for passing between methods
        self.geom_info = None
        self.column_info = None
        self.ddl = None
        self.m = None
        self.z = None

        # Intercept signals correctly (ctrl+c, docker stop) and cancel queries
        # reference: https://www.psycopg.org/docs/faq.html#faq-interrupt-query
        psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)

        # Setup our function to catch kill signals so we can gracefully exit.
        self.signal_catch_setup()

    def signal_catch_setup(self):
        #print("DEBUG! Setting up signal catching")
        # Handle terminations from AWS
        signal.signal(signal.SIGTERM, self.handleSigTERMKILL)
        # Handle ctrl+c
        signal.signal(signal.SIGINT, self.handleSigTERMKILL)

    # Process for gracefully exiting if the batch container is terminated.
    def handleSigTERMKILL(self, signum, frame):
        print("application received SIGTERM signal: " + str(signum))


        print("Cancelling PG query and closing the connection.")
        # Get the PID of any currently running queries on our main connection
        pid = self.conn.get_backend_pid()

        # Must create a new connection in order to cancel
        cancel_conn = psycopg2.connect(self.libpq_conn_string)
        cancel_cur = cancel_conn.cursor()
        cancel_cur.execute(f'SELECT pg_cancel_backend({pid})')

        #self.conn.close()
        cancel_conn.close()

        print("exiting the container gracefully")
        sys.exit(signum)

    @property
    def pg_cursor(self):
        if self._pg_cursor is None: 
            self.conn = psycopg2.connect(self.libpq_conn_string, connect_timeout=6)
            assert self.conn.closed == 0
            self.conn.autocommit = False
            self.conn.set_session(autocommit=False)
            self._pg_cursor = self.conn.cursor()
        return self._pg_cursor

    @property
    def oracle_cursor(self):
        if self._oracle_cursor is None: 
            conn = cx_Oracle.connect(self.oracle_conn_string)
            conn.autocommit = True
            self._oracle_cursor = conn.cursor()
        return self._oracle_cursor

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
    def staging_dataset_name(self):
        if self._staging_dataset_name is None:
            self._staging_dataset_name = f'{self.staging_schema}.{self.account_name.replace("GIS_","").lower()}__{self.table_name}'
        return self._staging_dataset_name

    @property
    def enterprise_dataset_name(self):
        if self._enterprise_dataset_name is None:
            if self.enterprise_schema == 'viewer' or self.enterprise_schema == 'import':
                self._enterprise_dataset_name = f'{self.account_name.replace("GIS_","").lower()}__{self.table_name}'
        # If we're simply copying into a department account, than the department table
        # is the "enterprise" table.
            else:
                self._enterprise_dataset_name = self.table_name
        return self._enterprise_dataset_name

    def confirm_table_existence(self):
        exist_stmt = f"SELECT to_regclass('{self.enterprise_schema}.{self.enterprise_dataset_name}');"
        self.logger.info(f'Table exists statement: {exist_stmt}')
        self.pg_cursor.execute(exist_stmt)
        table_exists_check = self.pg_cursor.fetchone()[0]
        assert table_exists_check

    def get_table_column_info_from_enterprise(self):
        """Queries the information_schema.columns table to get column names and data types"""

        col_info_stmt = f'''
            SELECT column_name, data_type 
            FROM information_schema.columns
            WHERE table_schema = '{self.enterprise_schema}' and table_name = '{self.enterprise_dataset_name}'
        '''
        self.logger.info('Running col_info_stmt: ' + col_info_stmt)
        self.pg_cursor.execute(col_info_stmt)

        # Format and transform data types:
        column_info = {i[0]: self.data_type_map.get(i[1], i[1]) for i in self.pg_cursor.fetchall()}

        self.logger.info(f'column_info: {column_info}')

        # Metadata column added into postgres tables by arc programs, not needed.
        if 'gdb_geomattr_data' in column_info.keys():
            column_info.pop('gdb_geomattr_data')

        # If the table doesn't exist, the above query silently fails.
        assert column_info
        self.column_info = column_info
        #return column_info

    def get_geom_column_info(self):
        """Queries the geometry_columns table to geom field and srid, then queries the sde table to get geom_type"""

        get_column_name_and_srid_stmt = f'''
            select f_geometry_column, srid from geometry_columns
            where f_table_schema = '{self.enterprise_schema}' and f_table_name = '{self.enterprise_dataset_name}'
        '''
        # Identify the geometry column values
        self.logger.info('Running get_column_name_and_srid_stmt' + get_column_name_and_srid_stmt)
        self.pg_cursor.execute(get_column_name_and_srid_stmt)

        #col_name = self.pg_cursor.fetchall()[0]
        col_name1 = self.pg_cursor.fetchall()
    
        # If the result is empty, there is no shape field and this is a table.
        # return empty dict that will evaluate to False.
        if not col_name1: 
            return {}
        self.logger.info(f'Got shape field and SRID back as: {col_name1}')

        col_name = col_name1[0]
        # Grab the column names
        header = [h.name for h in self.pg_cursor.description]
        # zip column names with the values
        geom_column_and_srid = dict(zip(header, list(col_name)))
        geom_column = geom_column_and_srid['f_geometry_column']
        srid = geom_column_and_srid['srid']

        # Get the type of geometry, e.g. point, line, polygon.. etc.
        # docs on this SDE function: https://desktop.arcgis.com/en/arcmap/latest/manage-data/using-sql-with-gdbs/st-geometrytype.htm
        # NOTE!: if the entreprise_schema is empty, e.g. this is a first run, this will fail, as a backup get the value from XCOM
        # Which will be populated by our "get_geomtype" task.
        geom_type_stmt = f'''
            select public.st_geometrytype({geom_column}) as geom_type 
            from {self.enterprise_schema}.{self.enterprise_dataset_name}
            where st_isempty({geom_column}) is False
            limit 1
        '''
        self.logger.info('Running geom_type_stmt: ' + geom_type_stmt)
        self.pg_cursor.execute(geom_type_stmt)
        result = self.pg_cursor.fetchone()
        if result is None:
            geom_type_stmt = f'''
            select geometry_type('{self.enterprise_schema}', '{self.enterprise_dataset_name}', '{geom_column}')
            '''
            self.logger.info('Running geom_type_stmt: ' + geom_type_stmt)
            self.pg_cursor.execute(geom_type_stmt)
            geom_type = self.pg_cursor.fetchone()[0]

            #geom_type = ti.xcom_pull(key=xcom_task_id_key + 'geomtype')
            assert geom_type
            #self.logger.info(f'Got our geom_type from xcom: {geom_type}') 
        else:
            geom_type = result[0].replace('ST_', '').capitalize()

        # Figure out if the dataset is 3D with either Z (elevation) or M ("linear referencing measures") properties
        # Grabbing this text out of the XML definition put in place by ESRI, can't find out how to do
        # it with PostGIS, doesn't seem to be a whole lot of support or awareness for these extra properties.
        has_m_or_z_stmt = f'''
            SELECT definition FROM sde.gdb_items
            WHERE name = 'databridge.{self.enterprise_schema}.{self.enterprise_dataset_name}'
        '''
        self.logger.info('Running has_m_or_z_stmt: ' + has_m_or_z_stmt)
        self.pg_cursor.execute(has_m_or_z_stmt)
        result = self.pg_cursor.fetchone()
        if result is None:
            # NO xml definition is in the sde.gdb_items yet, assume false
            self.m = False
            self.z = False
        else:
            xml_def = result[0]

            m = re.search("<HasM>\D*<\/HasM>", xml_def)[0]
            if 'false' in m:
                self.m = False
            elif 'true' in m:
                self.m = True

            z = re.search("<HasZ>\D*<\/HasZ>", xml_def)[0]
            if 'false' in z:
                self.z = False
            elif 'true' in z:
                self.z = True

        # This will ultimpately be the data type we create the table with,
        # example data type: 'shape geometry(MultipolygonZ, 2272)
        if self.m:
            geom_type = geom_type + 'M'
        if self.z:
            geom_type = geom_type + 'Z'
            

        self.geom_info = {'geom_field': geom_column,
                          'geom_type': geom_type,
                          'srid': srid}
        print(f'self.geom_info: {self.geom_info}')

        #return {'geom_field': geom_column, 'geom_type': geom_type, 'srid': srid}

    def generate_ddl(self):
        """Builds the DDL based on the table's generic and geom column info"""
        # If geom_info is not None
        if self.geom_info:
            column_type_map = [f'{k} {v}' for k,v in self.column_info.items() if k not in self.ignore_field_name and k != self.geom_info['geom_field']]
            srid = self.geom_info['srid']
            geom_column = self.geom_info['geom_field']

            geom_type = self.geom_info['geom_type']
            geom_column_string = f'{geom_column} geometry({geom_type}, {srid})'
            column_type_map.append(geom_column_string)
            column_type_map_string = ', '.join(column_type_map)
        # If this is a table with no geometry column..
        else:
            column_type_map = [f'{k} {v}' for k,v in self.column_info.items() if k not in self.ignore_field_name]
            column_type_map_string = ', '.join(column_type_map)

        #self.logger.info('DEBUG!!: ' + str(self.column_info))

        assert column_type_map_string

        ddl = f'''CREATE TABLE {self.staging_schema}.{self.enterprise_dataset_name}
            ({column_type_map_string})'''
        self.ddl = ddl
        #return ddl

    def run_ddl(self):
        drop_stmt = f'DROP TABLE IF EXISTS {self.staging_schema}.{self.enterprise_dataset_name}'
        # drop first so we have a total refresh
        self.logger.info('Running drop stmt: ' + drop_stmt)
        self.pg_cursor.execute(drop_stmt)
        self.pg_cursor.execute('COMMIT')
        # Identify the geometry column values
        self.logger.info('Running ddl stmt: ' + self.ddl)
        self.pg_cursor.execute(self.ddl)
        self.pg_cursor.execute('COMMIT')
        # Make sure we were successful
        try:
            check_stmt = f'''
                SELECT EXISTS
                    (SELECT FROM pg_tables
                    WHERE schemaname = \'{self.staging_schema}\'
                    AND tablename = \'{self.enterprise_dataset_name}\');
                    '''
            self.logger.info('Running check_stmt: ' + check_stmt)
            self.pg_cursor.execute(check_stmt)
            return_val = str(self.pg_cursor.fetchone()[0])
            assert (return_val == 'True' or return_val == 'False')
            if return_val == 'False':
                raise Exception('Table does not appear to have been created!')
            if return_val != 'True':
                raise Exception('This value from the check_stmt query is unexpected: ' + return_val)
            if return_val == 'True':
                self.logger.info(f'Table "{self.staging_schema}.{self.enterprise_dataset_name}" created successfully.')
        except Exception as e:
            raise Exception("DEBUG: " + str(e) + " RETURN: " + str(return_val) + " DDL: " + self.ddl + " check query" + check_stmt)

    def remove_locks(self, table, schema, lock_type=None):
        # Update 4-2-2024:
        # Because we're running ALTER statements, our final table rename process tries to get an AccessExclusiveLock on the table.
        # We cannot get this lock if anyone else is viewing the table through something like dbeaver (but not pro) because
        # dbeaver puts an AccessShareLock on the table when holding it open. Which blocks our alter.
        # We cannot get an AccessExclusive lock if ANY lock exists on the table, so find and kill them.
                
        lock_stmt = f'''
            SELECT pg_locks.pid,
                pg_locks.mode,
                pg_locks.granted,
                pg_class.relname AS table_name,
                pg_namespace.nspname AS schema_name,
                pg_stat_activity.query AS current_query
            FROM pg_locks
            JOIN pg_class ON pg_locks.relation = pg_class.oid
            JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
            JOIN pg_stat_activity ON pg_locks.pid = pg_stat_activity.pid
            WHERE pg_class.relname = '{table}' AND pg_namespace.nspname = '{schema}'
        '''
        if lock_type:
            lock_stmt +=  f" AND pg_locks.mode = '{lock_type}'"
        lock_stmt += ';'

        self.pg_cursor.execute(lock_stmt)
        locks = self.pg_cursor.fetchall()
        if locks:
            self.logger.info(f'Locks on table "{schema}.{table}" found!!')
            for p in locks:
                pid = p[0]
                lock = p[1]
                # Note: Can't seem to reliably get what the actual query is, not trusting
                # the pid matching between pg_locks and pg_stat_activity.
                #query = p[5]
                self.logger.info(f'Killing pid: "{pid}" with lock type "{lock}"')
                self.pg_cursor.execute(f'SELECT pg_terminate_backend({pid});')
                self.pg_cursor.execute(f'COMMIT')
        else:
            self.logger.info(f'No locks on table "{schema}.{table}" found.')

    def create_staging_from_enterprise(self):
        self.confirm_table_existence()
        self.get_table_column_info_from_enterprise()
        self.get_geom_column_info()
        self.generate_ddl()
        self.run_ddl()

    def copy_to_enterprise(self):
        ''''Copy from either department table or etl_staging temp table, depending on args passed.'''
        
        prod_table = f'{self.enterprise_schema}.{self.enterprise_dataset_name}'
        # If etl_staging, that means we got data uploaded from S3 or an ArcPy copy
        # so use the appropriate name which would be "etl_staging.dept_name__table_name"
        if self.copy_from_source_schema == 'etl_staging':
            stage_table = self.staging_dataset_name
        # If it's not etl_staging, that means we're copying directly from the dept table to entreprise
        # so appropriate name would be "dept_name.table_name"
        else:
            stage_table = f'{self.copy_from_source_schema}.{self.table_name}'

        get_enterprise_columns_stmt = f'''
        SELECT array_agg(COLUMN_NAME::text order by COLUMN_NAME)
        FROM information_schema.columns
        WHERE table_name = '{self.enterprise_dataset_name}' AND table_schema = '{self.enterprise_schema}'
        '''

        self.logger.info("Executing get_enterprise_columns_stmt: " + str(get_enterprise_columns_stmt))
        self.pg_cursor.execute(get_enterprise_columns_stmt)
        enterprise_columns = [column for column in self.pg_cursor.fetchall()[0]][0]
        # Assert we actually end up with a columns list, in case this table is messed up.
        assert enterprise_columns

        # Figure out what the official OBJECTID is (since there can be multiple like "OBJECTID_1")
        get_oid_column_stmt = f'''
            SELECT rowid_column FROM sde.sde_table_registry
            WHERE table_name = '{self.enterprise_dataset_name}' AND schema = '{self.enterprise_schema}'
            '''
        self.logger.info("Executing get_oid_column_stmt: " + str(get_oid_column_stmt))
        self.pg_cursor.execute(get_oid_column_stmt)
        oid_column_return = self.pg_cursor.fetchone()
        # Will be used later if the table is registered. If it's not registered, set oid_column to None.
        if oid_column_return:
            oid_column = oid_column_return[0]
        else:
            oid_column = None
        if oid_column and oid_column != 'objectid':
            self.logger.info(f'Non-standard OID column detected, you should correct this!!: {oid_column}')


        # Figure out if the source table is registered and we have a unique situation where source is unregistered
        # and viewer is registered (using force_viewer_registered yaml key)
        get_oid_column_stmt = f'''
            SELECT rowid_column FROM sde.sde_table_registry
            WHERE table_name = '{self.table_name}' AND schema = '{self.account_name}'
            '''
        self.logger.info("Executing get_oid_column_stmt: " + str(get_oid_column_stmt))
        self.pg_cursor.execute(get_oid_column_stmt)
        oid_column_return = self.pg_cursor.fetchone()
        # Will be used later if the table is registered. If it's not registered, set oid_column to None.
        if oid_column_return:
            source_oid_column = oid_column_return[0]
        else:
            source_oid_column = None
        if source_oid_column and source_oid_column != 'objectid':
            self.logger.info(f'Non-standard OID column detected, you should correct this!!: {source_oid_column}')
        

        # Metadata column added into postgres tables by arc programs, often empty, not needed.
        if 'gdb_geomattr_data' in enterprise_columns:
            enterprise_columns.remove('gdb_geomattr_data')

        ###############
        # Extra checks to see if we're registered
        reg_stmt=f'''
            SELECT registration_id FROM sde.sde_table_registry
            WHERE schema = '{self.enterprise_schema}' AND table_name = '{self.enterprise_dataset_name}'
        '''
        self.logger.info("Running reg_stmt: " + str(reg_stmt))
        self.pg_cursor.execute(reg_stmt)
        reg_id_return = self.pg_cursor.fetchone()
        if reg_id_return:
            if isinstance(reg_id_return, list) or isinstance(reg_id_return, tuple):
                reg_id = reg_id_return[0]
            else:
                reg_id = reg_id_return
        else:
            reg_id = False

        reg_stmt2 = f"select uuid from sde.gdb_items where LOWER(name) = 'databridge.{self.enterprise_schema}.{self.enterprise_dataset_name}';"
        print(reg_stmt2)
        self.logger.info("Running reg_stmt2: " + str(reg_stmt2))
        self.pg_cursor.execute(reg_stmt2)
        reg_uuid_return = self.pg_cursor.fetchone()
        if isinstance(reg_uuid_return, list) or isinstance(reg_uuid_return, tuple):
            reg_uuid = reg_uuid_return[0]
        else:
            reg_uuid = reg_uuid_return

        # Get the objectid sequence associated with the table, also needed for resetting objectid column counter.
        # Sometimes this may not exist somehow.
        seq_name = None
        if oid_column:
            seq_stmt = f'''
            SELECT
                S.relname AS sequence_name
            FROM
                pg_class S
            JOIN pg_depend D ON (S.oid = D.objid)
            JOIN pg_class T ON (D.refobjid = T.oid)
            JOIN pg_namespace n ON (S.relnamespace = n.oid)
            WHERE
                S.relkind = 'S' AND
                n.nspname = '{self.enterprise_schema}' AND
                T.relname = '{self.enterprise_dataset_name}' AND
                s.relname like '%objectid_seq';
            '''
            
            self.pg_cursor.execute(seq_stmt)
            seq_return = self.pg_cursor.fetchone()
            if not seq_return:
                self.logger.warning(f'Could not find the objectid sequence! Ran statement: \n {seq_stmt}')
                seq_name = False
            if isinstance(seq_return, list) or isinstance(seq_return, tuple):
                seq_name = seq_return[0]
            else:
                seq_name = seq_name
            if not seq_name:
                self.logger.info(f'Could not find an objectid sequence, ran statement: \n {seq_stmt}')
            else:
                print(f'Found objectid sequence in {self.enterprise_schema} table: {str(seq_name)}')


        if reg_id and reg_uuid and oid_column:
            fully_registered = True
        else:
            fully_registered = False

        if oid_column and reg_id:
            self.logger.info('Resetting SDE delta insert table counter...')

            # Get enterprise row_count, needed for resetting the objectid counter in our delta table
            # Some pending alters will prevent this from happening. Remove highest level locks only, which will be AccessExclusiveLock.
            self.remove_locks(self.enterprise_dataset_name, self.enterprise_schema, lock_type='AccessExclusiveLock')
            row_count_stmt=f'select count(*) from {self.enterprise_schema}.{self.enterprise_dataset_name}'
            self.pg_cursor.execute(row_count_stmt)
            row_count = self.pg_cursor.fetchone()[0]

            # Reset what the objectid field will start incrementing from.
            reset_stmt=f'''
                UPDATE {self.enterprise_schema}.i{reg_id} SET base_id=1, last_id=1
                WHERE id_type = 2
            '''
            self.logger.info("Running reset_stmt: " + str(reset_stmt))
            self.pg_cursor.execute(reset_stmt)
            self.pg_cursor.execute('COMMIT')

        if not oid_column and reg_id:
            raise AssertionError('SDE Registration mismatch! We found an objectid column from sde.sde_table_registry, but could not find a registration id. This table is broken and should be remade!')

        #############################
        # Prepare to insert

        # Only remove objectid from columns list if there is a seq stmt because then objectid column will be auto populated
        # Otherwise we need to fill in the oid column ourselves.
        if oid_column and seq_name:
            enterprise_columns.remove(oid_column)
        if 'objectid' in enterprise_columns and seq_name:
            enterprise_columns.remove('objectid')

        # Construct our columns string, which we'll use for our insert statement below
        # still deciding how to handle our oid column at this point.
        #
        # Some tables are made without a sequence, and can have an empty objectid column if someone decided
        # to make a different objectid than just "objectid" in oracle. This means that the table is initially made
        # without that column, but still an oid column called "objectid". In this rare scenario, the objectid column will be empty.
        if oid_column and not seq_name:
            null_oid_stmt = f'select {oid_column} from {prod_table} where {oid_column} is not null;'
            print(f'Checking if {oid_column} col is null...')
            self.pg_cursor.execute(null_oid_stmt)
            result = self.pg_cursor.fetchone()
            # If empty oid column, AND no oid sequence, insert using sde.next_rowid
            if not result:
                print(f'Falling back to using sde_nextrowid() function for inserting into {oid_column}..')
                # Remove so we can put objectid last and be sure of it's position.
                enterprise_columns.remove(oid_column)
                # Actually make a copy of the list and not just a pointer.
                staging_columns = list(enterprise_columns)
                staging_columns.append(oid_column)
                enterprise_columns.append(f"sde.next_rowid('{self.enterprise_schema}','{self.enterprise_dataset_name}')")
            else:
                staging_columns = enterprise_columns.copy()
        else:
            staging_columns = enterprise_columns.copy()

        # if no objectid in source, but objectid in enterprise, remove all objectid references and use next_rowid if no sequence
        # This scenario should only be encountered in "force_viewer_registered" DAGs, whereby the source table is unregistered but we want the viewer to be registered.
        if (not source_oid_column and oid_column) and not seq_name:
            # Put objectid at end.
            staging_columns.remove('objectid')
            staging_columns.append('objectid')
            try:
                enterprise_columns.remove('objectid')
            except:
                pass
            if f"sde.next_rowid('{self.enterprise_schema}','{self.enterprise_dataset_name}')" not in enterprise_columns:
                enterprise_columns.append(f"sde.next_rowid('{self.enterprise_schema}','{self.enterprise_dataset_name}')")

        # For scenarios where we have an objectid sequence on the viewer table, just remove objectid entirely and the database *should* take care of it.
        elif (not source_oid_column and oid_column) and not seq_name:
            staging_columns.remove('objectid')
            staging_columns.remove('objectid')

        # add double quotes around all column names to allow columns names with varying case
        staging_columns = [f'"{col}"' if 'sde.next_rowid' not in col else col for col in staging_columns]
        enterprise_columns = [f'"{col}"' if 'sde.next_rowid' not in col else col for col in enterprise_columns]

        staging_columns_str = ', '.join(staging_columns)
        enterprise_columns_str = ', '.join(enterprise_columns)

        self.logger.info('')
        self.logger.info('enterprise_columns: ' + str(enterprise_columns_str))
        self.logger.info('staging_columns: ' + str(staging_columns_str))
        self.logger.info('oid_column?: ' + str(oid_column))

        if fully_registered:
            self.logger.info('Table detected as fully registered.')
            # Reset these these to our row_count so next call to next_rowid() increments without collisions
            if seq_name:
                self.logger.info("Resetting oid sequence..")
                reset_oid_sequence_1 = f"SELECT setval('{self.enterprise_schema}.{seq_name}', 1, false)"
                self.logger.info(reset_oid_sequence_1)
                self.pg_cursor.execute(reset_oid_sequence_1)
            reset_oid_sequence_2 = f"UPDATE {self.enterprise_schema}.i{reg_id} SET base_id=1, last_id={row_count} WHERE id_type = 2;"
            self.logger.info("Resetting oid column..")
            self.logger.info(reset_oid_sequence_2)
            self.pg_cursor.execute(reset_oid_sequence_2)
            self.pg_cursor.execute('COMMIT;')
        else:
            self.logger.info('\nTable not registered.\n')

        try:
            update_stmt = f'''
                BEGIN;
                    -- Truncate our table (won't show until commit) 
                    DELETE FROM {prod_table};
                    INSERT INTO {prod_table} ({staging_columns_str})
                        SELECT {enterprise_columns_str}
                        FROM {stage_table};
                END;
                '''
            self.logger.info("Running update_stmt: " + str(update_stmt))
            self.remove_locks(self.enterprise_dataset_name, self.enterprise_schema, lock_type='AccessExclusiveLock')
            self.pg_cursor.execute(update_stmt)
            self.pg_cursor.execute('COMMIT;')
        except psycopg2.Error as e:
            self.logger.error(f'Error truncating and inserting into enterprise! Error: {str(e)}')
            self.pg_cursor.execute('ROLLBACK;')
            raise e

        # If successful, drop the etl_staging and old table when we're done to save space.
        # NOTE: don't do this for now to make task migration easier -Roland 9/25/2023
        #if self.copy_from_source_schema == 'etl_staging':
        #    self.pg_cursor.execute(f'DROP TABLE {stage_table}')
        #    self.pg_cursor.execute('COMMIT')

        # Manually run a vacuum on our tables for database performance
        self.pg_cursor.execute(f'VACUUM VERBOSE {prod_table}')
        self.pg_cursor.execute('COMMIT')

        # Run a quick select statement to test, use objectid if available
        if oid_column:
            select_test_stmt = f'''
            SELECT * FROM {prod_table} WHERE {oid_column} IS NOT NULL LIMIT 1
            '''
        # Else bare select
        else:
            select_test_stmt = f'''
            SELECT * FROM {prod_table} LIMIT 1
            '''
        self.logger.info("Running select_test_stmt: " + str(select_test_stmt))

        self.pg_cursor.execute(select_test_stmt)
        result = self.pg_cursor.fetchone()
        self.logger.info('Result of select test:')
        self.logger.info(str(result))
        assert result


        self.logger.info('\nTriple-checking row counts..')
        self.pg_cursor.execute(f'select count(*) from {prod_table}')
        dest_count = self.pg_cursor.fetchone()[0]
        self.pg_cursor.execute(f'select count(*) from {stage_table}')
        source_count = self.pg_cursor.fetchone()[0]
        stage_table
        print(f'Source count: {source_count}, dest_count: {dest_count}')
        assert source_count == dest_count

        self.logger.info("\nInstalling indexes, we will NOT fail if we can't make them so table doesn't overwrite several times.")
        if self.index_fields:
            try:
                for i in self.index_fields:
                    # if compound index
                    if '+' in i:
                        split_indexes = i.split('+')
                        # compile the indexes into a comma separated string because we don't know how many could be in the compound.
                        cols = ', '.join(split_indexes)
                        idx_name = '_'.join(split_indexes) + '_idx'
                        index_stmt = f'CREATE INDEX IF NOT EXISTS {idx_name}_idx ON {prod_table} USING btree ({cols});'
                    # If single index
                    else:
                        index_stmt = f'CREATE INDEX IF NOT EXISTS {i}_idx ON {prod_table} USING btree ({i});'
                    self.logger.info('Running index_stmt: ' + str(index_stmt))
                    self.pg_cursor.execute(index_stmt)
                    self.pg_cursor.execute('COMMIT;')
            except Exception as e:
                self.logger.error(f'Error creating index: {str(e)}')
                self.pg_cursor.execute('ROLLBACK;')

        print('\nSuccess!')

    def update_oracle_scn(self):
        
        # Commenting this out, instead lets pull the SCN from our recorded table
        #stmt = f'''SELECT MAX(ora_rowscn) FROM {self.account_name}.{self.table_name.upper()}'''
        #self.logger.info('Executing stmt: ' + str(stmt))
        #self.oracle_cursor.execute(stmt)
        #current_scn = self.oracle_cursor.fetchone()[0]

        stmt=f'''
            SELECT SCN FROM GIS_GSG.DB2_ORACLE_TRANSACTION_HISTORY
            WHERE TABLE_OWNER = '{self.account_name.upper()}'
            AND TABLE_NAME = '{self.table_name.upper()}'
            AND STATUS = 'RUNNING'
        '''
        self.oracle_cursor.execute(stmt)
        current_scn = self.oracle_cursor.fetchone()

        # If there is no SCN available, insert NULL which will work in an INT datatype column.
        if not current_scn:
            current_scn = 'NULL'
        else:
            current_scn = current_scn[0]

        stmt=f'''
            SELECT SCN FROM GIS_GSG.DB2_ORACLE_TRANSACTION_HISTORY
            WHERE TABLE_OWNER = '{self.account_name.upper()}'
            AND TABLE_NAME = '{self.table_name.upper()}'
            AND STATUS = 'FINISHED'
        '''
        self.logger.info('Executing stmt: ' + str(stmt))
        self.oracle_cursor.execute(stmt)
        old_scn = self.oracle_cursor.fetchone()

        # Because Oracle is an outdated database product, we don't have upsert and need to do
        # either an insert or update depending if the row we want already exists.
        if old_scn is None:
            stmt = f'''
            INSERT INTO GIS_GSG.DB2_ORACLE_TRANSACTION_HISTORY (TABLE_OWNER, TABLE_NAME, SCN, STATUS)
                VALUES('{self.account_name.upper()}', '{self.table_name.upper()}', {current_scn}, 'FINISHED')
            '''
        elif old_scn:
            stmt = f'''
            UPDATE GIS_GSG.DB2_ORACLE_TRANSACTION_HISTORY SET SCN={current_scn}
                WHERE TABLE_OWNER = '{self.account_name.upper()}'
                AND TABLE_NAME = '{self.table_name.upper()}'
                AND STATUS = 'FINISHED'
            '''
        self.logger.info('Executing stmt: ' + str(stmt))
        self.oracle_cursor.execute(stmt)
    
        # Remove RUNNING scn from the history table
        if current_scn:
            stmt=f'''
                DELETE FROM GIS_GSG.DB2_ORACLE_TRANSACTION_HISTORY
                WHERE TABLE_OWNER = '{self.account_name.upper()}'
                AND TABLE_NAME = '{self.table_name.upper()}'
                AND STATUS = 'RUNNING'
            '''
            self.logger.info('Executing stmt: ' + str(stmt))
            self.oracle_cursor.execute(stmt)


@click.group()
def cli():
    pass

if __name__ == '__main__':
    cli()
