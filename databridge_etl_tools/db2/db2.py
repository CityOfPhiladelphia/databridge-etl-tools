import os, sys, signal, time
import logging
import click
import json
import psycopg
from psycopg import sql
import re
from shapely import wkb
from shapely.ops import transform as shp_transform
import pyproj
from pyproj import Transformer
from pyproj.transformer import TransformerGroup
import numpy as np

class Db2():
    '''One-off functions for databridge v2 stuff'''
    _staging_dataset_name = None
    _enterprise_dataset_name = None

    def __init__(self,
                table_name,
                account_name,
                xshift,
                yshift,
                copy_from_source_schema = None,
                enterprise_schema = None,
                libpq_conn_string = None,
                index_fields = None,
                to_srid = None,
                timeout = None
                ):
        self.table_name = table_name
        self.account_name = account_name
        self.copy_from_source_schema = copy_from_source_schema
        self.enterprise_schema = enterprise_schema
        self.libpq_conn_string = libpq_conn_string
        self.index_fields = index_fields.split(',') if index_fields else None
        self.to_srid = int(to_srid) if to_srid else None
        # Manual nudge to more closely match ArcGIS's 3857 transformation
        # (yes I painstakingly lined up to get the default values of -0.20 and +1.18)
        self.xshift = xshift
        self.yshift = yshift
        self.staging_schema = 'etl_staging'
        self.timeout = int(timeout) * 60 * 1000 if timeout else 50 * 60 * 1000 # convert minutes to milliseconds for "statement_timeout" in our postgres connections.
        # use this to transform specific to more general data types for staging table
        self.data_type_map = {'character varying': 'text'}
        self.ignore_field_name = []
        # placeholders vars for passing between methods
        self.geom_info = None
        self.column_info = None
        self.ddl = None
        self.m = None
        self.z = None


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

    def confirm_table_existence(self, schema=None, table=None):
        if not schema and not table:
            exist_stmt = f"SELECT to_regclass('{self.enterprise_schema}.{self.enterprise_dataset_name}');"
            print(f'Table exists statement: {exist_stmt}')
            with psycopg.connect(self.libpq_conn_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SET statement_timeout = {self.timeout}")
                    cur.execute(exist_stmt)
                    table_exists_check = cur.fetchone()[0]
            return table_exists_check
        else:
            exist_stmt = f"SELECT to_regclass('{schema}.{table}');"
            print(f'Table exists statement: {exist_stmt}')
            with psycopg.connect(self.libpq_conn_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SET statement_timeout = {self.timeout}")
                    cur.execute(exist_stmt)
                    table_exists_check = cur.fetchone()[0]
            return table_exists_check

    def get_table_column_info_from_enterprise(self):
        """Queries the information_schema.columns table to get column names and data types"""

        col_info_stmt = f'''
            SELECT column_name, data_type 
            FROM information_schema.columns
            WHERE table_schema = '{self.enterprise_schema}' and table_name = '{self.enterprise_dataset_name}'
        '''
        print('Running col_info_stmt: ' + col_info_stmt)
        with psycopg.connect(self.libpq_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self.timeout}")
                cur.execute(col_info_stmt)
                # Format and transform data types:
                column_info = {i[0]: self.data_type_map.get(i[1], i[1]) for i in cur.fetchall()}

        print(f'column_info: {column_info}')

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
        print('Running get_column_name_and_srid_stmt' + get_column_name_and_srid_stmt)
        with psycopg.connect(self.libpq_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self.timeout}")
                cur.execute(get_column_name_and_srid_stmt)
                col_name1 = cur.fetchall()

            
                # If the result is empty, there is no shape field and this is a table.
                # return empty dict that will evaluate to False.
                if not col_name1: 
                    return {}
                print(f'Got shape field and SRID back as: {col_name1}')

                col_name = col_name1[0]
                # Grab the column names

                header = [h.name for h in cur.description]
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
                print('Running geom_type_stmt: ' + geom_type_stmt)
                cur.execute(geom_type_stmt)
                result = cur.fetchone()
                if result is None:
                    geom_type_stmt = f'''
                    select geometry_type('{self.enterprise_schema}', '{self.enterprise_dataset_name}', '{geom_column}')
                    '''
                    print('Running geom_type_stmt: ' + geom_type_stmt)
                    cur.execute(geom_type_stmt)
                    geom_type = cur.fetchone()[0]

                    #geom_type = ti.xcom_pull(key=xcom_task_id_key + 'geomtype')
                    assert geom_type
                    #print(f'Got our geom_type from xcom: {geom_type}') 
                else:
                    geom_type = result[0].replace('ST_', '').capitalize()

                # Figure out if the dataset is 3D with either Z (elevation) or M ("linear referencing measures") properties
                # Grabbing this text out of the XML definition put in place by ESRI, can't find out how to do
                # it with PostGIS, doesn't seem to be a whole lot of support or awareness for these extra properties.
                has_m_or_z_stmt = f'''
                    SELECT definition FROM sde.gdb_items
                    WHERE name = 'databridge.{self.enterprise_schema}.{self.enterprise_dataset_name}'
                '''
                print('Running has_m_or_z_stmt: ' + has_m_or_z_stmt)

                cur.execute(has_m_or_z_stmt)
                result = cur.fetchone()
                if result is None:
                    # NO xml definition is in the sde.gdb_items yet, assume false
                    self.m = False
                    self.z = False
                else:
                    xml_def = result[0]

                    m = re.search(r"<HasM>\D*<\/HasM>", xml_def)[0]
                    if 'false' in m:
                        self.m = False
                    elif 'true' in m:
                        self.m = True

                    z = re.search(r"<HasZ>\D*<\/HasZ>", xml_def)[0]
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

        #print('DEBUG!!: ' + str(self.column_info))

        assert column_type_map_string

        ddl = f'''CREATE TABLE {self.staging_schema}.{self.enterprise_dataset_name}
            ({column_type_map_string})'''
        self.ddl = ddl
        #return ddl

    def run_ddl(self):
        with psycopg.connect(self.libpq_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self.timeout}")
                drop_stmt = f'DROP TABLE IF EXISTS {self.staging_schema}.{self.enterprise_dataset_name}'
                # drop first so we have a total refresh
                print('Running drop stmt: ' + drop_stmt)
                cur.execute(drop_stmt)
                cur.execute('COMMIT')
                # Identify the geometry column values
                print('Running ddl stmt: ' + self.ddl)
                cur.execute(self.ddl)
                cur.execute('COMMIT')
                # Make sure we were successful
                check_stmt = f'''
                    SELECT EXISTS
                        (SELECT FROM pg_tables
                        WHERE schemaname = \'{self.staging_schema}\'
                        AND tablename = \'{self.enterprise_dataset_name}\');
                        '''
                print('Running check_stmt: ' + check_stmt)
                cur.execute(check_stmt)
                return_val = str(cur.fetchone()[0])
                assert (return_val == 'True' or return_val == 'False')
                if return_val == 'False':
                    raise Exception('Table does not appear to have been created!')
                if return_val != 'True':
                    raise Exception('This value from the check_stmt query is unexpected: ' + return_val)
                if return_val == 'True':
                    print(f'Table "{self.staging_schema}.{self.enterprise_dataset_name}" created successfully.')


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
        with psycopg.connect(self.libpq_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self.timeout}")
                cur.execute(lock_stmt)
                locks = cur.fetchall()
                if locks:
                    print(f'Locks on table "{schema}.{table}" found!!')
                    for p in locks:
                        pid = p[0]
                        lock = p[1]
                        # Note: Can't seem to reliably get what the actual query is, not trusting
                        # the pid matching between pg_locks and pg_stat_activity.
                        #query = p[5]
                        print(f'Killing pid: "{pid}" with lock type "{lock}"')
                        cur.execute(f'SELECT pg_terminate_backend({pid});')
                        cur.execute(f'COMMIT')
                else:
                    print(f'No locks on table "{schema}.{table}" found.')

    def create_staging_from_enterprise(self):
        assert self.confirm_table_existence()
        self.get_table_column_info_from_enterprise()
        self.get_geom_column_info()
        self.generate_ddl()
        self.run_ddl()

    def copy_to_enterprise(self):
        ''''Copy from either department table or etl_staging temp table, depending on args passed.'''
        self.get_geom_column_info()
        with psycopg.connect(self.libpq_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self.timeout}")
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

                print("Executing get_enterprise_columns_stmt: " + str(get_enterprise_columns_stmt))
                cur.execute(get_enterprise_columns_stmt)
                enterprise_columns = [column for column in cur.fetchall()[0]][0]
                # Assert we actually end up with a columns list, in case this table is messed up.
                assert enterprise_columns

                # Figure out what the official OBJECTID is (since there can be multiple like "OBJECTID_1")
                get_oid_column_stmt = f'''
                    SELECT rowid_column FROM sde.sde_table_registry
                    WHERE table_name = '{self.enterprise_dataset_name}' AND schema = '{self.enterprise_schema}'
                    '''
                print("Executing get_oid_column_stmt: " + str(get_oid_column_stmt))
                cur.execute(get_oid_column_stmt)
                oid_column_return = cur.fetchone()
                # Will be used later if the table is registered. If it's not registered, set oid_column to None.
                if oid_column_return:
                    oid_column = oid_column_return[0]
                else:
                    oid_column = None
                if oid_column and oid_column != 'objectid':
                    print(f'Non-standard OID column detected, you should correct this!!: {oid_column}')


                # Figure out if the source table is registered and we have a unique situation where source is unregistered
                # and viewer is registered (using force_viewer_registered yaml key)
                get_oid_column_stmt = f'''
                    SELECT rowid_column FROM sde.sde_table_registry
                    WHERE table_name = '{self.table_name}' AND schema = '{self.account_name}'
                    '''
                print("Executing get_oid_column_stmt: " + str(get_oid_column_stmt))
                cur.execute(get_oid_column_stmt)
                oid_column_return = cur.fetchone()
                # Will be used later if the table is registered. If it's not registered, set oid_column to None.
                if oid_column_return:
                    source_oid_column = oid_column_return[0]
                else:
                    source_oid_column = None
                if source_oid_column and source_oid_column != 'objectid':
                    print(f'Non-standard OID column detected, you should correct this!!: {source_oid_column}')
                

                # Metadata column added into postgres tables by arc programs, often empty, not needed.
                if 'gdb_geomattr_data' in enterprise_columns:
                    enterprise_columns.remove('gdb_geomattr_data')

                ###############
                # Extra checks to see if we're registered
                reg_stmt=f'''
                    SELECT registration_id FROM sde.sde_table_registry
                    WHERE schema = '{self.enterprise_schema}' AND table_name = '{self.enterprise_dataset_name}'
                '''
                print("Running reg_stmt: " + str(reg_stmt))
                cur.execute(reg_stmt)
                reg_id_return = cur.fetchone()
                if reg_id_return:
                    if isinstance(reg_id_return, list) or isinstance(reg_id_return, tuple):
                        reg_id = reg_id_return[0]
                    else:
                        reg_id = reg_id_return
                else:
                    reg_id = False

                reg_stmt2 = f"select uuid from sde.gdb_items where LOWER(name) = 'databridge.{self.enterprise_schema}.{self.enterprise_dataset_name}';"
                print(reg_stmt2)
                print("Running reg_stmt2: " + str(reg_stmt2))
                cur.execute(reg_stmt2)
                reg_uuid_return = cur.fetchone()
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
                    
                    cur.execute(seq_stmt)
                    seq_return = cur.fetchone()
                    if not seq_return:
                        print(f'Warning! Could not find the objectid sequence! Ran statement: \n {seq_stmt}')
                        seq_name = False
                    if isinstance(seq_return, list) or isinstance(seq_return, tuple):
                        seq_name = seq_return[0]
                    else:
                        seq_name = seq_name
                    if not seq_name:
                        print(f'Could not find an objectid sequence, ran statement: \n {seq_stmt}')
                    else:
                        print(f'Found objectid sequence in {self.enterprise_schema} table: {str(seq_name)}')


                if reg_id and reg_uuid and oid_column:
                    fully_registered = True
                else:
                    fully_registered = False

                if oid_column and reg_id:
                    print('Resetting SDE delta insert table counter...')

                    # Get enterprise row_count, needed for resetting the objectid counter in our delta table
                    # Some pending alters will prevent this from happening. Remove highest level locks only, which will be AccessExclusiveLock.
                    self.remove_locks(self.enterprise_dataset_name, self.enterprise_schema, lock_type='AccessExclusiveLock')
                    row_count_stmt=f'select count(*) from {self.enterprise_schema}.{self.enterprise_dataset_name}'
                    cur.execute(row_count_stmt)
                    row_count = cur.fetchone()[0]

                    # Reset what the objectid field will start incrementing from.
                    reset_stmt=f'''
                        UPDATE {self.enterprise_schema}.i{reg_id} SET base_id=1, last_id=1
                        WHERE id_type = 2
                    '''
                    print("Running reset_stmt: " + str(reset_stmt))
                    cur.execute(reset_stmt)
                    cur.execute('COMMIT')

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
                    cur.execute(null_oid_stmt)
                    result = cur.fetchone()
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

                print('')
                print('enterprise_columns: ' + str(enterprise_columns_str))
                print('staging_columns: ' + str(staging_columns_str))
                print('oid_column?: ' + str(oid_column))

                if fully_registered:
                    print('Table detected as fully registered.')
                    # Reset these these to our row_count so next call to next_rowid() increments without collisions
                    if seq_name:
                        print("Resetting oid sequence..")
                        reset_oid_sequence_1 = f"SELECT setval('{self.enterprise_schema}.{seq_name}', 1, false)"
                        print(reset_oid_sequence_1)
                        cur.execute(reset_oid_sequence_1)
                    reset_oid_sequence_2 = f"UPDATE {self.enterprise_schema}.i{reg_id} SET base_id=1, last_id={row_count} WHERE id_type = 2;"
                    print("Resetting oid column..")
                    print(reset_oid_sequence_2)
                    cur.execute(reset_oid_sequence_2)
                    cur.execute('COMMIT;')
                else:
                    print('\nTable not registered.\n')

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
                    print("Running update_stmt: " + str(update_stmt))
                    self.remove_locks(self.enterprise_dataset_name, self.enterprise_schema, lock_type='AccessExclusiveLock')
                    cur.execute(update_stmt)
                    cur.execute('COMMIT;')
                except psycopg.Error as e:
                    print(f'Error! truncating and inserting into enterprise! Error: {str(e)}')
                    cur.execute('ROLLBACK;')
                    raise e

                # If successful, drop the etl_staging and old table when we're done to save space.
                if self.copy_from_source_schema == 'etl_staging':
                    cur.execute(f'DROP TABLE {stage_table}')
                    cur.execute('COMMIT')

        # Note: autocommit takes it out of a transaction.
        with psycopg.connect(self.libpq_conn_string, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self.timeout}")
                # Manually run a vacuum on our tables for database performance
                cur.execute(f'VACUUM VERBOSE {prod_table};')
                cur.execute('COMMIT')

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
                print("Running select_test_stmt: " + str(select_test_stmt))

                cur.execute(select_test_stmt)
                result = cur.fetchone()
                print('Result of select test:')
                print(str(result))
                assert result


                print('\nTriple-checking row counts..')
                cur.execute(f'select count(*) from {prod_table}')
                dest_count = cur.fetchone()[0]
                cur.execute(f'select count(*) from {stage_table}')
                source_count = cur.fetchone()[0]
                stage_table
                print(f'Source count: {source_count}, dest_count: {dest_count}')
                assert source_count == dest_count

                if self.index_fields:
                    print("\nInstalling indexes, we will NOT fail if we can't make them so table doesn't overwrite several times.")
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
                            print('Running index_stmt: ' + str(index_stmt))
                            cur.execute(index_stmt)
                            cur.execute('COMMIT;')
                    except Exception as e:
                        print(f'Error when creating index: {str(e)}')
                        cur.execute('ROLLBACK;')

                # Try to make shape index always
                if self.geom_info:
                    try:
                        geom_column = self.geom_info['geom_field']
                        shape_index_stmt = f'CREATE INDEX IF NOT EXISTS {geom_column}_gist ON {prod_table} USING GIST ({geom_column});'
                        print('\nRunning shape_index_stmt: ' + str(shape_index_stmt))
                        cur.execute(shape_index_stmt)
                        cur.execute('COMMIT;')
                    except Exception as e:
                        print(f'Error creating shape index: {str(e)}')
                        cur.execute('ROLLBACK;')

                print('\nSuccess!')

    def build_reprojector(self):
        """
        sub-function of reproject_shapes().
        Return (callable, to_srid) that maps (x,y,[z]) -> transformed coords.

        - For 2272 -> 3857:
            1) 2272 -> 4269  (to NAD83 geographic)
            2) 4269 -> 4326  via EPSG:1515  (ArcGIS NAD_1983_To_WGS_1984_5)
            3) 4326 -> 3857
        - For 4326 -> 3857: single step.
        """
        if self.geom_info['srid'] == self.to_srid:
            raise NotImplementedError("No reprojection needed, source and target SRID are the same. Have not implemented manual nudge yet if thats what you want.")
            #print('No reprojection needed, source and target SRID are the same. Will only manually nudge!')
            #t_shift = Transformer.from_pipeline(
            #    f"+proj=pipeline +step +proj=affine +s11=1 +s22=1 +xoff={self.xshift} +yoff={self.yshift}"
            #)
            #def reproj_auto(*args):


        if self.geom_info['srid'] == 2272 and self.to_srid == 3857:
            t1 = Transformer.from_crs("EPSG:2272", "EPSG:4269", always_xy=True)    # ftUS -> deg
            # NOTE: 1950 is NAD83->NAD83(CSRS)(4). If you want ArcGIS “_5”, use EPSG:1515 instead.
            t2 = Transformer.from_pipeline("urn:ogc:def:coordinateOperation:EPSG::1950")
            # t2 = Transformer.from_pipeline("urn:ogc:def:coordinateOperation:EPSG::1515")
            t3 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)    # deg -> m

            # Manual nudge in cm to align with arcgis projections
            t_shift = Transformer.from_pipeline(
                f"+proj=pipeline +step +proj=affine +s11=1 +s22=1 +xoff={self.xshift} +yoff={self.yshift}"
            )

            def reproj_vec(xyz: np.ndarray) -> np.ndarray:
                """Shapely-2 vectorized path: xyz is (N,2) or (N,3); return same shape."""
                x0, y0 = xyz[:, 0], xyz[:, 1]
                has_z = (xyz.shape[1] == 3)
                z0 = xyz[:, 2] if has_z else None

                x1, y1 = t1.transform(x0, y0)      # 2272 -> 4269
                x2, y2 = t2.transform(x1, y1)      # 4269 -> 4326  (or 1515 if you switch)
                x3, y3 = t3.transform(x2, y2)      # 4326 -> 3857
                x4, y4 = t_shift.transform(x3, y3) # post-shift in meters

                if has_z:
                    return np.column_stack([x4, y4, z0])  # preserve Z as-is
                else:
                    return np.column_stack([x4, y4])

            # Shapely-1 fallback (scalar signature)
            def reproj_scalar(x, y, z=None):
                x, y = t1.transform(x, y)
                x, y = t2.transform(x, y)
                x, y = t3.transform(x, y)
                x, y = t_shift.transform(x, y)
                return (x, y) if z is None else (x, y, z)

            def reproj_auto(*args):
                # Shapely-2 calls with (array,), Shapely-1 with (x,y[,z])
                return reproj_vec(args[0]) if len(args) == 1 else reproj_scalar(*args)

            return reproj_auto
            
        if self.geom_info['srid'] == 4326 and self.to_srid == 3857:
            t = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
            def reproj_auto(*args):
                if len(args) == 1:
                    xyz = args[0]
                    x, y = t.transform(xyz[:, 0], xyz[:, 1])
                    return np.column_stack([x, y]) if xyz.shape[1] == 2 else np.column_stack([x, y, xyz[:, 2]])
                else:
                    x, y, z = (args + (None,))[:3]
                    x, y = t.transform(x, y)
                    return (x, y) if z is None else (x, y, z)
            return reproj_auto

        raise ValueError(
            f"This script currently supports src SRID 2272 or 4326 to dst 3857. Got {self.geom_info['srid']} -> {self.to_srid}."
    )


    def copy_rows_transformed(self, dst_table, reproj, batch=1000):
        """
        sub-function of reproject_shapes().
        Stream from src, transform geometry, insert into dst in batches.
        """

        non_geom_cols = [ col for col in self.column_info.keys() ]

        non_geom_cols.remove(self.geom_info['geom_field'])

        # Build SELECT on src: non-geom columns + ST_AsEWKB(geom)
        select_sql = sql.SQL("SELECT {cols}, ST_AsEWKB({geom}) FROM {src}").format(
            cols=sql.SQL(", ").join(map(sql.Identifier, non_geom_cols)),
            geom=sql.Identifier(self.geom_info['geom_field']),
            src=sql.Identifier(self.enterprise_schema, self.enterprise_dataset_name)
        )

        # Build INSERT for dst: same non-geom cols + geom
        insert_sql = sql.SQL(
            "INSERT INTO {dst} ({cols}, {geom}) VALUES ({vals}, ST_SetSRID(%s::geometry, %s))"
        ).format(
            dst=sql.Identifier(self.enterprise_schema, dst_table),
            cols=sql.SQL(", ").join(map(sql.Identifier, non_geom_cols)),
            geom=sql.Identifier(self.geom_info['geom_field']),
            vals=sql.SQL(", ").join(sql.Placeholder() for _ in non_geom_cols),
        )

        with psycopg.connect(self.libpq_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self.timeout}")
                print('Beginning transform/copy..')
                cur.execute(select_sql)
                rows = cur.fetchmany(batch)

                while rows:
                    params = []
                    for row in rows:
                        *attrs, ewkb = row
                        if ewkb is None:
                            # Null geometry: insert as NULL straight through
                            params.append(tuple(attrs) + (None, self.to_srid))
                            continue

                        g = wkb.loads(bytes(ewkb), hex=False)
                        g2 = shp_transform(reproj, g)
                        ewkb_out = memoryview(wkb.dumps(g2, hex=False))
                        params.append(tuple(attrs) + (ewkb_out, self.to_srid))

                    with conn.cursor() as cur2:
                        cur2.execute(f"SET statement_timeout = {self.timeout}")
                        cur2.executemany(insert_sql, params)
                    conn.commit()

                    rows = cur.fetchmany(batch)
                    # Print progress every 10k rows
                    if cur.rownumber % 10000 < batch:
                        print(f'  {cur.rownumber} rows processed...')
        print('Finished copying and transforming rows.')


    def reproject_shapes(self):
        """
        Copy geometries from a source table (SRID 2272 or 4326) to a destination table (SRID 3857),
        transforming geometries client-side with pyproj (ArcGIS-parity datum step EPSG:1515 for 2272->3857).
        All non-geometry columns are copied as-is. No in-place updates.
        """

        self.get_table_column_info_from_enterprise()
        self.get_geom_column_info()

        dest_table = self.enterprise_dataset_name + '_3857'

        # Build the coordinate transformer
        reproj = self.build_reprojector()

        with psycopg.connect(self.libpq_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self.timeout}")
                # confirm our source exists
                assert self.confirm_table_existence()
                # confirm our destination table exists, if not make it.
                if not self.confirm_table_existence(schema=self.enterprise_schema, table=dest_table):
                    print(f'Destination table {self.enterprise_schema}.{dest_table} does not exist, creating it now..')
                    create_stmt = sql.SQL('CREATE TABLE {schema}.{table} (LIKE {src_schema}.{src_table} EXCLUDING CONSTRAINTS)').format(
                        schema=sql.Identifier(self.enterprise_schema),
                        table=sql.Identifier(dest_table),
                        src_schema=sql.Identifier(self.copy_from_source_schema),
                        src_table=sql.Identifier(self.enterprise_dataset_name)
                    )
                    print('Running create_stmt: ' + str(create_stmt))
                    cur.execute(create_stmt)
                    conn.commit()
                    alter_owner = sql.SQL('ALTER TABLE {schema}.{table} OWNER TO {owner}').format(
                        schema=sql.Identifier(self.enterprise_schema),
                        table=sql.Identifier(dest_table),
                        owner=sql.Identifier(self.enterprise_schema)
                    )
                    cur.execute(alter_owner)
                    conn.commit()

                delete_stmt = sql.SQL('DELETE FROM {schema}.{table}').format(
                    schema=sql.Identifier(self.enterprise_schema),
                    table=sql.Identifier(dest_table)
                )
                print('Running delete_stmt: ' + str(delete_stmt))
                cur.execute(delete_stmt)
                conn.commit()

                # Stream, transform, insert
                self.copy_rows_transformed(dest_table, reproj)

        with psycopg.connect(self.libpq_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self.timeout}")
                # Try to make shape index always
                if self.geom_info:
                    geom_column = self.geom_info['geom_field']
                    shape_index_stmt = f'CREATE INDEX IF NOT EXISTS {geom_column}_gist ON {self.enterprise_schema}.{dest_table} USING GIST ({geom_column});'
                    print('\nRunning shape_index_stmt: ' + str(shape_index_stmt))
                    cur.execute(shape_index_stmt)
                    cur.execute('COMMIT;')

                # Optional: analyze for planner stats
                cur.execute(sql.SQL("ANALYZE {tbl}").format(tbl=sql.Identifier(self.enterprise_schema, dest_table)))
                conn.commit()


@click.group()
def cli():
    pass

if __name__ == '__main__':
    cli()
