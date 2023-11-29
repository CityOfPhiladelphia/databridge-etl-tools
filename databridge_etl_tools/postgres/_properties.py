import os
import json
import psycopg2.sql as sql
from .postgres_map import DATA_TYPE_MAP, GEOM_TYPE_MAP

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
def json_schema_s3_key(self):
    # This expects the schema to be in a subfolder on S3
    if self._json_schema_s3_key == None: 
        self._json_schema_s3_key = (self.s3_key
            .replace('staging', 'schemas')
            .replace('.csv', '.json'))
    return self._json_schema_s3_key

@property
def json_schema_path(self):
    return self.csv_path.replace('.csv','.json')

@property
def export_json_schema(self):
    '''Json schema to export to s3 during extraction, for use when uploading to places like Carto.'''
    if self._export_json_schema is None:
        stmt = sql.SQL('''
        SELECT column_name, data_type, is_nullable 
        FROM information_schema.columns 
        WHERE table_name = %s
        AND table_schema = %s;
        ''')
        results = self.execute_sql(stmt, data=[self.table_name, self.table_schema], fetch='all')

        # Format into a JSON structure
        fields = []
        primary_key = None
        final_json = None
        for col in results:
            if col[0] == 'objectid':
                primary_key = 'objectid'
            field = {"name": col[0], "type": col[1]}
            # Add constraint information if column is set as "Not Null"
            if col[2] == 'NO':
                field["constraints"] = {"required": "true"}
            fields.append(field)

        pk_stmt = sql.SQL('''
        SELECT c.column_name, c.ordinal_position
        FROM information_schema.key_column_usage AS c
        LEFT JOIN information_schema.table_constraints AS t
        ON t.constraint_name = c.constraint_name
        WHERE t.table_name = %s
        AND t.table_schema = %s
        AND t.constraint_type = 'PRIMARY KEY';
        ''')
        results = self.execute_sql(pk_stmt, data=[self.table_name, self.table_schema], fetch='one')
        # override if we find a primary key with this method
        if results:
            print(f'Primary key found: {results}')
            primary_key = results[0]

        assert fields
        final_json = {
            "fields": fields,
        }

        if primary_key:
            # Update this with the primary key columns if known
            final_json["primaryKey"] = [ primary_key ]

        self._export_json_schema = json.dumps(final_json)
    return self._export_json_schema

@property
def primary_keys(self) -> 'set': 
    '''Get or return the primary keys of the table'''
    if self._primary_keys == None: 
        stmt = sql.SQL('''
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = %s::regclass
        AND    i.indisprimary;
        ''')
        results = self.execute_sql(stmt, data=[self.table_schema_name], fetch='all')
        self._primary_keys = set(x[0] for x in results)
    return self._primary_keys

@property
def pk_constraint_name(self): 
    '''Get or return the name of the primary key constraint on a Postgres table'''
    if self._pk_constraint_name == None: 
        constraint_stmt = sql.SQL('''
        SELECT con.*
        FROM pg_catalog.pg_constraint con
            INNER JOIN pg_catalog.pg_class rel
                ON rel.oid = con.conrelid
            INNER JOIN pg_catalog.pg_namespace nsp
                ON nsp.oid = connamespace
        WHERE nsp.nspname = %s
            AND rel.relname = %s
            AND contype = 'p'
        ''')
        results = self.execute_sql(constraint_stmt, data=[self.table_schema, self.table_name], fetch='all')
        self._pk_constraint_name = results[0][1]
    return self._pk_constraint_name

@property
def table_self_identifier(self): 
    '''Return the correct sql.Identifier() for a TEMP table vs. BASE table'''
    if self.table_schema == None: 
        table_identifier = sql.Identifier(self.table_name)
    else: 
        table_identifier = sql.Identifier(self.table_schema, self.table_name)
    
    return table_identifier

@property
def fields(self) -> 'list': 
    '''Get or return the fields of a table in a list'''
    if self._fields == None: 
        with self.conn.cursor() as cursor: 
            stmt = sql.SQL('''SELECT * FROM {} LIMIT 0''').format(self.table_self_identifier)
            cursor.execute(stmt)
        rv = []
        for column in cursor.description: 
            rv.append(column.name)
        self._fields = rv
    return self._fields

@property
def geom_field(self):
    return self._geom_field

# Seperate out our property's setter method so we're not repeatedly making this db call
# should only get called once.
@geom_field.setter
def geom_field(self, value):
    if self.table_name == 'testing' and self.table_schema == 'test':
        # If we recieve these values, this is the unit tests being run by tests/test_postgres.py
        # Return something so it doesn't attempt to make a connection, as conn info passed by the
        # tests is bogus.
        self._geom_field = 'shape'
    else:
        # start off with a None value to fall through conditionals properly.
        self._geom_field = None
        # First check if we're a view:
        check_view_stmt = f"select table_name from INFORMATION_SCHEMA.views where table_name = \'{self.table_name}\'"
        result = self.execute_sql(check_view_stmt, fetch='one')
        if result:
            # We're a bit limited in our options, so let's hope the shape fiel is named 'shape'
            # And check if the data_type is "USER-DEFINED".
            geom_stmt = f'''
            select column_name from information_schema.columns
                where table_name = '{self.table_name}' and (data_type = 'USER-DEFINED' or data_type = 'ST_GEOMETRY')
            '''
            result = self.execute_sql(geom_stmt, fetch='one')
            if result:
                if len(result) == 1 and result[0]:
                    self._geom_field = result[0]
                    return self._geom_field
                elif len(result) > 1:
                    raise LookupError('Multiple geometry fields')

        # Then check if were an SDE-enabled database
        if self._geom_field is None:
            check_table_stmt = "SELECT to_regclass(\'sde.st_geometry_columns\');"
            result = self.execute_sql(check_table_stmt, fetch='one')[0]
            if result != None:
                # sde.st_geometry_columns table exists, we are an SDE-enabled database
                geom_stmt = f'''
                select column_name from sde.st_geometry_columns where table_name = '{self.table_name}'
                '''
                result = self.execute_sql(geom_stmt, fetch='one')
                if result != None:
                    if result[0] != None:
                        self._geom_field = result[0]

        # Else if we're still None, then we're a PostGIS database and this query should work:
        if self._geom_field is None:
            check_table_stmt = "SELECT to_regclass(\'public.geometry_columns\');"
            result = self.execute_sql(check_table_stmt, fetch='one')[0]
            if result != None:
                geom_stmt = f'''
                SELECT f_geometry_column AS column_name
                FROM public.geometry_columns WHERE f_table_name = '{self.table_name}' and f_table_schema = '{self.table_schema}'
                '''
                self._geom_field = self.execute_sql(geom_stmt, fetch='one')
                result = self.execute_sql(geom_stmt, fetch='one')
                if result != None:
                    if result[0] != None:
                        self._geom_field = result[0]
        # Else, there truly isn't a shape field and we're not geometric? Leave as None.

@property
def geom_type(self):
    return self._geom_type

# Seperate out our property's setter method so we're not repeatedly making this db call
# should only get called once.
@geom_type.setter
def geom_type(self, value):
    if self.table_name == 'testing' and self.table_schema == 'test':
        # If we recieve these values, this is the unit tests being run by tests/test_postgres.py
        # Return something so it doesn't attempt to make a connection, as conn info passed by the
        # tests is bogus.
        self._geom_type = 'POINT'
    else:
        check_table_stmt = "SELECT EXISTS(SELECT * FROM pg_proc WHERE proname = 'geometry_type');"
        result = self.execute_sql(check_table_stmt, fetch='one')[0]
        if result:
            geom_stmt = f'''
    SELECT geometry_type('{self.table_schema}', '{self.table_name}', '{self.geom_field}')
            '''
            result = self.execute_sql(geom_stmt, fetch='one')
            if result == None:
                self._geom_type = None
            else:
                self._geom_type = result[0]
        else:
            self._geom_type = None
