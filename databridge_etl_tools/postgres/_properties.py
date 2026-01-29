import os
import json
import psycopg2.sql as sql
from .postgres_map import DATA_TYPE_MAP, GEOM_TYPE_MAP


@property
def csv_path(self):
    if not self.local_csv_path:
        csv_file_name = self.table_name
        # On Windows, save to current directory
        if os.name == 'nt':
            csv_path = '{}.csv'.format(csv_file_name)
        # On Linux, save to tmp folder
        else:
            csv_path = '/tmp/{}.csv'.format(csv_file_name)
    else:
        csv_path = self.local_csv_path
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

        if self.geom_field:
            # first get srid
            stmt = sql.SQL('''
            SELECT srid FROM geometry_columns
            WHERE f_table_name = %s
            AND f_table_schema = %s;
            ''')
            results = self.execute_sql(stmt, data=[self.table_name, self.table_schema], fetch='one')
            if results:
                srid = results[0]
            assert srid

            if self.geom_type.lower() == 'multipolygon' or self.geom_type.lower() == 'polygon':
                # not sure why, but polygons have to be a generic "geometry" to work in carto.
                geom_type = 'geometry'
            else:
                geom_type = self.geom_type.lower()

            # Properly format shape field
            for field in fields:
                if field['name'] == self.geom_field:
                    field['type'] = 'geometry'
                    field['geometry_type'] = geom_type
                    field['srid'] = srid
                    break

        print(json.dumps(final_json, indent=2))

        self._export_json_schema = json.dumps(final_json, indent=2)
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
        results = self.execute_sql(stmt, data=[self.fully_qualified_table_name], fetch='all')
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
def fields_and_types(self):
    '''Returns a list of tuples, each containing field name [0] and type [1]'''
    if self._fields:
        return self._fields
    else:
        if self.database_object_type == 'table':
            stmt = """
                select column_name as name, data_type as type
                from information_schema.columns
                where table_schema = '{}' and table_name = '{}'
                """.format(self.table_schema, self.table_name)
        # Funny method for getting the column names and data types for views
        elif self.database_object_type == 'materialized_view' or self.database_object_type == 'view':
            stmt = """
                select 
                    attr.attname as name,
                    trim(leading '_' from tp.typname) as type
                from pg_catalog.pg_attribute as attr
                join pg_catalog.pg_class as cls on cls.oid = attr.attrelid
                join pg_catalog.pg_namespace as ns on ns.oid = cls.relnamespace
                join pg_catalog.pg_type as tp on tp.typelem = attr.atttypid
                where 
                    ns.nspname = '{}' and
                    cls.relname = '{}' and 
                    not attr.attisdropped and 
                    cast(tp.typanalyze as text) = 'array_typanalyze' and 
                    attr.attnum > 0
                order by 
                    attr.attnum
                """.format(self.table_schema,self.table_name)

        with self.conn.cursor() as cursor: 
            cursor.execute(stmt)
            fields = cursor.fetchall()
        # RealDictRows don't accept normal key removals like .pop for whatever reason
        # only removal by index number works.
        # gdb_geomattr_data is a postgis specific column added automatically by arc programs
        # we don't need to worry about this field so we should remove it.
        # docs: https://support.esri.com/en/technical-article/000001196
        for i,field in enumerate(fields):
            if field[0] == 'gdb_geomattr_data':
                del fields[i]
        return fields
@property
def database_object_type(self):
    """returns whether the object is a table, view, or materialized view using pg_class
    to figure out the type of object we're interacting with.
    docs: https://www.postgresql.org/docs/11/catalog-pg-class.html
    Right now we want to know whether its a table, view, or materialized view. Other
    things shouldn't be getting passed and we'll raise an exception if they are.
    """
    if self._database_object_type:
        return self._database_object_type
    type_map = {
        'r': 'table','i': 'index','S': 'sequence','t': 'TOAST_table','v': 'view', 'm': 'materialized_view',
        'c': 'composite_type','f': 'foreign_table','p': 'partitioned_table','I': 'partitioned_index'
    }
    stmt = f"""
        SELECT relkind FROM pg_class
        JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
        WHERE relname='{self.table_name}'
        """
    # temporary tables will come in with a None schema
    if self.table_schema:
        stmt += f"AND n.nspname='{self.table_schema}'"
    stmt += ';'
    with self.conn.cursor() as cursor: 
        cursor.execute(stmt)
        res = cursor.fetchone()
        if not res:
            raise AssertionError(f'Table doesnt appear to exist?: {self.table_schema}.{self.table_name}')
        relkind = res[0]
    if type_map[relkind] in ['table', 'materialized_view', 'view']:
        self._database_object_type = type_map[relkind]
        print('Database object type: {}.'.format(self._database_object_type))
        return self._database_object_type
    else:
        raise TypeError("""This database object is unsupported at this time.
        database object passed to us looks like a '{}'""".format(type_map[relkind]))

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
    valid_geom_types = ['point', 'linestring', 'polygon', 'multipoint', 'multilinestring', 'multipolygon']
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
    if len(self._geom_type.split(' ')) > 1 or self._geom_type.lower() not in valid_geom_types:
        raise Exception(f'Bad geometry type! Please fix this tables geom type! Got: {self._geom_type}')
