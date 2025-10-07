import pytest
import os, textwrap
import petl as etl
from databridge_etl_tools.postgres.postgres import Postgres, Postgres_Connector
from databridge_etl_tools.db2.db2 import Db2
from .constants import (S3_BUCKET, POINT_TABLE_2272_S3_KEY_CSV, 
                        POINT_TABLE_2272_NAME, FIXTURES_DIR, STAGING_DIR, 
                        POINT_TABLE_2272_CSV)


@pytest.fixture(scope='module', autouse=True) # Use this fixture without anything calling it
def write_to_s3(s3_point_csv):
    '''Write the CSV contained in this package to S3 by calling fixture in conftest.py'''
    pass

@pytest.fixture(scope='module') # Only create a connection once for all tests below
def connector(user, password, host, database): # These parameters are defined in conftest.py
    '''Yield a Postgres Connector object'''
    with Postgres_Connector(connection_string=f'postgresql://{user}:{password}@{host}:5432/{database}') as connector_obj: 
        yield connector_obj

@pytest.fixture(scope='module') # Only CREATE TABLE once for all tests below
def create_table(connector): 
    '''CREATE TABLE IF NOT EXISTS'''
    with connector.conn.cursor() as cursor: 
        cursor.execute(f'''
    CREATE TABLE if not exists test_dbtools_point_2272 (
        objectid int4 NOT NULL,
        textfield varchar(255) NULL,
        datefield timestamp NULL,
        numericfield numeric(38, 8) NULL,
        timezone timestamp NULL,
        "timestamp" timestamp NULL,
        newcol varchar(20) NULL,
        shape public.geometry NULL,
        CONSTRAINT enforce_srid_shape CHECK ((st_srid(shape) = 2272)),
        CONSTRAINT test_dbtools_point_2272_pk PRIMARY KEY (objectid)
    );
    CREATE INDEX if not exists a298_ix1 ON test_dbtools_point_2272 USING gist (shape);
    CREATE UNIQUE INDEX if not exists r496_sde_rowid_uk ON test_dbtools_point_2272 USING btree (objectid) WITH (fillfactor='75');
        ''')
        print('Created table "test_dbtools_point_2272"\n')

@pytest.fixture(scope='module')
def append_to_table(create_table, connector): # Only COPY data to table once for all tests below
    '''Delete any existing data in test_dbtools_point_2272 and append test data'''
    with connector.conn.cursor() as cursor:
        cursor.execute('DELETE FROM test_dbtools_point_2272;')     
        with open(os.path.join(FIXTURES_DIR, STAGING_DIR, POINT_TABLE_2272_CSV)) as f:
            stmt = 'COPY test_dbtools_point_2272 FROM STDIN WITH (FORMAT csv, HEADER true)'
            cursor.copy_expert(stmt, f)
    connector.conn.commit()
    print('Appended test data to table test_dbtools_point_2272\n')

@pytest.fixture(scope='module')
def pg(connector): 
    '''Yield a Postgres Table object'''
    with Postgres(connector=connector,
                  table_name=POINT_TABLE_2272_NAME,
                  table_schema='citygeo',
                  s3_bucket=S3_BUCKET,
                  s3_key=POINT_TABLE_2272_S3_KEY_CSV, 
                  with_srid=True) as pg_obj:
        yield pg_obj        

@pytest.fixture(scope='module')
def extract_data(append_to_table, pg):
    return pg.extract(return_data=True)

def assert_two_datasets_same(rows1, rows2): 
    '''Check for added or subtracted rows between two datasets using PETL
    
    #### Raises
    * assertion error - If the number of added or subtracted rows != 0
    '''
    
    added, subtracted = etl.recorddiff(rows1, rows2)
    added_nrows = etl.nrows(added)
    subtracted_nrows = etl.nrows(subtracted)
    
    assert added_nrows == 0 and subtracted_nrows == 0, textwrap.dedent(f'''
        Added rows ({added_nrows}) and/or deleted rows ({subtracted_nrows}) are not zero!''')

def test_postgres_upsert(extract_data, pg):
    pg.upsert('csv')
    upserted_data = pg.extract(return_data=True)
    assert_two_datasets_same(extract_data, upserted_data)

def test_postgres_load(extract_data, pg):
    pg.truncate()
    pg.load()
    loaded_data = pg.extract(return_data=True)
    assert_two_datasets_same(extract_data, loaded_data)

def test_postgres_json_schema_extract(pg):
    pg.load_json_schema_to_s3()

# Test DB2

@pytest.fixture(scope='module')
def connector_string(user, password, host, database): # These parameters are defined in conftest.py
    return f'postgresql://{user}:{password}@{host}:5432/{database}'

@pytest.fixture
def db2_client(connector_string):
    db2_interact_client = Db2(
            table_name='ghactions_test1',
            account_name='citygeo',
            copy_from_source_schema='citygeo',
            enterprise_schema='viewer_citygeo',
            libpq_conn_string=connector_string,
            index_fields='datefield,textfield+numericfield',
            to_srid='3857',
    )
    return db2_interact_client

def test_db2_copy_to_enterprise(db2_client):
    db2_client.copy_to_enterprise()

def test_db2_copy_to_enterprise(db2_client):
    db2_client.reproject_shapes()