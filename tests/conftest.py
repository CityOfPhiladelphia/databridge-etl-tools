'''pytest makes all the fixtures in this file available to all other test files without having to import them.'''
import pytest
import os

from moto import mock_aws
import boto3

from .constants import (
    S3_BUCKET,
    POINT_JSON_SCHEMA, POLYGON_JSON_SCHEMA, 
    POINT_TABLE_2272_CSV, POINT_TABLE_2272_S3_KEY_CSV, 
    POLYGON_CSV, FIXTURES_DIR, STAGING_DIR
)

# Makes it so output doesn't get truncated
from _pytest.assertion import truncate
truncate.DEFAULT_MAX_LINES = 9999
truncate.DEFAULT_MAX_CHARS = 9999

# Necessary for our tests to access the parameters/args as specified
# Fixtures are just functions that return objects that can be used by
# multiple tests
# in conftest.py
@pytest.fixture(scope='session')
def user(pytestconfig):
    return os.environ.get("DB_USER")
@pytest.fixture(scope='session')
def host(pytestconfig):
    return os.environ.get("DB_HOST")
@pytest.fixture(scope='session')
def password(pytestconfig):
    return os.environ.get("DB_PASSWORD")
@pytest.fixture(scope='session')
def database(pytestconfig):
    return os.environ.get("DATABASE")

@pytest.fixture(scope='session')
def ago_user(pytestconfig):
    return os.environ.get("AGO_USER")
@pytest.fixture(scope='session')
def ago_password(pytestconfig):
    return os.environ.get("AGO_PASSWORD")

@pytest.fixture(scope='session')
def carto_user(pytestconfig):
    return os.environ.get("CARTO_USER")
@pytest.fixture(scope='session')
def carto_password(pytestconfig):
    return os.environ.get("CARTO_PASSWORD")

@pytest.fixture(scope='session')
def graphapi_tenant_id(pytestconfig):
    return os.environ.get("GRAPHAPI_TENANT_ID")
@pytest.fixture(scope='session')
def graphapi_application_id(pytestconfig):
    return os.environ.get("GRAPHAPI_APPLICATION_ID")
@pytest.fixture(scope='session')
def graphapi_secret_value(pytestconfig):
    return os.environ.get("GRAPHAPI_SECRET_VALUE")


@pytest.fixture(scope='session')
def s3_client():
    return boto3.client('s3')
    
@pytest.fixture(scope='session')
def s3_bucket(s3_client):
    s3_client.create_bucket(Bucket=S3_BUCKET)
    return s3_client

@pytest.fixture(scope='session')
def s3_point_schema(s3_bucket):
    with open(os.path.join(FIXTURES_DIR, SCHEMA_DIR, POINT_JSON_SCHEMA)) as f:
        s3_bucket.put_object(Bucket=S3_BUCKET, Key=POINT_JSON_SCHEMA, Body=f.read())
    return s3_bucket

@pytest.fixture(scope='session')
def s3_polygon_schema(s3_bucket):
    with open(os.path.join(FIXTURES_DIR, SCHEMA_DIR, POLYGON_JSON_SCHEMA)) as f:
        s3_bucket.put_object(Bucket=S3_BUCKET, Key=POLYGON_JSON_SCHEMA, Body=f.read())
    return s3_bucket

@pytest.fixture(scope='session')
def s3_point_csv(s3_bucket):
    with open(os.path.join(FIXTURES_DIR, STAGING_DIR, POINT_TABLE_2272_CSV)) as f:
        s3_bucket.put_object(Bucket=S3_BUCKET, Key=POINT_TABLE_2272_S3_KEY_CSV, Body=f.read())
    print('Wrote CSV to S3\n')
    return s3_bucket

@pytest.fixture(scope='session')
def s3_polygon_csv(s3_bucket):
    with open(os.path.join(FIXTURES_DIR, STAGING_DIR, POLYGON_CSV)) as f:
        s3_bucket.put_object(Bucket=S3_BUCKET, Key=POLYGON_CSV, Body=f.read())
    return s3_bucket
