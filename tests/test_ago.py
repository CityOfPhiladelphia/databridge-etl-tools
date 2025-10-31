import pytest

from .constants import S3_BUCKET
from databridge_etl_tools.ago.ago import AGO

@pytest.fixture
def ago_point(ago_user, ago_password):
    ago_point_client = AGO(
        ago_org_url='https://phl.maps.arcgis.com',
        ago_item_name='POINT_TABLE_2272',
        ago_user=ago_user,
        ago_pw=ago_password,
        s3_bucket=S3_BUCKET,
        s3_key='staging/test/point_table_2272.csv',
        in_srid=2272
    )
    return ago_point_client

def test_ago_point_truncate_append(ago_point):
    ago_point.get_csv_from_s3()
    ago_point.append(truncate=True)
    ago_point.verify_count()


@pytest.fixture
def ago_multipolygon(ago_user, ago_password):
    ago_multi_client = AGO(
        ago_org_url='https://phl.maps.arcgis.com',
        ago_item_name='MULTIPOLYGON_TABLE_2272',
        ago_user=ago_user,
        ago_pw=ago_password,
        s3_bucket=S3_BUCKET,
        s3_key='staging/test/multipolygon_table_2272.csv',
        in_srid=2272
    )
    return ago_multi_client

def test_ago_multipolygon_truncate_append(ago_multipolygon):
    ago_multipolygon.get_csv_from_s3()
    ago_multipolygon.append(truncate=True)
    ago_multipolygon.verify_count()


@pytest.fixture
def ago_post_index(ago_user, ago_password):
    ago_post_index_client = AGO(
        ago_org_url='https://phl.maps.arcgis.com',
        ago_item_name='POINT_TABLE_2272',
        ago_user=ago_user,
        ago_pw=ago_password,
        s3_bucket=S3_BUCKET,
        s3_key='staging/test/multipolygon_table_2272.csv',
        index_fields='DATEFIELD,TEXTFIELD+NUMERICFIELD',
    )
    return ago_post_index_client

def test_ago_post_indexes(ago_post_index):
    ago_post_index.post_index_fields()