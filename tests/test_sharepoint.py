import pytest 
from .constants import S3_BUCKET
from databridge_etl_tools.sharepoint.sharepoint import Sharepoint

@pytest.fixture(scope='module')
def sharepoint_csv(graphapi_tenant_id,
           graphapi_application_id,
           graphapi_secret_value):
    sharepoint_csv_object = Sharepoint(
        graphapi_tenant_id=graphapi_tenant_id,
        graphapi_application_id=graphapi_application_id,
        graphapi_secret_value=graphapi_secret_value,
        site_name="ps360-metrics-share",
        file_path="etl_tools_test_sheet.csv",
        s3_bucket=S3_BUCKET,
        s3_key="staging/test/sharepoint_csv_test.csv",
        csv_path='/tmp/output.csv',
        debug=True
        )
    return sharepoint_csv_object

def test_sharepoint_csv_extract(sharepoint_csv):
    sharepoint_csv.extract()

@pytest.fixture(scope='module')
def sharepoint_xlsx(graphapi_tenant_id,
           graphapi_application_id,
           graphapi_secret_value):
    sharepoint_xlsx_object = Sharepoint(
        graphapi_tenant_id=graphapi_tenant_id,
        graphapi_application_id=graphapi_application_id,
        graphapi_secret_value=graphapi_secret_value,
        site_name="ps360-metrics-share",
        file_path="etl_tools_test_workbook.xlsx",
        s3_bucket=S3_BUCKET,
        s3_key="staging/test/sharepoint_xlsx_test.csv",
        sheet_name="Dataset",
        csv_path='/tmp/output.csv',
        debug=True
    )
    return sharepoint_xlsx_object

def test_sharepoint_xlsx_extract(sharepoint_xlsx):
    sharepoint_xlsx.extract()

# TODO: more tests:
    # - correct xlsx file name, missing sheet name
    # - correct xlsx file name, incorrect sheet name
    # - preservation of headers
    # - nonexistent/invalid file path
    # - test that a csv with specific data loads the same info to S3 as a xlsx sheet with that same data
    # (extract both, load both into memory, assert they are equal)