import pytest 
from databridge_etl_tools.sharepoint.sharepoint import Sharepoint
from .constants import S3_BUCKET

@pytest.fixture
def sp_csv(graphapi_tenant_id,
           graphapi_application_id,
           graphapi_secret_value):
    sharepoint_object = Sharepoint(
        graphapi_tenant_id=graphapi_tenant_id,
        graphapi_application_id=graphapi_application_id,
        graphapi_secret_value=graphapi_secret_value,
        site_name="ps360-metrics-share",
        file_path="etl_tools_test_sheet.csv"
        s3_bucket=S3_BUCKET,
        s3_key="staging/test/sharepoint_csv_test.csv"
    )
    return sharepoint_object

@pytest.fixture
def sp_xlsx(graphapi_tenant_id,
           graphapi_application_id,
           graphapi_secret_value):
    sharepoint_object = Sharepoint(
        graphapi_tenant_id=graphapi_tenant_id,
        graphapi_application_id=graphapi_application_id,
        graphapi_secret_value=graphapi_secret_value,
        site_name="ps360-metrics-share",
        file_path="etl_tools_test_workbook.xlsx",
        s3_bucket=S3_BUCKET,
        s3_key="staging/test/sharepoint_xlsx_test.csv"
        sheet_name="Dataset"
    )
    return sharepoint_object

# TODO: define tests:
    # - correct csv file name
    # - correct xlsx file name and sheet name
    # - correct xlsx file name, missing sheet name
    # - correct xlsx file name, incorrect sheet name
    # - preservation of headers
    # - nonexistent/invalid file path
    # - test that a csv with specific data loads the same info to S3 as a xlsx sheet with that same data