import logging 
import sys
import citygeo_secrets as cgs
from azure.identity import ClientSecretCredential
from msgraph_beta import GraphServiceClient
from datetime import datetime, date
import asyncio
import re
import boto3
from io import BytesIO
from openpyxl import load_workbook
import csv
import sys
import httpx

class Sharepoint():
    """Extracts a CSV from a .csv or .xlsx file in Sharepoint."""
    def __init__(self, 
                 graphapi_secret_name,
                 site_name,
                 file_path,
                 s3_bucket,
                 s3_key,
                 **kwargs):
        #TODO: consider whether to make SharepointClient its own object
        self.graphapi_secret_name = graphapi_secret_name
        self.client, self.access_token, self.remaining = \
            cgs.connect_with_secrets(self.get_client, self.graphapi_secret_name)
        self.site_name = site_name
        self.file_path = file_path
        self.file_extension = re.findall(r"(?<=\.)\w+$", self.file_path)[0]
        self.sheet_name = kwargs.get('sheet_name', None)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.csv_path = '/tmp/output.csv'
        # TODO: enable renaming fields
        # self.rename_fields = kwargs.get('rename_fields', None)
    
    def get_client(self, creds: dict) -> tuple[GraphServiceClient, str, int]:
        """Create a GraphAPI Client. Also returns the number of days for which 
        the secret is still valid (needs to be renewed every 365 days)"""
        creds_app = creds[self.graphapi_secret_name]
        credentials = ClientSecretCredential(
            # These field names might need to be updated if secret changes in Keeper.
            tenant_id=creds_app["Tenant ID"],
            client_id=creds_app["Application ID"],
            client_secret=creds_app["Secret Value"]
        )
        token = credentials.get_token("https://graph.microsoft.com/.default")
        access_token = token.token
        SCOPES = ['https://graph.microsoft.com/.default']
        client = GraphServiceClient(credentials=credentials, scopes=SCOPES)
        EXPIRATION_DATE_FORMAT = "%m/%d/%Y"
        expiration_date = datetime.strptime(creds_app['Secret Expiration'], EXPIRATION_DATE_FORMAT).date()
        remaining = expiration_date - date.today()

        return client, access_token, remaining

    async def get_raw_bytes(self, url):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/octet-stream"
        }
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
        return response.content

    async def get_sharepoint_content(self):
        GRAPHAPI_URL_START = "https://graph.microsoft.com/v1.0/sites"
        DOMAIN = "phila.sharepoint.com"
        site_url = f"{GRAPHAPI_URL_START}/{DOMAIN}:/sites/{self.site_name}"
        site_collection_response = await self.client.sites_with_url(site_url).get()
        # actual data is contained in returned JSOn as an attribute
        site = site_collection_response.additional_data
        site_id = site["id"]

        drive_url = f"{GRAPHAPI_URL_START}/{site_id}/drive"
        drive_collection_response = await self.client.sites.with_url(drive_url).get()
        drive_id = drive_collection_response.additional_data["id"]
        file_content_url = f"{drive_url}s/{drive_id}/root:/{self.file_path}:/content"
        content = await self.get_raw_bytes(file_content_url)
        return content 

    def write_to_temp(self, content):
        # TODO: make sure headers are handled properly
        if self.file_extension == "csv":
            with open(self.csv_path, "wb") as f:
                f.write(content)
        elif self.file_extension == "xlsx":
            content_stream = BytesIO(content)
            workbook = load_workbook(content_stream, read_only=True)
            if self.sheet_name is not None:
                try:
                    sheet = workbook[self.sheet_name]
                except:
                    raise KeyError(f"No sehet named '{self.sheet_name} in {self.file_path}")
                with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
                    # openpyxl only provides row-by-row access to sheet data
                    for row in sheet.iter_rows(values_only=True):
                        csv.writer(f).writerow(row)
        else:
            raise Exception(f"Sharepoint file should be of type .csv or .xlsx; given {self.file_extension}")

    def load_to_s3(self):
        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).put(Body=open(self.csv_path, 'rb'))

    def extract(self):
        content = asyncio.run(self.get_sharepoint_content())
        self.write_to_temp(content)
        self.load_to_s3()

        # TODO: consider whether to use logger instead
        if self.remaining.days < 30: 
            print('GraphAPI App for Sharepoint access is expiring soon!')
            print(f'1. Communicate with OIT Platform Engineering to create a new certificate for "{GRAPH_APP}"')
            print('2. Update "password" field of password manager record')
            print('3. Update "Expiration Date" of password manager record\n')
            print('Returning non-zero exit code to ensure notification; script otherwise completed successfully')
            sys.exit(1)

    @property
    def logger(self):
        if self._logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
            sh = logging.StreamHandler(sys.stdout)
            logger.addHandler(sh)
            self._logger = logger
        return self._logger