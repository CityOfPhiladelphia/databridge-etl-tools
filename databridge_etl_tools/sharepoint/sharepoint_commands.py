from .sharepoint import Sharepoint
from .. import utils 
import click

@click.group()
@click.pass_context
@click.option('--graphapi_tenant_id', required=True, help='Tenant ID credential for initializing Microsoft GraphAPI client. Should be obtained from Keeper.')
@click.option('--graphapi_application_id', required=True, help='Application ID credential for initializing Microsoft GraphAPI client. Should be obtained from Keeper.')
@click.option('--graphapi_secret_value', required=True, help='Secret Value credential for initializing Microsoft GraphAPI client. Should be obtained from Keeper.')
@click.option('--site_name', required=True, help='Name of the Sharepoint site in which the file is located.')
@click.option('--file_path', required=True, help='File path all the way down to the desired file from top of Sharepoint folder.')
@click.option('--s3_bucket', required=True, help='Bucket to place the extracted csv in.')
@click.option('--s3_key', required=True, help='key under the bucket, example: "staging/dept/table_name.csv')
@click.option('--sheet_name', required=False, help='Name of specified sheet to extract as csv if the Sharepoint file is an xlsx workbook.')
@click.option('--debug', required=False, is_flag=True)
def sharepoint(ctx, **kwargs):
    """Run ETL commands for Sharepoint"""
    ctx.obj = Sharepoint(**kwargs)

@sharepoint.command()
@click.pass_context
def extract(ctx):
    ctx.obj.extract()