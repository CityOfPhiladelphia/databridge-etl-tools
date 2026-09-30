import click

from ..models.models import SharePointListArgs
from .sharepoint_list import extract_func


@click.group()
@click.pass_context
@click.option('--graphapi_tenant_id', required=True, envvar='AZURE_TENANT_ID',
              help='Tenant ID credential for initializing Microsoft GraphAPI client. Should be obtained from Keeper.')
@click.option('--graphapi_application_id', required=True, envvar='AZURE_CLIENT_ID',
              help='Application ID credential for initializing Microsoft GraphAPI client. Should be obtained from Keeper.')
@click.option('--graphapi_secret_value', required=True, envvar='AZURE_CLIENT_SECRET',
              help='Secret Value credential for initializing Microsoft GraphAPI client. Should be obtained from Keeper.')
@click.option('--hostname', required=True, envvar='SHAREPOINT_HOSTNAME',
              help='The hostname of the SharePoint site.')
@click.option('--site_name', required=True, help='Name of the Sharepoint site in which the file is located.')
@click.option('--list_name', required=True, help='Name of the Sharepoint list.')
@click.option('--s3_bucket', required=False, help='Bucket to place the extracted csv in.')
@click.option('--s3_key', required=False, help='Key under the bucket, example: "staging/dept/table_name.csv')
@click.option('--csv_path', required=False, help='Local path to save the extracted csv to - required if s3_bucket and s3_key are not provided.')
@click.option('--debug', required=False, is_flag=True)
def sharepoint_list(ctx, **kwargs):
    """Run ETL commands for Sharepoint Lists"""
    args = SharePointListArgs(**kwargs)

    if not (args.s3_bucket and args.s3_key or args.csv_path):
        raise click.UsageError("Either --s3_bucket and s3_key or --csv_path must be provided.")
    if args.s3_bucket and args.s3_key and args.csv_path:
        raise click.UsageError("--s3_key and --s3_bucket and --csv_path cannot be used together.")

    ctx.obj = args

@sharepoint_list.command()
@click.pass_context
def extract(ctx):
    extract_func(ctx.obj)