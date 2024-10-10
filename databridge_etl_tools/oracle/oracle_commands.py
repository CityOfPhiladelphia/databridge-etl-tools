from .oracle import Oracle
from .. import utils
import click

@click.group()
@click.pass_context
@click.option('--table_name', required=True)
@click.option('--table_schema', required=True)
@click.option('--connection_string', required=True)
@click.option('--s3_bucket', required=False)
@click.option('--s3_key', required=False)
def oracle(ctx, **kwargs):
    '''Run ETL commands for Oracle'''
    ctx.obj = Oracle(**kwargs)

@oracle.command()
@click.pass_context
def extract(ctx): 
    """Extracts a dataset in Oracle into a CSV file in S3"""
    ctx.obj.extract()

@oracle.command()
@click.pass_context
def extract_json_schema(ctx): 
    """Extracts a dataset's schema in Oracle into a JSON file in S3"""
    ctx.obj.load_json_schema_to_s3()

@oracle.command()
@click.pass_context
def append(ctx,): 
    """Appends CSV file from S3 into an Oracle table"""
    ctx.obj.append()
    
@oracle.command()
@click.pass_context
def load(ctx,): 
    """Loads a CSV file from S3 into a temp Oracle table and overwrites a final table in one transaction (should have no downtime)"""
    ctx.obj.load()