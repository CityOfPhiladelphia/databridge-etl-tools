from .ago import AGO
from .. import utils
import click

@click.group()
@click.pass_context
@click.option('--ago_org_url', required=True)
@click.option('--ago_user', required=True)
@click.option('--ago_pw', required=True)
@click.option('--ago_item_name', required=True, help='Must match the AGO "service name" exactly.')
@click.option('--s3_bucket', required=False)
@click.option('--s3_key', required=False)
def ago(ctx, **kwargs):
    '''Run ETL commands for AGO'''
    ctx.obj = {}
    ctx = utils.pass_params_to_ctx(ctx, **kwargs)

@ago.group()
@click.pass_context
@click.option('--in_srid', type=click.INT, default=False, required=False,
            help='The SRID of the source datasets geometry features.')
@click.option('--clean_columns', type=click.STRING, default=False, required=False,
            help='Column, or comma separated list of column names to clean of AGO invalid characters.')
@click.option('--batch_size', type=click.INT, default=500, required=False,
            help='Size of batch updates to send to AGO')
def append_group(ctx, **kwargs): 
    '''Use this group for any commands that utilize append'''
    ctx = utils.pass_params_to_ctx(ctx, **kwargs)

@append_group.command()
@click.pass_context
def append(ctx):
    """Appends records to AGO without truncating. NOTE that this is NOT an upsert 
    and will absolutely duplicate rows if you run this multiple times."""
    ago = AGO(**ctx.obj)
    ago.get_csv_from_s3()
    ago.append(truncate=False)

@append_group.command()
@click.pass_context
@click.option('--primary_key', type=click.STRING, required=True)
def upsert(ctx, **kwargs):
    """Upserts records to AGO, requires a primary key. Upserts the entire CSV
    into AGO, it does not look for changes or differences."""
    ago = AGO(**ctx.obj, **kwargs) # Combine params
    ago.get_csv_from_s3()
    ago.upsert()

@append_group.command()
@click.pass_context
def truncate_append(ctx):
    """Truncates a dataset in AGO and appends to it from a CSV. CSV needs to be made
    from the postgres-extract command."""
    ago = AGO(**ctx.obj)
    ago.get_csv_from_s3()
    ago.append(truncate=True)
    ago.verify_count()

@ago.command()
@click.pass_context
@click.option('--index_fields', type=click.STRING, required=True)
def post_index_fields(ctx, **kwargs):
    '''Post index fields to AGO'''
    ago = AGO(**ctx.obj, **kwargs)
    ago.post_index_fields()