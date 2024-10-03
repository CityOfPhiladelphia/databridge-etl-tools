from .db2 import Db2
from .. import utils
import click

@click.group()
@click.pass_context
@click.option('--table_name', required=True)
@click.option('--account_name', required=True)
@click.option('--enterprise_schema', required=True)
@click.option('--copy_from_source_schema', required=True)
@click.option('--libpq_conn_string', required=True)
def db2(ctx, **kwargs):
    '''Run ETL commands for DB2'''
    ctx.obj = {}
    ctx = utils.pass_params_to_ctx(ctx, **kwargs)

@db2.command()
@click.pass_context
def copy_dept_to_enterprise(ctx, **kwargs):
    """Copy from the dept table directly to an enterpise able in a single transaction that can roll back if it fails."""
    db2 = Db2(**ctx.obj, **kwargs)
    db2.copy_to_enterprise()
