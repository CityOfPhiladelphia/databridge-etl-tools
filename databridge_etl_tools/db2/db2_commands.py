from .db2 import Db2
from .. import utils
import click

@click.group()
@click.pass_context
@click.option('--table_name', required=True)
@click.option('--account_name', required=True)
@click.option('--enterprise_schema', required=True, help='The destination schema to copy the table to')
@click.option('--copy_from_source_schema', required=True, help='The schema to copy the table from')
@click.option('--libpq_conn_string', required=True, help='Connection string for the database')
@click.option('--index_fields', required=False, help='Optionally specify the fields to index')
@click.option('--timeout', required=False, default=50, help='Optional timeout for our SQL statements to finish, in minutes')
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

@db2.command()
@click.pass_context
@click.option('--to_srid', required=True, show_default=True, help='''Reproject to a new table with this SRID''')
@click.option('--xshift', default=-0.20, required=False, help='Shift the x coordinates (East/West) by this amount of cm, default amounts are used exclusively for reprojecting 2272 to 3857')
@click.option('--yshift', default=+1.18, required=False, help='Shift the y coordinates (North/South) by this amount of cm, default amounts are used exclusively for reprojecting 2272 to 3857')
def reproject_shapes(ctx, **kwargs):
    """Reproject the table to a different spatial reference system, right now only supports reprojecting to 3857."""
    db2 = Db2(**ctx.obj, **kwargs)
    db2.reproject_shapes()
