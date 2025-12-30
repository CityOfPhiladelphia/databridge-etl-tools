from .postgres import Postgres, Postgres_Connector as Connector
from .. import utils
import click

# If debugging, in any command feel free to use
# for x in ctx.obj: 
#     print(f'{x}: {ctx.obj[x]}')

@click.group()
@click.pass_context
@click.option('--connection_string', required=True)
@click.option('--table_name', required=True)
@click.option('--table_schema', required=True)
@click.option('--s3_bucket', required=False)
@click.option('--s3_key', required=False)
@click.option('--local_csv_path', default='/tmp/output.csv', required=False)
def postgres(ctx, **kwargs):
    '''Run ETL commands for Postgres'''
    ctx.obj = {}
    ctx = utils.pass_params_to_ctx(ctx, **kwargs) # kwargs contains the click options and their values
    
    # Open Postgres db connection for a subcommand - equivalent to `with Connector(...) as connector: `
    # See https://click.palletsprojects.com/en/8.1.x/advanced/#managing-resources
    ctx.obj['connector'] = ctx.with_resource(Connector(**ctx.obj)) 

@postgres.command()
@click.pass_context
@click.option('--exclude_fields', required=False, help='Comma-separated list of fields to exclude from the extraction.')
@click.option('--with_srid', default=True, required=False, show_default=True, 
        help='''Likely only needed for certain views. This
        controls whether the geopetl frompostgis() function exports with geom_with_srid. That wont work
        for some views so just export without.''')
def extract(ctx, **kwargs):
    """Extracts data from a postgres table into a CSV file in S3. Has spatial and SRID detection
    and will output it in a way that the ago append commands will recognize."""
    with Postgres(**ctx.obj, **kwargs) as postgres: # Using db connection already made, make a Postgres table object
        postgres.extract()

@postgres.command()
@click.pass_context
def extract_json_schema(ctx):
    """Extracts a dataset's schema in Postgres into a JSON file in S3"""
    with Postgres(**ctx.obj) as postgres: 
        postgres.load_json_schema_to_s3()
    
@postgres.command()
@click.pass_context
@click.option('--truncate_before_load', is_flag=True, required=False, help='Optionally truncate table before loading.')
@click.option('--column_mappings', required=False, help='''
    A string that can be read as a dictionary using `ast.literal_eval()`. It should 
    take the form "{'data_col': 'db_table_col', 'data_col2': 'db_table_col2', ...}"''')
@click.option('--mappings_file', required=False, help='''
    A text file that can be opened with `open()` and that contains one Python dict
    that can be read with `ast.literal_eval()`. The file should take the form 
    {"data_col": "db_table_col", "data_col2": "db_table_col2", ... }. Note no quotes 
    around the curly braces `{}`.
''')
@click.option('--create_table', is_flag=True, required=False, help='Optionally try to create the table before loading.')
def load(ctx, **kwargs):
    """
    Prepare and COPY a CSV from S3 to a Postgres table. The keyword arguments 
    "column_mappings" or "mappings_file" can be used to map data file columns to 
    database table colums with different names. 
    
    Only one of column_mappings or mappings_file should be provided. Note that 
    only the columns whose headers differ between the data file and the database 
    table need to be included. All column names must be quoted. """
    with Postgres(**ctx.obj) as postgres: 
        postgres.load(**kwargs)

@postgres.command()
@click.option('--column_mappings', required=False, help='''
    A string that can be read as a dictionary using `ast.literal_eval()`. It should 
    take the form "{'data_col': 'db_table_col', 'data_col2': 'db_table_col2', ...}"''')
@click.option('--mappings_file', required=False, help='''
    A text file that can be opened with `open()` and that contains one Python dict
    that can be read with `ast.literal_eval()`. The file should take the form 
    {"data_col": "db_table_col", "data_col2": "db_table_col2", ... }. Note no quotes 
    around the curly braces `{}`.
''')
@click.option('--delete_stale', required=False, type=bool, help='''
    If True/t/yes, etc., delete rows from PROD table that do not appear in the STAGING table 
    used for upserting. ''')
@click.pass_context
def upsert_csv(ctx, **kwargs): 
    '''Upserts data from a CSV to a Postgres table, which must have at least one primary key.  
    The keyword arguments "column_mappings" or "mappings_file" can be used to map 
    data file columns to database table colums with different names. 
    
    Only one of column_mappings or mappings_file should be provided. Note that 
    only the columns whose headers differ between the data file and the database 
    table need to be included. All column names must be quoted. 
    '''
    with Postgres(**ctx.obj) as postgres: 
        postgres.upsert('csv', **kwargs)

@click.option('--staging_table', required=True, help='''Name of Postgres table or view to upsert from ''')
@click.option('--staging_schema', required=False, help='''Schema of Postgres table 
    to upsert from. If None, assume the same schema as the table being upserted to''')
@click.option('--column_mappings', required=False, help='''
    A string that can be read as a dictionary using `ast.literal_eval()`. It should 
    take the form "{'data_col': 'db_table_col', 'data_col2': 'db_table_col2', ...}"''')
@click.option('--mappings_file', required=False, help='''
    A text file that can be opened with `open()` and that contains one Python dict
    that can be read with `ast.literal_eval()`. The file should take the form 
    {"data_col": "db_table_col", "data_col2": "db_table_col2", ... }. Note no quotes 
    around the curly braces `{}`.''')
@click.option('--delete_stale', required=False, type=bool, help='''
    If True/t/yes, etc., delete rows from PROD table that do not appear in the STAGING table 
    used for upserting. ''')
@postgres.command()
@click.pass_context
def upsert_table(ctx, **kwargs): 
    '''Upserts data from a Postgres table or view to a Postgres table in the same database, 
    which must have at least one primary key. The keyword arguments 
    "column_mappings" or "mappings_file" can be used to map data file columns to 
    database table colums with different names. 
    
    Only one of column_mappings or mappings_file should be provided. Note that 
    only the columns whose headers differ between the data file and the database 
    table need to be included. All column names must be quoted. 
    '''
    with Postgres(**ctx.obj) as postgres: 
        postgres.upsert('table', **kwargs)
