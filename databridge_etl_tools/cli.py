import click

from .oracle import Oracle
from .carto_ import Carto
from .postgres import Postgres


@click.group()
def main():
    pass

@main.command()
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--s3_key')
def extract(table_name, table_schema, connection_string, s3_bucket, s3_key):
    oracle = Oracle(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        s3_key=s3_key)
    oracle.extract()

@main.command()
@click.option('--table_name')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--json_schema_s3_key')
@click.option('--csv_s3_key')
@click.option('--select_users')
@click.option('--index_fields')
def cartoupdate(table_name, 
                connection_string, 
                s3_bucket, 
                json_schema_s3_key, 
                csv_s3_key, 
                select_users,
                index_fields):
    carto = Carto(
        table_name=table_name,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        json_schema_s3_key=json_schema_s3_key,
        csv_s3_key=csv_s3_key,
        select_users=select_users,
        index_fields=index_fields)
    carto.run_workflow()

@main.command()
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--json_schema_s3_key')
@click.option('--csv_s3_key')
def load(table_name, 
         table_schema, 
         connection_string, 
         s3_bucket, 
         json_schema_s3_key, 
         csv_s3_key):
    postgres = Postgres(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        json_schema_s3_key=json_schema_s3_key,
        csv_s3_key=csv_s3_key)
    postgres.run_workflow()

if __name__ == '__main__':
    main()