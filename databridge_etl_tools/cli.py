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
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--select_users')
def cartoupdate(table_name, table_schema, connection_string, s3_bucket, select_users):
    carto = Carto(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        select_users=select_users)
    carto.run_workflow()

@main.command()
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
def load(table_name, table_schema, connection_string, s3_bucket):
    postgres = Postgres(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket)
    postgres.run_workflow()

if __name__ == '__main__':
    main()