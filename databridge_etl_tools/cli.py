import click

from .ago.ago_commands import ago
from .carto.carto_commands import carto
from .oracle.oracle_commands import oracle
from .db2.db2_commands import db2
from .opendata.opendata_commands import opendata
from .postgres.postgres_commands import postgres

@click.group()
def main():
    pass

main.add_command(ago)
main.add_command(carto)
main.add_command(db2)
main.add_command(oracle)
main.add_command(opendata)
main.add_command(postgres)
