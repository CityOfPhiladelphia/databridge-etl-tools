import click

from .ago.ago_commands import ago
from .airtable.airtable_commands import airtable
from .ais_geocoder.ais_geocoder_commands import ais_geocoder
from .carto.carto_commands import carto
from .db2.db2_commands import db2
from .knack.knack_commands import knack
from .opendata.opendata_commands import opendata
from .postgres.postgres_commands import postgres
from .sharepoint.sharepoint_commands import sharepoint
from .sharepoint.sharepoint_list_commands import sharepoint_list


@click.group()
def main():
    pass

main.add_command(ago)
main.add_command(carto)
main.add_command(db2)
main.add_command(opendata)
main.add_command(postgres)
main.add_command(knack)
main.add_command(airtable)
main.add_command(ais_geocoder)
main.add_command(sharepoint)
main.add_command(sharepoint_list)