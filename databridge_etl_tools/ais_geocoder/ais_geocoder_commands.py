from .ais_geocoder import AIS_Geocoder
from .. import utils
import click


@click.group()
@click.pass_context
@click.option('--s3-bucket', help='Working S3 bucket')
@click.option('--s3-input-key', help='Input CSV file in S3')
@click.option('--s3-output-key', help='Output CSV file in S3')
@click.option('--ais-url', help='Base URL for the AIS service')
@click.option('--ais-key', help='AIS Gatekeeper authentication token')
@click.option('--ais-user', help='Any string indicating the user or usage. This is used for usage and security analysis')
@click.option('--query-fields', help='Fields to query AIS with, comma separated. They are concatenated in order.')
@click.option('--ais-fields', help='AIS fields to include in the output, comma separated.')
@click.option('--remove-fields', help='Fields to remove post AIS query, comma separated.')
@click.option('--srid', default=4326, type=int, help='SRID of geom to request.')
def ais_geocoder(ctx, **kwargs):
    "Run geocoding or grabs additional fields from AIS"
    ctx.obj = AIS_Geocoder(**kwargs)

@ais_geocoder.command()
@click.pass_context
def geocode(ctx): 
    ctx.obj.ais_inner_geocode()