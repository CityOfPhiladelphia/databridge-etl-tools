import os
import sys
import logging
import zipfile
import click
import petl as etl
import boto3
import botocore
import pyproj
import shapely.wkt
import numpy as np
import csv
from pprint import pprint
import pandas as pd
from copy import deepcopy
#from threading import Thread
from shapely.ops import transform as shapely_transformer
from arcgis import GIS
from arcgis.features import FeatureLayerCollection
from time import sleep, time
import dateutil.parser
import requests
import json
from datetime import datetime


class AGO():
    _logger = None
    _org = None
    _item = None
    _geometric = None
    _item_fields = None
    _layer_object = None
    _ago_srid = None
    _projection = None
    _geometric = None
    _transformer = None
    _primary_key = None
    _json_schema_s3_key = None

    def __init__(self,
                 ago_org_url,
                 ago_user,
                 ago_pw,
                 ago_item_name,
                 s3_bucket,
                 s3_key,
                 **kwargs
                 ):
        self.ago_org_url = ago_org_url
        self.ago_user = ago_user
        self.ago_password = ago_pw
        if ago_item_name.rstrip().startswith('dbt_'):
            self.logger.info('stripping "dbt_" prefix from item name..')
            self.item_name = ago_item_name.rstrip().strip('dbt_')
        else:
            self.item_name = ago_item_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        # Our org id, publicly viewable so fine to hardcode.
        self.ago_org_id = 'fLeGjb7u4uXqeF9q'
        self.index_fields = kwargs.get('index_fields', None)
        self.in_srid = kwargs.get('in_srid', None)
        self.clean_columns = kwargs.get('clean_columns', None)
        self.primary_key = kwargs.get('primary_key', None)
        self.proxy_host = kwargs.get('proxy_host', None)
        self.proxy_port = kwargs.get('proxy_port', None)
        self.export_format = kwargs.get('export_format', None)
        self.export_zipped = kwargs.get('export_zipped', False)
        self.batch_size = kwargs.get('batch_size', 500)
        self.export_dir_path = kwargs.get('export_dir_path', os.getcwd() + '\\' + self.item_name.replace(' ', '_'))
        # Try to use /tmp dir, it should exist. Else, use our current user's home dir
        if not os.path.isdir('/tmp'):
            homedir = os.path.expanduser('~')
            self.csv_path = os.path.join(homedir, f'{self.item_name}.csv')
        else:
            self.csv_path = f'/tmp/{self.item_name}.csv'
        # Global variable to inform other processes that we're upserting
        self.upserting = None
        if self.clean_columns == 'False':
            self.clean_columns = None

    @property
    def logger(self):
        if self._logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
            sh = logging.StreamHandler(sys.stdout)
            logger.addHandler(sh)
            self._logger = logger
        return self._logger


    @property
    def org(self):
        if self._org is None:
            self.logger.info(f'Making connection to AGO account at {self.ago_org_url} with user {self.ago_user} ...')
            try:
                if self.proxy_host is None:
                    self._org = GIS(self.ago_org_url,
                                    self.ago_user,
                                    self.ago_password,
                                    verify_cert=True)
                else:
                    self._org = GIS(self.ago_org_url,
                                    self.ago_user,
                                    self.ago_password,
                                    proxy_host=self.proxy_host,
                                    proxy_port=self.proxy_port,
                                    verify_cert=False)

                self.logger.info('Connected to AGO.\n')
            except Exception as e:
                self.logger.error(f'Failed making connection to AGO account at {self.ago_org_url} with user {self.ago_user} ...')
                raise e
        return self._org

    
    @property
    def item(self):
        '''Find the AGO object that we can perform actions on, sends requests to it's AGS endpoint in AGO.
        Contains lots of attributes we'll need to access throughout this script.
        Note: your item name needs to match the back-end service name of the AGO item exactly!!'''
        if self._item is None:
            try:
                # "Feature Service" seems to pull up both spatial and table items in AGO
                assert self.item_name.strip()
                search_query = f'''name:"{self.item_name}" AND type:"Feature Service"'''
                self.logger.info(f'Searching for item with query: {search_query}')
                items = self.org.content.search(search_query, outside_org=False)
                if len(items) > 1:
                    print('Got multiple items back from our search.')
                    for item in items:
                        print(f'Item service name: {item.name}, Item ID: {item.id}')
                for item in items:
                    # For items with spaces in their titles, AGO will smartly change out spaces to underscores
                    # Test for this too.
                    self.logger.info(f'Seeing if item service name is a match: "{item.name}" to "{self.item_name}"..')
                    if item.name == self.item_name:
                        self._item = item
                        self.logger.info(f'Chosen item name: {self.item.name}, id: {self.item.id}, url: {self.item.url}')
                        return self._item
                # If item is still None, then fail out
                if self._item is None:
                    raise Exception(f'Failed searching for item with search_query = {search_query}')
            except Exception as e:
                self.logger.error(f'Failed searching for item with search_query = {search_query}')
                raise e
        return self._item


    @property
    def json_schema_s3_key(self):
        if self._json_schema_s3_key is None:
            self._json_schema_s3_key = self.s3_key.replace('staging', 'schemas').replace('.csv', '.json')
        return self._json_schema_s3_key


    @property
    def item_fields(self):
        '''Dictionary of the fields and data types of the dataset in AGO'''
        if self._item_fields:
            return self._item_fields
        #fields = [i.name.lower() for i in self.layer_object.properties.fields]
        fields = {i.name.lower(): i.type.lower() for i in self.layer_object.properties.fields }
        # shape field isn't included in this property of the AGO item, so check it its geometric first
        # so we can accurately use this variables for field comparisions
        if self.geometric and 'shape' not in fields.keys():
            fields['shape'] = self.geometric
        # AGO will show these fields for lines and polygons, so remove them for an accurate comparison to the CSV headers.
        if 'shape__area' in fields:
            del fields['shape__area']
        if 'shape__length' in fields:
            del fields['shape__length']
        #fields = tuple(fields)
        self._item_fields = fields
        return self._item_fields


    @property
    def layer_object(self):
        '''Get the item object that we can operate on Can be in either "tables" or "layers"
        but either way operations on it are the same.'''
        if self._layer_object is None:
            # Necessary to "get" our item after searching for it, as the returned
            # objects don't have equivalent attributes.
            feature_layer_item = self.org.content.get(self.item.id)
            if feature_layer_item.tables:
                if feature_layer_item.tables[0]:
                    self._layer_object = feature_layer_item.tables[0]
            elif feature_layer_item.layers:
                if feature_layer_item.layers[0]:
                    self._layer_object = feature_layer_item.layers[0]
            if self._layer_object is None:
                raise AssertionError('Could not locate our feature layer/table item in returned AGO object')
        return self._layer_object


    @property
    def ago_srid(self):
        '''detect the SRID of the dataset in AGO, we'll need it for formatting the rows we'll upload to AGO.
        record both the standard SRID (latestwkid) and ESRI's made up on (wkid) into a tuple.
        so for example for our standard PA state plane one, latestWkid = 2272 and wkid = 102729
        We'll need both of these.'''
        if self._ago_srid is None:
            # Don't ask why the SRID is all the way down here..
            assert self.layer_object.container.properties.initialExtent.spatialReference is not None
            self._ago_srid = (self.layer_object.container.properties.initialExtent.spatialReference['wkid'],self.layer_object.container.properties.initialExtent.spatialReference['latestWkid'])
        return self._ago_srid


    @property
    def geometric(self):
        '''Var telling us whether the item is geometric or just a table?
        If it's geometric, var will have geom type. Otherwise it is False.'''
        if self._geometric is None:
            self.logger.info('Determining geometric?...')
            geometry_type = None
            try:
                # Note, initially wanted to use hasGeometryProperties but it seems like it doesn't
                # show up for point layers. geometryType is more reliable I think?
                #is_geometric = self.layer_object.properties.hasGeometryProperties
                geometry_type = self.layer_object.properties.geometryType
            except KeyboardInterrupt as e:
                raise e
            except Exception as e:
                self._geometric = False
            if geometry_type:
                #self._geometric = True
                self.logger.info(f'Item detected as geometric, type: {geometry_type}\n')
                self._geometric = geometry_type
            else:
                self.logger.info(f'Item is not geometric.\n')
        return self._geometric


    @property
    def projection(self):
        '''Decide if we need to project our shape field. If the SRID in AGO is set
        to what our source dataset is currently, we don't need to project.'''
        if self._projection is None:
            if str(self.in_srid) == str(self.ago_srid[1]):
                self.logger.info(f'source SRID detected as same as AGO srid, not projecting. source: {self.in_srid}, ago: {self.ago_srid[1]}\n')
                self._projection = False
            else:
                self.logger.info(f'Shapes will be projected. source: "{self.in_srid}", ago: "{self.ago_srid[1]}"\n')
                self._projection = True
        return self._projection


    def unzip(self):
        # get path to zipfile:
        zip_path = ''
        for root, subdirectories, files in os.walk(self.export_dir_path):
            for file in files:
                if '.zip' in file:
                    zip_path = os.path.join(root, file)
        # Unzip:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(self.export_dir_path)


    def overwrite(self):
        '''
        Based off docs I believe this will only work with fgdbs or sd file
        or with non-spatial CSV files: https://developers.arcgis.com/python/sample-notebooks/overwriting-feature-layers
        '''
        if self.geometric:
            raise NotImplementedError('Overwrite with CSVs only works for non-spatial datasets (maybe?)')
        #self.logger.info(vars(self.item))
        flayer_collection = FeatureLayerCollection.fromitem(self.item)
        # call the overwrite() method which can be accessed using the manager property
        flayer_collection.manager.overwrite(self.csv_path)


    def truncate(self):
        try:
            count = self.layer_object.query(return_count_only=True)
            if count > 200000:
                raise AssertionError('Count is over 200,000, we dont recommend using this method for large datasets!')
        except Exception as e:
            pass
        # This is susceptible to gateway errors, so put in a retry.
        try:
            self.layer_object.manager.truncate()
        except Exception as e:
            if 'Your request has timed out' in str(e) or '504' in str(e):
                self.logger.info('Request timed out. Checking count after sleep...')
                sleep(60)
                count = self.layer_object.query(return_count_only=True)
                if count == 0:
                    pass
                else:
                    # if count is not 0, assume it actually failed and try again after another long sleep
                    sleep(120)
                    self.layer_object.manager.truncate()
            elif '502' in str(e):
                sleep(60)
                self.layer_object.manager.truncate()
            else:
                raise e
        count = self.layer_object.query(return_count_only=True)
        self.logger.info('count after truncate: ' + str(count))
        assert count == 0


    def get_csv_from_s3(self):
        self.logger.info('Fetching csv s3://{}/{}'.format(self.s3_bucket, self.s3_key))

        s3 = boto3.resource('s3')
        try:
            s3.Object(self.s3_bucket, self.s3_key).download_file(self.csv_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise AssertionError(f'CSV file doesnt appear to exist in S3 bucket! key: {self.s3_key}')
            if 'HeadObject operation: Not Found' in str(e):
                raise AssertionError(f'CSV file doesnt appear to exist in S3 bucket! key: {self.s3_key}')
            else:
                raise e

        self.logger.info('CSV successfully downloaded.\n'.format(self.s3_bucket, self.s3_key))


    def write_errors_to_s3(self, rows):
        try:
            ts = int(time())
            file_timestamp_name = f'-{ts}-errors.txt'
            error_s3_key = self.s3_key.replace('.csv', file_timestamp_name)
            self.logger.info(f'Writing bad rows to file in s3 {error_s3_key}...')
            if not os.path.isdir('/tmp'):
                homedir = os.path.expanduser('~')
                error_filepath = os.path.join(homedir, file_timestamp_name)
            else:
                error_filepath = f'/tmp/{file_timestamp_name}'
            with open(error_filepath, 'a') as csv_file:
                for i in rows:
                    csv_file.write(str(i))
                #csv_file.write(str(rows))
                #writer = csv.writer(csv_file)
                #writer.writerows(rows)

            s3 = boto3.resource('s3')
            s3.Object(self.s3_bucket, error_s3_key).put(Body=open(error_filepath, 'rb'))
        except KeyboardInterrupt as e:
            raise e
        except Exception as e:
            self.logger.info('Failed to put errors in csv and upload to S3.')
            self.logger.info(f'Error: {str(e)}')
        os.remove(error_filepath)


    @property
    def transformer(self):
        '''transformer needs to be defined outside of our row loop to speed up projections.'''
        if self._transformer is None:
            self._transformer = pyproj.Transformer.from_crs(f'epsg:{self.in_srid}',
                                                      f'epsg:{self.ago_srid[1]}',
                                                      always_xy=True)
        return self._transformer


    def project_and_format_shape(self, wkt_shape):
        ''' Helper function to help format spatial fields properly for AGO '''
        # Note: list of coordinates for polygons are called "rings" for some reason
        def format_ring(poly):
            if self.projection:
                transformed = shapely_transformer(self.transformer.transform, poly)
                xlist = list(transformed.exterior.xy[0])
                ylist = list(transformed.exterior.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
            else:
                xlist = list(poly.exterior.xy[0])
                ylist = list(poly.exterior.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
        def format_path(line):
            if self.projection:
                transformed = shapely_transformer(self.transformer.transform, line)
                xlist = list(transformed.coords.xy[0])
                ylist = list(transformed.coords.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
            else:
                xlist = list(line.coords.xy[0])
                ylist = list(line.coords.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
        if 'POINT' in wkt_shape:
            pt = shapely.wkt.loads(wkt_shape)
            if self.projection:
                x, y = self.transformer.transform(pt.x, pt.y)
                return x, y
            else:
                return pt.x, pt.y
        elif 'MULTIPOLYGON' in wkt_shape:
            multipoly = shapely.wkt.loads(wkt_shape)
            if not multipoly.is_valid:
                self.logger.info('Warning, shapely found this WKT to be invalid! Might want to fix this!')
                self.logger.info(wkt_shape)
            list_of_rings = []
            for poly in multipoly.geoms:
                if not poly.is_valid:
                    self.logger.info('Warning, shapely found this WKT to be invalid! Might want to fix this!')
                    self.logger.info(wkt_shape)
                # reference for polygon projection: https://gis.stackexchange.com/a/328642
                ring = format_ring(poly)
                list_of_rings.append(ring)
            return list_of_rings
        elif 'POLYGON' in wkt_shape:
            poly = shapely.wkt.loads(wkt_shape)
            if not poly.is_valid:
                self.logger.info('Warning, shapely found this WKT to be invalid! Might want to fix this!')
                self.logger.info(wkt_shape)
            ring = format_ring(poly)
            return ring
        elif 'MULTILINESTRING' in wkt_shape:
            multipaths = shapely.wkt.loads(wkt_shape)
            list_of_paths = []
            for path in multipaths.geoms:
                if not path.is_valid:
                    self.logger.info('Warning, shapely found this WKT to be invalid! Might want to fix this!')
                    self.logger.info(wkt_shape)
                path = format_path(path)
                list_of_paths.append(path)
            return list_of_paths
        elif 'LINESTRING' in wkt_shape:
            path = shapely.wkt.loads(wkt_shape)
            path = format_path(path)
            return path
        else:
            raise NotImplementedError('Shape unrecognized.')


    def return_coords_only(self,wkt_shape):
        ''' Do not perform project, simply extract and return our coords lists.'''
        poly = shapely.wkt.loads(wkt_shape)
        return poly.exterior.xy[0], poly.exterior.xy[1]


    def format_row(self,row):
        # Clean our designated row of non-utf-8 characters or other undesirables that makes AGO mad.
        # If you pass multiple values separated by a comma, it will perform on multiple colmns
        if self.clean_columns and self.clean_columns != 'False':
            #self.logger.info(f'Cleaning columns of invalid characters: {self.clean_columns}')
            for clean_column in self.clean_columns.split(','):
                row[clean_column] = row[clean_column].encode("ascii", "ignore").decode()
                row[clean_column] = row[clean_column].replace('\'','')
                row[clean_column] = row[clean_column].replace('"', '')
                row[clean_column] = row[clean_column].replace('<', '')
                row[clean_column] = row[clean_column].replace('>', '')

        # Convert None values to empty string
        # but don't convert date fields to empty strings,
        # Apparently arcgis API needs a None value to properly pass a value as 'null' to ago.
        for col in row.keys():
            if not row[col]:
                row[col] = None
            # check if dates need to be converted to a datetime object. arcgis api will handle that
            # and will also take timezones that way.
            # First get it's type from ago:
            data_type = self.item_fields[col]
            # Then make sure this row isn't empty and is of a date type in AGO
            if row[col] and data_type == 'esrifieldtypedate':
                # then try parsing with dateutil parser
                try:
                    adate = dateutil.parser.parse(row[col])
                    # if parse above works, convert
                    row[col] = adate
                except KeyboardInterrupt as e:
                    raise e
                except dateutil.parser._parser.ParserError as e:
                    pass
                #if 'datetime' in col and '+0000' in row[col]:
                #    dt_obj = datetime.strptime(row[col], "%Y-%m-%d %H:%M:%S %z")
                #    local_dt_obj = obj.astimezone(pytz.timezone('US/Eastern'))
                #    row[col] = local_db_obj.strftime("%Y-%m-%d %H:%M:%S %z")

        return row


    def append(self, truncate=True):
        '''
        Appends rows from our CSV into a matching item in AGO
        '''
        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:
            self.logger.info("Exception encountered trying to import rows wtih utf-8 encoding, trying latin-1...")
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')
        # Compare headers in the csv file vs the fields in the ago item.
        # If the names don't match and we were to upload to AGO anyway, AGO will not actually do 
        # anything with our rows but won't tell us anything is wrong!
        self.logger.info(f'Comparing AGO fields: {set(self.item_fields.keys())} ')
        self.logger.info(f'To CSV fields: {set(rows.fieldnames())} ')

        # Apparently we need to compare both ways even though we're sorting them into sets
        # Otherwise we'll miss out on differences.
        row_differences1 = set(self.item_fields.keys()) - set(rows.fieldnames())
        row_differences2 = set(rows.fieldnames()) - set(self.item_fields.keys())
        
        # combine both difference subtractions with a union
        row_differences = row_differences1.union(row_differences2)

        if row_differences:
            # Ignore differences if it's just objectid.
            if 'objectid' in row_differences and len(row_differences) == 1:
                pass
            elif 'esri_oid' in row_differences and len(row_differences) == 1:
                pass
            else:
                self.logger.info(f'Row differences found!: {row_differences}')
                assert tuple(self.item_fields.keys()) == rows.fieldnames()    
        self.logger.info('Fields are the same! Continuing.')

        # Check CSV file rows match the rows we pulled in
        # We've had these not match in the past.
        self._num_rows_in_upload_file = rows.nrows()
        row_dicts = rows.dicts()

        # First we should check that we can parse geometry before proceeding with truncate
        if self.geometric:
            loop_counter = 0
            # keep looping for awhile until we get a non-blank geom value
            for i, row in enumerate(row_dicts):
                # Bomb out at 500 rows and hope our geometry is good
                if loop_counter > 500:
                    break
                loop_counter =+ 1
                wkt = row.pop('shape')

                # Set WKT to empty string so next conditional doesn't fail on a Nonetype
                if not wkt.strip():
                    continue
                if 'SRID=' not in wkt and bool(wkt.strip()) is False and (not self.in_srid):
                    raise AssertionError("Receieved a row with blank geometry, you need to pass an --in_srid so we know if we need to project!")
                if 'SRID=' not in wkt and bool(wkt.strip()) is True and (not self.in_srid):
                    raise AssertionError("SRID not found in shape row! Please export your dataset with 'geom_with_srid=True'.")
                if 'POINT' in wkt:
                    assert self.geometric == 'esriGeometryPoint'
                    break
                elif 'MULTIPOINT' in wkt:
                    raise NotImplementedError("MULTIPOINTs not implemented yet..")
                elif 'MULTIPOLYGON' in wkt:
                    assert self.geometric == 'esriGeometryPolygon'
                    break
                elif 'POLYGON' in wkt:
                    assert self.geometric == 'esriGeometryPolygon'
                    break
                elif 'LINESTRING' in wkt:
                    assert self.geometric == 'esriGeometryPolyline'
                    break
                else:
                    self.logger.info('Did not recognize geometry in our WKT. Did we extract the dataset properly?')
                    self.logger.info(f'Geometry value is: {wkt}')
                    raise AssertionError('Unexpected/unreadable geometry value')


        # We're more sure that we'll succeed after prior checks, so let's truncate here..
        if truncate is True:
            self.truncate()

        # loop through and accumulate appends into adds[]
        adds = []
        if not self.geometric:
            for i, row in enumerate(row_dicts):
                # clean up row and perform basic non-geometric transformations
                row = self.format_row(row)

                adds.append({"attributes": row})
                if (len(adds) != 0) and (len(adds) % self.batch_size == 0):
                    start = time()
                    row_count = i+1
                    self.logger.info(f'Adding batch of {len(adds)}, at row #: {row_count}...')
                    self.edit_features(rows=adds, row_count=row_count, method='adds')
                    adds = []
                    self.logger.info(f'Duration: {time() - start}\n')
            if adds:
                start = time()
                row_count = i+1
                self.logger.info(f'Adding last batch of {len(adds)}, at row #: {row_count}...')
                self.edit_features(rows=adds, row_count=row_count, method='adds')
                self.logger.info(f'Duration: {time() - start}\n')
        elif self.geometric:
            for i, row in enumerate(row_dicts):
                row_count = i + 1
                # clean up row and perform basic non-geometric transformations
                row = self.format_row(row)

                # remove the shape field so we can replace it with SHAPE with the spatial reference key
                # and also store in 'wkt' var (well known text) so we can project it
                wkt = row.pop('shape')

                # Set WKT to empty string so next conditional doesn't fail on a Nonetype
                if wkt is None:
                    wkt = ''

                # if the wkt is not empty, and SRID isn't in it, fail out.
                # empty geometries come in with some whitespace, so test truthiness
                # after stripping whitespace.
                if 'SRID=' not in wkt and bool(wkt.strip()) is False and (not self.in_srid):
                    raise AssertionError("Receieved a row with blank geometry, you need to pass an --in_srid so we know if we need to project!")
                if 'SRID=' not in wkt and bool(wkt.strip()) is True and (not self.in_srid):
                    raise AssertionError("SRID not found in shape row! Please export your dataset with 'geom_with_srid=True'.")

                if (not self.in_srid) and 'SRID=' in wkt:
                    self.logger.info('Getting SRID from csv...')
                    self.in_srid = wkt.split(';')[0].strip("SRID=")
    
                # Get just the WKT from the shape, remove SRID after we extract it
                if 'SRID=' in wkt:
                    wkt = wkt.split(';')[1]

                # If the geometry cell is blank, properly pass a NaN or empty value to indicate so.
                # Also account for values like "POINT EMPTY"
                if not (bool(wkt.strip())) or 'EMPTY' in wkt: 
                    if self.geometric == 'esriGeometryPoint':
                        geom_dict = {"x": 'NaN',
                                     "y": 'NaN',
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    elif self.geometric == 'esriGeometryPolyline':
                        geom_dict = {"paths": [],
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    elif self.geometric == 'esriGeometryPolygon':
                        geom_dict = {"rings": [],
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    else:
                        raise TypeError(f'Unexpected geomtry type!: {self.geometric}')
                # For different types we can consult this for the proper json format:
                # https://developers.arcgis.com/documentation/common-data-types/geometry-objects.htm
                # If it's not blank,
                elif bool(wkt.strip()): 
                    if 'POINT' in wkt:
                        projected_x, projected_y = self.project_and_format_shape(wkt)
                        # Format our row, following the docs on this one, see section "In [18]":
                        # https://developers.arcgis.com/python/sample-notebooks/updating-features-in-a-feature-layer/
                        # create our formatted point geometry
                        geom_dict = {"x": projected_x,
                                     "y": projected_y,
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    elif 'MULTIPOINT' in wkt:
                        raise NotImplementedError("MULTIPOINTs not implemented yet..")
                    elif 'MULTIPOLYGON' in wkt:
                        rings = self.project_and_format_shape(wkt)
                        geom_dict = {"rings": rings,
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    elif 'POLYGON' in wkt:
                        #xlist, ylist = return_coords_only(wkt)
                        ring = self.project_and_format_shape(wkt)
                        geom_dict = {"rings": [ring],
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    elif 'MULTILINESTRING' in wkt:
                        paths = self.project_and_format_shape(wkt)
                        # Don't know why yet but some bug is sending us multilines with an already enclosing list
                        # Don't enclose in list if multilinestring
                        geom_dict = {"paths": paths,
                                    "spatial_reference": {"wkid": self.ago_srid[0], "latestWkid": self.ago_srid[1]}
                                    } 
                    elif 'LINESTRING' in wkt:
                        paths = self.project_and_format_shape(wkt)
                        geom_dict = {"paths": [paths],
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    else:
                        self.logger.info('Did not recognize geometry in our WKT. Did we extract the dataset properly?')
                        self.logger.info(f'Geometry value is: {wkt}')
                        raise AssertionError('Unexpected/unreadable geometry value')

                # Create our formatted row after geometric stuff
                try:
                    formatted_row = {"attributes": row,
                                     "geometry": geom_dict
                                     }
                except UnboundLocalError as e:
                    # If somehow geom_dict is unbound, self.logger.info the wkt to help figure out why
                    self.logger.info(f'DEBUG! {wkt}')
                    raise e

                adds.append(formatted_row)

                if (len(adds) != 0) and (len(adds) % self.batch_size == 0):
                    self.logger.info(f'Adding batch of {len(adds)}, at row #: {row_count}...')
                    start = time()
                    self.edit_features(rows=adds, row_count=row_count, method='adds')

                    # Commenting out multithreading for now.
                    #split_batches = np.array_split(adds,2)
                    # Where we actually append the rows to the dataset in AGO
                    #t1 = Thread(target=self.edit_features,
                    #            args=(list(split_batches[0]), 'adds'))
                    #t2 = Thread(target=self.edit_features,
                    #            args=(list(split_batches[1]), 'adds'))
                    #t1.start()
                    #t2.start()

                    #t1.join()
                    #t2.join()

                    adds = []
                    self.logger.info(f'Duration: {time() - start}\n')
            # add leftover rows outside the loop if they don't add up to 4000
            if adds:
                start = time()
                self.logger.info(f'Adding last batch of {len(adds)}, at row #: {i+1}...')
                #self.logger.info(f'Example row: {adds[0]}')
                #self.logger.info(f'batch: {adds}')
                self.edit_features(rows=adds, row_count=row_count, method='adds')
                self.logger.info(f'Duration: {time() - start}')

        ago_count = self.layer_object.query(return_count_only=True)
        self.logger.info(f'count after batch adds: {str(ago_count)}')
        assert ago_count != 0


    def edit_features(self, rows, row_count, method='adds'):
        '''
        Complicated function to wrap the edit_features arcgis function so we can handle AGO failing
        It will handle either:
        1. A reported rollback from AGO (1003) and try one more time,
        2. An AGO timeout, which can still be successful which we'll verify with a row count.
        '''
        assert rows

        def is_rolled_back(result):
            '''
            If we receieve a vague object back from AGO and it contains an error code of 1003
            docs:
            https://community.esri.com/t5/arcgis-api-for-python-questions/how-can-i-test-if-there-was-a-rollback/td-p/1057433
            ESRi lacks documentation here for us to really know what to expect..
            '''
            if result is None:
                self.logger.info('Returned result object is None? In cases like this the append seems to fail completely, possibly from bad encoding. Retrying.')
                try:
                    self.logger.info(f'Example row from this batch: {rows[0]}')
                except IndexError as e:
                    self.logger.info(f'Rows not of expected format??? type: {type(rows)}, self.logger.infoed: {rows}')
                    raise e
                self.logger.info(f'Returned object: {pprint(result)}')
                return True
            elif result["addResults"] is None:
                self.logger.info('Returned result not what we expected, assuming success.')
                self.logger.info(f'Returned object: {pprint(result)}')
                return False
            elif result["addResults"] is not None:
                for element in result["addResults"]:
                    if "error" in element and element["error"]["code"] == 1003:
                        self.logger.info('Error code 1003 received, we are rolled back...')
                        return True
                    elif "error" in element and element["error"]["code"] != 1000:
                        self.logger.info('Got a a character overflow error. Saving errors.') 
                        self.write_errors_to_s3(rows)
                    elif "error" in element and element["error"]["code"] != 1003:
                        raise Exception(f'Got this error returned from AGO (unhandled error): {element["error"]}')
                return False
            else:
                raise Exception(f'Unexpected result: {result}')

        success = False
        # save our result outside the while loop
        result = None
        tries = 0
        while success is False:
            tries += 1
            if tries > 5:
                raise Exception(
                    'Too many retries on this batch, there is probably something wrong with a row in here! Giving up!')
            # Is it still rolled back after a retry?
            if result is not None:
                if is_rolled_back(result):
                    #raise Exception("Retry on rollback didn't work.")
                    self.logger.info("Retry on rollback didn't work. Writing errors to file and continuing...")
                    self.write_errors_to_s3(rows)
                    success = True
                    continue

            # Add the batch
            try:
                if method == "adds":
                    result = self.layer_object.edit_features(adds=rows, rollback_on_failure=True)
                elif method == "updates":
                    result = self.layer_object.edit_features(updates=rows, rollback_on_failure=True)
                elif method == "deletes":
                    result = self.layer_object.edit_features(deletes=rows, rollback_on_failure=True)
            except Exception as e:
                if 'request has timed out' in str(e):
                    tries += 1
                    # If we're upserting, we obviously can't check counts
                    # Instead we'll just have to assume success (which it usually appears to be?)
                    if self.upserting:
                        self.logger.info(f'Got a request timed out back, assuming it worked... Error: {str(e)}')
                    if not self.upserting:
                        self.logger.info(f'Got a request timed out, checking counts. Error: {str(e)}')
                        # slow down requests if we're getting timeouts
                        sleep(300)
                        ago_count = None
                        # Account for timeouts everywhere
                        count_retries = 0 
                        while not ago_count or count_retries > 10:
                            try:
                                ago_count = self.layer_object.query(return_count_only=True)
                                # Yet another edge case, if our count is not divisible by our
                                # batch size, re-try the count after waiting. Usually means ago is still working.
                                if (ago_count % self.batch_size) != 0:
                                    sleep(60)
                                    ago_count = None
                            except:
                                sleep(10)
                            count_retries += 1
                        # If ago_count is still None...
                        if ago_count == None:
                            raise AssertionError('AGO count out of sync with our progress! Retry when AGO is less busy.')

                        self.logger.info(f'ago_count: {ago_count} == row_count: {row_count}')
                        if ago_count == row_count:
                            self.logger.info(f'Request was actually successful, ago_count matches our current row count.')
                            success = True
                        elif ago_count > row_count:
                            raise AssertionError('Error, ago_count is greater than our row_count! Some appends doubled up?')
                        elif ago_count < row_count:
                            self.logger.info(f'Request not successful, retrying.')
                        continue
                elif 'Unable to perform query' in str(e):
                    self.logger.info(f'"Unable to perform query" error received, retrying.')
                    tries += 1
                    sleep(20)
                    continue
                # Gateway error recieved, sleep for a bit longer.
                elif '502' in str(e):
                    self.logger.info(f'502 Gateway error received, retrying. Error: {str(e)}')
                    tries += 1
                    sleep(20)
                    continue
                elif '503' in str(e):
                    self.logger.info(f'503 Service Unavailable received, retrying. Error: {str(e)}')
                    tries += 1
                    sleep(20)
                    continue
                elif '504' in str(e):
                    self.logger.info(f'504 Gateway Timeout received, retrying. Error: {str(e)}')
                    tries += 1
                    sleep(60)
                    continue
                else:
                    self.logger.info(f'Unexpected Exception from AGO on this batch! Writing these rows to error file and continuing to next batch...')
                    self.logger.info('If this is a fail on a specific row, consider passing it into the --clean_columns arg.')
                    self.logger.info(f'Exception error: {str(e)}')
                    self.write_errors_to_s3(rows)
                    success = True
                    continue

            if is_rolled_back(result):
                self.logger.info("Results rolled back, retrying our batch adds in 60 seconds....")
                sleep(60)
                try:
                    if method == "adds":
                        result = self.layer_object.edit_features(adds=rows, rollback_on_failure=True)
                    elif method == "updates":
                        result = self.layer_object.edit_features(updates=rows, rollback_on_failure=True)
                    elif method == "deletes":
                        result = self.layer_object.edit_features(deletes=rows, rollback_on_failure=True)
                except Exception as e:
                    if 'request has timed out' in str(e):
                        tries += 1
                        # If we're upserting, we obviously can't check counts
                        # Instead we'll just have to assume success (which it usually appears to be?)
                        if self.upserting:
                            self.logger.info(f'Got a request timed out back, assuming it worked... Error: {str(e)}')
                        if not self.upserting:
                            self.logger.info(f'Got a request timed out, checking counts. Error: {str(e)}')
                            # slow down requests if we're getting timeouts
                            sleep(300)
                            ago_count = None
                            # Account for timeouts everywhere
                            count_retries = 0 
                            while not ago_count or count_retries > 10:
                                try:
                                    ago_count = self.layer_object.query(return_count_only=True)
                                    # Yet another edge case, if our count is not divisible by our
                                    # batch size, re-try the count after waiting. Usually means ago is still working.
                                    if (ago_count % self.batch_size) != 0:
                                        sleep(60)
                                        ago_count = None
                                except:
                                    sleep(10)
                                count_retries += 1
                            # If ago_count is still None...
                            if ago_count == None:
                                raise AssertionError('AGO count out of sync with our progress! Retry when AGO is less busy.')

                            self.logger.info(f'ago_count: {ago_count} == row_count: {row_count}')
                            if ago_count == row_count:
                                self.logger.info(f'Request was actually successful, ago_count matches our current row count.')
                                success = True
                            elif ago_count > row_count:
                                raise AssertionError('Error, ago_count is greater than our row_count! Some appends doubled up?')
                            elif ago_count < row_count:
                                self.logger.info(f'Request not successful, retrying.')
                            continue
                    elif 'Unable to perform query' in str(e):
                        self.logger.info('"Unable to perform query" error received, retrying.')
                        tries += 1
                        sleep(20)
                        continue
                    # Gateway error recieved, sleep for a bit longer.
                    elif '502' in str(e):
                        self.logger.info(f'502 Gateway error received, retrying. Error: {str(e)}')
                        tries += 1
                        sleep(20)
                        continue
                    elif '503' in str(e):
                        self.logger.info(f'503 Service Unavailable received, retrying. Error: {str(e)}')
                        tries += 1
                        sleep(20)
                        continue
                    elif '504' in str(e):
                        self.logger.info(f'504 Gateway Timeout received, retrying. Error: {str(e)}')
                        tries += 1
                        sleep(60)
                        continue
                    else:
                        self.logger.info(f'Unexpected Exception from AGO on this batch! Writing these rows to error file and continuing to next batch...')
                        self.logger.info('If this is a fail on a specific row, consider passing it into the --clean_columns arg.')
                        self.logger.info(f'Exception error: {str(e)}')
                        self.write_errors_to_s3(rows)
                        success = True
                        continue

            # If we didn't get rolled back, batch of adds successfully added.
            else:
                success = True


    def verify_count(self):
        ago_count = self.layer_object.query(return_count_only=True)
        self.logger.info(f'Asserting csv equals ago count: {self._num_rows_in_upload_file} == {ago_count}')
        assert self._num_rows_in_upload_file == ago_count


    def export(self):
        # TODO: delete any existing files in export_dir_path
        # test parameters
        # parameters = {"layers" : [ { "id" : 0, "out_sr": 2272 } ] }
        # result = self.item.export(f'{self.item.title}', self.export_format, parameters=parameters, enforce_fld_vis=True, wait=True)
        result = self.item.export(f'{self.item.title}', self.export_format, enforce_fld_vis=True, wait=True)
        result.download(self.export_dir_path)
        # Delete the item after it downloads to save on space
        result.delete()
        # unzip, unless argument export_zipped = True
        if not self.export_zipped:
            self.unzip()


    def convert_geometry(self, wkt):
        '''Convert WKT geometry to the special type AGO requires.'''
        if 'SRID=' not in wkt:
            raise AssertionError("SRID not found in shape row! Please export your dataset with 'geom_with_srid=True'.")
        if self.in_srid == None:
            self.in_srid = wkt.split(';')[0].strip("SRID=")
        wkt = wkt.split(';')[1]
        # For different types we can consult this for the proper json format:
        # https://developers.arcgis.com/documentation/common-data-types/geometry-objects.htm
        if 'POINT' in wkt:
            projected_x, projected_y = self.project_and_format_shape(wkt)
                           # Format our row, following the docs on this one, see section "In [18]":
            # https://developers.arcgis.com/python/sample-notebooks/updating-features-in-a-feature-layer/
            # create our formatted point geometry
            geom_dict = {"x": projected_x,
                         "y": projected_y,
                         "spatial_reference": {"wkid": self.ago_srid[0], "latestWkid": self.ago_srid[1]}
                         }
            #row_to_append = {"attributes": row,
            #                 "geometry": geom_dict}
        elif 'MULTIPOINT' in wkt:
            raise NotImplementedError("MULTIPOINTs not implemented yet..")
        elif 'MULTIPOLYGON' in wkt:
            rings = self.project_and_format_shape(wkt)
            geom_dict = {"rings": rings,
                         "spatial_reference": {"wkid": self.ago_srid[0], "latestWkid": self.ago_srid[1]}
                         }
            #row_to_append = {"attributes": row,
            #                 "geometry": geom_dict
            #                 }
        elif 'POLYGON' in wkt:
            #xlist, ylist = return_coords_only(wkt)
            ring = self.project_and_format_shape(wkt)
            geom_dict = {"rings": [ring],
                         "spatial_reference": {"wkid": self.ago_srid[0], "latestWkid": self.ago_srid[1]}
                         }
            #row_to_append = {"attributes": row,
            #                 "geometry": geom_dict
            #                 }
        elif 'MULTILINESTRING' in wkt:
            paths = self.project_and_format_shape(wkt)
            # Don't know why yet but some bug is sending us multilines with an already enclosing list
            # Don't enclose in list if multilinestring
            geom_dict = {"paths": paths,
                         "spatial_reference": {"wkid": self.ago_srid[0], "latestWkid": self.ago_srid[1]}
                         } 
        elif 'LINESTRING' in wkt:
            paths = self.project_and_format_shape(wkt)
            geom_dict = {"paths": [paths],
                         "spatial_reference": {"wkid": self.ago_srid[0], "latestWkid": self.ago_srid[1]}
                         }
            #row_to_append = {"attributes": row,
            #                 "geometry": geom_dict
            #                 }
        return geom_dict


    def upsert(self):
        '''
        Upserts rows from a CSV into a matching AGO item. The upsert works by first taking a unique primary key
        and searching in AGO for that. If the row exists in AGO, it will get the AGO objectid. We then take our
        updated row, and switch out the objectid for the AGO objectid.

        Then using the AGO API "edit_features", we pass the rows as "updates", and AGO should know what rows to
        update based on the matching objectid. The CSV objectid is ignored (which is also true for appends actually).

        For new rows, it will pass them as "adds" into the edit_features api, and they'll be appended into the ago item.
        '''
        # Assert we got a primary_key passed and it's not None.
        assert self.primary_key

        # Global variable to inform other processes that we're upserting
        self.upserting = True

        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:
            self.logger.info("Exception encountered trying to import rows wtih utf-8 encoding, trying latin-1...")
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')
        # Compare headers in the csv file vs the fields in the ago item.
        # If the names don't match and we were to upload to AGO anyway, AGO will not actually do
        # anything with our rows but won't tell us anything is wrong!
        self.logger.info(f'Comparing AGO fields: "{tuple(self.item_fields.keys())}" and CSV fields: "{rows.fieldnames()}"')
        row_differences = set(self.item_fields.keys()) - set(rows.fieldnames())
        if row_differences:
            # Ignore differences if it's just objectid.
            if 'objectid' in row_differences and len(row_differences) == 1:
                pass
            elif 'esri_oid' in row_differences and len(row_differences) == 1:
                pass
            else:
                self.logger.info(f'Row differences found!: {row_differences}')
                assert tuple(self.item_fields.keys()) == rows.fieldnames()
        self.logger.info('Fields are the same! Continuing.')

        self._num_rows_in_upload_file = rows.nrows()
        row_dicts = rows.dicts()
        adds = []
        updates = []
        if not self.geometric:
            for i, row in enumerate(row_dicts):
                row_count = i + 1

                # We need an OBJECTID in our row for upserting. Assert that we have that, bomb out if we don't
                assert row['objectid']

                # clean up row and perform basic non-geometric transformations
                row = self.format_row(row)

                # Figure out if row exists in AGO, and what it's object ID is.
                row_primary_key = row[self.primary_key]
                wherequery = f"{self.primary_key} = '{row_primary_key}'"
                ago_row = self.query_features(wherequery=wherequery)

                # Should be length 0 or 1
                # If we got two or more, we're doubled up and we can delete one.
                if len(ago_row.sdf) == 2:
                    self.logger.info(f'Got two results for one primary key "{row_primary_key}". Deleting second one.')
                    # Delete the 2nd one.
                    del_objectid = ago_row.sdf.iloc[1]['OBJECTID']
                    # Docs say you can simply pass only the ojbectid as a string and it should work.
                    self.edit_features(rows=str(del_objectid), row_count=row_count, method='deletes')
                # If it's more than 2, then just except out.
                elif len(ago_row.sdf) > 1:
                    raise AssertionError(f'Should have only gotten 1 or 0 rows from AGO! Instead we got: {len(ago_row.sdf)}')

                # If our row is in AGO, then we need the objectid for the upsert/update
                if not ago_row.sdf.empty:
                    ago_objectid = ago_row.sdf.iloc[0]['OBJECTID']
                else:
                    #self.logger.info(f'DEBUG! ago_row is empty?: {ago_row}')
                    self.logger.info(ago_row.sdf)
                    ago_objectid = False

                #self.logger.info(f'DEBUG! ago_objectid: {ago_objectid}')
    
                # Reassign the objectid or assign it to match the row in AGO. This will
                # make it work with AGO's 'updates' endpoint and work like an upsert.
                row['objectid'] = ago_objectid

                # If we didn't get anything back from AGO, then we can simply append our row
                if ago_row.sdf.empty:
                    adds.append({"attributes": row})

                # If we did get something back from AGO, then we're upserting our row
                if ago_objectid:
                    updates.append({"attributes": row})

                if (len(adds) != 0) and (len(adds) % self.batch_size == 0):
                    start = time()
                    self.logger.info(f'(non geometric) Adding batch of appends, {len(adds)}, at row #: {row_count}...')
                    self.edit_features(rows=adds, row_count=row_count, method='adds')
                    adds = []
                    self.logger.info(f'Duration: {time() - start}\n')
                if (len(updates) != 0) and (len(adds) % self.batch_size == 0):
                    start = time()
                    self.logger.info(f'(non geometric) Adding batch of updates {len(updates)}, at row #: {row_count}...')
                    self.edit_features(rows=updates, row_count=row_count, method='updates')
                    updates = []
                    self.logger.info(f'Duration: {time() - start}\n')
            if adds:
                start = time()
                self.logger.info(f'(non geometric) Adding last batch of appends, {len(adds)}, at row #: {row_count}...')
                self.edit_features(rows=adds, row_count=row_count, method='adds')
                self.logger.info(f'Duration: {time() - start}\n')
            if updates:
                start = time()
                self.logger.info(f'(non geometric) Adding last batch of updates, {len(updates)}, at row #: {row_count}...')
                self.edit_features(rows=updates, row_count=row_count, method='updates')
                self.logger.info(f'Duration: {time() - start}\n')

        elif self.geometric:
            for i, row in enumerate(row_dicts):
                row_count = i+1
                # We need an OBJECTID in our row for upserting. Assert that we have that, bomb out if we don't
                assert row['objectid']

                # clean up row and perform basic non-geometric transformations
                row = self.format_row(row)

                # Figure out if row exists in AGO, and what it's object ID is.
                row_primary_key = row[self.primary_key]
                wherequery = f"{self.primary_key} = '{row_primary_key}'"
                ago_row = self.query_features(wherequery=wherequery)

                # Should be length 0 or 1
                # If we got two or more, we're doubled up and we can delete one.
                if len(ago_row.sdf) == 2:
                    self.logger.info(f'Got two results for one primary key "{row_primary_key}". Deleting second one.')
                    # Delete the 2nd one.
                    del_objectid = ago_row.sdf.iloc[1]['OBJECTID']
                    # Docs say you can simply pass only the ojbectid as a string and it should work.
                    self.edit_features(rows=str(del_objectid), row_count=row_count, method='deletes')
                # Should be length 0 or 1
                elif len(ago_row.sdf) > 1:
                    raise AssertionError(f'Should have only gotten 1 or 0 rows from AGO! Instead we got: {len(ago_row.sdf)}')

                # If our row is in AGO, then we need the objectid for the upsert/update
                if not ago_row.sdf.empty:
                    ago_objectid = ago_row.sdf.iloc[0]['OBJECTID']
                else:
                    ago_objectid = False

                #self.logger.info(f'DEBUG! ago_objectid: {ago_objectid}')
    
                # Reassign the objectid or assign it to match the row in AGO. This will
                # make it work with AGO's 'updates' endpoint and work like an upsert.
                row['objectid'] = ago_objectid

                # remove the shape field so we can replace it with SHAPE with the spatial reference key
                # and also store in 'wkt' var (well known text) so we can project it
                wkt = row.pop('shape')

                # if the wkt is not empty, and SRID isn't in it, fail out.
                # empty geometries come in with some whitespace, so test truthiness
                # after stripping whitespace.
                if 'SRID=' not in wkt and bool(wkt.strip()) is False and (not self.in_srid):
                    raise AssertionError("Receieved a row with blank geometry, you need to pass an --in_srid so we know if we need to project!")
                if 'SRID=' not in wkt and bool(wkt.strip()) is True and (not self.in_srid):
                    raise AssertionError("SRID not found in shape row! Please export your dataset with 'geom_with_srid=True'.")

                if (not self.in_srid) and 'SRID=' in wkt:
                    self.logger.info('Getting SRID from csv...')
                    self.in_srid = wkt.split(';')[0].strip("SRID=")

                # Get just the WKT from the shape, remove SRID after we extract it
                if 'SRID=' in wkt:
                    wkt = wkt.split(';')[1]

                # If the geometry cell is blank, properly pass a NaN or empty value to indicate so.
                if not (bool(wkt.strip())):
                    if self.geometric == 'esriGeometryPoint':
                        geom_dict = {"x": 'NaN',
                                     "y": 'NaN',
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    elif self.geometric == 'esriGeometryPolyline':
                        geom_dict = {"paths": [],
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    elif self.geometric == 'esriGeometryPolygon':
                        geom_dict = {"rings": [],
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    else:
                        raise TypeError(f'Unexpected geomtry type!: {self.geometric}')
                # For different types we can consult this for the proper json format:
                # https://developers.arcgis.com/documentation/common-data-types/geometry-objects.htm
                if bool(wkt.strip()): 
                    if 'POINT' in wkt:
                        projected_x, projected_y = self.project_and_format_shape(wkt)
                        # Format our row, following the docs on this one, see section "In [18]":
                        # https://developers.arcgis.com/python/sample-notebooks/updating-features-in-a-feature-layer/
                        # create our formatted point geometry
                        geom_dict = {"x": projected_x,
                                     "y": projected_y,
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    elif 'MULTIPOINT' in wkt:
                        raise NotImplementedError("MULTIPOINTs not implemented yet..")
                    elif 'MULTIPOLYGON' in wkt:
                        rings = self.project_and_format_shape(wkt)
                        geom_dict = {"rings": rings,
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    elif 'POLYGON' in wkt:
                        #xlist, ylist = return_coords_only(wkt)
                        ring = self.project_and_format_shape(wkt)
                        geom_dict = {"rings": [ring],
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    elif 'MULTILINESTRING' in wkt:
                        paths = self.project_and_format_shape(wkt)
                        # Don't know why yet but some bug is sending us multilines with an already enclosing list
                        # Don't enclose in list if multilinestring
                        geom_dict = {"paths": paths,
                                    "spatial_reference": {"wkid": self.ago_srid[0], "latestWkid": self.ago_srid[1]}
                                    } 
                    elif 'LINESTRING' in wkt:
                        paths = self.project_and_format_shape(wkt)
                        geom_dict = {"paths": [paths],
                                     "spatial_reference": {"wkid": self.ago_srid[1]}
                                     }
                    else:
                        self.logger.info('Did not recognize geometry in our WKT. Did we extract the dataset properly?')
                        self.logger.info(f'Geometry value is: {wkt}')
                        raise AssertionError('Unexpected/unreadable geometry value')

                # Once we're done our shape stuff, put our row into it's final format
                formatted_row = {"attributes": row,
                                 "geometry": geom_dict
                                 }
                ##################################
                # END geometry handling
                ##################################

                # If we didn't get anything back from AGO, then we can simply append our row
                if ago_row.sdf.empty:
                    adds.append(formatted_row)

                # If we did get something back from AGO, then we're upserting our row
                if ago_objectid:
                    updates.append(formatted_row)

                if (len(adds) != 0) and (len(adds) % self.batch_size == 0):
                    self.logger.info(f'Adding batch of appends, {len(adds)}, at row #: {row_count}...')
                    start = time()
                    self.edit_features(rows=adds, row_count=row_count, method='adds')

                    # Commenting out multithreading for now.
                    #split_batches = np.array_split(adds,2)
                    # Where we actually append the rows to the dataset in AGO
                    #t1 = Thread(target=self.edit_features,
                    #            args=(list(split_batches[0]), 'adds'))
                    #t2 = Thread(target=self.edit_features,
                    #            args=(list(split_batches[1]), 'adds'))
                    #t1.start()
                    #t2.start()

                    #t1.join()
                    #t2.join()

                    adds = []
                    self.logger.info(f'Duration: {time() - start}\n')

                if (len(updates) != 0) and (len(updates) % self.batch_size == 0):
                    self.logger.info(f'Adding batch of updates, {len(updates)}, at row #: {row_count}...')
                    start = time()
                    self.edit_features(rows=updates, row_count=row_count, method='updates')

                    # Commenting out multithreading for now.
                    #split_batches = np.array_split(updates,2)
                    # Where we actually append the rows to the dataset in AGO
                    #t1 = Thread(target=self.edit_features,
                    #            args=(list(split_batches[0]), 'updates'))
                    #t2 = Thread(target=self.edit_features,
                    #            args=(list(split_batches[1]), 'updates'))
                    #t1.start()
                    #t2.start()

                    #t1.join()
                    #t2.join()

                    updates = []
                    self.logger.info(f'Duration: {time() - start}\n')
            # add leftover rows outside the loop if they don't add up to 4000
            if adds:
                start = time()
                self.logger.info(f'Adding last batch of appends, {len(adds)}, at row #: {row_count}...')
                self.edit_features(rows=adds, row_count=row_count, method='adds')
                self.logger.info(f'Duration: {time() - start}')
            if updates:
                start = time()
                self.logger.info(f'Adding last batch of updates, {len(updates)}, at row #: {row_count}...')
                self.edit_features(rows=updates, row_count=row_count, method='updates')
                self.logger.info(f'Duration: {time() - start}')

        ago_count = self.layer_object.query(return_count_only=True)
        self.logger.info(f'count after batch adds: {str(ago_count)}')
        assert ago_count != 0


    # Wrapped AGO function in a retry while loop because AGO is very unreliable.
    def query_features(self, wherequery=None, outstats=None):
        tries = 0
        while True:
            if tries > 5:
                raise RuntimeError("AGO keeps failing on our query!")
            try:

                # outstats is used for grabbing the MAX value of updated_datetime.
                if outstats:
                    output = self.layer_object.query(outStatistics=outstats, outFields='*')
                elif wherequery:
                    output = self.layer_object.query(where=wherequery)
                return output
            except Exception as e:
                if 'request has timed out' in str(e):
                    self.logger.info(f'Request timed out, retrying. Error: {str(e)}')
                    tries += 1
                    sleep(5)
                    continue
                # Ambiguous mysterious error returned to us sometimes1
                if 'Unable to perform query' in str(e):
                    self.logger.info('"Unable to perform query" error received, retrying.')
                    self.logger.info(f'wherequery used is: "{wherequery}"')
                    tries += 1
                    sleep(20)
                    continue
                # Gateway error recieved, sleep for a bit longer.
                if '502' in str(e):
                    self.logger.info(f'502 Gateway error received, retrying. Error: {str(e)}')
                    tries += 1
                    sleep(20)
                    continue
                if '503' in str(e):
                    self.logger.info(f'503 Gateway error received, retrying. Error: {str(e)}')
                    tries += 1
                    sleep(20)
                    continue
                if '504' in str(e):
                    self.logger.info(f'503 Gateway Timeout received, retrying. Error: {str(e)}')
                    tries += 1
                    sleep(20)
                    continue
                else:
                    raise e


    def post_index_fields(self):
        """
        Posts indexes to AGO via the requests module.
        First generate an access token, which we get with user credentials that
        we can then use to interact with the AGO Portal API:
        http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#//02r3000000m5000000
        Then loop through the index fields we were passed and attempt to update the AGO item definition
        with the new index
        """
        url = 'https://arcgis.com/sharing/rest/generateToken'
        data = {'username': self.ago_user,
                'password': self.ago_password,
                'referer': 'https://www.arcgis.com',
                'f': 'json'}
        #ago_token = requests.post(url, data, verify=False).json()['token']
        ago_token = requests.post(url, data).json()['token']

        # Import field information from the json schema file generated by dbtools extract (postgres or oracle)
        # We will loop through it and see if any of these fields are unique.
        s3 = boto3.resource('s3')
        json_local_path = '/tmp/' + self.item_name + '_schema.json'
        try:
            s3.Object(self.s3_bucket, self.json_schema_s3_key).download_file(json_local_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise AssertionError(f'CSV file doesnt appear to exist in S3! key: {self.json_schema_s3_key}')
            else:
                raise e
        with open(json_local_path) as json_file:
            schema = json.load(json_file).get('fields', '')
        schema_fields_info = schema


        def post_index(field, is_unique):
            '''script that actually does the posting'''
            now = datetime.now().strftime("%m/%d/%Y")

            # Check for composite indexes, which have pluses denoting them
            # ex: "...,field1+field2,...
            # Then make it a comma for the json index definition
            if '+' in field:
                field = field.replace('+',',')

            index_json = {
              "indexes": [
              {
                "name": field.replace(',','_') + '_idx',
                "fields": field,
                "isUnique": is_unique,
                "isAscending": 'true',
                "description": f'installed by dbtools on {now}'
              }
             ]
            }
            jsonData = json.dumps(index_json)

            # This endpoint is publicly viewable on AGO while not logged in.
            url = f'https://services.arcgis.com/{self.ago_org_id}/arcgis/rest/admin/services/{self.item_name}/FeatureServer/0/addToDefinition'

            headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
            self.logger.info(f'\nPosting index for {field} against {url}...')
            self.logger.info(f'Data passed: {jsonData}')
            self.logger.info(jsonData)
            r = requests.post(f'{url}?token={ago_token}', data = {'f': 'json', 'addToDefinition': jsonData }, headers=headers, timeout=360)


            if 'Invalid definition' in r.text:
                self.logger.info('''
                Index appears to already be set, got "Invalid Definition" error (this is usually a good thing, but still
                possible your index was actually rejected. ESRI just doesnt code in proper errors).
                ''')
            # Seen this error before that prevents an index from being added. Sleep and try to add again.
            elif 'Invalid URL' in r.text:
                print('Invalid URL error, does your map name differ from the table name?? Please fix if so.')
                sys.exit(1)
            elif 'Operation failed. The index entry of length' in r.text:
                self.logger.info('Got a retriable error, retrying in 10 minutes...')
                sleep(200)
                self.logger.info(f'Error was: {r.text}')
                self.logger.info(f"Posting the index for '{field}'..")
                headers = {'Content-Type': 'application/x-www-form-urlencoded'}
                r = requests.post(f'{url}?token={ago_token}', data={'f': 'json', 'addToDefinition': jsonData}, headers=headers,
                                  timeout=3600)
                if 'success' not in r.text:
                    self.logger.info('Retry on this index failed. Returned AGO error:')
                    self.logger.info(r.text)
            # Retry once on timeout.
            elif 'Your request has timed out' in r.text:
                self.logger.info('Got a timeout error, retrying in 10 minutes...')
                sleep(200)
                self.logger.info(f'Error was: {r.text}')
                self.logger.info(f"Posting the index for '{field}'..")
                headers = {'Content-Type': 'application/x-www-form-urlencoded'}
                r = requests.post(f'{url}?token={ago_token}', data={'f': 'json', 'addToDefinition': jsonData}, headers=headers,
                                  timeout=3600)
                if 'success' not in r.text:
                    self.logger.info('Retry on this index failed. Returned AGO error:')
                    self.logger.info(r.text)
            else:
                self.logger.info(r.text)


        ################################
        # Loop through and post indexes.

        for field in self.index_fields.split(','):
            # Loop through the json schema file and look for uniques
            is_unique = 'false'
            for field_dict in schema_fields_info:
                if field_dict['name'] == field:
                    if 'unique' in field_dict.keys():
                        is_unique = field_dict['unique']

            post_index(field, is_unique)
            sleep(2)

            
        ##############################################
        # now double check to make sure all indexes were made.

        self.logger.info('Checking for missing indexes..')
        self.logger.info('Sleep for 5 minutes first in hopes that index creation finishes by then..')
        sleep(300)
        check_url = f'https://services.arcgis.com/{self.ago_org_id}/ArcGIS/rest/services/{self.item_name}/FeatureServer/0?f=pjson'
        self.logger.info(f'Item defintion json URL: {check_url}')
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        r = requests.get(f'{check_url}&token={ago_token}', headers=headers, timeout=3600)
        data = r.json()
        # Pull indexes out of the feature server json definition
        ago_indexes = data['indexes']

        # Get just the names of the indexes
        ago_indexes_list = [ x['name'] for x in ago_indexes ]

        # compile a list of index names for comparison later when we double-check what
        # was successfully posted.
        index_names = []
        for field in self.index_fields.split(','):
            if '+' in field:
                index_names.append(field.replace('+','_') + '_idx')
            else:
                index_names.append(field + '_idx')

        # Subtract to see what is supposedly missing from AGO
        missing_indexes = set(index_names) - set(ago_indexes_list)

        if missing_indexes:
            self.logger.info('It appears that not all indexes were added, although often AGO just doesnt accurately list installed indexes in the feature server definition. We will retry adding them anyway.')
            for missing_index in missing_indexes:
                post_index(missing_index, 'false')
        else:
            self.logger.info('No missing indexes found.')


@click.group()
def cli():
    pass

if __name__ == '__main__':
    cli()
