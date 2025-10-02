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
import math
from pprint import pprint
import pandas as pd
from copy import deepcopy
#from threading import Thread
from shapely.ops import transform as shapely_transformer
from time import sleep, time
import dateutil.parser
import requests
import json
from datetime import datetime, timezone


class AGO():
    _fields = None
    _org = None
    _item = None
    _geometric = None
    _geometry_type = None
    _ago_metadata = None
    _fields = None
    _layer_object = None
    _layer_num = None
    _ago_token = None
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
        self.ago_item_name = ago_item_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        # Our org id, publicly viewable so fine to hardcode.
        self.ago_org_id = 'fLeGjb7u4uXqeF9q'
        self.ago_rest_url = f'https://services.arcgis.com/{self.ago_org_id}/arcgis/rest/services/{ago_item_name}/FeatureServer'
        self.index_fields = kwargs.get('index_fields', None)
        self.in_srid = kwargs.get('in_srid', None)
        self.clean_columns = kwargs.get('clean_columns', None)
        self.primary_key = kwargs.get('primary_key', None)
        self.proxy_host = kwargs.get('proxy_host', None)
        self.proxy_port = kwargs.get('proxy_port', None)
        self.export_format = kwargs.get('export_format', None)
        self.export_zipped = kwargs.get('export_zipped', False)
        self.batch_size = kwargs.get('batch_size', 500)
        self.export_dir_path = kwargs.get('export_dir_path', os.getcwd() + '\\' + self.ago_item_name.replace(' ', '_'))
        # Try to use /tmp dir, it should exist. Else, use our current user's home dir
        if not os.path.isdir('/tmp'):
            homedir = os.path.expanduser('~')
            self.csv_path = os.path.join(homedir, f'{self.ago_item_name}.csv')
        else:
            self.csv_path = f'/tmp/{self.ago_item_name}.csv'
        # Global variable to inform other processes that we're upserting
        self.upserting = None
        if self.clean_columns == 'False':
            self.clean_columns = None


    # Make these 2 blank functions so AGO class can be instantiated directly in python if need be
    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        pass


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
    def json_schema_s3_key(self):
        if self._json_schema_s3_key is None:
            self._json_schema_s3_key = self.s3_key.replace('staging', 'schemas').replace('.csv', '.json')
        return self._json_schema_s3_key


    @property
    def ago_metadata(self):
        '''Dictionary of the metadata of the AGO item'''
        if self._ago_metadata is None:
            print(f'Attempting to query against service URL: {self.ago_rest_url}')
            data = requests.get(self.ago_rest_url + '?f=pjson' + f'&token={self.ago_token}')
            if 'error' in data.json().keys():
                if data.json()['error']['message'] == 'Invalid URL':
                    print(f"Service not found with name '{table_name_to_search}'")
                else:
                    print(f"Service not found with name '{table_name_to_search}'")
            else:
                print(f"Found itemid with service name query!")
                self.itemid = data.json()['serviceItemId']
                self._ago_metadata = data.json()

        return self._ago_metadata

    @property
    def fields(self):
        if self._fields is None:
            print('Fetching field names from AGO...')
            # Fetch field names from AGO REST API
            #url = f"https://www.arcgis.com/sharing/rest/content/items/{itemid}/data"
            params = {'token': self.ago_token, 'f': 'pjson'}
            response = requests.get(self.ago_rest_url + f'/{self.layer_num}', params=params)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch field names, response: {response.text}")
            elif response.status_code == 200:
                try:
                    # Attempt to parse the response as JSON
                    metadata = response.json()
                except requests.exceptions.JSONDecodeError:
                    raise ValueError(f"Failed to parse JSON response: {response.text}")

                fields = {f['name'].lower(): f['type'].lower() for f in metadata.get('fields', []) if f.get('name') and f.get('type')}

                # Determine shape field name, it can either be "shape" or "geometry". Can be found in the indexes section of the metadata.
                if 'shape' not in fields.keys() and 'geometry' not in fields.keys():
                    # Default to "shape" in case we can't find it in indexes, most AGO items should be "shape".
                    spatial_field = 'shape'
                    print("No 'shape' or 'geometry' field found in fields, checking indexes for geometry field name...")
                    indexes = metadata.get('indexes', [])
                    for index in indexes:
                        if index['description'] == 'Shape Index':
                            spatial_field = index['fields'].lower()
                            print(f"Found spatial field name from indexes: {spatial_field}")
                            break

                # Also determine if geometric, and whether we should include "shape" in the field names
                geometry_type = metadata.get('geometryType')
                print(f"Geometry type: {geometry_type}\n")
                if geometry_type and geometry_type.lower() in ['esrigeometrypoint', 'esrigeometryline', 'esrigeometrypolyline', 'esrigeometrypolygon']:
                    fields[spatial_field] = geometry_type.lower()
                if geometry_type and not spatial_field:
                    raise Exception("Geometry type found from AGO but we didn't match on it! Please evaluate this section of code!")

                assert fields, f'Couldn\'t get field names from AGO, do you have the right REST item name? API response: {response.text}'

            if spatial_field:
                # Remove esri specific rider-on spatial fields that we don't need, if they exist.
                fields.pop('shape__len', None)
                fields.pop('shape__area', None)
                fields.pop('shape__length', None)
            self._fields = fields
        return self._fields
    
    @property
    def ago_token(self):
        if self._ago_token is None:
            url = 'https://arcgis.com/sharing/rest/generateToken'
            data = {'username': self.ago_user,
                    'password': self.ago_password,
                    'referer': 'https://www.arcgis.com',
                    'f': 'json'}
            ago_token = requests.post(url, data).json()['token']
            if not ago_token:
                raise AssertionError('Failed to get AGO token!')
            self._ago_token = ago_token
        return self._ago_token

    @property
    def layer_num(self):
        if self._layer_num is None:
            # first figure out the proper layer number to use
            layer_num_success = False
            layer_num = 0
            print('Attempting to find the right layer number..')
            while not layer_num_success:
                try_url = f'{self.ago_rest_url}/{layer_num}?f=pjson'
                params = {
                    "token": self.ago_token
                }
                r = requests.get(try_url, params=params, timeout=30)
                data = r.json()
                if 'error' in data.keys():
                    print(data['error']['details'])
                elif 'name' in data.keys():
                    if data['name'].lower() == self.ago_item_name.lower():
                        layer_num_success = True
                        print(f'Found layer {layer_num} for item {self.ago_item_name}!')
                        break
                if layer_num > 10:
                    print(f'Layer number with matching name {self.ago_item_name} not found, giving up.')
                    raise AssertionError(f'Layer number with matching name {self.ago_item_name} not found, giving up.')
                layer_num += 1
            self._layer_num = layer_num
        return self._layer_num


    @property
    def geometry_type(self):
        if self._geometry_type is None:
            self._geometry_type = self.ago_metadata['layers'][self.layer_num]['geometryType']
        return self._geometry_type

    @property
    def geometric(self):
        '''Var telling us whether the item is geometric or just a table?
        If it's geometric, var will have geom type. Otherwise it is False.'''
        if self._geometric is None:
            print('Determining geometric?...')
            geometry_type = None
            try:
                # Note, initially wanted to use hasGeometryProperties but it seems like it doesn't
                # show up for point layers. geometryType is more reliable I think?
                #is_geometric = self.layer_object.properties.hasGeometryProperties
                geometry_type = self.ago_metadata['layers'][self.layer_num]['geometryType']
            except KeyboardInterrupt as e:
                raise e
            except Exception as e:
                self._geometric = False
            if geometry_type:
                #self._geometric = True
                print(f'Item detected as geometric, type: {geometry_type}\n')
                self._geometric = geometry_type
            else:
                print(f'Item is not geometric.\n')
        return self._geometric


    @property
    def projection(self):
        '''Decide if we need to project our shape field. If the SRID in AGO is set
        to what our source dataset is currently, we don't need to project.'''
        if self._projection is None:
            if str(self.in_srid) == str(self.ago_metadata['spatialReference']['latestWkid']):
                print(f"source SRID detected as same as AGO srid, not projecting. source: {self.in_srid}, ago: {self.ago_metadata['spatialReference']['latestWkid']}\n")
                self._projection = False
            else:
                print(f"Shapes will be projected. source: {self.in_srid}, ago: {self.ago_metadata['spatialReference']['latestWkid']}\n")
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


    def truncate(self):
        if self.layer_count > 200000:
            raise AssertionError('Count is over 200,000, we dont recommend using this method for large datasets!')

        truncate_url = f'https://services.arcgis.com/{self.ago_org_id}/arcgis/rest/admin/services/{self.ago_item_name}/FeatureServer/{self.layer_num}/truncate'
        data = {
            "f": "json",
            "token": self.ago_token,
            "async": "false",
            "attachmentOnly": "false"
        }
        r = requests.post(truncate_url, data=data, timeout=30)
        if 'error' in r.json().keys():
            raise AssertionError(f'Error truncating AGO layer!: {r.json()["error"]}')
        elif r.json()['success']:
            print('Truncate successful!')
        else:
            raise Exception(f'Truncate failed!: {r.text}')
        after_truncate_count = self.layer_count
        assert after_truncate_count == 0, f"Truncate failed, layer count not zero! Got: {after_truncate_count}"


    def get_csv_from_s3(self):
        print('Fetching csv s3://{}/{}'.format(self.s3_bucket, self.s3_key))

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

        print('CSV successfully downloaded.\n'.format(self.s3_bucket, self.s3_key))


    def write_errors_to_s3(self, rows):
        try:
            ts = int(time())
            file_timestamp_name = f'-{ts}-errors.txt'
            error_s3_key = self.s3_key.replace('.csv', file_timestamp_name)
            print(f'Writing bad rows to file in s3 {error_s3_key}...')
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
            print('Failed to put errors in csv and upload to S3.')
            print(f'Error: {str(e)}')
        os.remove(error_filepath)


    @property
    def transformer(self):
        '''transformer needs to be defined outside of our row loop to speed up projections.'''
        if self._transformer is None:
            self._transformer = pyproj.Transformer.from_crs(f"epsg:{self.in_srid}",
                                                      f"epsg:{str(self.ago_metadata['spatialReference']['latestWkid'])}",
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
                assert not math.isinf(xlist[0]), f'Projected x coordinate is infinity, something went wrong with projection! Returned shape: {coords}'
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
                assert not math.isinf(xlist[0]), f'Projected x coordinate is infinity, something went wrong with projection! Returned shape: {coords}'
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
                print('Warning, shapely found this WKT to be invalid! Might want to fix this!')
                print(wkt_shape)
            list_of_rings = []
            for poly in multipoly.geoms:
                if not poly.is_valid:
                    print('Warning, shapely found this WKT to be invalid! Might want to fix this!')
                    print(wkt_shape)
                # reference for polygon projection: https://gis.stackexchange.com/a/328642
                ring = format_ring(poly)
                list_of_rings.append(ring)
            return list_of_rings
        elif 'POLYGON' in wkt_shape:
            poly = shapely.wkt.loads(wkt_shape)
            if not poly.is_valid:
                print('Warning, shapely found this WKT to be invalid! Might want to fix this!')
                print(wkt_shape)
            ring = format_ring(poly)
            return ring
        elif 'MULTILINESTRING' in wkt_shape:
            multipaths = shapely.wkt.loads(wkt_shape)
            list_of_paths = []
            for path in multipaths.geoms:
                if not path.is_valid:
                    print('Warning, shapely found this WKT to be invalid! Might want to fix this!')
                    print(wkt_shape)
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
            #print(f'Cleaning columns of invalid characters: {self.clean_columns}')
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
            data_type = self.fields[col]
            # Then make sure this row isn't empty and is of a date type in AGO
            if row[col] and data_type == 'esrifieldtypedate':
                # then try parsing with dateutil parser
                try:
                    adate = dateutil.parser.parse(row[col])
                    if adate.tzinfo is None:
                        adate = adate.replace(tzinfo=timezone.est)
                    row[col] = int(adate.astimezone(timezone.utc).timestamp() * 1000)

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

        # First assert that our item in AGO has "create" capabilities:
        # Actually I don't think this is necessary maybe?
        #if 'Create' not in self.ago_metadata['capabilities']:
        #    raise AssertionError(f'Item in AGO does not have "Create" capabilities, cannot append! Please enable editing. Capabilities are: {self.ago_metadata["capabilities"]}')

        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:
            print("Exception encountered trying to import rows wtih utf-8 encoding, trying latin-1...")
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')
        # Compare headers in the csv file vs the fields in the ago item.
        # If the names don't match and we were to upload to AGO anyway, AGO will not actually do 
        # anything with our rows but won't tell us anything is wrong!
        print(f'Comparing AGO fields: {set(self.fields.keys())} ')
        print(f'To CSV fields: {set(rows.fieldnames())} ')

        # Apparently we need to compare both ways even though we're sorting them into sets
        # Otherwise we'll miss out on differences.
        row_differences1 = set(self.fields.keys()) - set(rows.fieldnames())
        row_differences2 = set(rows.fieldnames()) - set(self.fields.keys())
        
        # combine both difference subtractions with a union
        row_differences = row_differences1.union(row_differences2)

        if row_differences:
            # Ignore differences if it's just objectid.
            if 'objectid' in row_differences and len(row_differences) == 1:
                pass
            elif 'esri_oid' in row_differences and len(row_differences) == 1:
                pass
            else:
                print(f'Row differences found!: {row_differences}')
                assert tuple(self.fields.keys()) == rows.fieldnames()    
        print('Fields are the same! Continuing.')

        # Check CSV file rows match the rows we pulled in
        # We've had these not match in the past.
        self._num_rows_in_upload_file = rows.nrows()
        row_dicts = rows.dicts()

        # First we should check that we can parse geometry before proceeding with truncate.
        # Find a single non-geometric row and try parsing it's geometry.
        if self.geometric:
            loop_counter = 0
            # keep looping for awhile until we get a non-blank geom value
            for i, row in enumerate(row_dicts):
                # Bomb out at 500 rows and hope our geometry is good
                if loop_counter > 500:
                    break
                loop_counter += 1
                wkt = row['shape']

                # Set WKT to empty string so next conditional doesn't fail on a Nonetype
                if not wkt.strip():
                    continue
                if 'SRID=' not in wkt and bool(wkt.strip()) is False and (not self.in_srid):
                    raise AssertionError("Receieved a row with blank geometry, you need to pass an --in_srid so we know if we need to project!")
                if 'SRID=' not in wkt and bool(wkt.strip()) is True and (not self.in_srid):
                    raise AssertionError("SRID not found in shape row! Please export your dataset with 'geom_with_srid=True'.")
                if 'POINT' in wkt:
                    assert self.geometry_type == 'esriGeometryPoint'
                    break
                elif 'MULTIPOINT' in wkt:
                    raise NotImplementedError("MULTIPOINTs not implemented yet..")
                elif 'MULTIPOLYGON' in wkt:
                    assert self.geometry_type == 'esriGeometryPolygon'
                    break
                elif 'POLYGON' in wkt:
                    assert self.geometry_type == 'esriGeometryPolygon'
                    break
                elif 'LINESTRING' in wkt:
                    assert self.geometry_type == 'esriGeometryPolyline'
                    break
                else:
                    print('Did not recognize geometry in our WKT. Did we extract the dataset properly?')
                    print(f'Geometry value is: {wkt}')
                    raise AssertionError('Unexpected/unreadable geometry value')


        # We're more sure that we'll succeed after prior checks, so let's truncate here..
        if truncate is True:
            self.truncate()

        # loop through and accumulate appends into adds[]
        adds = []
        for i, row in enumerate(row_dicts):
            # clean up row and perform basic non-geometric transformations
            row = self.format_row(row)

            if self.geometric:
                # remove the shape field so we can replace it with SHAPE with the spatial reference key
                # and also store in 'wkt' var (well known text) so we can project it
                wkt = row.pop('shape')
                geom_dict = self.convert_geometry(wkt)
                # Create our formatted row with properly made AGO geometry
                formatted_row = {"attributes": row,
                                "geometry": geom_dict
                                }
            else:
                formatted_row = {"attributes": row}
            adds.append(formatted_row)

            # Apply once we reach out designated batch amount then reset
            if (len(adds) != 0) and (len(adds) % self.batch_size == 0):
                start = time()
                row_count = i+1
                print(f'Adding batch of adds ({len(adds)} rows) at row #: {row_count}...')
                self.edit_features(rows=adds, method='addFeatures')
                adds = []
                print(f'Duration: {time() - start}\n')
        # For any leftover rows that didn't make a full batch
        if adds:
            start = time()
            row_count = i+1
            print(f'Adding last batch of adds ({len(adds)} rows) at row #: {row_count}...')
            self.edit_features(rows=adds, method='addFeatures')
            print(f'Duration: {time() - start}\n')

        ago_count = self.layer_count
        print(f'count after batch adds: {str(ago_count)}')
        assert ago_count != 0


    def edit_features(self, rows, method):
        '''
        ESRI docs:
        https://developers.arcgis.com/rest/services-reference/enterprise/add-features/
        https://developers.arcgis.com/rest/services-reference/enterprise/delete-features/
        https://developers.arcgis.com/rest/services-reference/enterprise/update-features/
        '''
        edit_url = f'https://services.arcgis.com/{self.ago_org_id}/arcgis/rest/services/{self.ago_item_name}/FeatureServer/{self.layer_num}/{method}'
        resp = requests.post(
            edit_url,
            data={
                "f": "json",
                "token": self.ago_token,
                "rollbackOnFailure": "false",
                "features": json.dumps(rows)
            },
            timeout=30,
        )
        try:
            resp.json()
        except Exception as e:
            print(f'Error parsing edit features response as JSON!: {resp.text}')

        if resp.json().get('error'):
            print(f'Error editing features!: {resp.json()["error"]}')
            raise AssertionError(f'Error editing features!: {resp.json()["error"]}')

        if resp.status_code != 200:
            print(f'Error editing features: {resp.content}')
            raise


    def verify_count(self):
        ago_count = self.layer_count
        print(f'Asserting csv equals ago count: {self._num_rows_in_upload_file} == {ago_count}')
        assert self._num_rows_in_upload_file == ago_count


    def convert_geometry(self, wkt):
        '''Convert WKT geometry to the special type AGO requires.'''
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

        if not self.in_srid:
            if 'SRID=' in wkt:
                print('Getting SRID from csv...')
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
                                "spatialReference": {"wkid": self.ago_metadata['spatialReference']['latestWkid']}
                                }
            elif self.geometric == 'esriGeometryPolyline':
                geom_dict = {"paths": [],
                                "spatialReference": {"wkid": self.ago_metadata['spatialReference']['latestWkid']}
                                }
            elif self.geometric == 'esriGeometryPolygon':
                geom_dict = {"rings": [],
                                "spatialReference": {"wkid": self.ago_metadata['spatialReference']['latestWkid']}
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
                                "spatialReference": {"wkid": self.ago_metadata['spatialReference']['latestWkid']}
                                }
            elif 'MULTIPOINT' in wkt:
                raise NotImplementedError("MULTIPOINTs not implemented yet..")
            elif 'MULTIPOLYGON' in wkt:
                rings = self.project_and_format_shape(wkt)
                geom_dict = {"rings": rings,
                                "spatialReference": {"wkid": self.ago_metadata['spatialReference']['latestWkid']}
                                }
            elif 'POLYGON' in wkt:
                #xlist, ylist = return_coords_only(wkt)
                ring = self.project_and_format_shape(wkt)
                geom_dict = {"rings": [ring],
                                "spatialReference": {"wkid": self.ago_metadata['spatialReference']['latestWkid']}
                                }
            elif 'MULTILINESTRING' in wkt:
                paths = self.project_and_format_shape(wkt)
                # Don't know why yet but some bug is sending us multilines with an already enclosing list
                # Don't enclose in list if multilinestring
                geom_dict = {"paths": paths,
                            "spatialReference": {"wkid": self.ago_metadata['spatialReference']['wkid'], "latestWkid": self.ago_metadata['spatialReference']['latestWkid']}
                            }
            elif 'LINESTRING' in wkt:
                paths = self.project_and_format_shape(wkt)
                geom_dict = {"paths": [paths],
                                "spatialReference": {"wkid": self.ago_metadata['spatialReference']['latestWkid']}
                                }
            else:
                print('Did not recognize geometry in our WKT. Did we extract the dataset properly?')
                print(f'Geometry value is: {wkt}')
                raise AssertionError('Unexpected/unreadable geometry value')
        return geom_dict


    def upsert(self):
        '''
        Upserts rows from a CSV into a matching AGO item. The upsert works by first taking a unique primary key
        and searching in AGO for that. If the row exists in AGO, it will get the AGO objectid. We then take our
        updated row, and switch out the objectid for the AGO objectid.

        Then using the AGO API "edit_features", we pass the rows as "updates", and AGO should know what rows to
        update based on the matching objectid. The CSV objectid is ignored (which is also true for appends actually).

        For new rows, it will pass them into the "addFeatures" api endpoint.
        For rows that do exist, it will pass them to "updateFeatures".

        NOTE: This will not delete rows UNLESS it finds rows with the same primary key in AGO. If it finds two rows
        with the same primary key, it will delete the second one.
        '''
        # Assert we got a primary_key passed and it's not None.
        assert self.primary_key

        # Global variable to inform other processes that we're upserting
        self.upserting = True

        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:
            print("Exception encountered trying to import rows wtih utf-8 encoding, trying latin-1...")
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')
        # Compare headers in the csv file vs the fields in the ago item.
        # If the names don't match and we were to upload to AGO anyway, AGO will not actually do
        # anything with our rows but won't tell us anything is wrong!
        print(f'Comparing AGO fields: "{tuple(self.fields.keys())}" and CSV fields: "{rows.fieldnames()}"')
        row_differences = set(self.fields.keys()) - set(rows.fieldnames())
        if row_differences:
            # Ignore differences if it's just objectid.
            if 'objectid' in row_differences and len(row_differences) == 1:
                pass
            elif 'esri_oid' in row_differences and len(row_differences) == 1:
                pass
            else:
                assert tuple(self.fields.keys()) == rows.fieldnames(), f'Row differences found!: {row_differences}'
        print('Fields are the same! Continuing.')

        self._num_rows_in_upload_file = rows.nrows()
        row_dicts = rows.dicts()
        adds = []
        updates = []
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
            if len(ago_row) == 2:
                print(f'Got two results for one primary key "{row_primary_key}". Deleting second one.')
                # Delete the 2nd one.
                del_objectid = ago_row[1]['attributes']['objectid']
                # Docs say you can simply pass only the ojbectid as a string and it should work.
                self.edit_features(rows=str(del_objectid), method='deleteFeatures')
            # Should be length 0 or 1
            elif len(ago_row) > 1:
                raise AssertionError(f'Should have only gotten 1 or 0 rows from AGO! Instead we got: {len(ago_row)}')

            # If our row is in AGO, then we need the objectid for the upsert/update
            if ago_row:
                ago_objectid = ago_row[0]['attributes']['objectid']
            else:
                ago_objectid = False

            # Reassign the objectid or assign it to match the row in AGO. This will
            # make it work with AGO's 'updates' endpoint and work like an upsert.
            row['objectid'] = ago_objectid

            if self.geometric:
                # remove the shape field so we can replace it with SHAPE with the spatial reference key
                # and also store in 'wkt' var (well known text) so we can project it
                wkt = row.pop('shape')
                geom_dict = self.convert_geometry(wkt)
                # Once we're done our shape stuff, put our row into it's final format
                formatted_row = {"attributes": row,
                                "geometry": geom_dict
                                }
            else:
                formatted_row = {"attributes": row}


            if not ago_row:
                adds.append(formatted_row)    
            # If we did get something back from AGO, then we're upserting our row
            if ago_objectid:
                updates.append(formatted_row)
            
            # Apply once we reach out designated batch amount then reset
            if (len(adds) != 0) and (len(adds) % self.batch_size == 0):
                print(f'Adding batch of appends ({len(adds)} rows) at row #: {row_count}...')
                start = time()
                self.edit_features(rows=adds, method='addFeatures')

                adds = []
                print(f'Duration: {time() - start}\n')

            if (len(updates) != 0) and (len(updates) % self.batch_size == 0):
                print(f'Adding batch of updates ({len(updates)} rows) at row #: {row_count}...')
                start = time()
                self.edit_features(rows=updates, method='updateFeatures')

                updates = []
                print(f'Duration: {time() - start}\n')
        # For any leftover rows that didn't make a full batch
        if adds:
            start = time()
            print(f'Adding last batch of appends ({len(adds)} rows) at row #: {row_count}...')
            self.edit_features(rows=adds, method='addFeatures')
            print(f'Duration: {time() - start}')
        if updates:
            start = time()
            print(f'Adding last batch of updates ({len(updates)} rows) at row #: {row_count}...')
            self.edit_features(rows=updates, method='updateFeatures')
            print(f'Duration: {time() - start}')


    @property
    def layer_count(self) -> int:
        url = f'{self.ago_rest_url}/{self.layer_num}/query?where=1%3D1&returnCountOnly=true'
        response = requests.get(url, params={"f": "json", "token": self.ago_token})
        response.raise_for_status()
        data = response.json()
        return data.get('count')


    # Wrapped AGO function in a retry while loop because AGO is very unreliable.
    def query_features(self, wherequery='1=1', outstats=None, batch_fetch_amount=1000, fields=['*']):
        """Fetch data from the ArcGIS Online feature class."""

        features = []
        result_offset = 0
        query_url = self.ago_rest_url + f'/{self.layer_num}' + '/query'

        while True:
            QUERY_PARAMS = {
                "where": wherequery,
                "outFields": ",".join(fields),
                "outStatistics": outstats,
                "f": "json",
                "token": self.ago_token,
                "resultOffset": result_offset,
                "resultRecordCount": batch_fetch_amount
            }
            response = requests.get(query_url, params=QUERY_PARAMS)
            response.raise_for_status()
            if not response.status_code == 200:
                raise AssertionError("Error querying AGO! Status code: " + str(response.status_code) + "\nResponse text: " + response.text + "\nQuery used: " + query_url + str(QUERY_PARAMS))
            if "Cannot perform query. Invalid query parameters." in response.text:
                raise AssertionError("ESRI returned an error: " + response.text + "\nQuery used: " + query_url + str(QUERY_PARAMS))
            data = response.json()
            current_features = data.get("features", [])
            if not current_features:
                #print('Done fetching data from AGO.\n')
                break
            features.extend(current_features)
            result_offset += len(current_features)
            #print(f"Fetched {len(current_features)} records, total so far: {len(features)}")
        return features


    def post_index_fields(self):
        """
        Posts indexes to AGO via the requests module.
        First generate an access token, which we get with user credentials that
        we can then use to interact with the AGO Portal API:
        http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#//02r3000000m5000000
        Then loop through the index fields we were passed and attempt to update the AGO item definition
        with the new index
        """

        # Import field information from the json schema file generated by dbtools extract (postgres or oracle)
        # We will loop through it and see if any of these fields are unique.
        s3 = boto3.resource('s3')
        json_local_path = '/tmp/' + self.ago_item_name + '_schema.json'
        try:
            s3.Object(self.s3_bucket, self.json_schema_s3_key).download_file(json_local_path)
            with open(json_local_path) as json_file:
                schema = json.load(json_file).get('fields', '')
            schema_fields_info = schema
        except botocore.exceptions.ClientError as e:
            schema_fields_info = None
            if e.response['Error']['Code'] == "404":
                print(f'CSV file doesnt appear to exist in S3! key: {self.json_schema_s3_key}')
                print(f'Cannot determine if fields are unique in the Database and need a unique index.')
            else:
                raise e

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
            url = f'https://services.arcgis.com/{self.ago_org_id}/arcgis/rest/admin/services/{self.ago_item_name}/FeatureServer/{self.layer_num}/addToDefinition'

            headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
            print(f'\nPosting index for {field} against {url}...')
            print(f'Data passed: {jsonData}')
            print(jsonData)
            r = requests.post(f'{url}?token={self.ago_token}', data = {'f': 'json', 'addToDefinition': jsonData }, headers=headers, timeout=360)


            if 'Invalid definition' in r.text:
                print('''
                Index appears to already be set, got "Invalid Definition" error (this is usually a good thing, but still
                possible your index was actually rejected. ESRI just doesnt code in proper errors).
                ''')
            # Seen this error before that prevents an index from being added. Sleep and try to add again.
            elif 'Invalid URL' in r.text:
                print('Invalid URL error, does your map name differ from the table name?? Please fix if so.')
                sys.exit(1)
            elif 'Operation failed. The index entry of length' in r.text:
                print('Got a retriable error, retrying in 10 minutes...')
                sleep(200)
                print(f'Error was: {r.text}')
                print(f"Posting the index for '{field}'..")
                headers = {'Content-Type': 'application/x-www-form-urlencoded'}
                r = requests.post(f'{url}?token={self.ago_token}', data={'f': 'json', 'addToDefinition': jsonData}, headers=headers,
                                  timeout=3600)
                if 'success' not in r.text:
                    print('Retry on this index failed. Returned AGO error:')
                    print(r.text)
            # Retry once on timeout.
            elif 'Your request has timed out' in r.text:
                print('Got a timeout error, retrying in 10 minutes...')
                sleep(200)
                print(f'Error was: {r.text}')
                print(f"Posting the index for '{field}'..")
                headers = {'Content-Type': 'application/x-www-form-urlencoded'}
                r = requests.post(f'{url}?token={self.ago_token}', data={'f': 'json', 'addToDefinition': jsonData}, headers=headers,
                                  timeout=3600)
                if 'success' not in r.text:
                    print('Retry on this index failed. Returned AGO error:')
                    print(r.text)
            else:
                print(r.text)


        ################################
        # Loop through and post indexes.

        for field in self.index_fields.split(','):
            # Loop through the json schema file and look for uniques
            is_unique = 'false'
            # only loop through if we found a json schema file
            if schema_fields_info:
                for field_dict in schema_fields_info:
                    if field_dict['name'] == field:
                        if 'unique' in field_dict.keys():
                            is_unique = field_dict['unique']

            post_index(field, is_unique)
            sleep(2)

            
        ##############################################
        # now double check to make sure all indexes were made.

        print('Checking for missing indexes..')
        print('Sleep for 5 minutes first in hopes that index creation finishes by then..')
        sleep(300)
        check_url = f'https://services.arcgis.com/{self.ago_org_id}/ArcGIS/rest/services/{self.ago_item_name}/FeatureServer/{self.layer_num}?f=pjson'
        print(f'Item defintion json URL: {check_url}')
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        r = requests.get(f'{check_url}&token={self.ago_token}', headers=headers, timeout=3600)
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
            print('It appears that not all indexes were added, although often AGO just doesnt accurately list installed indexes in the feature server definition. We will retry adding them anyway.')
            for missing_index in missing_indexes:
                post_index(missing_index, 'false')
        else:
            print('No missing indexes found.')


@click.group()
def cli():
    pass

if __name__ == '__main__':
    cli()
