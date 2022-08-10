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
from pprint import pprint
import pandas as pd
from copy import deepcopy
from threading import Thread
from shapely.ops import transform as shapely_transformer
from arcgis import GIS
from arcgis.features import FeatureLayerCollection
from time import sleep, time


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
        self.item_name = ago_item_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.in_srid = kwargs.get('in_srid', None)
        self.clean_columns = kwargs.get('clean_columns', None)
        self.primary_key = kwargs.get('primary_key', None)
        self.proxy_host = kwargs.get('proxy_host', None)
        self.proxy_port = kwargs.get('proxy_port', None)
        self.export_format = kwargs.get('export_format', None)
        self.export_zipped = kwargs.get('export_zipped', False)
        self.export_dir_path = kwargs.get('export_dir_path', os.getcwd() + '\\' + self.item_name.replace(' ', '_'))
        # unimportant since this will be run in AWS batch
        self.csv_path = '/home/worker/temp.csv'


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
                                    verify_cert=False)
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
        Contains lots of attributes we'll need to access throughout this script.'''
        if self._item is None:
            try:
                # "Feature Service" seems to pull up both spatial and table items in AGO
                search_query = f'''owner:"{self.ago_user}" AND title:"{self.item_name}" AND type:"Feature Service"'''
                print(f'Searching for item with query: {search_query}')
                items = self.org.content.search(search_query, outside_org=False)
                for item in items:
                    # For items with spaces in their titles, AGO will smartly change out spaces to underscores
                    # Test for this too.
                    if (item.title == self.item_name) or (item.title == self.item_name.replace(' ', '_')):
                        self._item = item
                        self.logger.info(f'Found item, url and id: {self.item.url}, {self.item.id}')
                        return self._item
                # If item is still None, then fail out
                if self._item is None:
                    raise Exception(f'Failed searching for item owned by {self.ago_user} with title: {self.item_name} and type:"Feature Service"')
            except Exception as e:
                self.logger.error(f'Failed searching for item owned by {self.ago_user} with title: {self.item_name} and type:"Feature Service"')
                raise e
        return self._item


    @property
    def item_fields(self):
        '''Fields of the dataset in AGO'''
        fields = [i.name.lower() for i in self.layer_object.properties.fields]
        # shape field isn't included in this property of the AGO item, so check it its geometric first
        # so we can accurately use this variables for field comparisions
        if self.geometric and 'shape' not in fields:
            fields.append('shape')
        # AGO will show these fields for lines and polygons, so remove them for an accurate comparison to the CSV headers.
        if 'shape__area' in fields:
            fields.remove('shape__area')
        if 'shape__length' in fields:
            fields.remove('shape__length')
        fields = tuple(fields)
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
        '''Boolean telling us whether the item is geometric or just a table?'''
        if self._geometric is None:
            self.logger.info('Determining geometric?...')
            geometry_type = None
            try:
                # Note, initially wanted to use hasGeometryProperties but it seems like it doesn't
                # show up for point layers. geometryType is more reliable I think?
                #is_geometric = self.layer_object.properties.hasGeometryProperties
                geometry_type = self.layer_object.properties.geometryType
            except:
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
        #print(vars(self.item))
        flayer_collection = FeatureLayerCollection.fromitem(self.item)
        # call the overwrite() method which can be accessed using the manager property
        flayer_collection.manager.overwrite(self.csv_path)


    def truncate(self):
        count = self.layer_object.query(return_count_only=True)
        if count > 100000:
            raise AssertionError('Count is over 100,000, we dont recommend using this method for large datasets!')
        self.layer_object.manager.truncate()
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
                raise AssertionError(f'CSV file doesnt appear to exist in S3! key: {self.s3_key}')
            else:
                raise e

        self.logger.info('CSV successfully downloaded.\n'.format(self.s3_bucket, self.s3_key))


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
        if self.clean_columns:
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
            if col in ['introduction', 'planning_commission', 'rules', 'mayor_signed']:
                if not row[col]:
                    row[col] = None
            elif not row[col]:
                row[col] = ''

        # Check to make sure rows aren't incorrectly set as UTC. Convert to EST/EDT if so.
        #    if row[col]:
        #        if 'datetime' in col and '+0000' in row[col]:
        #            dt_obj = datetime.strptime(row[col], "%Y-%m-%d %H:%M:%S %z")
        #            local_dt_obj = obj.astimezone(pytz.timezone('US/Eastern'))
        #            row[col] = local_db_obj.strftime("%Y-%m-%d %H:%M:%S %z")
        return row


    def append(self, truncate=True):
        '''
        Appends rows from our CSV into a matching item in AGO
        '''
        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:
            logger.info("Exception encountered trying to import rows wtih utf-8 encoding, trying latin-1...")
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')
        # Compare headers in the csv file vs the fields in the ago item.
        # If the names don't match and we were to upload to AGO anyway, AGO will not actually do 
        # anything with our rows but won't tell us anything is wrong!
        self.logger.info(f'Comparing AGO fields: "{self.item_fields}" and CSV fields: "{rows.fieldnames()}"')
        row_differences = set(self.item_fields) - set(rows.fieldnames())
        if row_differences:
            # Ignore differences if it's just objectid.
            if 'objectid' in row_differences and len(row_differences) == 1:
                pass
            elif 'esri_oid' in row_differences and len(row_differences) == 1:
                pass
            else:
                print(f'Row differences found!: {row_differences}')
                assert self.item_fields == rows.fieldnames()    
        self.logger.info('Fields are the same! Continuing.\n')

        # We're more sure that we'll succeed after prior checks, so let's truncate here..
        if truncate is True:
            self.truncate()

        self._num_rows_in_upload_file = rows.nrows()
        row_dicts = rows.dicts()
        adds = []
        batch_size = 2000
        if not self.geometric:
            for i, row in enumerate(row_dicts):
                # clean up row and perform basic non-geometric transformations
                row = self.format_row(row)

                adds.append({"attributes": row})
                if (len(adds) != 0) and (len(adds) % batch_size == 0):
                    self.logger.info(f'Adding batch of {len(adds)}, at row #: {i}...')
                    self.edit_features(rows=adds, method='adds')
                    adds = []
            if adds:
                self.logger.info(f'Adding last batch of {len(adds)}, at row #: {i}...')
                self.edit_features(rows=adds, method='adds')
        elif self.geometric:
            for i, row in enumerate(row_dicts):
                # clean up row and perform basic non-geometric transformations
                row = self.format_row(row)

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
                    print('Getting SRID from csv...')
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
                elif 'LINESTRING' in wkt:
                    paths = self.project_and_format_shape(wkt)
                    geom_dict = {"paths": [paths],
                                 "spatial_reference": {"wkid": self.ago_srid[1]}
                                 }

                # Create our formatted row after geometric stuff
                formatted_row = {"attributes": row,
                                 "geometry": geom_dict
                                 }
                adds.append(formatted_row)

                if (len(adds) != 0) and (len(adds) % batch_size == 0):
                    self.logger.info(f'Adding batch of {len(adds)}, at row #: {i+1}...')
                    start = time()
                    #self.add_features(adds, i)

                    split_batches = np.array_split(adds,2)
                    # Where we actually append the rows to the dataset in AGO
                    t1 = Thread(target=self.edit_features,
                                args=(list(split_batches[0]), 'adds'))
                    t2 = Thread(target=self.edit_features,
                                args=(list(split_batches[1]), 'adds'))
                    t1.start()
                    t2.start()

                    t1.join()
                    t2.join()
                    adds = []
                    print(f'Duration: {time() - start}\n')
            # add leftover rows outside the loop if they don't add up to 4000
            if adds:
                start = time()
                self.logger.info(f'Adding last batch of {len(adds)}, at row #: {i+1}...')
                #self.logger.info(f'Example row: {adds[0]}')
                #self.logger.info(f'batch: {adds}')
                self.edit_features(rows=adds, method='adds')
                print(f'Duration: {time() - start}')

        ago_count = self.layer_object.query(return_count_only=True)
        self.logger.info(f'count after batch adds: {str(ago_count)}')
        assert ago_count != 0


    def edit_features(self, rows, method='adds'):
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
                print('Returned result object is None? In cases like this the append seems to fail completely, possibly from bad encoding. Retrying.')
                try:
                    print(f'Example row from this batch: {rows[0]}')
                except IndexError as e:
                    print(f'Rows not of expected format??? type: {type(rows)}, printed: {rows}')
                    raise e
                print(f'Returned object: {pprint(result)}')
                return True
            elif result["addResults"] is None:
                print('Returned result not what we expected, assuming success.')
                print(f'Returned object: {pprint(result)}')
                return False
            elif result["addResults"] is not None:
                for element in result["addResults"]:
                    if "error" in element and element["error"]["code"] == 1003:
                        print('Error code 1003 received, we are rolled back...')
                        return True
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
                    raise Exception("Retry on rollback didn't work.")

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
                    print(f'Request timed out, retrying. Error: {str(e)}')
                    count += 1
                    sleep(5)
                    continue
                if 'Unable to perform query' in str(e):
                    print(f'Dumb error received, retrying. Error: {str(e)}')
                    count += 1
                    sleep(5)
                    continue
                # Gateway error recieved, sleep for a bit longer.
                if '502' in str(e):
                    print(f'502 Gateway error received, retrying. Error: {str(e)}')
                    count += 1
                    sleep(15)
                    continue
                else:
                    raise e

            if is_rolled_back(result):
                print("Results rolled back, retrying our batch adds in 15 seconds....")
                sleep(15)
                try:
                    if method == "adds":
                        result = self.layer_object.edit_features(adds=rows, rollback_on_failure=True)
                    elif method == "updates":
                        result = self.layer_object.edit_features(updates=rows, rollback_on_failure=True)
                    elif method == "deletes":
                        result = self.layer_object.edit_features(deletes=rows, rollback_on_failure=True)
                except Exception as e:
                    if 'request has timed out' in str(e):
                        print(f'Request timed out, retrying. Error: {str(e)}')
                        count += 1
                        sleep(5)
                        continue
                    if 'Unable to perform query' in str(e):
                        print(f'Dumb error received, retrying. Error: {str(e)}')
                        count += 1
                        sleep(5)
                        continue
                    # Gateway error recieved, sleep for a bit longer.
                    if '502' in str(e):
                        print(f'502 Gateway error received, retrying. Error: {str(e)}')
                        count += 1
                        sleep(15)
                        continue
                    else:
                        raise e

            # If we didn't get rolled back, batch of adds successfully added.
            else:
                success = True


    def verify_count(self):
        ago_count = self.layer_object.query(return_count_only=True)
        print(f'Asserting csv equals ago count: {self._num_rows_in_upload_file} == {ago_count}')
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

        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:
            logger.info("Exception encountered trying to import rows wtih utf-8 encoding, trying latin-1...")
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')
        # Compare headers in the csv file vs the fields in the ago item.
        # If the names don't match and we were to upload to AGO anyway, AGO will not actually do
        # anything with our rows but won't tell us anything is wrong!
        self.logger.info(f'Comparing AGO fields: "{self.item_fields}" and CSV fields: "{rows.fieldnames()}"')
        row_differences = set(self.item_fields) - set(rows.fieldnames())
        if row_differences:
            # Ignore differences if it's just objectid.
            if 'objectid' in row_differences and len(row_differences) == 1:
                pass
            elif 'esri_oid' in row_differences and len(row_differences) == 1:
                pass
            else:
                print(f'Row differences found!: {row_differences}')
                assert self.item_fields == rows.fieldnames()
        self.logger.info('Fields are the same! Continuing.\n')

        self._num_rows_in_upload_file = rows.nrows()
        row_dicts = rows.dicts()
        adds = []
        updates = []
        batch_size = 2000
        if not self.geometric:
            for i, row in enumerate(row_dicts):
                # We need an OBJECTID in our row for upserting. Assert that we have that, bomb out if we don't
                assert row['objectid']

                # clean up row and perform basic non-geometric transformations
                row = self.format_row(row)

                # Figure out if row exists in AGO, and what it's object ID is.
                row_primary_key = row[self.primary_key]
                wherequery = f"{self.primary_key} = {row_primary_key}"
                #print(f'DEBUG! wherequery: {wherequery}')
                ago_row = self.query_features(wherequery=wherequery)

                # Should be length 0 or 1
                if len(ago_row.sdf) > 1:
                    raise AssertionError(f'Should have only gotten 1 or 0 rows from AGO! Instead we got: {len(ago_row.sdf)}')

                # If our row is in AGO, then we need the objectid for the upsert/update
                if not ago_row.sdf.empty:
                    ago_objectid = ago_row.sdf.iloc[0]['OBJECTID']
                else:
                    #print(f'DEBUG! ago_row is empty?: {ago_row}')
                    print(ago_row.sdf)
                    ago_objectid = False

                #print(f'DEBUG! ago_objectid: {ago_objectid}')
    
                # Reassign the objectid or assign it to match the row in AGO. This will
                # make it work with AGO's 'updates' endpoint and work like an upsert.
                row['objectid'] = ago_objectid

                # If we didn't get anything back from AGO, then we can simply append our row
                if ago_row.sdf.empty:
                    adds.append({"attributes": row})

                # If we did get something back from AGO, then we're upserting our row
                if ago_objectid:
                    updates.append({"attributes": row})

                if (len(adds) != 0) and (len(adds) % batch_size == 0):
                    self.logger.info(f'(non geometric) Adding batch of appends, {len(adds)}, at row #: {i}...')
                    self.edit_features(rows=adds, method='adds')
                    adds = []
                if (len(updates) != 0) and (len(adds) % batch_size == 0):
                    self.logger.info(f'(non geometric) Adding batch of updates {len(updates)}, at row #: {i}...')
                    self.edit_features(rows=updates, method='updates')
                    updates = []
            if adds:
                self.logger.info(f'(non geometric) Adding last batch of appends, {len(adds)}, at row #: {i}...')
                self.edit_features(rows=adds, method='adds')
            if updates:
                self.logger.info(f'(non geometric) Adding last batch of updates, {len(updates)}, at row #: {i}...')
                self.edit_features(rows=updates, method='updates')

        elif self.geometric:
            for i, row in enumerate(row_dicts):
                # We need an OBJECTID in our row for upserting. Assert that we have that, bomb out if we don't
                assert row['objectid']

                # clean up row and perform basic non-geometric transformations
                row = self.format_row(row)

                # Figure out if row exists in AGO, and what it's object ID is.
                row_primary_key = row[self.primary_key]
                wherequery = f"{self.primary_key} = {row_primary_key}"
                #print(f'DEBUG! wherequery: {wherequery}')
                ago_row = self.query_features(wherequery=wherequery)

                # Should be length 0 or 1
                if len(ago_row.sdf) > 1:
                    raise AssertionError(f'Should have only gotten 1 or 0 rows from AGO! Instead we got: {len(ago_row.sdf)}')

                # If our row is in AGO, then we need the objectid for the upsert/update
                if not ago_row.sdf.empty:
                    ago_objectid = ago_row.sdf.iloc[0]['OBJECTID']
                else:
                    ago_objectid = False

                #print(f'DEBUG! ago_objectid: {ago_objectid}')
    
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
                    print('Getting SRID from csv...')
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
                elif 'LINESTRING' in wkt:
                    paths = self.project_and_format_shape(wkt)
                    geom_dict = {"paths": [paths],
                                 "spatial_reference": {"wkid": self.ago_srid[1]}
                                 }

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

                if (len(adds) != 0) and (len(adds) % batch_size == 0):
                    self.logger.info(f'Adding batch of appends, {len(adds)}, at row #: {i+1}...')
                    start = time()
                    #self.add_features(adds, i)

                    split_batches = np.array_split(adds,2)
                    # Where we actually append the rows to the dataset in AGO
                    t1 = Thread(target=self.edit_features,
                                args=(list(split_batches[0]), 'adds'))
                    t2 = Thread(target=self.edit_features,
                                args=(list(split_batches[1]), 'adds'))
                    t1.start()
                    t2.start()

                    t1.join()
                    t2.join()
                    adds = []
                    print(f'Duration: {time() - start}\n')

                if (len(updates) != 0) and (len(updates) % batch_size == 0):
                    self.logger.info(f'Adding batch of updates, {len(adds)}, at row #: {i+1}...')
                    start = time()
                    #self.add_features(adds, i)

                    split_batches = np.array_split(updates,2)
                    # Where we actually append the rows to the dataset in AGO
                    t1 = Thread(target=self.edit_features,
                                args=(list(split_batches[0]), 'updates'))
                    t2 = Thread(target=self.edit_features,
                                args=(list(split_batches[1]), 'updates'))
                    t1.start()
                    t2.start()

                    t1.join()
                    t2.join()
                    updates = []
                    print(f'Duration: {time() - start}\n')
            # add leftover rows outside the loop if they don't add up to 4000
            if adds:
                start = time()
                self.logger.info(f'Adding last batch of appends, {len(adds)}, at row #: {i+1}...')
                self.edit_features(rows=adds, method='adds')
                print(f'Duration: {time() - start}')
            if updates:
                start = time()
                self.logger.info(f'Adding last batch of updates, {len(updates)}, at row #: {i+1}...')
                self.edit_features(rows=updates, method='updates')
                print(f'Duration: {time() - start}')

        ago_count = self.layer_object.query(return_count_only=True)
        self.logger.info(f'count after batch adds: {str(ago_count)}')
        assert ago_count != 0


    # Wrapped AGO function in a retry while loop because AGO is very unreliable.
    def query_features(self, wherequery=None, outstats=None):
        count = 0
        while True:
            if count > 5:
                raise RuntimeError("AGO keeps failing on our query!")
            try:
                # outstats is used for grabbing the MAX value of updated_datetime.
                if outstats:
                    output = self.layer_object.query(outStatistics=outstats, outFields='*')
                elif wherequery:
                    output = self.layer_object.query(where=wherequery)
                return output
            except RuntimeError as e:
                if 'request has timed out' in str(e):
                    print(f'Request timed out, retrying. Error: {str(e)}')
                    count += 1
                    sleep(5)
                    continue
                # Ambiguous mysterious error returned to us sometimes1
                if 'Unable to perform query' in str(e):
                    print(f'Dumb error received, retrying. Error: {str(e)}')
                    count += 1
                    sleep(5)
                    continue
                # Gateway error recieved, sleep for a bit longer.
                if '502' in str(e):
                    print(f'502 Gateway error received, retrying. Error: {str(e)}')
                    count += 1
                    sleep(15)
                    continue
                else:
                    raise e



@click.group()
def cli():
    pass

if __name__ == '__main__':
    cli()
