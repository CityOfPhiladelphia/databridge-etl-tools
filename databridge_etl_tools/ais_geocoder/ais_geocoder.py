import sys, csv
from smart_open import open as smopen
import boto3
from hurry.filesize import size
from .ais_request import ais_request

csv.field_size_limit(sys.maxsize)


class AIS_Geocoder():
    '''
    For geocoding addresses and inserting other information from AIS results into our dataset.
    '''
    def __init__(self,
            ais_url, 
            ais_key, 
            ais_user,
            s3_bucket,
            s3_input_key,
            s3_output_key,
            query_fields,
            ais_fields,
            remove_fields,
            srid
            **kwargs):
        
        self.ais_url = ais_url
        self.ais_key = ais_key
        self.ais_user = ais_user
        self.s3_bucket = s3_bucket
        self.s3_input_key = s3_input_key
        self.s3_output_key = s3_output_key
        self.query_fields = query_fields.split(',')
        self.ais_fields = ais_fields.split(',')
        self.remove_fields = remove_fields.split(',') if remove_fields else None
        self.csv_path = '/tmp/output.csv'
        self.srid = srid


    def ais_inner_geocode(self):

        out_rows = None
        session = boto3.Session()

        # Remove first slash if passed in with key.
        if self.s3_input_key[0] == '/':
            self.s3_input_key = self.s3_input_key[1:-1]
        if self.s3_output_key[0] == '/':
            self.s3_output_key = self.s3_output_key[1:-1]

        input_file = f's3://{self.s3_bucket}/{self.s3_input_key}'
        output_file = f's3://{self.s3_bucket}/{self.s3_output_key}'

        errors = 0

        # Use smart_open imported over built-in open, which will opaquely use boto3 to stream our data out of and back into S3
        # https://github.com/piskvorky/smart_open
        with smopen(input_file, 'r', transport_params={'client': session.client('s3')}) as input_stream:
            with smopen(output_file, 'w', transport_params={'client': session.client('s3')}) as output_stream:

                rows = csv.DictReader(input_stream)

                for row in rows:
                    query_elements = []
                    for query_field in self.query_fields:
                        query_elements.append(row[query_field])

                    result = None

                    if not result:
                        result = ais_request(self.ais_url, self.ais_key, self.ais_user, query_elements, self.srid)

                    if result and 'features' in result and len(result['features']) > 0:

                        feature = result['features'][0]
                        for ais_field in self.ais_fields:
                            if ais_field == 'lon' or ais_field == 'longitude':
                                row[ais_field] = feature['geometry']['coordinates'][0] if feature['geometry']['coordinates'][0] is not None else ''
                            elif ais_field == 'lat' or ais_field == 'latitude':
                                row[ais_field] = feature['geometry']['coordinates'][1]  if feature['geometry']['coordinates'][0] is not None else ''
                            elif ais_field == 'shape':
                                coords = feature['geometry']['coordinates']
                                row[ais_field] = 'SRID={srid};POINT ({x} {y})'.format(x=coords[0], y=coords[1], srid=self.srid) if coords[0] and coords[1] else ''
                             elif ais_field == 'geocode_type':
                                row[ais_field] = feature['geometry']['geocode_type']
                            elif ais_field == 'normalized':
                                row[ais_field] = result['normalized']
                            elif ais_field == 'match_type':
                                row[ais_field] = feature['match_type']
                            else:
                                row[ais_field] = feature['properties'][ais_field] if feature['properties'].get(ais_field, '') else ''

                    else:
                        print('Could not geocode "{}"'.format(query_elements))
                        errors += 1

                    if out_rows == None:
                        headers = rows._fieldnames + self.ais_fields
                        if self.remove_fields:
                            headers = [x for x in headers if x not in self.remove_fields]
                        out_rows = csv.DictWriter(output_stream, headers)
                        out_rows.writeheader()

                    if self.remove_fields:
                        filtered_row = {field: row[field] for field in row if field not in self.remove_fields}
                        out_rows.writerow(filtered_row)
                    else:
                        out_rows.writerow(row)

        s3 = boto3.resource('s3')
        object = s3.Object(self.s3_bucket, self.s3_input_key)
        filesize = size(object.content_length)

        print(f'Saved to s3://{self.s3_bucket}/{self.s3_output_key} with {errors} geocoding errors. Output size: {filesize}')
