
import requests
import csv
import json
import os,sys
import boto3
import stringcase
from datetime import datetime
from hurry.filesize import size

csv.field_size_limit(sys.maxsize)

class Knack():
    '''
    Extracts a CSV from Knack (https://dashboard.knack.com/apps)
    ''' 
    def __init__(self,
                 knack_objectid,
                 app_id, 
                 api_key, 
                 s3_bucket, 
                 s3_key,
                 rename_fields,
                 **kwargs):
        self.knack_objectid = knack_objectid
        self.app_id = app_id
        self.api_key = api_key
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.rename_fields = rename_fields
        self.csv_path = '/tmp/output.csv'

    def get_type(self, knack_type):
        if knack_type == 'boolean':
            return 'boolean'
        if knack_type == 'number':
            return 'number'
        if knack_type == 'auto_increment':
            return 'integer'
        if knack_type == 'date_time':
            return 'datetime'
        if knack_type == 'multiple_choice':
            return 'array'
        if knack_type == 'address':
            return 'object'
        if knack_type == 'connection':
            return 'array'
        return 'string'

    def convert_knack_schema(self, knack_fields):
        fields = [{
            'name': 'id',
            'type': 'string',
            'constraints': {
                'required': True
            }
        }]

        for field in knack_fields:
            name = (
                stringcase
                .snakecase(field['label'])
                .replace(' ', '')
                .replace('(', '')
                .replace(')', '')
                .replace('__', '_')
                .replace('_i_d', '_id')
            )

            field_def = {
                'name': name,
                'knack_key': field['key'],
                'knack_type': field['type'],
                'type': self.get_type(field['type'])
            }

            if field['required'] == True:
                field_def['constraints'] = { 'required': True }

            fields.append(field_def)

        return {
            'primaryKey': ['id'],
            'missingValues': [''],
            'fields': fields
        }

    def get_schema(self):
        response = requests.get(
            f'https://api.knack.com/v1/objects/{self.knack_objectid}/fields',
            headers={
                'X-Knack-Application-Id': self.app_id,
                'X-Knack-REST-API-KEY': self.api_key
            })

        try:
            data = response.json()
        except Exception as e:
            print(f'Exception reading data from Knack. URL used: https://api.knack.com/v1/objects/{self.knack_objectid}/fields')
            print(f'Knack response: {response.text}')
            raise e

        return self.convert_knack_schema(data['fields'])

    def get_records(self, page=1, rows_per_page=1000):
        response = requests.get(
            f'https://api.knack.com/v1/objects/{self.knack_objectid}/records',
            params={
                'rows_per_page': rows_per_page,
                'page': page
            },
            headers={
                'X-Knack-Application-Id': self.app_id,
                'X-Knack-REST-API-KEY':  self.api_key
            })

        data = response.json()

        if not data['records']:
            raise Exception(f"Failed to fetch data. Status Code: {response.status_code}. Reason: {response.text}")

        yield data['records']

        if int(data['current_page']) < data['total_pages']:
            yield from self.get_records(page=int(data['current_page']) + 1)

    def convert_type(self, local_type, knack_type, value):
        if value == None or value == '':
            return None
        if knack_type == 'connection':
            return json.dumps(list(map(lambda x: x['id'], value)))
        if knack_type == 'phone':
            return value['full']
        if knack_type == 'date_time':
            return datetime.strptime(value['timestamp'], '%m/%d/%Y %I:%M %p').isoformat() + 'Z'
        if local_type == 'array':
            if not isinstance(value, list):
                return json.dumps([value])
            return json.dumps(value)
        if local_type == 'object':
            return json.dumps(value)
        return value


    def convert_to_csv_row(self, schema, record):
        out = {}

        for field in schema['fields']:
            if 'knack_key' in field and (field['knack_key'] + '_raw') in record:
                value = record[field['knack_key'] + '_raw']
            elif 'knack_key' in field and field['knack_key'] in record:
                value = record[field['knack_key']]
            else:
                value = record[field['name']]

            if 'knack_type' in field:
                out[field['name']] = self.convert_type(field['type'], field['knack_type'], value)
            else:
                out[field['name']] = value

        return out

    def rename_csv_fields(self):
        # Convert rename_fields string into a dict
        rename_dict = dict(field.split(':') for field in self.rename_fields.split(','))

        # Read the CSV file
        with open(self.csv_path, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            rows = list(reader)

        # Get the header and rename fields
        header = rows[0]
        print("Original header:", header)
        updated_header = [rename_dict.get(field, field) for field in header]
        print("Updated header:", updated_header)

        # Write the updated CSV back
        with open(self.csv_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(updated_header)  # Write the updated header
            writer.writerows(rows[1:])  # Write the remaining data

    def load_to_s3(self):
        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).put(Body=open(self.csv_path, 'rb'))

    def extract(self):
        schema = self.get_schema()

        # Knack API Endpoint
        endpoint = f'https://api.knack.com/v1/objects/{self.knack_objectid}/records'
        print(f'Starting extract from Knack endpoint: {endpoint}, app_id: {self.app_id}')

        headers = []
        for field in schema['fields']:
            headers.append(field['name'])
 
        with open(self.csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)

            writer.writeheader()

            for records_batch in self.get_records():
                for record in records_batch:
                    out_record = self.convert_to_csv_row(schema, record)
                    writer.writerow(out_record)
        
        if self.rename_fields:
            self.rename_csv_fields()
                    
        num_lines = sum(1 for _ in open(self.csv_path)) - 1
        assert num_lines > 0, 'CSV file contains 0 lines??'
        file_size = size(os.path.getsize(self.csv_path))
        print(f'Extraction successful? File size: {file_size}, total lines: {num_lines}')
        self.load_to_s3()

        
