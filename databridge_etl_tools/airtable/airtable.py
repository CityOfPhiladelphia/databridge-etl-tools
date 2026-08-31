import csv
import json
import os
import re
import sys
from typing import Dict, List, Optional, Type

import boto3
import click
from hurry.filesize import size
import requests


def clean_column_name(name: str) -> str:
    """Sanitize column names for standard RDBMS (e.g., PostgreSQL).

    - Replaces non-alphanumeric chars (spaces, punctuation, slashes, hyphens) with underscores.
    - Collapses consecutive underscores into a single underscore.
    - Trims leading/trailing underscores and converts to lowercase.
    """
    cleaned = re.sub(r'[^a-zA-Z0-9]+', '_', name.strip())
    cleaned = re.sub(r'_+', '_', cleaned).strip('_')
    return cleaned.lower()


class Airtable:

    def __init__(
        self,
        app_id: str,
        pat_token: str,
        table_name: str,
        s3_bucket: str,
        s3_key: str,
        add_objectid: bool,
        get_fields: Optional[str] = None,
    ):
        self.app_id = app_id
        self.pat_token = pat_token
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.offset = None
        self.rows_per_page = 1000
        self.add_objectid = add_objectid
        self.get_fields = get_fields
        self.csv_path = f'/tmp/{self.table_name}.csv'
        self.counter = 0

    def get_fieldnames(self) -> List[str]:
        '''Get field names with an initial request, sanitize them, and filter if needed.'''
        request_stmt = f'https://api.airtable.com/v0/{self.app_id}/{self.table_name}?maxRecords={self.rows_per_page}'

        if self.get_fields:
            for field in self.get_fields.split(','):
                request_stmt += f'&fields%5B%5D={field}'

        print(f'Airtable endpoint: {request_stmt}')

        response = requests.get(
            request_stmt,
            headers={'Authorization': f'Bearer {self.pat_token}'},
        )
        data = response.json()

        fieldnames = []
        try:
            for record in data['records']:
                for raw_field in record['fields'].keys():
                    cleaned_field = clean_column_name(raw_field)
                    if cleaned_field not in fieldnames:
                        fieldnames.append(cleaned_field)
        except KeyError:
            print(data)
            raise Exception(
                'Got unexpected response trying to determine headers!'
            )

        if self.add_objectid:
            fieldnames.insert(0, 'objectid')

        return fieldnames

    def get_records(self, offset: Optional[str] = None):
        '''Recursive generator to fetch paginated Airtable records.'''
        request_stmt = f'https://api.airtable.com/v0/{self.app_id}/{self.table_name}?maxRecords={self.rows_per_page}'

        if self.get_fields:
            for field in self.get_fields.split(','):
                request_stmt += f'&fields%5B%5D={field}'

        params = {'offset': offset} if offset else {}

        response = requests.get(
            request_stmt,
            headers={'Authorization': f'Bearer {self.pat_token}'},
            params=params,
        )

        data = response.json()
        yield data.get('records', [])

        if 'offset' in data:
            yield from self.get_records(offset=data['offset'])

    def process_row(self, row: Dict) -> Dict:
        """Sanitize field names and format list values to JSON."""
        cleaned_row = {}

        for key, value in row.items():
            cleaned_key = clean_column_name(key)
            if isinstance(value, list):
                cleaned_row[cleaned_key] = json.dumps(value)
            else:
                cleaned_row[cleaned_key] = value

        if self.add_objectid:
            self.counter += 1
            cleaned_row['objectid'] = self.counter

        return cleaned_row

    def load_to_s3(self):
        s3 = boto3.resource('s3')
        with open(self.csv_path, 'rb') as f:
            s3.Object(self.s3_bucket, self.s3_key).put(Body=f)

    def clean_up(self) -> None:
        if os.path.isfile(self.csv_path):
            os.remove(self.csv_path)

    def extract(self):
        fieldnames = self.get_fieldnames()

        with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f, fieldnames=fieldnames, extrasaction='ignore'
            )
            writer.writeheader()

            for records_batch in self.get_records():
                for record in records_batch:
                    row = self.process_row(record.get('fields', {}))
                    writer.writerow(row)

        num_lines = sum(1 for _ in open(self.csv_path, encoding='utf-8')) - 1
        assert num_lines > 0, 'CSV file contains 0 lines??'
        file_size = size(os.path.getsize(self.csv_path))
        print(
            f'Extraction successful. File size: {file_size}, total lines: {num_lines}'
        )

        self.load_to_s3()
        self.clean_up()
