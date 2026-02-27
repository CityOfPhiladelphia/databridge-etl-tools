import boto3
import os

def _interact_with_s3(self, method: str, path: str, s3_key: str): 
    '''
    - method should be one of "get", "load"
    '''
    self.logger.info(f"{method.upper()}-ing file: s3://{self.s3_bucket}/{s3_key}")

    s3 = boto3.resource('s3')
    if method == 'get': 
        s3.Object(self.s3_bucket, s3_key).download_file(path)
        self.logger.info(f'File successfully downloaded from S3 to {path}\n')
    elif method == 'load': 
        s3.Object(self.s3_bucket, s3_key).put(Body=open(path, 'rb'))
        self.logger.info(f'File successfully uploaded from {path} to S3\n')

def get_json_schema_from_s3(self):
    _interact_with_s3(self, 'get', self.json_schema_path, self.json_schema_s3_key)

def get_csv_from_s3(self):
    _interact_with_s3(self, 'get', self.csv_path, self.s3_key)

def load_json_schema_to_s3(self):
    with open(self.json_schema_path, 'w') as f:
        f.write(self.export_json_schema)
    
    if not self.local_json_schema_path:
        _interact_with_s3(self, 'load', self.json_schema_path, self.json_schema_s3_key)
    else:
        os.popen(f'cp {self.json_schema_path} {self.local_json_schema_path}') 
        print(f'Saved json schema to {self.local_json_schema_path}')

def load_csv_to_s3(self, path):
    '''Path of file to load - generally should be self.csv_path'''
    _interact_with_s3(self, 'load', path, self.s3_key)
