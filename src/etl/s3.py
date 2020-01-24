import boto3

# project specific configuration parameters.
from .config import CONFIG


class S3:

    def __init__(self, region='us-west-2'):
        self.s3 = boto3.client('s3', region_name=region)

    def download(self, bucket, file_name):
        self.s3.download_file(Bucket=bucket, Key=file_name, Filename=file_name)

    def get_file(self, bucket, file_name):
        s3ref = self.s3.get_object(Bucket=bucket, Key=file_name)
        data = s3ref['Body'].read().decode('utf-8')
        return data
