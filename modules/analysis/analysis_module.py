import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from resources.config.s3_config import S3_Config

bucket_name = S3_Config.bucket_name
s3 = boto3.client('s3',
                    aws_access_key_id=S3_Config.aws_access_key_id,
                    aws_secret_access_key=S3_Config.aws_secret_access,
                    region_name=S3_Config.region_name)

def module_get_s3_file(bucket_name, file_name):
    s3_object = s3.get_object(Bucket=bucket_name, Key=file_name)
    return s3_object

def upload_s3_file(file_path, bucket_name, object_name=None):
    s3.upload_file(file_path, bucket_name, object_name)