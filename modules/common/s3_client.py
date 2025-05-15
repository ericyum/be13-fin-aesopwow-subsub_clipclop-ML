import boto3
from resources.config.s3_config import S3_Config

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=S3_Config.aws_access_key_id,
        aws_secret_access_key=S3_Config.aws_secret_access,
        region_name=S3_Config.region_name
    )

bucket_name = S3_Config.bucket_name
