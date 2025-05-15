from modules.common.s3_client import get_s3_client, bucket_name

s3 = get_s3_client()

def module_get_s3_file(bucket_name, file_name):
    s3_object = s3.get_object(Bucket=bucket_name, Key=file_name)
    return s3_object

def upload_s3_file(file_path, bucket_name, object_name=None):
    s3.upload_file(file_path, bucket_name, object_name)