from dotenv import load_dotenv
import os

load_dotenv()  # .env 파일을 로드

class S3_Config:
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access = os.getenv('AWS_SECRET_ACCESS')
    region_name = os.getenv('REGION_NAME')
    bucket_name = os.getenv('BUCKET_NAME')