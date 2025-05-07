from flask import Blueprint, jsonify
from models.analysis import Analysis
import boto3
from botocore.exceptions import NoCredentialsError
from resources.config.s3_config import S3_Config

analysis_bp = Blueprint('python-api/analysis', __name__)

@analysis_bp.route('', methods=['GET'])
def get_analysis_all():
    analysises = Analysis.query.all()
    return jsonify([analysis.to_dict() for analysis in analysises])

s3 = boto3.client('s3',
                    aws_access_key_id=S3_Config.aws_access_key_id,
                    aws_secret_access_key=S3_Config.aws_secret_access,
                    region_name=S3_Config.region_name) 

@analysis_bp.route('', methods=['POST'])
def upload_s3_file():
    # upload_file_to_s3("local_file.txt", "python-aesop", "external/local_file.txt")
    file_path = "/Users/songhyeonjun/Desktop/prj_fin/be13-fin-aesopwow-subsub_clipclop-ML/routes/local_file.txt"
    bucket_name = "python-aesop"
    object_name = None

    if object_name is None:
        object_name = file_path.split("/")[-1]

    try:
        s3.upload_file(file_path, bucket_name, object_name)
        print(f"✅ '{file_path}' has been uploaded to '{bucket_name}/{object_name}'")
    except FileNotFoundError:
        print("❌ The file was not found.")
    except NoCredentialsError:
        print("❌ AWS credentials not available.")

    return jsonify({"message": "File uploaded successfully!"})