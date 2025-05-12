from flask import Blueprint, jsonify, Response, request
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from resources.config.s3_config import S3_Config

from models.analysis import Analysis
from modules.info_db.info_db_module import get_info_db_by_info_db_no
from modules.analysis.ml_test import test_convert_data

analysis_bp = Blueprint('python-api/analysis', __name__)

@analysis_bp.route('', methods=['GET'])
def get_s3_file():
    file_name = request.args.get('file_name', type=str)
    # file_name = "local_file.txt"
    bucket_name = "python-aesop"

    try:
        s3_object = get_s3_file(bucket_name, file_name)
        return Response(
            s3_object['Body'].read(),
            mimetype=s3_object['ContentType'],
            headers={"Content-Disposition": f"attachment;filename={file_name}"}
        )
    except ClientError as e:
        return jsonify({'error': str(e)}), 404
    except NoCredentialsError:
        return jsonify({'error': 'AWS credentials not found.'}), 403

@analysis_bp.route('', methods=['POST'])
def upload_s3_file():
    info_db_no = request.args.get('info_db_no', type=int)
    analysis_no = request.args.get('analysis_no', type=int)

    info_db = get_info_db_by_info_db_no(info_db_no)

    # upload_file_to_s3("local_file.txt", "python-aesop", "external/local_file.txt")
    file_path = "/Users/songhyeonjun/Desktop/prj_fin/be13-fin-aesopwow-subsub_clipclop-ML/routes/test.csv"
    object_name = None

    if object_name is None:
        object_name = file_path.split("/")[-1]

    try:
        upload_s3_file(file_path, object_name)
        print(f"✅ '{file_path}' has been uploaded.'")
    except FileNotFoundError:
        return jsonify({'error': 'The file was not found.'}), 403
    except NoCredentialsError:
        return jsonify({'error': 'AWS credentials not available.'}), 403

    return jsonify({"message": "File uploaded successfully!"})

@analysis_bp.route('/test', methods=['GET'])
def test_convert():
    info_db_no = request.args.get('info_db_no', type=int)
    origin_table = request.args.get('origin_table', type=str)

    mapped_row = test_convert_data(info_db_no, origin_table)
    if mapped_row:
        return jsonify(mapped_row)
    else:
        return jsonify({"error": "Mapped row not found"}), 404