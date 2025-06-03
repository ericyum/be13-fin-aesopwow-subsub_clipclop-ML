from ctypes import c_buffer
import re
from flask import Blueprint, jsonify, Response, make_response,request, send_file
from flask_restx import Namespace, Resource, reqparse
from botocore.exceptions import NoCredentialsError, ClientError
from sqlalchemy import create_engine, MetaData, Table, select
from collections import Counter
from datetime import datetime
import pandas as pd
import io

from modules.analysis.analysis_module import cohort_list_s3_files, cohort_s3_file

from modules.analysis.cohort import analysis_cohort_FavGenre, analysis_cohort_LastLogin, analysis_cohort_PCL, analysis_cohort_SubscriptionType
from modules.info_db.info_db_module import get_info_db_by_info_db_no
from modules.analysis.ml_test import test_convert_data, test_ml

analysis_bp = Blueprint('analysis', __name__, url_prefix='/analysis')
analysis_ns = Namespace('analysis', description="Analysis related APIs")

# @analysis_bp.route('', methods=['GET'])
# def get_s3_file():
#     file_name = request.args.get('file_name', type=str)
#     # file_name = "local_file.txt"
#     bucket_name = "python-aesop"

#     try:
#         s3_object = module_get_s3_file(bucket_name, file_name)
#         return Response(
#             s3_object['Body'].read(),
#             mimetype=s3_object['ContentType'],
#             headers={"Content-Disposition": f"attachment;filename={file_name}"}
#         )
#     except ClientError as e:
#         return jsonify({'error': str(e)}), 404
#     except NoCredentialsError:
#         return jsonify({'error': 'AWS credentials not found.'}), 403

# @analysis_bp.route('', methods=['POST'])
# def upload_s3_file():
#     info_db_no = request.args.get('info_db_no', type=int)
#     analysis_no = request.args.get('analysis_no', type=int)

#     info_db = get_info_db_by_info_db_no(info_db_no)

#     # upload_file_to_s3("local_file.txt", "python-aesop", "external/local_file.txt")
#     file_path = "/Users/songhyeonjun/Desktop/prj_fin/be13-fin-aesopwow-subsub_clipclop-ML/routes/test.csv"
#     object_name = None

#     if object_name is None:
#         object_name = file_path.split("/")[-1]

#     try:
#         upload_s3_file(file_path, object_name)
#         print(f"✅ '{file_path}' has been uploaded.'")
#     except FileNotFoundError:
#         return jsonify({'error': 'The file was not found.'}), 403
#     except NoCredentialsError:
#         return jsonify({'error': 'AWS credentials not available.'}), 403

#     return jsonify({"message": "File uploaded successfully!"})

# @analysis_bp.route('/segment', methods=['POST'])
# def upload_s3_file():
#     info_db_no = request.args.get('info_db_no', type=int)
#     origin_table = request.args.get('origin_table', type=str)

#     mapped_row = test_convert_data(info_db_no, origin_table)
#     if mapped_row is None:
#         return jsonify({"error": "Mapped row not found"}), 404

#     # upload_file_to_s3("local_file.txt", "python-aesop", "external/local_file.txt")
#     file_path = "/Users/songhyeonjun/Desktop/prj_fin/be13-fin-aesopwow-subsub_clipclop-ML/routes/test.csv"
#     object_name = None

#     if object_name is None:
#         object_name = file_path.split("/")[-1]

#     try:
#         upload_s3_file(file_path, object_name)
#         print(f"✅ '{file_path}' has been uploaded.'")
#     except FileNotFoundError:
#         return jsonify({'error': 'The file was not found.'}), 403
#     except NoCredentialsError:
#         return jsonify({'error': 'AWS credentials not available.'}), 403

#     return jsonify({"message": "File uploaded successfully!"})


# @analysis_bp.route('/test', methods=['GET'])
# def test_convert():
#     info_db_no = request.args.get('info_db_no', type=int)
#     origin_table = request.args.get('origin_table', type=str)

#     mapped_row = test_convert_data(info_db_no, origin_table)
#     if mapped_row:
#         return jsonify(mapped_row)
#     else:
#         return jsonify({"error": "Mapped row not found"}), 404
    

# from modules.info_column.info_column_module import get_info_columns_by_info_db_no_origin_table

# target_column_map = {
#     "users_watch_time_hours": "users_watch_time_hours",
#     "users_subscription_type": "users_subscription_type",
#     "users_favorite_genre": "users_favorite_genre",
#     "users_last_login": "users_last_login"
# }

# @analysis_ns.route('/cohort')
# class CohortAnalysis(Resource):
#     @analysis_ns.response(200, "Analysis successful")
#     @analysis_ns.response(400, "Bad request")
#     @analysis_ns.response(500, "Server error")
#     def post(self):
#         """Cohort analysis API (POST, returns CSV)"""
#         data = request.get_json()
#         info_db_no = data.get("info_db_no")
#         user_info = data.get("user_info")
#         user_sub_info = data.get("user_sub_info")
#         year = data.get("year")
#         display_column = data.get("target_column")
#         target_column = target_column_map.get(display_column)

#         try:
#             info_db = get_info_db_by_info_db_no(info_db_no)
#             if not info_db:
#                 return {"success": False, "error": "Database information not found."}, 400

#             info_db = info_db.to_dict()
#             engine = create_engine(
#                 f"mysql+pymysql://{info_db['user']}:{info_db['password']}@{info_db['host']}:{info_db['port']}/{info_db['name']}"
#             )

#             metadata = MetaData()
#             external_table = Table(user_info, metadata, autoload_with=engine)

#             with engine.connect() as conn:
#                 query = select(external_table.c[target_column])
#                 result = conn.execute(query).scalars().all()

#             if not result:
#                 return {"success": False, "error": "No data available."}, 400

#             total = len(result)

#             # 분석 결과 생성 (아래는 기존 로직과 동일)
#             if target_column == 'users_watch_time_hours':
#                 buckets = {"Low": 0, "Medium": 0, "High": 0}
#                 for val in result:
#                     if val is None:
#                         continue
#                     if val < 200:
#                         buckets["Low"] += 1
#                     elif val < 700:
#                         buckets["Medium"] += 1
#                     else:
#                         buckets["High"] += 1
#                 result_data = {k: f"{v / total * 100:.0f}%" for k, v in buckets.items()}

#             elif target_column == 'users_subscription_type':
#                 counter = Counter(result)
#                 result_data = {k: f"{v / total * 100:.0f}%" for k, v in counter.items()}

#             elif target_column == 'users_favorite_genre':
#                 counter = Counter(result)
#                 top_10 = counter.most_common(10)
#                 top_genres = {k: v for k, v in top_10}
#                 others = total - sum(top_genres.values())
#                 result_data = {k: f"{v / total * 100:.0f}%" for k, v in top_genres.items()}
#                 if others > 0:
#                     result_data["Others"] = f"{others / total * 100:.0f}%"

#             elif target_column == 'users_last_login':
#                 months = [val.strftime('%Y-%m') for val in result if isinstance(val, datetime)]
#                 counter = Counter(months)
#                 result_data = {k: f"{v / total * 100:.0f}%" for k, v in counter.items()}

#             else:
#                 return {"success": False, "error": "Unsupported column."}, 400

#             # 결과를 DataFrame으로 변환 후 CSV로 메모리 버퍼에 저장
#             df = pd.DataFrame(list(result_data.items()), columns=["Category", "Percentage"])
#             output = io.StringIO()
#             df.to_csv(output, index=False)
#             output.seek(0)

#             # CSV 파일 반환
#             return send_file(
#                 io.BytesIO(output.getvalue().encode()),
#                 mimetype='text/csv',
#                 as_attachment=True,
#                 download_name='result.csv'
#             )

#         except Exception as e:
#             return {"success": False, "error": str(e)}, 500

cohort_get_one = reqparse.RequestParser()
cohort_get_one.add_argument("info_db_no", type=int, required=True, help="Info DB 번호 (필수)")
cohort_get_one.add_argument("analysis_type", type=str, required=True, help="분석 타입(PCL, SubscriptionType, FavGenre, LastLogin)(필수)")
cohort_get_one.add_argument("filename", type=str, required=True, help="S3 파일 이름 (필수)")

cohort_post = reqparse.RequestParser()
cohort_post.add_argument("info_db_no", type=int, required=True, help="Info DB 번호 (필수)")
cohort_post.add_argument("target_table_user", type=str, required=True, help="유저 테이블명(user_info) (필수)")
cohort_post.add_argument("target_table_sub", type=str, required=False, help="구독 테이블명(user_sub_info) (필수)")
cohort_post.add_argument("analysis_type", type=str, required=True, help="분석 타입(PCL, SubscriptionType, FavGenre, LastLogin)(필수)")
cohort_post.add_argument("target_date", type=str, required=True, help="분석 날짜 (YYYY-MM-DD 형식, 필수)")

@analysis_ns.route('/cohort')
class CohortAnalysis(Resource):
    @analysis_ns.expect(cohort_get_one)
    def get(self):
        args = cohort_get_one.parse_args()
        info_db_no = args["info_db_no"]
        analysis_type = args["analysis_type"]
        filename = args["filename"]

        file_content = cohort_s3_file(info_db_no, analysis_type, filename)

        return Response(
            file_content,
            mimetype='text/csv',
            headers={
                "Content-Disposition": f"attachment; filename={filename}.csv"
            }
        )

    @analysis_ns.expect(cohort_post)
    def post(self):
        args = cohort_post.parse_args()
        info_db_no = args["info_db_no"]
        target_table_user = args["target_table_user"]
        target_table_sub = args["target_table_sub"]
        analysis_type = args["analysis_type"]
        target_date = args.get("target_date")

        if analysis_type == "PCL":
            result = analysis_cohort_PCL(info_db_no, target_table_user, target_date)
        elif analysis_type == "SubscriptionType":
            result = analysis_cohort_SubscriptionType(info_db_no, target_table_user, target_date)
        elif analysis_type == "FavGenre":
            result = analysis_cohort_FavGenre(info_db_no, target_table_user, target_table_sub, target_date)
        elif analysis_type == "LastLogin":
            result = analysis_cohort_LastLogin(info_db_no, target_table_user, target_table_sub, target_date)
        else:
            return jsonify({'success': False, 'message': '잘못된 분석 타입입니다.'}), 500
            
        if result:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False}), 500
        
cohort_get_list = reqparse.RequestParser()
cohort_get_list.add_argument("info_db_no", type=int, required=True, help="Info DB 번호(필수)")
cohort_get_list.add_argument("analysis_type", type=str, required=False, help="분석 타입(PCL, SubscriptionType, FavGenre, LastLogin)")

@analysis_ns.route('/cohort/list')
class CohortAnalysis(Resource):
    @analysis_ns.expect(cohort_get_list)
    def get(self):
        args = cohort_get_list.parse_args()
        info_db_no = args["info_db_no"]
        analysis_type = args["analysis_type"]

        file_list = cohort_list_s3_files(info_db_no, analysis_type)
        return jsonify(file_list)