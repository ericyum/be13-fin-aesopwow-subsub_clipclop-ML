from flask import Blueprint, jsonify, Response,request, send_file
from flask_restx import Namespace, Resource, reqparse
from botocore.exceptions import NoCredentialsError, ClientError
from sqlalchemy import create_engine, MetaData, Table, select
from collections import Counter
from datetime import datetime
import pandas as pd
import io

from modules.analysis.analysis_module import module_get_s3_file

from modules.analysis.cohort import cohort_analysis, get_user_sub_info_by_year
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

# # @analysis_bp.route('', methods=['POST'])
# # def upload_s3_file():
# #     info_db_no = request.args.get('info_db_no', type=int)
# #     analysis_no = request.args.get('analysis_no', type=int)

# #     info_db = get_info_db_by_info_db_no(info_db_no)

# #     # upload_file_to_s3("local_file.txt", "python-aesop", "external/local_file.txt")
# #     file_path = "/Users/songhyeonjun/Desktop/prj_fin/be13-fin-aesopwow-subsub_clipclop-ML/routes/test.csv"
# #     object_name = None

# #     if object_name is None:
# #         object_name = file_path.split("/")[-1]

# #     try:
# #         upload_s3_file(file_path, object_name)
# #         print(f"✅ '{file_path}' has been uploaded.'")
# #     except FileNotFoundError:
# #         return jsonify({'error': 'The file was not found.'}), 403
# #     except NoCredentialsError:
# #         return jsonify({'error': 'AWS credentials not available.'}), 403

# #     return jsonify({"message": "File uploaded successfully!"})

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
    

from modules.info_column.info_column_module import get_info_columns_by_info_db_no_origin_table

target_column_map = {
    "users_watch_time_hours": "users_watch_time_hours",
    "users_subscription_type": "users_subscription_type",
    "users_favorite_genre": "users_favorite_genre",
    "users_last_login": "users_last_login"
}

@analysis_ns.route('/cohort')
class CohortAnalysis(Resource):
    @analysis_ns.response(200, "Analysis successful")
    @analysis_ns.response(400, "Bad request")
    @analysis_ns.response(500, "Server error")
    def post(self):
        """Cohort analysis API (POST, returns CSV)"""
        data = request.get_json()
        info_db_no = data.get("info_db_no")
        user_info = data.get("user_info")
        user_sub_info = data.get("user_sub_info")
        year = data.get("year")
        display_column = data.get("target_column")
        target_column = target_column_map.get(display_column)

        try:
            info_db = get_info_db_by_info_db_no(info_db_no)
            if not info_db:
                return {"success": False, "error": "Database information not found."}, 400

            info_db = info_db.to_dict()
            engine = create_engine(
                f"mysql+pymysql://{info_db['user']}:{info_db['password']}@{info_db['host']}:{info_db['port']}/{info_db['name']}"
            )

            metadata = MetaData()
            external_table = Table(user_info, metadata, autoload_with=engine)

            with engine.connect() as conn:
                query = select(external_table.c[target_column])
                result = conn.execute(query).scalars().all()

            if not result:
                return {"success": False, "error": "No data available."}, 400

            total = len(result)

            # 분석 결과 생성 (아래는 기존 로직과 동일)
            if target_column == 'users_watch_time_hours':
                buckets = {"Low": 0, "Medium": 0, "High": 0}
                for val in result:
                    if val is None:
                        continue
                    if val < 30:
                        buckets["Low"] += 1
                    elif val < 60:
                        buckets["Medium"] += 1
                    else:
                        buckets["High"] += 1
                result_data = {k: f"{v / total * 100:.0f}%" for k, v in buckets.items()}

            elif target_column == 'users_subscription_type':
                counter = Counter(result)
                result_data = {k: f"{v / total * 100:.0f}%" for k, v in counter.items()}

            elif target_column == 'users_favorite_genre':
                counter = Counter(result)
                top_10 = counter.most_common(10)
                top_genres = {k: v for k, v in top_10}
                others = total - sum(top_genres.values())
                result_data = {k: f"{v / total * 100:.0f}%" for k, v in top_genres.items()}
                if others > 0:
                    result_data["Others"] = f"{others / total * 100:.0f}%"

            elif target_column == 'users_last_login':
                months = [val.strftime('%Y-%m') for val in result if isinstance(val, datetime)]
                counter = Counter(months)
                result_data = {k: f"{v / total * 100:.0f}%" for k, v in counter.items()}

            else:
                return {"success": False, "error": "Unsupported column."}, 400

            # 결과를 DataFrame으로 변환 후 CSV로 메모리 버퍼에 저장
            df = pd.DataFrame(list(result_data.items()), columns=["Category", "Percentage"])
            output = io.StringIO()
            df.to_csv(output, index=False)
            output.seek(0)

            # CSV 파일 반환
            return send_file(
                io.BytesIO(output.getvalue().encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name='result.csv'
            )

        except Exception as e:
            return {"success": False, "error": str(e)}, 500

analyze_parser = reqparse.RequestParser()
analyze_parser.add_argument("info_db_no", type=int, required=True, help="Info DB 번호 (필수)")
analyze_parser.add_argument("user_info", type=str, required=True, help="user_info 테이블명 (필수)")
analyze_parser.add_argument("user_sub_info", type=str, required=True, help="user_sub_info 테이블명 (필수)")
analyze_parser.add_argument("year", type=int, help="기준년도")

@analysis_ns.route('/test')
class TestAnalysis(Resource):
    def get(self):
        """테스트용 API"""
        # 여기에 테스트용 로직을 추가할 수 있습니다.
        # 예를 들어, 간단한 메시지를 반환하는 API
        return {"message": "This is a test API for analysis."}, 200

    @analysis_ns.expect(analyze_parser)
    def post(self):
        args = analyze_parser.parse_args()
        info_db_no = args["info_db_no"]
        user_info = args["user_info"]
        user_sub_info = args["user_sub_info"]
        year = args["year"]

        # mapped_row = test_convert_data(info_db_no, origin_table)
        # result = cohort_analysis(mapped_row)

        # light_user, core_user, power_user = cohort_analysis(mapped_row)
        test = get_user_sub_info_by_year(info_db_no, user_info, user_sub_info, year)
        # result = {
        #     "light_user": light_user,
        #     "core_user": core_user,
        #     "power_user": power_user
        # }
        # print(result)
        # if result:
        #     return result
        # else:
        #     return jsonify({"error": "Mapped row not found"}), 404
        return jsonify(test), 200