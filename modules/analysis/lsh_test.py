from flask import request, send_file
from flask_restx import Namespace, Resource
from sqlalchemy import create_engine, MetaData, Table, select
from collections import Counter
from datetime import datetime
import pandas as pd
import io

from modules.info_db.info_db_module import get_info_db_by_info_db_no
from modules.info_column.info_column_module import get_info_columns_by_info_db_no_origin_table

cohort_ns = Namespace("cohort", description="Cohort analysis related APIs")

target_column_map = {
    "users_watch_time_hours": "users_watch_time_hours",
    "users_subscription_type": "users_subscription_type",
    "users_favorite_genre": "users_favorite_genre",
    "users_last_login": "users_last_login"
}

@cohort_ns.route('/analyze')
class CohortAnalysis(Resource):
    @cohort_ns.response(200, "Analysis successful")
    @cohort_ns.response(400, "Bad request")
    @cohort_ns.response(500, "Server error")
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
                    if val < 200:
                        buckets["Low"] += 1
                    elif val < 700:
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
