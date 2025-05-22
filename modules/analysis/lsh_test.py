from flask import request
from flask_restx import Namespace, Resource, reqparse
from sqlalchemy import create_engine, MetaData, Table, select
from collections import Counter
from datetime import datetime

from modules.info_db.info_db_module import get_info_db_by_info_db_no
from modules.info_column.info_column_module import get_info_columns_by_info_db_no_origin_table

cohort_ns = Namespace("cohort", description="코호트 분석 관련 API")

# ✅ 한국어 → 영문 컬럼명 매핑
target_column_map = {
    "누적시청시간": "users_watch_time_hours",
    "구독 유형": "users_subscription_type",
    "장르": "users_favorite_genre",
    "최근 접속일": "users_last_login"
}

# ✅ Request parser 설정: 한국어 드롭다운
analyze_parser = reqparse.RequestParser()
analyze_parser.add_argument("info_db_no", type=int, required=True, help="Info DB 번호 (필수)")
analyze_parser.add_argument("origin_table", type=str, required=True, help="원본 테이블명 (필수)")
analyze_parser.add_argument(
    "target_column",
    type=str,
    choices=list(target_column_map.keys()),  # ✅ 한국어 드롭다운 항목만 보이게
    required=True,
    help="분석 기준 컬럼명 (드롭다운 선택)"
)

@cohort_ns.route('/analyze')
class CohortByInfoDB(Resource):
    @cohort_ns.expect(analyze_parser)
    @cohort_ns.response(200, "성공")
    @cohort_ns.response(400, "입력 오류 또는 DB 정보 없음")
    @cohort_ns.response(500, "서버 에러")
    def get(self):
        """코호트 분석: 입력한 컬럼 기준으로 % 비율 반환"""
        args = analyze_parser.parse_args()
        info_db_no = args["info_db_no"]
        origin_table = args["origin_table"]
        display_column = args["target_column"]  # '누적시청시간' 등
        target_column = target_column_map.get(display_column)  # 실제 DB 컬럼명

        try:
            # 1. 외부 DB 정보 조회
            info_db = get_info_db_by_info_db_no(info_db_no)
            if not info_db:
                return {"success": False, "error": "해당 info_db_no에 대한 DB 정보가 없습니다."}, 400

            info_db = info_db.to_dict()
            engine = create_engine(
                f"mysql+pymysql://{info_db['user']}:{info_db['password']}@{info_db['host']}:{info_db['port']}/{info_db['name']}"
            )
            metadata = MetaData()
            external_table = Table(origin_table, metadata, autoload_with=engine)

            # 2. 쿼리 실행
            with engine.connect() as conn:
                query = select(external_table.c[target_column])
                result = conn.execute(query).scalars().all()

            if not result:
                return {"success": False, "error": "데이터가 없습니다."}, 400

            total = len(result)

            # 3. 기준별 분석
            if target_column == 'users_watch_time_hours':
                buckets = {"낮음": 0, "중간": 0, "높음": 0}
                for val in result:
                    if val is None:
                        continue
                    if val < 300:
                        buckets["낮음"] += 1
                    elif val < 700:
                        buckets["중간"] += 1
                    else:
                        buckets["높음"] += 1
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
                    result_data["기타"] = f"{others / total * 100:.0f}%"

            elif target_column == 'users_last_login':
                months = [val.strftime('%Y-%m') for val in result if isinstance(val, datetime)]
                counter = Counter(months)
                result_data = {k: f"{v / total * 100:.0f}%" for k, v in counter.items()}

            else:
                return {"success": False, "error": "지원하지 않는 target_column입니다."}, 400

            return {"success": True, "result": result_data}, 200

        except Exception as e:
            return {"success": False, "error": str(e)}, 500

