from flask import Response, request, make_response, Blueprint
from flask_restx import Namespace, Resource, reqparse
from io import StringIO
import csv
from datetime import datetime, timezone
from modules.analysis.analysis_module import dashboard_s3_file, dashboard_s3_list, save_Dashboard_csv_to_s3
from modules.common.user import user_utils
from modules.dash_board.stacked_bar import get_monthly_total_subscriptions, get_monthly_cancelled_subscriptions
from modules.dash_board.stat_cards import get_increase_decrease_rate, get_cancellation_rate
from modules.dash_board.line_graph import calculate_increase_decrease_per  # 추가

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')
dashboard_ns = Namespace('dashboard', description="Dashboard related APIs")

# 파일 업로드 파서 설정
dashboard_parser = reqparse.RequestParser()
dashboard_parser.add_argument("info_db_no", type=int, required=True, help="info DB 번호 (필수)")
dashboard_parser.add_argument("user_info", type=str, required=True, help="사용자 정보 테이블명(필수)")
dashboard_parser.add_argument("user_sub_info", type=str, required=True, help="사용자 구독 정보 테이블명 (필수)")

@dashboard_ns.route('')
class TestAnalysis(Resource):
    @dashboard_ns.expect(dashboard_parser)
    def get(self):
        args = dashboard_parser.parse_args()
        info_db_no = args["info_db_no"]
        user_info = args["user_info"]
        user_sub_info = args["user_sub_info"]

        before_file_first = dashboard_s3_list(info_db_no)

        print(f"Before file first: {before_file_first}")

        now = datetime.now(timezone.utc)
        last_modified = before_file_first.get('LastModified') if before_file_first else None

        # save_Dashboard_csv_to_s3(info_db_no, user_info, user_sub_info)

        # after_file_first = dashboard_s3_list(info_db_no)
        
        if (before_file_first is None or last_modified.year != now.year or last_modified.month != now.month):
            save_Dashboard_csv_to_s3(info_db_no, user_info, user_sub_info)

            after_file_first = dashboard_s3_list(info_db_no)
        else:
            after_file_first = before_file_first
        
        file_content_key = after_file_first['Key']
        file_content = dashboard_s3_file(file_content_key)

        return Response(
            file_content,
            mimetype='text/csv',
            headers={
                "Content-Disposition": f'attachment; filename="{file_content_key}"'
            }
        )