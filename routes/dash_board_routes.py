from flask import request, make_response, Blueprint
from io import StringIO
import csv
from datetime import datetime, UTC
from modules.common.user import user_utils
from modules.dash_board.stacked_bar import get_monthly_total_subscriptions, get_monthly_cancelled_subscriptions
from modules.dash_board.stat_cards import get_increase_decrease_rate, get_cancellation_rate
from modules.devide.subscription import get_subscription_data

dashboard_bp = Blueprint('python-api/dashboard', __name__)

@dashboard_bp.route('', methods=['GET'])
def dashboard_index():
    info_db_no = request.args.get('info_db_no', type=int)
    origin_table = request.args.get('origin_table', type=str)

    # 1. 기본 지표
    metrics = {
        'entire_users': user_utils.get_total_users(info_db_no, origin_table),
        'new_users': user_utils.get_new_users(info_db_no, origin_table),
        'active_users': user_utils.get_active_users(info_db_no, origin_table),
        'dormant_users': user_utils.get_dormant_users(info_db_no, origin_table),
        'increase_decrease_rate': get_increase_decrease_rate(info_db_no, origin_table),
        'cancellation_rate': get_cancellation_rate(info_db_no, origin_table)
    }

    # 2. 월별 스택바 데이터 (예: 구독 모델별 비율)
    monthly_total = get_monthly_total_subscriptions(info_db_no, origin_table)
    monthly_cancelled = get_monthly_cancelled_subscriptions(info_db_no, origin_table)
    new_users_data = get_subscription_data(info_db_no, origin_table, 'new', False)

    # 3. CSV 작성
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)

    # 3-1. 메트릭 헤더 및 데이터
    writer.writerow(['metric', 'value', 'timestamp'])
    now_utc = datetime.now(UTC).isoformat()
    for key, value in metrics.items():
        writer.writerow([key.replace('_', ' ').title(), value, now_utc])

    # 3-2. 월별 스택바 헤더 및 데이터
    writer.writerow([])  # 빈 줄로 구분
    writer.writerow(['month', 'type', 'basic(%)', 'premium(%)', 'ultimate(%)'])

    # 전체 사용자
    for month, (basic, premium, ultimate) in monthly_total.items():
        writer.writerow([month, 'active', basic, premium, ultimate])

    # 해지 사용자
    for month, (basic, premium, ultimate) in monthly_cancelled.items():
        writer.writerow([month, 'cancelled', basic, premium, ultimate])

    # 신규 사용자 (SubscriptionData)
    writer.writerow([])  # 빈 줄로 구분
    writer.writerow(['month', 'type', 'basic(%)', 'premium(%)', 'ultimate(%)'])
    for month, (basic, premium, ultimate) in new_users_data.items():
        writer.writerow([month, 'new', basic, premium, ultimate])

    # 4. 응답 반환
    response = make_response(csv_buffer.getvalue())
    response.headers['Content-Type'] = 'text/csv'
    response.headers['Content-Disposition'] = f'attachment; filename={origin_table}_metrics.csv'

    return response