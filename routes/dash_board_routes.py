from flask import Blueprint, make_response, request
from io import StringIO
import csv
from datetime import datetime
from modules.common.user import user_utils
from modules.dash_board.stat_cards import get_increase_decrease_rate, get_cancellation_rate

dashboard_bp = Blueprint('python-api/dashboard', __name__)

@dashboard_bp.route('', methods=['GET'])
def dashboard_index():
    info_db_no = request.args.get('info_db_no', type=int)
    origin_table = request.args.get('origin_table', type=str)  # 테이블명 추가

    metrics = {
        'entire_users': user_utils.get_entire_users(info_db_no, origin_table),
        'new_users': user_utils.get_new_users(info_db_no, origin_table),
        'active_users': user_utils.get_active_users(info_db_no, origin_table),
        'dormant_users': user_utils.get_dormant_users(info_db_no, origin_table),
        'increase_decrease_rate': get_increase_decrease_rate(info_db_no, origin_table),
        'cancellation_rate': get_cancellation_rate(info_db_no, origin_table)
    }

    csv_buffer = StringIO()
    fieldnames = ['metric', 'value', 'timestamp']
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows([{
        'metric': key.replace('_', ' ').title(),
        'value': value,
        'timestamp': datetime.utcnow().isoformat()
    } for key, value in metrics.items()])

    response = make_response(csv_buffer.getvalue())
    response.headers['Content-Type'] = 'text/csv'
    response.headers['Content-Disposition'] = f'attachment; filename={origin_table}_metrics.csv'

    return response
