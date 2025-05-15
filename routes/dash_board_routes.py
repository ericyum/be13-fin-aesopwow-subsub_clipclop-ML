from flask import Blueprint, make_response, request
from io import StringIO
import csv
from datetime import datetime
from modules.common.user import user_utils
from modules.dash_board.stat_cards import get_increase_decrease_rate, get_cancellation_rate

dashboard_bp = Blueprint('python-api/dashboard', __name__)

@dashboard_bp.route('', methods=['GET'])
def dashboard_index():
    company_no = request.args.get('company_no', type=int)
    entire_users = user_utils.get_entire_users(company_no)
    new_users = user_utils.get_new_users(company_no)
    active_users = user_utils.get_active_users(company_no)
    dormant_users = user_utils.get_dormant_users(company_no)
    increase_decrease_rate = get_increase_decrease_rate(company_no)
    cancellation_rate = get_cancellation_rate(company_no)

    data = [
        {
            'metric': 'Total Users',
            'value': entire_users,
            'timestamp': datetime.utcnow().isoformat()
        },
        {
            'metric': 'New Users',
            'value': new_users,
            'timestamp': datetime.utcnow().isoformat()
        },
        {
            'metric': 'Active Users',
            'value': active_users,
            'timestamp': datetime.utcnow().isoformat()
        },
        {
            'metric': 'Dormant Users',
            'value': dormant_users,
            'timestamp': datetime.utcnow().isoformat()
        },
        {
            'metric': 'Increase Decrease Users',
            'value': increase_decrease_rate,
            'timestamp': datetime.utcnow().isoformat()
        },
        {
            'metric': 'Cancellation Rate',
            'value': cancellation_rate,
            'timestamp': datetime.utcnow().isoformat()
        }
    ]

    csv_buffer = StringIO()
    fieldnames = ['metric', 'value', 'timestamp']
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()
    csv_buffer.close()

    response = make_response(csv_data)
    response.headers['Content-Type'] = 'text/csv'
    response.headers['Content-Disposition'] = 'attachment; filename=user_metrics.csv'

    return response
