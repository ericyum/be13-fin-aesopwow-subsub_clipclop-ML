from datetime import datetime, timedelta
from sqlalchemy import func
from models.user import User
from models import db


def get_increase_decrease_rate(company_no, period_days=30):
    now = datetime.utcnow()
    current_start = now - timedelta(days=period_days)
    previous_start = current_start - timedelta(days=period_days)

    current_count = (
        db.session.query(func.count(User.company_no))
        .filter(User.company_no == company_no)
        .filter(User.created_at >= current_start)
        .scalar()
    )

    previous_count = (
        db.session.query(func.count(User.company_no))
        .filter(User.company_no == company_no)
        .filter(User.created_at >= previous_start)
        .filter(User.created_at < current_start)
        .scalar()
    )

    if previous_count == 0:
        return None  # 이전 데이터 없음

    rate = ((current_count - previous_count) / previous_count) * 100

    return round(rate, 2)

def get_cancellation_rate(company_no, period_days=30):
    now = datetime.utcnow()
    period_start = now - timedelta(days=period_days)

    cancelled_users = (
        db.session.query(func.count(User.company_no))
        .filter(User.company_no == company_no)
        .filter(User.logined_at >= period_start)
        .filter(User.logined_at <= now)
        .scalar()
    )

    active_users = (
        db.session.query(func.count(User.company_no))
        .filter(User.company_no == company_no)
        .filter(User.created_at <= period_start)
        .filter((User.logined_at.is_(None)) | (User.logined_at >= period_start))
        .scalar()
    )

    if active_users == 0:
        return None

    rate = (cancelled_users / active_users) * 100

    return round(rate, 2)