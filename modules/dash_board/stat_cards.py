from datetime import datetime, timedelta
from sqlalchemy import func
from models.info_db import Info_db
from models import db


def get_increase_decrease_rate(info_db_no, period_days=30):
    now = datetime.utcnow()
    current_start = now - timedelta(days=period_days)
    previous_start = current_start - timedelta(days=period_days)

    current_count = (
        db.session.query(func.count(Info_db.user_id))
        .filter(Info_db.info_db_no == info_db_no)
        .filter(Info_db.created_at >= current_start)
        .scalar()
    )

    previous_count = (
        db.session.query(func.count(Info_db.user_id))
        .filter(Info_db.info_db_no == info_db_no)
        .filter(Info_db.created_at >= previous_start)
        .filter(Info_db.created_at < current_start)
        .scalar()
    )

    if previous_count == 0:
        return None  # 이전 데이터 없음

    rate = ((current_count - previous_count) / previous_count) * 100

    return round(rate, 2)

def get_cancellation_rate(info_db_no, period_days=30):
    now = datetime.utcnow()
    period_start = now - timedelta(days=period_days)

    cancelled_users = (
        db.session.query(func.count(Info_db.user_id))
        .filter(Info_db.info_db_no == info_db_no)
        .filter(Info_db.ended_at >= period_start)
        .filter(Info_db.ended_at <= now)
        .scalar()
    )

    active_users = (
        db.session.query(func.count(Info_db.user_id))
        .filter(Info_db.info_db_no == info_db_no)
        .filter(Info_db.started_at <= period_start)
        .filter((Info_db.ended_at.is_(None)) | (Info_db.ended_at >= period_start))
        .scalar()
    )

    if active_users == 0:
        return None

    rate = (cancelled_users / active_users) * 100

    return round(rate, 2)
