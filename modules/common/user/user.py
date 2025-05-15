from models.info_db import Info_db
from datetime import datetime, timedelta
from sqlalchemy import func
from models import db

def get_entire_users(info_db_no):
    total_users = (db.session.query(func.count(Info_db.user))
                   .filter(Info_db.info_db_no == info_db_no)
                   .scalar())

    return total_users

def get_new_users(info_db_no):
    one_month_ago = datetime.utcnow() - timedelta(days=30)
    new_users = (db.session.query(func.count(Info_db.user))
                 .filter(Info_db.info_db_no == info_db_no)
                 .filter(Info_db.created_at >= one_month_ago)
                 .scalar())

    return new_users

def get_active_users(info_db_no):
    now = datetime.utcnow()
    active_users = (
        db.session.query(func.count(Info_db.user_id))
        .filter(Info_db.info_db_no == info_db_no)
        .filter(Info_db.Subscription_Type.isnot(None))
        .filter(Info_db.started_at <= now)
        .filter(
            (Info_db.ended_at.is_(None)) | (Info_db.ended_at >= now)
        )
        .scalar()
    )

    return active_users

def get_dormant_users(info_db_no):
    three_months_ago = datetime.utcnow() - timedelta(days=90)
    dormant_users = (db.session.query(func.count(Info_db.user))
                     .filter(Info_db.info_db_no == info_db_no)
                     .filter(Info_db.last_login < three_months_ago)
                     .scalar())

    return dormant_users