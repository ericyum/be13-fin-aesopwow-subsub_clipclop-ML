from models.user import User
from datetime import datetime, timedelta
from sqlalchemy import func
from models import db

def get_entire_users(company_no):
    total_users = (
        db.session.query(func.count(User.user_no))
        .filter(User.company_no == company_no)
        .scalar()
    )

    return total_users

def get_new_users(company_no):
    one_month_ago = datetime.utcnow() - timedelta(days=30)
    new_users = (
        db.session.query(func.count(User.user_no))
        .filter(User.company_no == company_no)
        .filter(User.created_at >= one_month_ago)
        .scalar()
    )

    return new_users

def get_active_users(company_no):
    now = datetime.utcnow()
    active_users = (
        db.session.query(func.count(User.user_no))
        .filter(User.company_no == company_no)
        .filter(User.created_at <= now)
        .filter(
            (User.logined_at.is_(None)) | (User.logined_at >= now)
        )
        .scalar()
    )

    return active_users

def get_dormant_users(company_no):
    three_months_ago = datetime.utcnow() - timedelta(days=90)
    dormant_users = (
        db.session.query(func.count(User.user_no))
        .filter(User.company_no == company_no)
        .filter(User.logined_at < three_months_ago)
        .scalar()
    )

    return dormant_users