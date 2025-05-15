from models.info_db import User

def get_info_db_all():
    info_dbs = User.query.all()
    return info_dbs

def get_info_db_by_company(company_no):
    info_dbs = User.query.filter_by(company_no=company_no).all()
    return info_dbs

def get_info_db_by_info_db_no(info_db_no):
    info_db = User.query.filter_by(info_db_no=info_db_no).first()
    return info_db


