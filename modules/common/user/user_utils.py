import pandas as pd
from datetime import datetime, timedelta
from modules.common.convert_data import convert_data

# 공통 데이터 변환 함수
def load_data(info_db_no: int, origin_table: str) -> pd.DataFrame:
    data = convert_data(info_db_no, origin_table)
    return pd.DataFrame(data)

# 전체 사용자 수
def get_entire_users(info_db_no: int, origin_table: str, company_no: int):
    df = load_data(info_db_no, origin_table)
    return df

# 신규 사용자 (30일 이내 생성)
def get_new_users(info_db_no: int, origin_table: str, company_no: int):
    df = load_data(info_db_no, origin_table)
    cutoff = pd.Timestamp(datetime.utcnow() - timedelta(days=30))
    new_users = df[
        (df['created_at'] >= cutoff)
    ]
    return new_users

# 활성 사용자 (종료일 없거나 미래)
def get_active_users(info_db_no: int, origin_table: str, company_no: int):
    df = load_data(info_db_no, origin_table)
    now = pd.Timestamp(datetime.utcnow())
    active_users = df[
        ((df['ended_at'].isna()) | (df['ended_at'] >= now))
    ]
    return active_users

# 휴면 사용자 (90일 이상 미접속)
def get_dormant_users(info_db_no: int, origin_table: str, company_no: int):
    df = load_data(info_db_no, origin_table)
    cutoff = pd.Timestamp(datetime.utcnow() - timedelta(days=90))
    dormant_users = df[
        (df['logined_at'] < cutoff)
    ]
    return dormant_users

# 해지 사용자 (종료일 과거)
def get_canceled_users(info_db_no: int, origin_table: str, company_no: int):
    df = load_data(info_db_no, origin_table)
    now = pd.Timestamp(datetime.utcnow())
    canceled_users = df[
        (df['ended_at'].notna()) &
        (df['ended_at'] < now)
    ]
    return canceled_users