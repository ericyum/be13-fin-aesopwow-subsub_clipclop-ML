import pandas as pd
from datetime import datetime, timedelta, UTC
from modules.common.convert_data import convert_data
from typing import List, Dict, Tuple

# 공통 데이터 변환 함수
def load_data(info_db_no: int, origin_table: str) -> pd.DataFrame:
    data = convert_data(info_db_no, origin_table)
    return pd.DataFrame(data)

# 전체 사용자 수
def get_total_users(info_db_no: int, origin_table: str):
    df = load_data(info_db_no, origin_table)
    return df

# 신규 사용자 (30일 이내 생성)
def get_new_users(info_db_no: int, origin_table: str):
    df = load_data(info_db_no, origin_table)
    cutoff = pd.Timestamp(datetime.now(UTC) - timedelta(days=30))
    new_users = df[
        (df['created_at'] >= cutoff)
    ]
    return new_users

# 활성 사용자 (종료일 없거나 미래)
def get_active_users(info_db_no: int, origin_table: str):
    df = load_data(info_db_no, origin_table)
    now = pd.Timestamp(datetime.now(UTC))
    active_users = df[
        ((df['ended_at'].isna()) | (df['ended_at'] >= now))
    ]
    return active_users

# 휴면 사용자 (90일 이상 미접속)
def get_dormant_users(info_db_no: int, origin_table: str):
    df = load_data(info_db_no, origin_table)
    cutoff = pd.Timestamp(datetime.now(UTC) - timedelta(days=90))
    dormant_users = df[
        (df['logined_at'] < cutoff)
    ]
    return dormant_users

# 해지 사용자 (종료일 과거)
def get_canceled_users(info_db_no: int, origin_table: str):
    df = load_data(info_db_no, origin_table)
    now = pd.Timestamp(datetime.now(UTC))
    canceled_users = df[
        (df['ended_at'].notna()) &
        (df['ended_at'] < now)
    ]
    return canceled_users

# 구독 모델 판별
def determine_subscription_model(
        users: pd.DataFrame,
) -> Tuple[List[Dict], List[Dict], List[Dict]]:

    basic: List[Dict] = []
    premium: List[Dict] = []
    ultimate: List[Dict] = []

    for user in users:
        sub_type = (user.get('prev_subscription') or '').lower()
        if sub_type == 'basic':
            basic.append(user)
        elif sub_type == 'premium':
            premium.append(user)
        elif sub_type == 'ultimate':
            ultimate.append(user)

    return basic, premium, ultimate