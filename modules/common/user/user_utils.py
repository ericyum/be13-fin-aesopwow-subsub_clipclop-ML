import pandas as pd
from datetime import datetime, timedelta, timezone
from modules.common.convert_data import convert_data
from typing import  List, Dict, Tuple, Union

# 공통 데이터 변환 함수
def load_data(info_db_no: int, origin_table: str) -> pd.DataFrame:
    data = convert_data(info_db_no, origin_table)
    df = pd.DataFrame(data)

    for col in ['created_at', 'ended_at', 'logined_at']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.tz_localize(None)  # timezone 제거해서 naive datetime으로 변환
        else:
            df[col] = pd.NaT

    return df

# 전체 사용자 수
def get_total_users(info_db_no: int, origin_table: str):
    df = load_data(info_db_no, origin_table)
    return df

# 신규 사용자 (30일 이내 생성)
def get_new_users(info_db_no: int, origin_table: str):
    df = load_data(info_db_no, origin_table)
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)
    new_users = df[df['created_at'] >= cutoff]
    return new_users

# 활성 사용자 (종료일 없거나 미래)
def get_active_users(info_db_no: int, origin_table: str):
    df = load_data(info_db_no, origin_table)
    now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive datetime

    if 'ended_at' not in df.columns:
        return df

    active_users = df[(df['ended_at'].isna()) | (df['ended_at'] >= now)]
    return active_users

# 휴면 사용자 (30일 이상 미접속)
def get_dormant_users(info_db_no: int, origin_table: str):
    df = load_data(info_db_no, origin_table)
    cutoff = datetime.now().replace(tzinfo=None) - timedelta(days=30)

    # logined_at tz 체크는 load_data에서 이미 tz 제거했으니 생략 가능
    dormant_users = df[df['logined_at'] < cutoff]
    return dormant_users

# 해지 사용자 (종료일 과거)
def get_canceled_users(info_db_no: int, origin_table: str):
    df = load_data(info_db_no, origin_table)
    now = datetime.now(timezone.utc).replace(tzinfo=None)  # datetime으로 맞춤

    canceled_users = df[
        (df['ended_at'].notna()) &
        (df['ended_at'] < now)
    ]
    return canceled_users

# 구독 모델 판별

def determine_subscription_model(
    users: pd.DataFrame,
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """유저를 basic, standard, premium 구독 타입으로 분류"""

    basic: List[Dict] = []
    standard: List[Dict] = []
    premium: List[Dict] = []

    for _, user in users.iterrows():
        sub_type = (user.get('subscription_type') or '').strip().lower()

        if sub_type == 'basic':
            basic.append(user.to_dict())
        elif sub_type == 'standard':
            standard.append(user.to_dict())
        elif sub_type == 'premium':
            premium.append(user.to_dict())

    return basic, standard, premium

def calculate_percentages(*subscription_groups: List) -> Union[tuple[float, float, float], tuple[float, ...]]:
    """구독 모델 비율 계산"""
    total = sum(len(group) for group in subscription_groups)
    if total == 0:
        return 0.0, 0.0, 0.0
    return tuple(round((len(group) / total) * 100, 1) for group in subscription_groups)