from dateutil.relativedelta import relativedelta
import pandas as pd
from modules.common.user.user_utils import get_canceled_users, get_entire_users, determine_subscription_model
from typing import Dict, List, Tuple, Literal, Union
from datetime import datetime, UTC
from collections import defaultdict

SubscriptionType = Literal['basic', 'premium', 'ultimate']
SubscriptionData = Dict[str, Tuple[float, float, float]]

def convert_to_dataframe(data: List[Dict]) -> pd.DataFrame:
    """Dictionary 타입을 DataFrame으로 변환"""
    return pd.DataFrame(data) if isinstance(data, list) else data

def get_month_range(now: datetime, month_offset: int) -> Tuple[datetime, datetime]:
    """주어진 오프셋에 대한 월 범위 계산"""
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - relativedelta(months=month_offset)
    return month_start, month_start + relativedelta(months=1)

def calculate_percentages(*subscription_groups: List) -> Union[tuple[float, float, float], tuple[float, ...]]:
    """구독 모델 비율 계산"""
    total = sum(len(group) for group in subscription_groups)
    if total == 0:
        return 0.0, 0.0, 0.0
    return tuple(round((len(group) / total) * 100, 1) for group in subscription_groups)

def get_subscription_breakdown(data: pd.DataFrame) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """유저를 구독 타입 별로 분류"""
    return determine_subscription_model(data)

def get_monthly_subscription_data(
        info_db_no: int,
        origin_table: str,
        user_type: Literal['entire', 'cancelled']
) -> SubscriptionData:
    """월별 구독 데이터를 처리하는 함수"""
    now = datetime.now(UTC)  # 타임존 인식 UTC 객체
    monthly_data = defaultdict(lambda: (0.0, 0.0, 0.0))

    data_fetcher = get_entire_users if user_type == 'entire' else get_canceled_users

    for month_offset in range(12):
        month_start, month_end = get_month_range(now, month_offset)

        raw_data = data_fetcher(info_db_no, origin_table)
        df = convert_to_dataframe(raw_data)

        if user_type == 'cancelled':
            filtered = df[df['ended_at'].between(month_start, month_end, inclusive='left')]
        else:
            filtered = df[
                (df['created_at'] <= month_end) &
                (df['ended_at'].isna() | (df['ended_at'] >= month_start))
            ]

        basic, premium, ultimate = get_subscription_breakdown(filtered)
        percentages = calculate_percentages(basic, premium, ultimate)

        monthly_data[month_start.strftime('%Y-%m')] = percentages

    return dict(monthly_data)

def combine_subscription_data(
        active_data: SubscriptionData,
        cancelled_data: SubscriptionData,
        subscription_type: SubscriptionType
) -> Dict[str, Tuple[float, float]]:
    """특정 구독 유형에 대해 전체 유저와 해지한 유저 데이터 결합"""
    subscription_index = {'basic': 0, 'premium': 1, 'ultimate': 2}
    idx = subscription_index[subscription_type]

    combined = {}
    for month in sorted(active_data.keys()):
        active_val = active_data[month][idx]
        cancelled_val = cancelled_data[month][idx]
        combined[month] = (active_val, cancelled_val)
    return combined

def get_monthly_active_subscriptions(info_db_no: int, origin_table: str) -> SubscriptionData:
    """월별 전체 사용자 구독 비율"""
    return get_monthly_subscription_data(info_db_no, origin_table, 'entire')

def get_monthly_cancelled_subscriptions(info_db_no: int, origin_table: str) -> SubscriptionData:
    """월별 취소된 사용자 구독 비율"""
    return get_monthly_subscription_data(info_db_no, origin_table, 'cancelled')

def get_subscription_model_breakdown(
        info_db_no: int,
        origin_table: str,
        user_type: Literal['active', 'cancelled']
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """전체 사용자 기반에 대한 구독 모델 세부 정보"""
    data_fetcher = get_entire_users if user_type == 'active' else get_canceled_users
    raw_data = data_fetcher(info_db_no, origin_table)
    return get_subscription_breakdown(convert_to_dataframe(raw_data))