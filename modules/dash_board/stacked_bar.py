from modules.common.user.user_utils import get_canceled_users, get_total_users
from typing import Dict, List, Tuple
from modules.devide.subscription import SubscriptionData, SubscriptionType, user_type
from modules.common.utils.util_module import convert_to_dataframe
from modules.devide.subscription import get_subscription_data, get_subscription_breakdown
import pandas as pd


def combine_subscription_data(
        active_data: SubscriptionData,
        cancelled_data: SubscriptionData,
        subscription_type: SubscriptionType
) -> Dict[str, Tuple[float, float]]:
    """특정 구독 유형에 대해 전체 유저와 해지한 유저 데이터 결합"""
    subscription_index = {'basic': 0, 'standard': 1, 'premium': 2}
    idx = subscription_index[subscription_type]

    combined = {}
    for month in sorted(active_data.keys()):
        total_val = active_data[month][idx]
        cancelled_val = cancelled_data[month][idx]
        combined[month] = (total_val, cancelled_val)
    return combined


def get_monthly_total_subscriptions(info_db_no: int, origin_table: str) -> SubscriptionData:
    """월별 전체 사용자 구독 비율"""
    return get_subscription_data(info_db_no, origin_table, 'total', True)


def get_monthly_cancelled_subscriptions(info_db_no: int, origin_table: str) -> SubscriptionData:
    """월별 취소된 사용자 구독 비율"""
    return get_subscription_data(info_db_no, origin_table, 'cancelled', True)


def get_subscription_model_breakdown(
        info_db_no: int,
        origin_table: str,
        users_type: user_type
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """전체 사용자 기반에 대한 구독 모델 세부 정보"""
    data_fetcher = get_total_users if users_type == 'total' else get_canceled_users
    raw_data = data_fetcher(info_db_no, origin_table)

    # 만약 raw_data에 날짜 컬럼이 있다면 DataFrame 변환 후 날짜 타입 변환
    df = convert_to_dataframe(raw_data)
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'])

    return get_subscription_breakdown(df)