from typing import Literal, Dict, Tuple, List, Union
from collections import defaultdict
from datetime import datetime, UTC
import pandas as pd
from modules.common.user.user_utils import get_canceled_users, get_total_users, calculate_percentages, determine_subscription_model
from modules.common.utils.util_module import get_month_range, convert_to_dataframe

SubscriptionType = Literal['basic', 'premium', 'ultimate']
SubscriptionData = Dict[str, Tuple[float, float, float]]
user_type = Literal['total', 'cancelled', 'new', 'active', 'dormant']

def get_subscription_data(
    info_db_no: int,
    origin_table: str,
    users_type: user_type,
    monthly: bool = False
) -> Union[SubscriptionData, Tuple[float, float, float]]:
    """구독 데이터를 집계하는 함수 (월별 또는 전체)"""
    # 현재 시간 미리 계산
    now = datetime.now(UTC)
    one_year_ago = now - pd.DateOffset(months=12)
    data_fetcher = get_canceled_users if users_type == 'cancelled' else get_total_users
    raw_data = data_fetcher(info_db_no, origin_table)
    df = convert_to_dataframe(raw_data)

    if monthly:
        # 월별 구독 데이터 집계
        subscription_data = defaultdict(lambda: (0.0, 0.0, 0.0))
        for month_offset in range(12):
            month_start, month_end = get_month_range(now, month_offset)
            filtered = _filter_user_data(df, users_type, month_start, month_end, one_year_ago)
            basic, premium, ultimate = get_subscription_breakdown(filtered)
            subscription_data[month_start.strftime('%Y-%m')] = calculate_percentages(basic, premium, ultimate)
        return dict(subscription_data)
    else:
        # 전체 구독 데이터 집계
        filtered = _filter_user_data(df, users_type, one_year_ago=one_year_ago)
        basic, premium, ultimate = get_subscription_breakdown(filtered)
        return calculate_percentages(basic, premium, ultimate)

def _filter_user_data(df: pd.DataFrame, users_type: user_type, start: datetime = None, end: datetime = None, one_year_ago: datetime = None) -> pd.DataFrame:
    """사용자 유형별 데이터 필터링"""
    if users_type == 'cancelled':
        # 구독 해지된 사용자
        if start and end:
            return df[df['ended_at'].between(start, end, closed='left')]
        return df[df['ended_at'].notna()]
    elif users_type == 'total':
        # 전체 사용자
        if start and end:
            return df[(df['created_at'] <= end) & (df['ended_at'].isna() | (df['ended_at'] >= start))]
        return df[df['created_at'].notna()]
    elif users_type == 'new':
        # 신규 사용자 (최근 12개월 이내 가입)
        if start and end:
            return df[df['created_at'].between(start, end, closed='left')]
        return df[df['created_at'] >= one_year_ago]
    elif users_type == 'active':
        # 활성 사용자 (최근 활동 기록 있음)
        if start and end:
            return df[(df['created_at'] <= end) & (df['ended_at'].isna() | (df['ended_at'] >= start)) & (df['last_activity'] >= start)]
        return df[(df['created_at'].notna()) & (df['ended_at'].isna() | (df['ended_at'] >= one_year_ago)) & (df['last_activity'] >= one_year_ago)]
    elif users_type == 'dormant':
        # 휴면 사용자 (12개월 이상 활동 없음)
        if start and end:
            return df[(df['created_at'] < start) & (df['ended_at'].notna()) & (df['ended_at'] < start)]
        return df[(df['created_at'] < one_year_ago) & (df['ended_at'].notna()) & (df['ended_at'] < one_year_ago)]
    else:
        raise ValueError(f"잘못된 사용자 유형 입니다: {users_type}")

def get_subscription_breakdown(data: pd.DataFrame) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """유저를 구독 타입 별로 분류"""
    return determine_subscription_model(data)