from typing import Literal, Dict, Tuple, List, Union
from collections import defaultdict
from datetime import datetime, timezone
import pandas as pd
from modules.common.user.user_utils import get_canceled_users, get_total_users, calculate_percentages, determine_subscription_model
from modules.common.utils.util_module import get_month_range, convert_to_dataframe

SubscriptionType = Literal['basic', 'standard', 'premium']
SubscriptionData = Dict[str, Tuple[float, float, float]]
user_type = Literal['total', 'cancelled', 'new', 'active', 'dormant']

def get_subscription_data(
    info_db_no: int,
    origin_table: str,
    users_type: user_type,
    monthly: bool = False
) -> Union[SubscriptionData, Tuple[float, float, float]]:
    now = datetime.now(timezone.utc)
    now = now.replace(tzinfo=None)  # tz 제거 (naive datetime)
    one_year_ago = now - pd.DateOffset(months=12)

    data_fetcher = get_canceled_users if users_type == 'cancelled' else get_total_users
    raw_data = data_fetcher(info_db_no, origin_table)
    df = convert_to_dataframe(raw_data)

    # datetime 컬럼들 tz 제거 (naive로 변환)
    for col in ['started_at', 'ended_at', 'last_activity']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)

    if monthly:
        subscription_data = defaultdict(lambda: (0.0, 0.0, 0.0))
        for month_offset in range(12):
            month_start, month_end = get_month_range(now, month_offset)
            month_start = month_start.replace(tzinfo=None)
            month_end = month_end.replace(tzinfo=None)
            filtered = _filter_user_data(df, users_type, month_start, month_end, one_year_ago)

            basic, standard, premium = get_subscription_breakdown(filtered)

            subscription_data[month_start.strftime('%Y-%m')] = calculate_percentages(basic, standard, premium)
        return dict(subscription_data)
    else:
        filtered = _filter_user_data(df, users_type, one_year_ago=one_year_ago)
        basic, standard, premium = get_subscription_breakdown(filtered)
        return calculate_percentages(basic, standard, premium)

def _filter_user_data(
        df: pd.DataFrame,
        users_type: user_type,
        start: datetime = None,
        end: datetime = None,
        one_year_ago: datetime = None
) -> pd.DataFrame:
    for col in ['started_at', 'ended_at', 'last_activity']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)

    if users_type == 'cancelled':
        if start and end:
            return df[df['ended_at'].between(start, end, inclusive='left')]
        return df[df['ended_at'].notna()]

    elif users_type == 'total':
        if start and end:
            return df[(df['started_at'] <= end) & ((df['ended_at'].isna()) | (df['ended_at'] >= start))]
        return df[df['started_at'].notna()]

    elif users_type == 'new':
        if start and end:
            return df[df['started_at'].between(start, end, inclusive='left')]
        return df[df['started_at'] >= one_year_ago]

    elif users_type == 'active':
        if 'last_activity' not in df.columns:
            return pd.DataFrame(columns=df.columns)
        if start and end:
            return df[
                (df['started_at'] <= end) &
                ((df['ended_at'].isna()) | (df['ended_at'] >= start)) &
                (df['last_activity'] >= start)
            ]
        return df[
            (df['started_at'].notna()) &
            ((df['ended_at'].isna()) | (df['ended_at'] >= one_year_ago)) &
            (df['last_activity'] >= one_year_ago)
        ]

    elif users_type == 'dormant':
        if start and end:
            return df[
                (df['started_at'] < start) &
                (df['ended_at'].notna()) &
                (df['ended_at'] < start)
            ]
        return df[
            (df['started_at'] < one_year_ago) &
            (df['ended_at'].notna()) &
            (df['ended_at'] < one_year_ago)
        ]

    else:
        raise ValueError(f"잘못된 사용자 유형 입니다: {users_type}")

def get_subscription_breakdown(data: pd.DataFrame) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """유저를 구독 타입 별로 분류"""
    return determine_subscription_model(data)