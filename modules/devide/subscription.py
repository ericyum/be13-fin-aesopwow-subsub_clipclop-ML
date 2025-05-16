from typing import Literal, Dict, Tuple, List
from collections import defaultdict
from datetime import datetime, UTC
import pandas as pd
from modules.common.user.user_utils import get_canceled_users, get_total_users, calculate_percentages, \
    determine_subscription_model
from modules.common.utils.util_module import get_month_range, convert_to_dataframe

SubscriptionType = Literal['basic', 'premium', 'ultimate']
SubscriptionData = Dict[str, Tuple[float, float, float]]
user_type = Literal['total', 'cancelled', 'new', 'active', 'dormant']

def get_monthly_subscription_data(
        info_db_no: int,
        origin_table: str,
        users_type: user_type
) -> dict[str, tuple[float, float, float]]:
    """월별 구독 데이터를 처리하는 함수"""
    now = datetime.now(UTC)
    monthly_data = defaultdict(lambda: (0.0, 0.0, 0.0))

    # 데이터 소스 결정
    data_fetcher = get_canceled_users if users_type == 'cancelled' else get_total_users

    for month_offset in range(12):
        month_start, month_end = get_month_range(now, month_offset)
        raw_data = data_fetcher(info_db_no, origin_table)
        df = convert_to_dataframe(raw_data)

        # 사용자 유형별 필터링 로직
        if users_type == 'cancelled':
            filtered = df[df['ended_at'].between(month_start, month_end, inclusive='left')]

        elif users_type == 'total':
            filtered = df[
                (df['created_at'] <= month_end) &
                (df['ended_at'].isna() | (df['ended_at'] >= month_start))
            ]

        elif users_type == 'new':
            filtered = df[df['created_at'].between(month_start, month_end, inclusive='left')]

        elif users_type == 'active':
            filtered = df[
                (df['created_at'] <= month_end) &
                (df['ended_at'].isna() | (df['ended_at'] >= month_start))
            ][df['last_activity'] >= month_start]  # 최근 활동 기준 추가

        elif users_type == 'dormant':
            filtered = df[
                (df['created_at'] < month_start) &
                (df['ended_at'].notna()) &
                (df['ended_at'] < month_start)
            ]

        else:
            raise ValueError(f"Invalid user type: {users_type}")

        basic, premium, ultimate = get_subscription_breakdown(filtered)
        monthly_data[month_start.strftime('%Y-%m')] = calculate_percentages(basic, premium, ultimate)

    return dict(monthly_data)


def get_subscription_breakdown(data: pd.DataFrame) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """유저를 구독 타입 별로 분류"""
    return determine_subscription_model(data)