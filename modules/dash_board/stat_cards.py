import pandas as pd
from datetime import datetime, timedelta
from modules.common.convert_data import convert_data  # 경로 확인 필수


def get_increase_decrease_rate(
        info_db_no: int,
        origin_table: str,
        period_days: int = 30
) -> float:
    data = convert_data(info_db_no, origin_table)
    df = pd.DataFrame(data)

    now = pd.Timestamp(datetime.utcnow())
    current_start = now - pd.Timedelta(days=period_days)
    previous_start = current_start - pd.Timedelta(days=period_days)

    current = df[
        (df['created_at'] >= current_start)
    ]

    previous = df[
        (df['created_at'] >= previous_start) &
        (df['created_at'] < current_start)
        ]

    if len(previous) == 0:
        return None
    rate = ((len(current) - len(previous)) / len(previous)) * 100
    return round(rate, 2)

def get_cancellation_rate(
        info_db_no: int,
        origin_table: str,
) -> float:
    data = convert_data(info_db_no, origin_table)
    df = pd.DataFrame(data)

    now = pd.Timestamp(datetime.utcnow())

    # 해지 사용자 (구독 상태 없음)
    cancelled = df[
        (df['subscription_type'].isna())  # null 체크
    ]

    # 활성 사용자 (구독 상태 있음)
    active = df[
        (df['subscription_type'].notna())  # not null 체크
    ]

    if len(active) == 0:
        return None
    rate = (len(cancelled) / len(active)) * 100
    return round(rate, 2)