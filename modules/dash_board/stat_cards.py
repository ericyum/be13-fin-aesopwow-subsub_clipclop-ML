import pandas as pd
from datetime import timezone
from modules.common.convert_data import convert_data

def get_increase_decrease_rate(
        info_db_no: int,
        origin_table: str,
        period_days: int = 30
) -> float:
    data = convert_data(info_db_no, origin_table)
    df = pd.DataFrame(data)

    # created_at 컬럼 datetime으로 변환 및 timezone 제거 (naive)
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce').dt.tz_localize(None)

    now = pd.Timestamp.now(tz='UTC').tz_localize(None)  # naive datetime
    current_start = now - pd.Timedelta(days=period_days)
    previous_start = current_start - pd.Timedelta(days=period_days)

    current = df[df['created_at'] >= current_start]
    previous = df[(df['created_at'] >= previous_start) & (df['created_at'] < current_start)]

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

    cancelled = df[df['subscription_type'].isna()]
    active = df[df['subscription_type'].notna()]

    if len(active) == 0:
        return None
    rate = (len(cancelled) / len(active)) * 100
    return round(rate, 2)