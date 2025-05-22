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


# def get_cancellation_rate(
#         info_db_no: int,
#         origin_table: str,
# ) -> float:
#     data = convert_data(info_db_no, origin_table)
#     df = pd.DataFrame(data)

#     cancelled = df[df['subscription_type'].isna()]
#     active = df[df['subscription_type'].notna()]

#     if len(active) == 0:
#         return None
#     rate = (len(cancelled) / len(active)) * 100
#     return round(rate, 2)

def get_cancellation_rate(info_db_no: int, origin_table: str) -> float:
    from datetime import datetime, timedelta

    data = convert_data(info_db_no, origin_table)
    df = pd.DataFrame(data)

    # 현재 시각 (naive datetime)
    now = datetime.now().replace(tzinfo=None)
    cutoff = now - timedelta(days=30)

    # 1. subscription_type이 있는 유저만 사용
    df = df[df['subscription_type'].notna()]

    # 2. 전체 유저 수 (구독 정보 있는 유저만 대상)
    total = len(df)
    if total == 0:
        return None

    # 3. 활성 유저: ended_at이 없거나 미래
    df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
    active = df[(df['ended_at'].isna()) | (df['ended_at'] >= now)]

    # 4. 최근 한 달 안에 ended_at이 있는 유저 중 active 아닌 유저 → 해지자
    cancelled = df[
        (~df.index.isin(active.index)) &  # active 유저는 제외
        (df['ended_at'].notna()) &
        (df['ended_at'] >= cutoff) &
        (df['ended_at'] < now)
    ]

    # 5. 비율 계산
    rate = (len(cancelled) / total) * 100

    return round(rate, 2)