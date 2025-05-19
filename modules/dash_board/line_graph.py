import pandas as pd
import numpy as np
from datetime import datetime, timezone
from modules.common.convert_data import convert_data
from modules.common.utils.util_module import convert_to_dataframe

def calculate_increase_decrease_per(info_db_no: int, origin_table: str) -> pd.DataFrame:
    """월별 구독자 증감률 계산 (subscription_type이 없으면 해지로 간주)"""
    # 1. 데이터 불러오기
    data = convert_data(info_db_no, origin_table)
    df = convert_to_dataframe(data)

    # 2. 필수 컬럼 검증
    required_columns = ['created_at', 'subscription_type']
    if not all(col in df.columns for col in required_columns):
        missing = [col for col in required_columns if col not in df.columns]
        raise KeyError(f"필수 컬럼 누락: {missing}")

    # 3. 타임존 일치 및 인덱스 설정
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True, errors='coerce')
    df = df.set_index('created_at')

    # 4. subscription_type 정제 (빈 문자열도 NaN 처리)
    df['subscription_type'] = df['subscription_type'].replace(r'^\s*$', np.nan, regex=True)

    # 5. 12개월 데이터 필터링
    now = datetime.now(timezone.utc)
    one_year_ago = now - pd.DateOffset(months=12)
    df = df[df.index >= one_year_ago]

    # 6. 월 범위 생성 (월초~월말, 고정)
    if df.index.empty:
        # 데이터가 없을 때 빈 DataFrame 반환
        return pd.DataFrame(columns=['month', 'increase_decrease_per'])
    start_date = df.index.min().replace(day=1)
    end_date = df.index.max().replace(day=1) + pd.offsets.MonthEnd(0)
    date_range = pd.date_range(start=start_date, end=end_date, freq='M')

    # 7. 월별 신규 가입자 계산
    monthly_counts = (
        df.resample('M')
        .size()
        .reindex(date_range, fill_value=0)
    )

    # 8. 월별 해지자 계산 (subscription_type이 없는 row)
    monthly_cancellations = (
        df[df['subscription_type'].isna()]
        .resample('M')
        .size()
        .reindex(date_range, fill_value=0)
    )

    # 9. 데이터 병합 및 증감률 계산
    monthly_data = pd.DataFrame({
        'created': monthly_counts,
        'cancelled': monthly_cancellations
    })
    monthly_data['net'] = monthly_data['created'] - monthly_data['cancelled']
    monthly_data['cumulative'] = monthly_data['net'].cumsum()
    monthly_data['increase_decrease_per'] = (
        monthly_data['cumulative'].pct_change() * 100
    ).fillna(0).replace([np.inf, -np.inf], 100)

    # 10. 결과 반환 (월, 증감률)
    monthly_data = monthly_data.reset_index().rename(columns={'index': 'month'})
    monthly_data.rename(columns={'index': 'month', 'created_at': 'month'}, inplace=True)
    monthly_data['month'] = monthly_data['month'].dt.strftime('%Y-%m')

    return monthly_data[['month', 'increase_decrease_per']]