from datetime import datetime
from typing import Tuple, Dict, List
import pandas as pd
from dateutil.relativedelta import relativedelta

def get_month_range(now: datetime, month_offset: int) -> Tuple[datetime, datetime]:
    """월 범위 계산"""
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - relativedelta(months=month_offset)
    return month_start, month_start + relativedelta(months=1)

def convert_to_dataframe(data: List[Dict]) -> pd.DataFrame:
    """Dictionary 타입을 DataFrame 으로 변환"""
    return pd.DataFrame(data) if isinstance(data, list) else data
