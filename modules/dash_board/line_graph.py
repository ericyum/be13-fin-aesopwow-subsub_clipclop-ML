import pandas as pd
import numpy as np
from datetime import datetime, timezone

from sqlalchemy import MetaData, Table, create_engine, func, select
from modules.common.convert_data import convert_data
from modules.common.utils.util_module import convert_to_dataframe
from modules.info_column.info_column_module import get_info_columns_by_info_db_no_origin_table
from modules.info_db.info_db_module import get_info_db_by_info_db_no

def calculate_increase_decrease_per(info_db_no: int, user_info: str) -> pd.DataFrame:
    info_db = get_info_db_by_info_db_no(info_db_no).to_dict()
    if not info_db:
        return pd.DataFrame()

    user, host, password, port, name = (
        info_db.get('user'),
        info_db.get('host'),
        info_db.get('password'),
        info_db.get('port'),
        info_db.get('name'),
    )

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")
    target_table_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_info)
    origin_mapped_columns = {col.analysis_column: col.origin_column for col in target_table_columns}

    metadata = MetaData()
    table = Table(user_info, metadata, autoload_with=engine)

    started_at_col = getattr(table.c, origin_mapped_columns['started_at'])
    started_month = func.date_format(started_at_col, "%Y-%m").label("month")

    with engine.connect() as conn:
        stmt = (
            select(started_month, func.count().label("monthly"))
            .group_by("month")
            .order_by("month")
        )
        result = conn.execute(stmt).fetchall()

    # 결과를 DataFrame으로 정리
    df = pd.DataFrame(result, columns=["month", "monthly"])

    # 증감률 계산
    df["increase_decrease_per"] = df["monthly"].pct_change() * 100
    df["increase_decrease_per"] = df["increase_decrease_per"].round(2)

    return df