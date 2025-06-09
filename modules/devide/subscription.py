import calendar
from typing import Literal, Dict, Tuple, List, Union
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import MetaData, Table, and_, create_engine, func, or_, select
from modules.common.user.user_utils import get_canceled_users, get_total_users, calculate_percentages, determine_subscription_model
from modules.common.utils.util_module import get_month_range, convert_to_dataframe
from modules.info_column.info_column_module import get_info_columns_by_info_db_no_origin_table
from modules.info_db.info_db_module import get_info_db_by_info_db_no

SubscriptionType = Literal['basic', 'standard', 'premium']
SubscriptionData = Dict[str, Tuple[float, float, float]]
user_type = Literal['total', 'cancelled', 'new', 'active', 'dormant']

def get_total_subscription_data(
    info_db_no: int,
    user_sub_info: str
) -> Dict[str, Dict[str, float]]:

    info_db = get_info_db_by_info_db_no(info_db_no).to_dict()
    if not info_db:
        return {}

    user, host, password, port, name = (
        info_db.get('user'),
        info_db.get('host'),
        info_db.get('password'),
        info_db.get('port'),
        info_db.get('name'),
    )

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")
    target_table_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_sub_info)
    target_table_mapped_columns = {col.origin_column: col.analysis_column for col in target_table_columns}
    origin_mapped_columns = {col.analysis_column: col.origin_column for col in target_table_columns}

    metadata = MetaData()
    target_table = Table(user_sub_info, metadata, autoload_with=engine)

    # 실제 컬럼 객체
    started_at_col = getattr(target_table.c, origin_mapped_columns.get('started_at'))
    subscription_type_col = getattr(target_table.c, origin_mapped_columns.get('subscription_type'))

    # DATE_FORMAT(started_at, '%Y-%m') 구문을 SQLAlchemy에서 raw SQL로 작성
    month_expr = func.date_format(started_at_col, "%Y-%m").label("month")

    with engine.connect() as conn:
        stmt = (
            select(
                month_expr,
                subscription_type_col,
                func.count().label("count")
            )
            .group_by(month_expr, subscription_type_col)
            .order_by(month_expr)
        )

        results = conn.execute(stmt).fetchall()

    # 결과 집계: { '2025-06': { 'BASIC': 10.0, 'STANDARD': 20.0, ... }, ... }
    monthly_counts = defaultdict(lambda: defaultdict(int))

    for row in results:
        month = row[0]       # DATE_FORMAT(started_at, '%Y-%m')
        sub_type = row[1]    # subscription_type
        count = row[2]       # count
        monthly_counts[month][sub_type] += count

    # 비율 계산
    monthly_percentages: Dict[str, Dict[str, float]] = {}

    for month, type_counts in monthly_counts.items():
        total = sum(type_counts.values())
        if total == 0:
            continue
        monthly_percentages[month] = {
            sub_type: round((count / total) * 100, 1)
            for sub_type, count in type_counts.items()
        }

    return dict(monthly_percentages)

def get_cancelled_subscription_data(
    info_db_no: int,
    user_sub_info: str
) -> Dict[str, Dict[str, float]]:

    info_db = get_info_db_by_info_db_no(info_db_no).to_dict()
    if not info_db:
        return {}

    user, host, password, port, name = (
        info_db.get('user'),
        info_db.get('host'),
        info_db.get('password'),
        info_db.get('port'),
        info_db.get('name'),
    )

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")
    target_table_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_sub_info)
    origin_mapped_columns = {
        col.analysis_column: col.origin_column for col in target_table_columns
    }

    metadata = MetaData()
    target_table = Table(user_sub_info, metadata, autoload_with=engine)

    started_at_col = getattr(target_table.c, origin_mapped_columns['started_at'])
    ended_at_col = getattr(target_table.c, origin_mapped_columns['ended_at'])
    subscription_type_col = getattr(target_table.c, origin_mapped_columns['subscription_type'])

    now = datetime.now()
    this_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_end = this_month_start - timedelta(seconds=1)
    last_month_start = last_month_end.replace(day=1)

    with engine.connect() as conn:
        stmt = (
            select(
                subscription_type_col,
                func.count().label("count")
            )
            .where(
                and_(
                    started_at_col <= last_month_end,
                    or_(
                        ended_at_col.is_(None),
                        ended_at_col > last_month_start
                    ),
                    ended_at_col < this_month_start
                )
            )
            .group_by(subscription_type_col)
        )

        results = conn.execute(stmt).fetchall()

    total = sum(row[1] for row in results)
    if total == 0:
        return {}

    percentages = {
        row[0]: round((row[1] / total) * 100, 1)
        for row in results
    }

    month_key = this_month_start.strftime("%Y-%m")
    return {month_key: percentages}


def get_new_subscription_data(
    info_db_no: int,
    user_sub_info: str
) -> Dict[str, Dict[str, float]]:

    info_db = get_info_db_by_info_db_no(info_db_no).to_dict()
    if not info_db:
        return {}

    user, host, password, port, name = (
        info_db.get('user'),
        info_db.get('host'),
        info_db.get('password'),
        info_db.get('port'),
        info_db.get('name'),
    )

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")
    target_table_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_sub_info)
    origin_mapped_columns = {
        col.analysis_column: col.origin_column for col in target_table_columns
    }

    metadata = MetaData()
    target_table = Table(user_sub_info, metadata, autoload_with=engine)

    # 실제 컬럼 객체
    user_id_col = getattr(target_table.c, origin_mapped_columns['user_id'])
    started_at_col = getattr(target_table.c, origin_mapped_columns['started_at'])
    subscription_type_col = getattr(target_table.c, origin_mapped_columns['subscription_type'])

    # 현재 월 계산
    now = datetime.now()
    this_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_day = calendar.monthrange(now.year, now.month)[1]
    this_month_end = this_month_start.replace(day=last_day, hour=23, minute=59, second=59)

    with engine.connect() as conn:
        # 1. 사용자별 첫 구독일 조회
        subquery = (
            select(
                user_id_col.label('user_id'),
                func.min(started_at_col).label('first_start')
            )
            .group_by(user_id_col)
            .subquery()
        )

        # 2. 이번 달이 첫 구독인 사용자와 구독 유형 매칭
        stmt = (
            select(
                subscription_type_col.label('subscription_type'),
                func.count().label('count')
            )
            .select_from(target_table.join(
                subquery,
                and_(
                    target_table.c[origin_mapped_columns['user_id']] == subquery.c.user_id,
                    target_table.c[origin_mapped_columns['started_at']] == subquery.c.first_start
                )
            ))
            .where(
                subquery.c.first_start.between(this_month_start, this_month_end)
            )
            .group_by(subscription_type_col)
        )

        results = conn.execute(stmt).mappings().all()

    total = sum(row.count for row in results)
    if total == 0:
        return {}

    percentages = {
        row['subscription_type']: round((row['count'] / total) * 100, 1)
        for row in results
    }

    month_key = this_month_start.strftime("%Y-%m")
    return {month_key: percentages}


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