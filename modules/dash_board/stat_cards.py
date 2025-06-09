import pandas as pd
from datetime import datetime, timedelta, timezone

from sqlalchemy import MetaData, Table, and_, create_engine, func, select
from modules.common.convert_data import convert_data
from modules.info_column.info_column_module import get_info_columns_by_info_db_no_origin_table
from modules.info_db.info_db_module import get_info_db_by_info_db_no

def get_increase_decrease_rate(
        info_db_no: int,
        user_sub_info: str
) -> float:
    info_db = get_info_db_by_info_db_no(info_db_no).to_dict()
    if not info_db:
        return None

    user, host, password, port, name = (
        info_db.get('user'),
        info_db.get('host'),
        info_db.get('password'),
        info_db.get('port'),
        info_db.get('name'),
    )

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")
    target_table_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_sub_info)
    origin_mapped_columns = {col.analysis_column: col.origin_column for col in target_table_columns}

    metadata = MetaData()
    table = Table(user_sub_info, metadata, autoload_with=engine)

    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_this_month = now.replace(day=1)
    start_of_last_month = (start_of_this_month - timedelta(days=1)).replace(day=1)
    end_of_last_month = start_of_this_month - timedelta(seconds=1)

    started_at_col = getattr(table.c, origin_mapped_columns['started_at'])
    user_id_col = getattr(table.c, origin_mapped_columns['user_id'])

    with engine.connect() as conn:
        # 저번 달 신규 구독자
        last_month_subquery = (
            select(user_id_col, func.min(started_at_col).label("first_start"))
            .group_by(user_id_col)
            .having(func.min(started_at_col).between(start_of_last_month, end_of_last_month))
        )
        last_month_count = conn.execute(
            select(func.count()).select_from(last_month_subquery.alias())
        ).scalar()

        # 이번 달 신규 구독자
        this_month_subquery = (
            select(user_id_col, func.min(started_at_col).label("first_start"))
            .group_by(user_id_col)
            .having(func.min(started_at_col).between(start_of_this_month, now))
        )
        this_month_count = conn.execute(
            select(func.count()).select_from(this_month_subquery.alias())
        ).scalar()

    # 증감률 계산
    if last_month_count == 0:
        growth_rate = None  # 나누기 0 방지
    else:
        growth_rate = round(((this_month_count - last_month_count) / last_month_count) * 100, 2)

    return growth_rate


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

# def get_cancellation_rate(info_db_no: int, origin_table: str) -> float:
#     from datetime import datetime, timedelta

#     data = convert_data(info_db_no, origin_table)
#     df = pd.DataFrame(data)

#     # 현재 시각 (naive datetime)
#     now = datetime.now().replace(tzinfo=None)
#     cutoff = now - timedelta(days=30)

#     # 1. subscription_type이 있는 유저만 사용
#     df = df[df['subscription_type'].notna()]

#     # 2. 전체 유저 수 (구독 정보 있는 유저만 대상)
#     total = len(df)
#     if total == 0:
#         return None

#     # 3. 활성 유저: ended_at이 없거나 미래
#     df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
#     active = df[(df['ended_at'].isna()) | (df['ended_at'] >= now)]

#     # 4. 최근 한 달 안에 ended_at이 있는 유저 중 active 아닌 유저 → 해지자
#     cancelled = df[
#         (~df.index.isin(active.index)) &  # active 유저는 제외
#         (df['ended_at'].notna()) &
#         (df['ended_at'] >= cutoff) &
#         (df['ended_at'] < now)
#     ]

#     # 5. 비율 계산
#     rate = (len(cancelled) / total) * 100

#     return round(rate, 2)

def get_cancellation_rate(info_db_no: int, user_sub_info: str) -> float:
    info_db = get_info_db_by_info_db_no(info_db_no).to_dict()
    if not info_db:
        return 0.0

    user, host, password, port, name = (
        info_db.get('user'),
        info_db.get('host'),
        info_db.get('password'),
        info_db.get('port'),
        info_db.get('name'),
    )

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")
    target_table_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_sub_info)
    origin_mapped_columns = {col.analysis_column: col.origin_column for col in target_table_columns}

    metadata = MetaData()
    table = Table(user_sub_info, metadata, autoload_with=engine)

    started_at_col = getattr(table.c, origin_mapped_columns['started_at'])
    ended_at_col = getattr(table.c, origin_mapped_columns['ended_at'])

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    start_of_this_month = now.replace(day=1)
    end_of_this_month = (start_of_this_month + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)

    with engine.connect() as conn:
        # 이번 달에 구독 시작한 사용자 수
        total_stmt = (
            select(func.count())
            .where(
                and_(
                    started_at_col >= start_of_this_month,
                    started_at_col <= end_of_this_month,
                )
            )
        )
        total_count = conn.execute(total_stmt).scalar() or 0

        # 이번 달에 구독을 해지한 사용자 수
        cancelled_stmt = (
            select(func.count())
            .where(
                and_(
                    ended_at_col >= start_of_this_month,
                    ended_at_col <= end_of_this_month,
                )
            )
        )
        cancelled_count = conn.execute(cancelled_stmt).scalar() or 0

    # 해지율 계산
    if total_count == 0:
        return 0.0

    cancellation_rate = round((cancelled_count / total_count) * 100, 2)
    return cancellation_rate
