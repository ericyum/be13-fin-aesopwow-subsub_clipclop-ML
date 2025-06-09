import pandas as pd
from datetime import datetime, timedelta, timezone

from sqlalchemy import MetaData, Table, and_, create_engine, distinct, func, select
from modules.common.convert_data import convert_data
from typing import List, Dict, Tuple

from modules.info_column.info_column_module import get_info_columns_by_info_db_no_origin_table
from modules.info_db.info_db_module import get_info_db_by_info_db_no

def load_data(info_db_no: int, user_info: str) -> pd.DataFrame:
    # 필요에 따라 user_info, user_sub_info, target_column을 조합해 origin_table을 만들 수도 있습니다.
    # 예시: origin_table = f"{user_info}_{user_sub_info}"
    # 아래는 예시로 user_info만 origin_table로 사용
    origin_table = user_info
    data = convert_data(info_db_no, origin_table)
    df = pd.DataFrame(data)

    for col in ['started_at', 'ended_at', 'logined_at']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)
        else:
            df[col] = pd.NaT
    return df

def get_total_users(info_db_no: int, user_info: str) -> int:
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
    target_table_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_info)
    target_table_mapped_columns = {col.origin_column: col.analysis_column for col in target_table_columns}
    origin_mapped_columns = {col.analysis_column: col.origin_column for col in target_table_columns}

    metadata = MetaData()
    target_table_external_table = Table(user_info, metadata, autoload_with=engine)

    with engine.connect() as conn:
        user_id = getattr(target_table_external_table.c, origin_mapped_columns.get('user_id'))

        target_users_query = select(func.count(user_id))
        
        result = conn.execute(target_users_query).scalar()

    return result

def get_new_users(info_db_no: int, user_sub_info: str) -> int:
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
    target_table_mapped_columns = {col.origin_column: col.analysis_column for col in target_table_columns}
    origin_mapped_columns = {col.analysis_column: col.origin_column for col in target_table_columns}

    metadata = MetaData()
    target_table_external_table = Table(user_sub_info, metadata, autoload_with=engine)

    with engine.connect() as conn:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        start_of_this_month = now - timedelta(days=30)
        start_of_last_month = start_of_this_month - timedelta(days=30)

        started_col = getattr(target_table_external_table.c, origin_mapped_columns['started_at'])
        ended_col = getattr(target_table_external_table.c, origin_mapped_columns['ended_at'])
        user_no_col = getattr(target_table_external_table.c, origin_mapped_columns['user_id'])

        # 하위 쿼리: 지난달에 구독하고 있던 사용자
        subquery_prev_month_users = (
            select(distinct(user_no_col))
            .where(
                and_(
                    started_col <= start_of_last_month + timedelta(days=30),  # 시작일이 지난달 말 이전
                    ended_col >= start_of_last_month                         # 종료일이 지난달 시작 이후
                )
            )
        )

        # 메인 쿼리: 이번달에 새로 구독 시작한 사용자 중, 지난달엔 없던 사용자
        target_users_query = (
            select(func.count(distinct(user_no_col)))
            .where(
                and_(
                    started_col >= start_of_this_month,
                    started_col <= now,
                    ~user_no_col.in_(subquery_prev_month_users)
                )
            )
        )

        result = conn.execute(target_users_query).scalar()

    return result

def get_active_users(info_db_no: int, user_info: str) -> int:
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
    target_table_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_info)
    target_table_mapped_columns = {col.origin_column: col.analysis_column for col in target_table_columns}
    origin_mapped_columns = {col.analysis_column: col.origin_column for col in target_table_columns}

    metadata = MetaData()
    target_table_external_table = Table(user_info, metadata, autoload_with=engine)

    with engine.connect() as conn:
        user_id = getattr(target_table_external_table.c, origin_mapped_columns.get('user_id'))

        target_users_query = select(func.count(user_id)).where(
            getattr(target_table_external_table.c, origin_mapped_columns.get('last_login')) >=
            datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)
        )
        
        result = conn.execute(target_users_query).scalar()

    return result

def get_dormant_users(info_db_no: int, user_info: str) -> int:
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
    target_table_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_info)
    target_table_mapped_columns = {col.origin_column: col.analysis_column for col in target_table_columns}
    origin_mapped_columns = {col.analysis_column: col.origin_column for col in target_table_columns}

    metadata = MetaData()
    target_table_external_table = Table(user_info, metadata, autoload_with=engine)

    with engine.connect() as conn:
        user_id = getattr(target_table_external_table.c, origin_mapped_columns.get('user_id'))

        target_users_query = select(func.count(user_id)).where(
            getattr(target_table_external_table.c, origin_mapped_columns.get('last_login')) <
            datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)
        )
        
        result = conn.execute(target_users_query).scalar()

    return result

def get_canceled_users(info_db_no: int, user_info: str) -> int:
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
    target_table_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_info)
    target_table_mapped_columns = {col.origin_column: col.analysis_column for col in target_table_columns}
    origin_mapped_columns = {col.analysis_column: col.origin_column for col in target_table_columns}

    metadata = MetaData()
    target_table_external_table = Table(user_info, metadata, autoload_with=engine)

    with engine.connect() as conn:
        user_id = getattr(target_table_external_table.c, origin_mapped_columns.get('user_id'))

        target_users_query = select(func.count(user_id)).where(
            getattr(target_table_external_table.c, origin_mapped_columns.get('ended_at')).isnot(None)
        )
        
        result = conn.execute(target_users_query).scalar()

    return result

def determine_subscription_model(users: pd.DataFrame) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    basic, standard, premium = [], [], []
    for _, user in users.iterrows():
        sub_type = str(user.get('subscription_type') or '').strip().lower()
        if sub_type == 'basic':
            basic.append(user.to_dict())
        elif sub_type == 'standard':
            standard.append(user.to_dict())
        elif sub_type == 'premium':
            premium.append(user.to_dict())
    return basic, standard, premium

def determine_watch_time_segment(users: pd.DataFrame) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    light, core, power = [], [], []
    for _, user in users.iterrows():
        segment = str(user.get('watch_time_segment') or '').strip().lower()
        if segment == 'light':
            light.append(user.to_dict())
        elif segment == 'core':
            core.append(user.to_dict())
        elif segment == 'power':
            power.append(user.to_dict())
    return light, core, power

def determine_genre_segment(users: pd.DataFrame) -> Tuple[List[Dict], ...]:
    segments = {k: [] for k in ['drama', 'sci-fi', 'comedy', 'documentary', 'romance', 'action', 'horror']}
    for _, user in users.iterrows():
        segment = str(user.get('genre_segment') or '').strip().lower()
        if segment in segments:
            segments[segment].append(user.to_dict())
        else:
            segments['drama'].append(user.to_dict())
    return tuple(segments[genre] for genre in ['drama', 'sci-fi', 'comedy', 'documentary', 'romance', 'action', 'horror'])

def determine_last_login_segment(users: pd.DataFrame) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    forgotten, dormant, frequent = [], [], []
    for _, user in users.iterrows():
        segment = str(user.get('last_login_segment') or '').strip().lower()
        if segment == 'forgotten':
            forgotten.append(user.to_dict())
        elif segment == 'dormant':
            dormant.append(user.to_dict())
        elif segment == 'frequent':
            frequent.append(user.to_dict())
    return forgotten, dormant, frequent

def calculate_percentages(*groups: List) -> Tuple[float, ...]:
    total = sum(len(group) for group in groups)
    if total == 0:
        return tuple(0.0 for _ in groups)
    return tuple(round((len(group) / total) * 100, 1) for group in groups)
