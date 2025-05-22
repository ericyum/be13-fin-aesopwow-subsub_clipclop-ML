import pandas as pd
from datetime import datetime, timedelta, timezone
from modules.common.convert_data import convert_data
from typing import List, Dict, Tuple

def load_data(info_db_no: int, origin_table: str) -> pd.DataFrame:
    data = convert_data(info_db_no, origin_table)
    df = pd.DataFrame(data)

    for col in ['created_at', 'ended_at', 'logined_at']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)
        else:
            df[col] = pd.NaT
    return df

def get_total_users(info_db_no: int, origin_table: str) -> pd.DataFrame:
    return load_data(info_db_no, origin_table)

def get_new_users(info_db_no: int, origin_table: str) -> pd.DataFrame:
    df = load_data(info_db_no, origin_table)
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)
    return df[df['created_at'] >= cutoff]

def get_active_users(info_db_no: int, origin_table: str) -> pd.DataFrame:
    df = load_data(info_db_no, origin_table)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if 'ended_at' not in df.columns:
        return df
    return df[(df['ended_at'].isna()) | (df['ended_at'] >= now)]

def get_dormant_users(info_db_no: int, origin_table: str) -> pd.DataFrame:
    df = load_data(info_db_no, origin_table)
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)
    return df[df['logined_at'] < cutoff]

def get_canceled_users(info_db_no: int, origin_table: str) -> pd.DataFrame:
    df = load_data(info_db_no, origin_table)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return df[(df['ended_at'].notna()) & (df['ended_at'] < now)]

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
