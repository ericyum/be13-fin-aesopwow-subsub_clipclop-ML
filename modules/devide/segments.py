from typing import Literal, List, Dict
import pandas as pd
from datetime import datetime, timedelta

# 세그먼트 타입 정의
WatchTimeSegment = Literal['light', 'core', 'power']
GenreSegment = Literal[
    'drama',
    'sci-fi',
    'comedy',
    'documentary',
    'romance',
    'action',
    'horror'
]
LastLoginSegment = Literal['forgotten', 'dormant', 'frequent']

# 세그먼트 기준값 상수화
POWER_USER_THRESHOLD = 700
CORE_USER_THRESHOLD = 200

def classify_watch_time(watch_time: float) -> WatchTimeSegment:
    """누적 시청 시간 세그먼트 분류 함수"""
    if watch_time >= POWER_USER_THRESHOLD:
        return 'power'
    elif watch_time >= CORE_USER_THRESHOLD:
        return 'core'
    else:
        return 'light'

def classify_genre(genre: str) -> GenreSegment:
    """선호 장르 세그먼트 분류 함수"""
    genre = genre.lower()
    mapping = {
        'drama': 'drama',
        'sci-fi': 'sci-fi',
        'comedy': 'comedy',
        'documentary': 'documentary',
        'romance': 'romance',
        'action': 'action',
        'horror': 'horror'
    }
    if genre not in mapping:
        # 미등록 장르는 기본값 'drama'로 처리
        # print(f"Unknown genre '{genre}', defaulting to 'drama'")
        pass
    return mapping.get(genre, 'drama')

def classify_last_login(last_login_at, now: datetime = None) -> LastLoginSegment:
    """마지막 접속일 세그먼트 분류 함수"""
    if now is None:
        now = datetime.now()
    try:
        last_login = pd.to_datetime(last_login_at, utc=True).tz_localize(None)
        if pd.isnull(last_login):
            return 'forgotten'
    except Exception:
        return 'forgotten'
    days_diff = (now - last_login).days
    if days_diff <= 7:
        return 'frequent'
    elif days_diff <= 30:
        return 'dormant'
    else:
        return 'forgotten'

def add_segments(df: pd.DataFrame) -> pd.DataFrame:
    """데이터프레임에 세그먼트 컬럼 추가"""
    now = datetime.now()
    # 컬럼 존재 여부 체크 및 예외처리
    if 'watch_time' not in df.columns:
        df['watch_time'] = 0
    if 'favorite_genre' not in df.columns:
        df['favorite_genre'] = 'drama'
    if 'last_login_at' not in df.columns:
        df['last_login_at'] = pd.NaT

    df['watch_time_segment'] = df['watch_time'].apply(classify_watch_time)
    df['genre_segment'] = df['favorite_genre'].apply(classify_genre)
    df['last_login_segment'] = df['last_login_at'].apply(lambda x: classify_last_login(x, now))
    return df
