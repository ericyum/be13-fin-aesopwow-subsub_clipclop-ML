import pandas as pd

class UserData:
    """
    사용자 데이터를 로딩하고 전처리하는 클래스
    """
    def __init__(self, data):
        """
        data: list of dict 또는 list of lists (각 유저의 피처와 타겟 포함)
        """
        self.df = pd.DataFrame(data)

    def preprocess(self):
        """
        - 범주형 컬럼을 category 타입으로 변환
        - 마지막 접속일을 숫자형으로 변환
        """
        if 'sub_type' in self.df.columns:
            self.df['sub_type'] = self.df['sub_type'].astype('category')
        if 'genre_pref' in self.df.columns:
            self.df['genre_pref'] = self.df['genre_pref'].astype('category')
        if 'last_login_days' in self.df.columns:
            self.df['last_login_days'] = pd.to_numeric(self.df['last_login_days'], errors='coerce')
        return self.df