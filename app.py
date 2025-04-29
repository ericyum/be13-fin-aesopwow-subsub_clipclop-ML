import pandas as pd
import random

# CSV 파일 경로
file_path = '/Users/iseonghun/Desktop/final-project/be13-fin-aesopwow-subsub_clipclop-ML/resources/data/netflix_users.csv'

# CSV 파일 읽기
df = pd.read_csv(file_path)

# 성별 'gender' 컬럼을 F 또는 M으로 랜덤하게 생성
df['Gender'] = [random.choice(['F', 'M']) for _ in range(len(df))]

# 변경된 데이터프레임을 기존 파일에 덮어쓰기
df.to_csv(file_path, index=False)

print("성별 컬럼이 추가되었고, 파일이 덮어써졌습니다.")
