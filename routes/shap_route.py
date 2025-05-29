import random
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score
import shap
import lightgbm as lgb

# 1. 샘플 데이터 생성 (범주형 값 고르게 분포)
sub_types = ['Basic', 'Standard', 'Premium']
genre_prefs = ['Drama', 'Sci-Fi', 'Comedy', 'Documentary', 'Romance', 'Action', 'Horror']
user_data = []

for i in range(1, 101):  # 100개로 확장
    sub_type = sub_types[(i - 1) % len(sub_types)]
    genre_pref = genre_prefs[(i - 1) % len(genre_prefs)]
    if sub_type == 'Basic':
        total_watch_time = random.randint(50, 300)
    elif sub_type == 'Standard':
        total_watch_time = random.randint(200, 700)
    else:
        total_watch_time = random.randint(600, 950)
    last_login_days = random.randint(1, 60)
    if last_login_days > 30:
        churned = random.choices([1, 0], weights=[0.8, 0.2])[0]
    else:
        churned = random.choices([0, 1], weights=[0.7, 0.3])[0]

    user_data.append({
        'user_id': i,
        'total_watch_time': total_watch_time,
        'sub_type': sub_type,
        'genre_pref': genre_pref,
        'last_login_days': last_login_days,
        'churned': churned
    })

df = pd.DataFrame(user_data)

# 2. 전처리 클래스 정의
class UserData:
    def __init__(self, df):
        self.df = df

    def preprocess(self):
        self.df['sub_type'] = self.df['sub_type'].astype('category')
        self.df['genre_pref'] = self.df['genre_pref'].astype('category')
        self.df['last_login_days'] = pd.to_numeric(self.df['last_login_days'], errors='coerce')
        return self.df

# 3. SHAP 분석 클래스 정의
class SHAPAnalysis:
    def __init__(self, df):
        self.df = df
        self.model = None
        self.explainer = None
        self.shap_values = None
        self.X_test = None
        self.y_test = None
        self.feature_names = None

    def preprocess(self):
        X = self.df.drop(columns=['churned', 'user_id'])
        y = self.df['churned']
        cat_cols = X.select_dtypes(include=['category', 'object']).columns.tolist()
        num_cols = X.select_dtypes(include=['int64', 'float64']).columns.tolist()

        cat_pipeline = Pipeline([
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
        ])
        num_pipeline = Pipeline([
            ('imputer', SimpleImputer(strategy='mean'))
        ])

        self.preprocessor = ColumnTransformer(
            transformers=[
                ('cat', cat_pipeline, cat_cols),
                ('num', num_pipeline, num_cols)
            ]
        )

        X_processed = self.preprocessor.fit_transform(X)

        # 피처명 추출
        feature_names = []
        if cat_cols:
            ohe = self.preprocessor.named_transformers_['cat'].named_steps['onehot']
            cat_feature_names = ohe.get_feature_names_out(cat_cols)
            feature_names.extend(cat_feature_names)
        feature_names.extend(num_cols)
        self.feature_names = feature_names

        return X_processed, y

    def train_model(self):
        X, y = self.preprocess()
        # stratify로 클래스 불균형 방지
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, random_state=42, stratify=y
        )

        # 피처가 0개면 예외 발생
        if X_train.shape[1] == 0:
            raise ValueError("전처리 후 학습에 사용할 수 있는 피처가 없습니다. 데이터와 전처리 로직을 확인하세요.")

        self.model = lgb.LGBMClassifier(random_state=42)
        self.model.fit(X_train, y_train)

        y_pred = self.model.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        print(f"Model accuracy: {acc:.4f}")

        self.explainer = shap.TreeExplainer(self.model)
        shap_values = self.explainer.shap_values(X_test)
        # 이진분류: [0]=잔존, [1]=이탈
        if isinstance(shap_values, list):
            self.shap_values = shap_values[1]
        else:
            self.shap_values = shap_values
        self.X_test = X_test
        self.y_test = y_test

    def save_shap_csv(self, original_df, csv_path="shap_result.csv"):
        # SHAP 값 shape 방어 코드
        if len(self.shap_values.shape) == 1:
            self.shap_values = self.shap_values.reshape(-1, 1)
        if self.shap_values.shape[1] != len(self.feature_names):
            print(f"[경고] SHAP 값 shape {self.shap_values.shape}와 feature_names 개수 {len(self.feature_names)}가 다릅니다.")
            self.feature_names = [f"feature_{i}" for i in range(self.shap_values.shape[1])]

        shap_df = pd.DataFrame(self.shap_values, columns=self.feature_names)
        pred_proba = self.model.predict_proba(self.X_test)[:, 1]
        shap_df['predicted_churn_proba'] = pred_proba
        shap_df['actual_churn'] = self.y_test.values

        test_indices = self.y_test.index
        shap_df['user_id'] = original_df.iloc[test_indices]['user_id'].values

        cols = ['user_id'] + self.feature_names + ['predicted_churn_proba', 'actual_churn']
        shap_df = shap_df[cols]

        shap_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"SHAP 분석 결과가 {csv_path}에 저장되었습니다.")
        return csv_path

# 4. 전체 실행
user_data_instance = UserData(df)
df_processed = user_data_instance.preprocess()

shap_analysis = SHAPAnalysis(df_processed)
shap_analysis.train_model()
csv_path = shap_analysis.save_shap_csv(df_processed, "shap_result.csv")
