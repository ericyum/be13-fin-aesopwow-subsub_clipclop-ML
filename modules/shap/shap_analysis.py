import shap
import lightgbm as lgb
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score

class SHAPAnalysis:
    """
    SHAP 분석을 위한 모델 학습 및 SHAP 값 계산 클래스
    """
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
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42)

        self.model = lgb.LGBMClassifier(random_state=42)
        self.model.fit(X_train, y_train)

        y_pred = self.model.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        print(f"Model accuracy: {acc:.4f}")

        self.explainer = shap.TreeExplainer(self.model)
        self.shap_values = self.explainer.shap_values(X_test)[1]  # 이진분류: [0]=잔존, [1]=이탈
        self.X_test = X_test
        self.y_test = y_test

    def save_shap_csv(self, original_df, csv_path="shap_result.csv"):
        """
        SHAP 값, 예측값, 실제값을 포함한 CSV 파일 저장
        """
        # SHAP 값과 feature_names shape 체크 및 조정
        if len(self.shap_values.shape) < 2 or self.shap_values.shape[1] != len(self.feature_names):
            print(f"[경고] SHAP 값 shape {self.shap_values.shape}와 feature_names 개수 {len(self.feature_names)}가 다릅니다.")
            # SHAP 값이 1차원(피처 1개)일 경우 reshape
            if len(self.shap_values.shape) == 1:
                self.shap_values = self.shap_values.reshape(-1, 1)
            self.feature_names = [f"feature_{i}" for i in range(self.shap_values.shape[1])]

        # SHAP 값 DataFrame 생성
        shap_df = pd.DataFrame(self.shap_values, columns=self.feature_names)
        pred_proba = self.model.predict_proba(self.X_test)[:, 1]
        shap_df['predicted_churn_proba'] = pred_proba
        shap_df['actual_churn'] = self.y_test.values

        test_indices = self.y_test.index
        shap_df['user_id'] = original_df.iloc[test_indices]['user_id'].values

        cols = ['user_id'] + self.feature_names + ['predicted_churn_proba', 'actual_churn']
        shap_df = shap_df[cols]

        # 예시: save_shap_csv 내에서
        print("shap_values shape:", self.shap_values.shape)
        print("pred_proba shape:", pred_proba.shape)
        print("actual_churn shape:", self.y_test.shape)
        print("user_id shape:", original_df.iloc[test_indices]['user_id'].shape)

        shap_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"SHAP 분석 결과가 {csv_path}에 저장되었습니다.")
        return csv_path