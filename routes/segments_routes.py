from flask import Blueprint, request, send_file
import os
from datetime import datetime
import pandas as pd
from modules.common.user.user_utils import load_data

segments_bp = Blueprint('segments', __name__, url_prefix='/api/segment')

@segments_bp.route('/download', methods=['GET'])
def segment_download():
    info_db_no = request.args.get('info_db_no', type=int)
    user_info = request.args.get('user_info', type=str)
    user_sub_info = request.args.get('user_sub_info', type=str)
    target_column = request.args.get('target_column', type=str)

    if info_db_no is None or not user_info or not user_sub_info or not target_column:
        return "info_db_no, user_info, user_sub_info, target_column 파라미터가 필요합니다.", 400

    try:
        df_user = load_data(info_db_no, user_info, user_sub_info, target_column)
        df_sub = load_data(info_db_no, user_sub_info, user_info, target_column)
    except Exception as e:
        return f"데이터 로드 중 오류: {str(e)}", 500

    if 'user_no' in df_user.columns:
        df_user.rename(columns={'user_no': 'user_id'}, inplace=True)
    if 'user_no' in df_sub.columns:
        df_sub.rename(columns={'user_no': 'user_id'}, inplace=True)

    user_columns = ['user_id', 'name', 'age', 'country', 'watch_time_hour', 'favorite_genre', 'last_login', 'gender']
    sub_columns = ['user_id', 'subscription_type']

    missing_user = [col for col in user_columns if col not in df_user.columns]
    missing_sub = [col for col in sub_columns if col not in df_sub.columns]
    if missing_user:
        return f"df_user에 다음 컬럼이 없습니다: {missing_user}", 500
    if missing_sub:
        return f"df_sub에 다음 컬럼이 없습니다: {missing_sub}", 500

    df_user = df_user[user_columns]
    df_sub = df_sub[sub_columns]
    df = pd.merge(df_user, df_sub, on='user_id', how='left')

    now = datetime.now()

    if target_column == 'watch_time':
        def get_watch_time_segment(hour):
            if pd.isna(hour):
                return 'unknown'
            if hour < 30:
                return 'Light User'
            elif hour < 60:
                return 'Core User'
            else:
                return 'Power User'
        df['segment'] = df['watch_time_hour'].apply(get_watch_time_segment)

    elif target_column == 'subscription':
        def get_sub_segment(sub):
            if pd.isna(sub):
                return 'unknown'
            sub = str(sub).lower()
            if sub == 'basic':
                return 'Basic'
            elif sub == 'standard':
                return 'Standard'
            elif sub == 'premium':
                return 'Premium'
            else:
                return 'unknown'
        df['segment'] = df['subscription_type'].apply(get_sub_segment)

    elif target_column == 'favorite_genre':
        def get_genre_segment(genre):
            if pd.isna(genre):
                return 'unknown'
            return str(genre)
        df['segment'] = df['favorite_genre'].apply(get_genre_segment)

    elif target_column == 'last_login':
        def get_login_segment(last_login):
            if pd.isna(last_login):
                return 'unknown'
            if isinstance(last_login, str):
                try:
                    last_login_dt = datetime.strptime(last_login, "%Y-%m-%d")
                except:
                    return 'unknown'
            else:
                last_login_dt = last_login
            diff = (now - last_login_dt).days
            if diff <= 7:
                return 'Frequent User'
            elif diff <= 30:
                return 'Dormant User'
            else:
                return 'Forgotten User'
        df['segment'] = df['last_login'].apply(get_login_segment)
    else:
        return f"Unknown target_column: {target_column}", 400

    # 파일 저장 경로
    now_str = now.strftime("%Y%m%d%H%M%S")
    save_dir = "csv_exports"
    os.makedirs(save_dir, exist_ok=True)
    filename = f"segment_{target_column}_{now_str}.csv"
    file_path = os.path.join(save_dir, filename)

    final_columns = ['segment'] + user_columns + ['subscription_type']
    df.to_csv(file_path, columns=final_columns, index=False, encoding="utf-8")

    # 파일 자체 반환
    return send_file(
        file_path,
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )
