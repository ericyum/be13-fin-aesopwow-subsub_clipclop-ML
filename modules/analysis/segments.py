# # modules/analysis/segments.py

# import os
# from datetime import datetime
# from flask import jsonify
# import pandas as pd

# def segment_common_download(
#     info_db_no,
#     user_info,
#     user_sub_info,
#     target_column,
#     segment_func,
#     user_columns=None,
#     sub_columns=None
# ):
#     # 기본 컬럼 세팅
#     if user_columns is None:
#         user_columns = ['user_id', 'name', 'age', 'country', 'watch_time_hour', 'favorite_genre', 'last_login', 'gender']
#     if sub_columns is None:
#         sub_columns = ['user_id', 'subscription_type']

#     try:
#         from routes.segments_routes import load_data, upload_file_to_s3  # 경로에 따라 import 위치 조정 필요
#         df_user = load_data(info_db_no, user_info, user_sub_info, target_column)
#         df_sub = load_data(info_db_no, user_sub_info, user_info, target_column)
#     except Exception as e:
#         return jsonify({"success": False, "message": f"데이터 로드 중 오류: {str(e)}"}), 500

#     if 'user_no' in df_user.columns:
#         df_user.rename(columns={'user_no': 'user_id'}, inplace=True)
#     if 'user_no' in df_sub.columns:
#         df_sub.rename(columns={'user_no': 'user_id'}, inplace=True)

#     missing_user = [col for col in user_columns if col not in df_user.columns]
#     missing_sub = [col for col in sub_columns if col not in df_sub.columns]
#     if missing_user:
#         return jsonify({"success": False, "message": f"df_user에 다음 컬럼이 없습니다: {missing_user}"}), 500
#     if missing_sub:
#         return jsonify({"success": False, "message": f"df_sub에 다음 컬럼이 없습니다: {missing_sub}"}), 500

#     df_user = df_user[user_columns]
#     df_sub = df_sub[sub_columns]
#     df = pd.merge(df_user, df_sub, on='user_id', how='left')

#     # 세그먼트 분류
#     df['segment'] = segment_func(df)

#     now = datetime.now()
#     now_str = now.strftime("%Y%m%d%H%M%S")
#     save_dir = "csv_exports"
#     os.makedirs(save_dir, exist_ok=True)
#     local_filename = f"{info_db_no}_segment_{target_column}_{now_str}.csv"
#     file_path = os.path.join(save_dir, local_filename)

#     final_columns = ['segment'] + user_columns + ['subscription_type']
#     try:
#         df.to_csv(file_path, columns=final_columns, index=False, encoding="utf-8")
#     except Exception as e:
#         return jsonify({"success": False, "message": f"CSV 저장 중 오류: {str(e)}"}), 500

#     s3_key = f"{info_db_no}/segment/{target_column}/{local_filename}"
#     try:
#         upload_file_to_s3(file_path, s3_key)
#     except Exception as e:
#         return jsonify({"success": False, "message": f"S3 업로드 중 오류: {str(e)}"}), 500

#     return jsonify({
#         "success": True,
#         "filename": local_filename,
#         "s3_key": s3_key
#     }), 200
