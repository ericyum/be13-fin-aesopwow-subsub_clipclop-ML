from flask import Blueprint, request, jsonify
import re
import csv
from io import StringIO
import os

from modules.common.user.user_utils import (
    determine_subscription_model,
    determine_watch_time_segment,
    determine_genre_segment,
    determine_last_login_segment,
    load_data
)
from modules.devide.segments import add_segments

segments_bp = Blueprint('segments', __name__)

@segments_bp.route('/csv', methods=['GET'])
def segments_csv():
    info_db_no = request.args.get('info_db_no', type=int)
    user_info = request.args.get('user_info', type=str)
    user_sub_info = request.args.get('user_sub_info', type=str)
    target_column = request.args.get('target_column', type=str)

    # 필수 파라미터 체크
    if info_db_no is None or not user_info or not user_sub_info or not target_column:
        return "info_db_no, user_info, user_sub_info, target_column 파라미터가 필요합니다.", 400
    
    print("info_db_no:", info_db_no)
    print("user_info:", user_info)
    print("user_sub_info:", user_sub_info)
    print("target_column:", target_column)

    try:
        print("Calling load_data...")
        df = load_data(info_db_no, user_info, user_sub_info, target_column)
        print("DataFrame loaded:", df.shape)
        if df.empty:
            return "해당 테이블에 데이터가 없습니다.", 404
        print("Calling add_segments...")
        df = add_segments(df)
    except Exception as e:
        import traceback
        traceback.print_exc()  # 콘솔에 전체 에러 출력
        return f"데이터 로드 중 오류: {str(e)}", 500

    # CSV 파일 생성
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)

    user_columns = list(df.columns)

    if target_column == 'subscription':
        basic, standard, premium = determine_subscription_model(df)
        writer.writerow(['segment'] + user_columns)
        for user in basic:
            writer.writerow(['basic'] + [user.get(col, "") for col in user_columns])
        for user in standard:
            writer.writerow(['standard'] + [user.get(col, "") for col in user_columns])
        for user in premium:
            writer.writerow(['premium'] + [user.get(col, "") for col in user_columns])

    elif target_column == 'watch_time':
        light, core, power = determine_watch_time_segment(df)
        writer.writerow(['segment'] + user_columns)
        for user in light:
            writer.writerow(['light'] + [user.get(col, "") for col in user_columns])
        for user in core:
            writer.writerow(['core'] + [user.get(col, "") for col in user_columns])
        for user in power:
            writer.writerow(['power'] + [user.get(col, "") for col in user_columns])

    elif target_column == 'genre':
        genre_names = ["drama", "sci-fi", "comedy", "documentary", "romance", "action", "horror"]
        genre_groups = determine_genre_segment(df)
        writer.writerow(['segment'] + user_columns)
        for name, group in zip(genre_names, genre_groups):
            for user in group:
                writer.writerow([name] + [user.get(col, "") for col in user_columns])

    elif target_column == 'last_login':
        forgotten, dormant, frequent = determine_last_login_segment(df)
        writer.writerow(['segment'] + user_columns)
        for user in forgotten:
            writer.writerow(['forgotten'] + [user.get(col, "") for col in user_columns])
        for user in dormant:
            writer.writerow(['dormant'] + [user.get(col, "") for col in user_columns])
        for user in frequent:
            writer.writerow(['frequent'] + [user.get(col, "") for col in user_columns])

    else:
        return f"Unknown target_column: {target_column}", 400

    # 파일명 및 저장 경로 생성
    safe_table_name = re.sub(r'[^\w\-_]', '_', user_info)
    filename = f"{safe_table_name}_{target_column}_segments.csv"
    save_dir = "./csv_exports"
    os.makedirs(save_dir, exist_ok=True)
    file_path = os.path.join(save_dir, filename)

    # 파일 저장
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        f.write(csv_buffer.getvalue())

    print(f"CSV 파일 저장 완료: {file_path}")

    # 파일명 응답
    return jsonify({
        "message": "CSV 파일이 성공적으로 생성되었습니다.",
        "file_path": file_path,
        "file_name": filename
    })
