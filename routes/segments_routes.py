from flask import Blueprint, request, make_response
import re
import csv
from io import StringIO

from modules.common.user.user_utils import (
    determine_subscription_model,
    calculate_percentages,
    determine_watch_time_segment,
    determine_genre_segment,
    determine_last_login_segment,
    load_data
)
from modules.devide.segments import add_segments

segments_bp = Blueprint('python-api/segments', __name__)

@segments_bp.route('/csv', methods=['GET'])
def segments_csv():
    info_db_no = request.args.get('info_db_no', type=int)
    origin_table = request.args.get('origin_table', type=str)
    domain = request.args.get('domain', 'subscription')

    if info_db_no is None or not origin_table:
        return "info_db_no와 origin_table 파라미터가 필요합니다.", 400

    try:
        df = load_data(info_db_no, origin_table)
        if df.empty:
            return "해당 테이블에 데이터가 없습니다.", 404
        df = add_segments(df)
    except Exception as e:
        return f"데이터 로드 중 오류: {str(e)}", 500

    # CSV 파일 생성
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)

    # 헤더 작성
    if domain == 'subscription':
        writer.writerow(['segment', 'count', 'percentage'])
        basic, standard, premium = determine_subscription_model(df)
        perc = calculate_percentages(basic, standard, premium)
        writer.writerow(['basic', len(basic), perc[0]])
        writer.writerow(['standard', len(standard), perc[1]])
        writer.writerow(['premium', len(premium), perc[2]])
    elif domain == 'watch_time':
        writer.writerow(['segment', 'count', 'percentage'])
        light, core, power = determine_watch_time_segment(df)
        perc = calculate_percentages(light, core, power)
        writer.writerow(['light', len(light), perc[0]])
        writer.writerow(['core', len(core), perc[1]])
        writer.writerow(['power', len(power), perc[2]])
    elif domain == 'genre':
        genre_names = ["drama", "sci-fi", "comedy", "documentary", "romance", "action", "horror"]
        genre_groups = determine_genre_segment(df)
        perc = calculate_percentages(*genre_groups)
        writer.writerow(['segment', 'count', 'percentage'])
        for name, group, p in zip(genre_names, genre_groups, perc):
            writer.writerow([name, len(group), p])
    elif domain == 'last_login':
        writer.writerow(['segment', 'count', 'percentage'])
        forgotten, dormant, frequent = determine_last_login_segment(df)
        perc = calculate_percentages(forgotten, dormant, frequent)
        writer.writerow(['forgotten', len(forgotten), perc[0]])
        writer.writerow(['dormant', len(dormant), perc[1]])
        writer.writerow(['frequent', len(frequent), perc[2]])
    else:
        return f"Unknown domain: {domain}", 400

    # 응답 반환
    response = make_response(csv_buffer.getvalue())
    response.headers['Content-Type'] = 'text/csv'
    safe_table_name = re.sub(r'[^\w\-_]', '_', origin_table)
    response.headers['Content-Disposition'] = f'attachment; filename={safe_table_name}_{domain}_segments.csv'
    return response