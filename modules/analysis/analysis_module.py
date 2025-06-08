from modules.common.s3_client import get_s3_client, bucket_name
import boto3
import csv
from datetime import datetime, timezone
from io import StringIO
from flask import jsonify

from modules.common.user import user_utils
from modules.dash_board.line_graph import calculate_increase_decrease_per
from modules.dash_board.stacked_bar import get_monthly_cancelled_subscriptions, get_monthly_total_subscriptions
from modules.dash_board.stat_cards import get_cancellation_rate, get_increase_decrease_rate
from modules.devide.subscription import get_subscription_data

s3 = get_s3_client()

def module_get_s3_file(bucket_name, file_name):
    s3_object = s3.get_object(Bucket=bucket_name, Key=file_name)
    return s3_object

def upload_s3_file(file_path, bucket_name, object_name=None):
    s3.upload_file(file_path, bucket_name, object_name)

def save_pcl_csv_to_s3(info_db_no, p_retention, c_retention, l_retention):
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f'Cohort_PCL_{now}.csv'

    # CSV 내용을 StringIO로 만들기 (메모리 내 파일)
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    header = ['월', 'P 그룹 잔여율', 'C 그룹 잔여율', 'L 그룹 잔여율']
    writer.writerow(header)
    for month in range(1, 13):
        row = [
            month,
            p_retention.get(month, 0),
            c_retention.get(month, 0),
            l_retention.get(month, 0)
        ]
        writer.writerow(row)

    # S3 버킷 및 키 지정
    s3_key = f'{info_db_no}/cohort/PCL/{file_name}'

    # S3에 CSV 파일 업로드
    response = s3.put_object(
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            Bucket='python-aesop',
            ContentType='text/csv'
        )

    if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
        return False
    else:
        print("S3 파일 리스트를 성공적으로 등록하였습니다.")
        return True

def cohort_list_s3_files(info_db_no, analysis_type):
    if analysis_type is not None:
        file_path = f'{info_db_no}/cohort/{analysis_type}/'
    else:
        file_path = f'{info_db_no}/cohort/'

    # 리스트 불러오기
    response = s3.list_objects_v2(Bucket='python-aesop',Prefix=file_path)

    if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
        print("S3 파일 리스트를 불러오는 데 실패했습니다.")
        return None
    else:
        print("S3 파일 리스트를 성공적으로 불러왔습니다.")
        return response
    
def s3_file(filename):
    # S3에서 파일 직접 읽기
    response = s3.get_object(Bucket='python-aesop', Key=filename)

    if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
        print("S3 파일 가져오기 실패")
        return None

    # 파일 내용 읽기
    file_content = response['Body'].read()

    return file_content
    
def cohort_s3_file(info_db_no, analysis_type,filename):
    file_path = f'{info_db_no}/cohort/{analysis_type}/{filename}.csv'

    # S3에서 파일 직접 읽기
    response = s3.get_object(Bucket='python-aesop', Key=file_path)

    if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
        print("S3 파일 가져오기 실패")
        return None

    # 파일 내용 읽기
    file_content = response['Body'].read()

    return file_content

def save_SubscriptionType_csv_to_s3(info_db_no, premium_retention, standard_retention, bassic_retention):
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f'Cohort_SubscriptionType_{now}.csv'

    # CSV 내용을 StringIO로 만들기 (메모리 내 파일)
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    header = ['월', 'Premium 잔여율', 'Standard 잔여율', 'Basic 잔여율']
    writer.writerow(header)
    for month in range(1, 13):
        row = [
            month,
            premium_retention.get(month, 0),
            standard_retention.get(month, 0),
            bassic_retention.get(month, 0)
        ]
        writer.writerow(row)

    # S3 버킷 및 키 지정
    s3_key = f'{info_db_no}/cohort/SubscriptionType/{file_name}'

    # S3에 CSV 파일 업로드
    response = s3.put_object(
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            Bucket='python-aesop',
            ContentType='text/csv'
        )

    if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
        return False
    else:
        print("S3 파일 리스트를 성공적으로 등록하였습니다.")
        return True
    
def save_FavGenre_csv_to_s3(info_db_no, comedy_retention, horror_retention, drama_retention, romance_retention, action_retention, documentary_retention, sci_fi_retention):
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f'Cohort_FavGenre_{now}.csv'

    # CSV 내용을 StringIO로 만들기 (메모리 내 파일)
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    header = ['월', 'Comedy 잔여율', 'Horror 잔여율', 'Drama 잔여율', 'Romance 잔여율', 'Action 잔여율', 'Documentary 잔여율', 'Sci-Fi 잔여율']
    writer.writerow(header)
    for month in range(1, 13):
        row = [
            month,
            comedy_retention.get(month, 0),
            horror_retention.get(month, 0),
            drama_retention.get(month, 0),
            romance_retention.get(month, 0),
            action_retention.get(month, 0),
            documentary_retention.get(month, 0),
            sci_fi_retention.get(month, 0)
        ]
        writer.writerow(row)

    # S3 버킷 및 키 지정
    s3_key = f'{info_db_no}/cohort/FavGenre/{file_name}'

    # S3에 CSV 파일 업로드
    response = s3.put_object(
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            Bucket='python-aesop',
            ContentType='text/csv'
        )

    if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
        return False
    else:
        print("S3 파일 리스트를 성공적으로 등록하였습니다.")
        return True
    
def save_LastLogin_csv_to_s3(info_db_no, frequent_retention, dormant_retention, forgotten_retention):
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f'Cohort_LastLogin_{now}.csv'

    # CSV 내용을 StringIO로 만들기 (메모리 내 파일)
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    header = ['월', 'Frequent 잔여율', 'Dormant 잔여율', 'Forgotten 잔여율']
    writer.writerow(header)
    for month in range(1, 13):
        row = [
            month,
            frequent_retention.get(month, 0),
            dormant_retention.get(month, 0),
            forgotten_retention.get(month, 0)
        ]
        writer.writerow(row)

    # S3 버킷 및 키 지정
    s3_key = f'{info_db_no}/cohort/LastLogin/{file_name}'

    # S3에 CSV 파일 업로드
    response = s3.put_object(
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            Bucket='python-aesop',
            ContentType='text/csv'
        )

    if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
        return False
    else:
        print("S3 파일 리스트를 성공적으로 등록하였습니다.")
        return True
    
def save_Dashboard_csv_to_s3(info_db_no, user_info, user_sub_info):
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f'Dashboard_{now}.csv'

    # 1. 기본 지표
    metrics = {
        'entire_users': user_utils.get_total_users(info_db_no, user_info),
        'new_users': user_utils.get_new_users(info_db_no, user_info),
        'active_users': user_utils.get_active_users(info_db_no, user_info),
        'dormant_users': user_utils.get_dormant_users(info_db_no, user_info),
        'increase_decrease_rate': get_increase_decrease_rate(info_db_no, user_sub_info),
        'cancellation_rate': get_cancellation_rate(info_db_no, user_sub_info)
    }

    # 2. 월별 스택바 데이터 (예: 구독 모델별 비율)
    monthly_total = get_monthly_total_subscriptions(info_db_no, user_sub_info)
    monthly_cancelled = get_monthly_cancelled_subscriptions(info_db_no, user_sub_info)

    # 3. 신규 사용자는 월별 데이터로 받아오기 (딕셔너리 반환하도록 monthly=True)
    new_users_data = get_subscription_data(info_db_no, user_sub_info, 'new', monthly=True)

    # 4. 월별 증감률 계산
    increase_decrease_df = calculate_increase_decrease_per(info_db_no, user_sub_info)

    # 5. CSV 작성
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)

    # 5-1. 메트릭 헤더 및 데이터
    writer.writerow(['metric', 'value', 'timestamp'])
    now_utc = datetime.now(timezone.utc).isoformat()
    for key, value in metrics.items():
        writer.writerow([key.replace('_', ' ').title(), value, now_utc])

    # 5-2. 월별 스택바 헤더 및 데이터
    writer.writerow([])  # 빈 줄로 구분
    writer.writerow(['month', 'type', 'basic(%)', 'standard(%)', 'premium(%)'])

    # 전체 사용자
    for month, (basic, standard, premium) in monthly_total.items():
        writer.writerow([month, 'active', basic, standard, premium])

    # 해지 사용자
    for month, (basic, standard, premium) in monthly_cancelled.items():
        writer.writerow([month, 'cancelled', basic, standard, premium])

    # 신규 사용자 (월별 데이터)
    writer.writerow([])  # 빈 줄로 구분
    writer.writerow(['month', 'type', 'basic(%)', 'standard(%)', 'premium(%)'])
    for month, (basic, standard, premium) in new_users_data.items():
        writer.writerow([month, 'new', basic, standard, premium])

    # 5-3. 월별 증감률 데이터
    writer.writerow([])  # 빈 줄로 구분
    writer.writerow(['month', 'subscribers', 'rate(%)'])
    for _, row in increase_decrease_df.iterrows():
        writer.writerow([
            row['month'],
            round(row.get('cumulative', 0), 2),  # 누적 가입자 수 (있다면)
            round(row['increase_decrease_per'], 2)  # 증감률 소수점 둘째 자리
        ])

    # S3 버킷 및 키 지정
    s3_key = f'{info_db_no}/dashboard/{file_name}'

    # S3에 CSV 파일 업로드
    response = s3.put_object(
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            Bucket='python-aesop',
            ContentType='text/csv'
        )

    if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
        return False
    else:
        print("S3 파일 리스트를 성공적으로 등록하였습니다.")
        return True
    
def dashboard_s3_list(info_db_no):
    file_path = f'{info_db_no}/dashboard/'

    # S3에서 파일 직접 읽기
    response = s3.list_objects_v2(Bucket='python-aesop',Prefix=file_path)

    if 'Contents' not in response:
        print("S3 파일 리스트가 비어있습니다.")
        return None

    sorted_objects = sorted(
        response['Contents'],
        key=lambda obj: obj['LastModified'],
        reverse=True
    )

    return sorted_objects[0]

def dashboard_s3_file(file_path):
    # S3에서 파일 직접 읽기
    response = s3.get_object(Bucket='python-aesop', Key=file_path)

    if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
        print("S3 파일 가져오기 실패")
        return None

    # 파일 내용 읽기
    file_content = response['Body'].read()

    return file_content