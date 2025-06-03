from modules.common.s3_client import get_s3_client, bucket_name
import boto3
import csv
from datetime import datetime
from io import StringIO
from flask import jsonify

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