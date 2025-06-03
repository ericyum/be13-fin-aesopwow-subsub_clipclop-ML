import csv
import io
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, select

from modules.info_db.info_db_module import get_info_db_by_info_db_no
from modules.info_column.info_column_module import get_info_columns_by_info_db_no_origin_table

def module_analysis_segments(data):
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerows(data)
    return csv_buffer.getvalue()

def test_convert_data(info_db_no: int, user_info: str, user_sub_info: str, target_column: str):
    """
    info_db_no: 첫 번째 DB 번호
    user_info: 첫 번째 DB의 테이블명
    user_sub_info: 두 번째 DB의 테이블명
    target_column: 분석에 사용될 컬럼명 (필요 시 사용)
    """

    # 첫 번째 DB 연결 정보
    info_db_1 = get_info_db_by_info_db_no(info_db_no).to_dict()
    if not info_db_1:
        print("첫 번째 DB 정보가 없습니다.")
        return None

    engine_1 = create_engine(
        f"mysql+pymysql://{info_db_1['user']}:{info_db_1['password']}@{info_db_1['host']}:{info_db_1['port']}/{info_db_1['name']}"
    )

    # 첫 번째 테이블 컬럼 매핑
    info_columns_1 = get_info_columns_by_info_db_no_origin_table(info_db_no, user_info)
    mapped_columns_1 = {col.origin_column: col.analysis_column for col in info_columns_1}

    metadata_1 = MetaData()
    table_1 = Table(user_info, metadata_1, autoload_with=engine_1)

    # 첫 번째 테이블 데이터 조회
    with engine_1.connect() as conn_1:
        query_1 = select(table_1)
        result_1 = conn_1.execute(query_1).mappings().all()
        mapped_result_1 = [
            {mapped_columns_1.get(k, k): v for k, v in row.items()}
            for row in result_1
        ]

    # 두 번째 DB 연결 정보 (예시로 info_db_no + 1 사용, 실제 로직에 맞게 수정)
    info_db_no_2 = info_db_no + 1  # 실제로는 파라미터로 받아야 할 수도 있음
    info_db_2 = get_info_db_by_info_db_no(info_db_no_2).to_dict()
    if not info_db_2:
        print("두 번째 DB 정보가 없습니다.")
        return None

    engine_2 = create_engine(
        f"mysql+pymysql://{info_db_2['user']}:{info_db_2['password']}@{info_db_2['host']}:{info_db_2['port']}/{info_db_2['name']}"
    )

    # 두 번째 테이블 컬럼 매핑
    info_columns_2 = get_info_columns_by_info_db_no_origin_table(info_db_no_2, user_sub_info)
    mapped_columns_2 = {col.origin_column: col.analysis_column for col in info_columns_2}

    metadata_2 = MetaData()
    table_2 = Table(user_sub_info, metadata_2, autoload_with=engine_2)

    # 두 번째 테이블 데이터 조회
    with engine_2.connect() as conn_2:
        query_2 = select(table_2)
        result_2 = conn_2.execute(query_2).mappings().all()
        mapped_result_2 = [
            {mapped_columns_2.get(k, k): v for k, v in row.items()}
            for row in result_2
        ]

    # 필요에 따라 두 테이블의 데이터를 합치거나, target_column을 기준으로 분석
    # 예시: 두 테이블 데이터를 합쳐서 반환
    combined_result = {
        'table_1': mapped_result_1,
        'table_2': mapped_result_2
    }

    print(combined_result)
    return combined_result
