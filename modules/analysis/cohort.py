import csv
import io
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, extract, select

from modules.info_db.info_db_module import get_info_db_by_info_db_no
from modules.info_column.info_column_module import get_info_columns_by_info_db_no_origin_table

def cohort_analysis(data):

    i = 1
    light_user = []
    core_user = []
    power_user = []

    for user in data:
        user_watch_time_hours = user.get('watch_time_hours', 0)
        # user_
        i += 1
        if user_watch_time_hours < 200:
            light_user.append(user.get('user_id'))
        elif user_watch_time_hours < 700:
            core_user.append(user.get('user_id'))
        else:
            power_user.append(user.get('user_id'))
    
    # csv_buffer = io.StringIO()
    # writer = csv.writer(csv_buffer)

    # writer.writerows(data)

    # return csv_buffer.getvalue()
    return light_user, core_user, power_user

def test_convert_data(info_db_no, origin_table):

    info_db = get_info_db_by_info_db_no(info_db_no).to_dict()
    user = info_db.get('user')
    host = info_db.get('host')
    password = info_db.get('password')
    port = info_db.get('port')
    name = info_db.get('name')

    if not info_db:
        return None
    else:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")

        info_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, origin_table)

        mapped_columns = {}

        for col in info_columns:
            mapped_columns[col.origin_column] = col.analysis_column

        metadata = MetaData()

        external_table = Table(origin_table, metadata, autoload_with=engine)

        with engine.connect() as conn:
            query = select(external_table)
            result = conn.execute(query).mappings().all()

            mapped_result = [
                {mapped_columns.get(k, k): v for k, v in row.items()}
                for row in result
            ]

        print(mapped_result)
        return mapped_result
    
def get_user_info_by_year(info_db_no, origin_table, year):

    info_db = get_info_db_by_info_db_no(info_db_no).to_dict()
    user = info_db.get('user')
    host = info_db.get('host')
    password = info_db.get('password')
    port = info_db.get('port')
    name = info_db.get('name')

    if not info_db:
        return None
    else:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")

        info_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, origin_table)

        mapped_columns = {}

        for col in info_columns:
            mapped_columns[col.origin_column] = col.analysis_column

        metadata = MetaData()

        external_table = Table(origin_table, metadata, autoload_with=engine)

        with engine.connect() as conn:
            query = select(external_table)
            result = conn.execute(query).mappings().all()

            mapped_result = [
                {mapped_columns.get(k, k): v for k, v in row.items()}
                for row in result
            ]

        print(mapped_result)
        return mapped_result
    
def get_user_sub_info_by_year(info_db_no, user_info, user_sub_info, year):
    info_db = get_info_db_by_info_db_no(info_db_no).to_dict()
    user = info_db.get('user')
    host = info_db.get('host')
    password = info_db.get('password')
    port = info_db.get('port')
    name = info_db.get('name')

    if not info_db:
        return None
    else:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")

        # Get user info columns
        info_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_info)

        user_info_mapped_columns = {}

        for col in info_columns:
            user_info_mapped_columns[col.origin_column] = col.analysis_column

        metadata = MetaData()

        user_info_external_table = Table(user_info, metadata, autoload_with=engine)

        # Get user sub info columns
        user_sub_info_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, user_sub_info)

        user_sub_info_mapped_columns = {}
        for col in user_sub_info_columns:
            user_sub_info_mapped_columns[col.origin_column] = col.analysis_column

            user_sub_info_external_table = Table(user_sub_info, metadata, autoload_with=engine)

        with engine.connect() as conn:
            query = (
                select(user_info_external_table)
                .where(extract('year', user_info_external_table.c.last_login) == year)
                )
            result = conn.execute(query).mappings().all()

            user_info_mapped_result = [
                {user_info_mapped_columns.get(k, k): v for k, v in row.items()}
                for row in result
            ]

            query = (
                select(user_sub_info_external_table)
                .where(extract('year', user_sub_info_external_table.c.ended_at) == year)
                )
            result = conn.execute(query).mappings().all()

            user_sub_info_mapped_result = [
                {user_sub_info_mapped_columns.get(k, k): v for k, v in row.items()}
                for row in result
            ]

        print(user_info_mapped_result)
        print(user_sub_info_mapped_result)
        return None