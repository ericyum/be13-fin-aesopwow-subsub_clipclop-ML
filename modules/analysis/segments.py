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