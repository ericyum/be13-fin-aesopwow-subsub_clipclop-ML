from models.info_column import Info_column

def get_info_columns_by_info_db_no(info_db_no):
    info_columns = Info_column.query.filter_by(info_db_no=info_db_no).all()
    return info_columns

def get_info_columns_by_info_db_no_origin_table(info_db_no, origin_table):
    info_columns = Info_column.query.filter_by(info_db_no=info_db_no, origin_table=origin_table).all()
    return info_columns

