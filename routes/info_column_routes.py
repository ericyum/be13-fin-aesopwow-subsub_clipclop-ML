from flask import Blueprint, json, Response, request
from models.info_column import Info_column

info_column_bp = Blueprint('python-api/info_column', __name__)

@info_column_bp.route('', methods=['GET'])
def get_info_column_all():
    info_db_no = request.args.get('info_db_no', type=int)
    origin_table = request.args.get('origin_table', type=str)

    if info_db_no:
        if origin_table:
            info_columns = Info_column.query.filter_by(info_db_no=info_db_no, origin_table=origin_table).all()
        else:
            info_columns = Info_column.query.filter_by(info_db_no=info_db_no).all()
        
        if info_columns:
            return Response(
                json.dumps([info_column.to_dict() for info_column in info_columns], ensure_ascii=False),
                content_type="application/json; charset=utf-8"
            )
        else:
            return Response({"error": "Info columns not found"}), 404
    else:
        return Response({"error": "Info dbs not found"}), 404
    
@info_column_bp.route('/<int:info_db_no>', methods=['GET'])
def get_info_column_by_info_db_no(info_db_no):
    info_columns = Info_column.query.filter_by(info_db_no=info_db_no).all()
    if info_columns:
        return Response([info_column.to_dict() for info_column in info_columns])
    else:
        return Response({"error": "Info columns not found"}), 404