from flask import Blueprint, json, Response, request
from flask_restx import Namespace, Resource
from models.info_column import Info_column

from modules.info_column.info_column_module import get_info_columns_by_info_db_no, get_info_columns_by_info_db_no_origin_table

info_column_bp = Blueprint('info_column', __name__, url_prefix='/info_column')
info_column_ns = Namespace('info_column', description="Info_column related APIs")

@info_column_bp.route('', methods=['GET'])
def get_info_column_all():
    info_db_no = request.args.get('info_db_no', type=int)
    origin_table = request.args.get('origin_table', type=str)

    if info_db_no:
        if origin_table:
            info_columns = get_info_columns_by_info_db_no_origin_table(info_db_no, origin_table)
        else:
            info_columns = get_info_columns_by_info_db_no(info_db_no)
        
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
    info_columns = get_info_columns_by_info_db_no(info_db_no)
    if info_columns:
        return Response([info_column.to_dict() for info_column in info_columns])
    else:
        return Response({"error": "Info columns not found"}), 404
    
@info_column_ns.route('/test')
class TestAnalysis(Resource):
    def get(self):
        """테스트용 API"""
        # 여기에 테스트용 로직을 추가할 수 있습니다.
        # 예를 들어, 간단한 메시지를 반환하는 API
        return {"message": "This is a test API for analysis."}, 200