from flask import Blueprint, jsonify, request
from flask_restx import Namespace, Resource
from models.info_db import User
from modules.info_db.info_db_module import get_info_db_all, get_info_db_by_company

info_db_bp = Blueprint('info_db', __name__, url_prefix='/info_db')
info_db_ns = Namespace('info_db', description="Info_db related APIs")


@info_db_bp.route('', methods=['GET'])
def get_info_db():
    company_no = request.args.get('company_no', type=int)

    if company_no:
        info_dbs = get_info_db_by_company(company_no)

        if info_dbs:
            return jsonify([info_db.to_dict() for info_db in info_dbs])
        else:
            return jsonify({"error": "Info dbs not found"}), 404
    else:
        info_dbs = get_info_db_all()
        return jsonify([info_db.to_dict() for info_db in info_dbs])
    
@info_db_ns.route('/test')
class TestAnalysis(Resource):
    def get(self):
        """테스트용 API"""
        # 여기에 테스트용 로직을 추가할 수 있습니다.
        # 예를 들어, 간단한 메시지를 반환하는 API
        return {"message": "This is a test API for analysis."}, 200