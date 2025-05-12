from flask import Blueprint, jsonify, request
from models.info_db import Info_db
from modules.info_db.info_db_module import get_info_db_all, get_info_db_by_company

info_db_bp = Blueprint('python-api/info_db', __name__)

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