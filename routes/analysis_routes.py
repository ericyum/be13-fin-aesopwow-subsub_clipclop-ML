from ctypes import c_buffer
import re
from flask import Blueprint, jsonify, Response, make_response,request, send_file
from flask_restx import Namespace, Resource, reqparse
from botocore.exceptions import NoCredentialsError, ClientError
from sqlalchemy import create_engine, MetaData, Table, select
from collections import Counter
from datetime import datetime
import pandas as pd
import io

from modules.analysis.analysis_module import cohort_list_s3_files, cohort_s3_file

from modules.analysis.cohort import analysis_cohort_FavGenre, analysis_cohort_LastLogin, analysis_cohort_PCL, analysis_cohort_SubscriptionType
from modules.info_db.info_db_module import get_info_db_by_info_db_no
from modules.analysis.ml_test import test_convert_data, test_ml

analysis_bp = Blueprint('analysis', __name__, url_prefix='/analysis')
analysis_ns = Namespace('analysis', description="Analysis related APIs")

cohort_get_one = reqparse.RequestParser()
cohort_get_one.add_argument("info_db_no", type=int, required=True, help="Info DB 번호 (필수)")
cohort_get_one.add_argument("analysis_type", type=str, required=True, help="분석 타입(PCL, SubscriptionType, FavGenre, LastLogin)(필수)")
cohort_get_one.add_argument("filename", type=str, required=True, help="S3 파일 이름 (필수)")

cohort_post = reqparse.RequestParser()
cohort_post.add_argument("info_db_no", type=int, required=True, help="Info DB 번호 (필수)")
cohort_post.add_argument("target_table_user", type=str, required=True, help="유저 테이블명(user_info) (필수)")
cohort_post.add_argument("target_table_sub", type=str, required=False, help="구독 테이블명(user_sub_info) (필수)")
cohort_post.add_argument("analysis_type", type=str, required=True, help="분석 타입(PCL, SubscriptionType, FavGenre, LastLogin)(필수)")
cohort_post.add_argument("target_date", type=str, required=True, help="분석 날짜 (YYYY-MM-DD 형식, 필수)")

@analysis_ns.route('/cohort')
class CohortAnalysis(Resource):
    @analysis_ns.expect(cohort_get_one)
    def get(self):
        args = cohort_get_one.parse_args()
        info_db_no = args["info_db_no"]
        analysis_type = args["analysis_type"]
        filename = args["filename"]

        file_content = cohort_s3_file(info_db_no, analysis_type, filename)

        return Response(
            file_content,
            mimetype='text/csv',
            headers={
                "Content-Disposition": f'attachment; filename="{filename}.csv"'
            }
        )

    @analysis_ns.expect(cohort_post)
    def post(self):
        args = cohort_post.parse_args()
        info_db_no = args["info_db_no"]
        target_table_user = args["target_table_user"]
        target_table_sub = args["target_table_sub"]
        analysis_type = args["analysis_type"]
        target_date = datetime.strptime(args.get("target_date"), "%Y-%m-%d")

        if analysis_type == "PCL":
            result = analysis_cohort_PCL(info_db_no, target_table_sub, target_date)
        elif analysis_type == "SubscriptionType":
            result = analysis_cohort_SubscriptionType(info_db_no, target_table_sub, target_date)
        elif analysis_type == "FavGenre":
            result = analysis_cohort_FavGenre(info_db_no, target_table_user, target_table_sub, target_date)
        elif analysis_type == "LastLogin":
            result = analysis_cohort_LastLogin(info_db_no, target_table_user, target_table_sub, target_date)
        else:
            return jsonify({'success': False, 'message': '잘못된 분석 타입입니다.'}), 500
            
        if result:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False}), 500
        
cohort_get_list = reqparse.RequestParser()
cohort_get_list.add_argument("info_db_no", type=int, required=True, help="Info DB 번호(필수)")
cohort_get_list.add_argument("analysis_type", type=str, required=False, help="분석 타입(PCL, SubscriptionType, FavGenre, LastLogin)")

@analysis_ns.route('/cohort/list')
class CohortAnalysis(Resource):
    @analysis_ns.expect(cohort_get_list)
    def get(self):
        args = cohort_get_list.parse_args()
        info_db_no = args["info_db_no"]
        analysis_type = args["analysis_type"]

        file_list = cohort_list_s3_files(info_db_no, analysis_type)
        return jsonify(file_list)