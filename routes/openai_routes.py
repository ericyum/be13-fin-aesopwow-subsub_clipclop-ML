import io
from flask import Blueprint, request, jsonify
from flask_restx import Namespace, Resource, reqparse
import pandas as pd
from werkzeug.datastructures import FileStorage
import uuid
import os

from modules.analysis.analysis_module import s3_file
from modules.openai.csv_agent_module import analyze_csv, analyze_csv_from_bytes
from modules.openai.insight_service_module import extract_insight_and_recommendation

# Blueprint와 Namespace 설정
openai_bp = Blueprint('openai', __name__, url_prefix='/openai')
openai_ns = Namespace('openai', description="OpenAI related APIs")

# 파일 업로드 파서 설정
analyze_parser = reqparse.RequestParser()
analyze_parser.add_argument(
    "filename", type=str, required=True, help="CSV 파일명 (필수)"
)

PROMPT_TEMPLATE = """
너는 데이터 분석가야. 반드시 아래의 형식을 따라 답변해줘.

형식:
Thought: (네 생각)
Action: python_repl_ast
Action Input: (파이썬 코드)
Observation: (코드 실행 결과)
... (필요하다면 반복)
Final Answer: 
요약: (데이터의 주요 특징을 한 문장으로 요약)
인사이트: (데이터에서 발견한 중요한 인사이트를 2~3개로 정리)
행동 추천: (데이터 기반으로 실질적으로 도움이 되는 구체적이고 친절한 행동 추천을 반드시 3가지 제시해줘. 각 추천은 이유와 함께, 구체적인 실행 방법이나 예시도 포함해줘.)

예시:
Thought: 데이터에서 고객 이탈률을 확인해보자.
Action: python_repl_ast
Action Input: df['churn'].mean()
Observation: 0.23
Final Answer: 
요약: 전체 고객의 약 23%가 이탈했다.
인사이트: 이탈률이 업계 평균보다 높다. 특정 요인(예: 요금제, 서비스 불만)이 영향을 준 것으로 보인다.
행동 추천: 
1. 이탈 고객을 대상으로 설문조사를 실시하여, 이탈의 주요 원인을 파악하세요. 예를 들어, 이메일 또는 전화 설문을 통해 서비스 불만족 요인을 구체적으로 수집할 수 있습니다.
2. 불만족 요인이 높은 항목(예: 요금제, 고객 지원 등)에 대해 개선 방안을 마련하고, 개선된 내용을 뉴스레터나 공지사항을 통해 고객에게 적극적으로 안내하세요.
3. 이탈 위험이 높은 고객(최근 이용 빈도 감소 등)을 대상으로 맞춤형 할인 쿠폰이나 특별 프로모션을 제공하여 재이용을 유도하세요. 예시: 1개월 무료 이용권 제공 등.

질문: {user_question}
"""

@openai_ns.route('/analyze')
class OpenaiAnalyze(Resource):
    @openai_ns.expect(analyze_parser)
    def post(self):
        """
        CSV 파일과 자연어 질문을 받아 분석 및 인사이트/추천을 반환하는 엔드포인트
        """
        args = analyze_parser.parse_args()
        filename = args["filename"]

        file_content = s3_file(filename)
        if not file_content:
            return {"error": "S3에서 파일을 불러올 수 없습니다."}, 500

        # if hasattr(file, 'content_length') and file.content_length > 10 * 1024 * 1024:
        #     return {"error": "파일 크기는 10MB를 초과할 수 없습니다."}, 400

        user_question = args.get("question") or '이 데이터에서 주요 인사이트와 행동 추천을 한국어로 알려줘'

        full_prompt = PROMPT_TEMPLATE.format(user_question=user_question)

        try:
            # 1. LangChain + OpenAI로 분석 (temperature=0 등 옵션은 analyze_csv 내부에서 적용)
            llm_response = analyze_csv_from_bytes(file_content, full_prompt)

            # 2. 인사이트 및 행동 추천 추출
            insight = extract_insight_and_recommendation(llm_response)

            # 3. 결과 반환
            return {
                "summary": insight.summary,
                "recommendations": insight.recommendations
            }, 200
        except Exception as e:
            # 에러 발생 시 메시지 반환
            return {"error": "분석 중 오류가 발생했습니다." + str(e)}, 500