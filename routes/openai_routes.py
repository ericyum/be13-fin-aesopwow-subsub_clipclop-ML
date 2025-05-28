from flask import Blueprint, request, jsonify
from flask_restx import Namespace, Resource, reqparse
from werkzeug.datastructures import FileStorage
import uuid
import os

from modules.openai.csv_agent_module import analyze_csv
from modules.openai.insight_service_module import extract_insight_and_recommendation

# Blueprint와 Namespace 설정
openai_bp = Blueprint('openai', __name__, url_prefix='/openai')
openai_ns = Namespace('openai', description="OpenAI related APIs")

# 파일 업로드 파서 설정
analyze_parser = reqparse.RequestParser()
analyze_parser.add_argument(
    "file", type=FileStorage, location="files", required=True, help="CSV 파일 (필수)"
)
analyze_parser.add_argument(
    "question", type=str, location="form", required=False, help="질문 (선택)"
)

PROMPT_TEMPLATE = """
너는 데이터 분석가야. 반드시 아래의 형식을 따라 답변해줘.

형식:
Thought: (네 생각)
Action: python_repl_ast["파이썬 코드"]
Observation: (코드 실행 결과)
... (필요하다면 반복)
Final Answer: 
요약: (데이터의 주요 특징을 한 문장으로 요약)
인사이트: (데이터에서 발견한 중요한 인사이트를 2~3개로 정리)
행동 추천: (데이터 기반으로 추천할 구체적인 행동을 2~3개로 제시)

예시:
Thought: 데이터에서 고객 이탈률을 확인해보자.
Action: python_repl_ast["df['churn'].mean()"]
Observation: 0.23
Final Answer: 
요약: 전체 고객의 약 23%가 이탈했다.
인사이트: 이탈률이 업계 평균보다 높다. 특정 요인(예: 요금제, 서비스 불만)이 영향을 준 것으로 보인다.
행동 추천: 이탈 고객 대상 설문조사를 실시하고, 불만 요인을 개선하는 프로모션을 진행하자.

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
        file = args["file"]
        user_question = args.get("question") or '이 데이터에서 주요 인사이트와 행동 추천을 한국어로 알려줘'

        # 파일명 중복 방지
        unique_filename = f"{uuid.uuid4().hex}_{file.filename}"
        csv_path = f"/tmp/{unique_filename}"
        file.save(csv_path)

        full_prompt = PROMPT_TEMPLATE.format(user_question=user_question)

        try:
            # 1. LangChain + OpenAI로 분석 (temperature=0 등 옵션은 analyze_csv 내부에서 적용)
            llm_response = analyze_csv(csv_path, full_prompt)

            # 2. 인사이트 및 행동 추천 추출
            insight = extract_insight_and_recommendation(llm_response)

            # 3. 결과 반환
            return {
                "summary": insight.summary,
                "recommendations": insight.recommendations
            }, 200
        except Exception as e:
            # 에러 발생 시 메시지 반환
            return {"error": str(e)}, 500
        finally:
            # 임시 파일 삭제
            if os.path.exists(csv_path):
                os.remove(csv_path)

def analyze():
    """
    CSV 파일과 자연어 질문을 받아 분석 및 인사이트/추천을 반환하는 엔드포인트
    """
    file = request.files['file']
    user_question = request.form.get('question', '이 데이터에서 주요 인사이트와 행동 추천을 한국어로 알려줘')

    unique_filename = f"{uuid.uuid4().hex}_{file.filename}"
    csv_path = f"/tmp/{unique_filename}"
    file.save(csv_path)

    full_prompt = PROMPT_TEMPLATE.format(user_question=user_question)

    try:
        llm_response = analyze_csv(csv_path, full_prompt)
        insight = extract_insight_and_recommendation(llm_response)
        return jsonify({
            "summary": insight.summary,
            "recommendations": insight.recommendations
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if os.path.exists(csv_path):
            os.remove(csv_path)
