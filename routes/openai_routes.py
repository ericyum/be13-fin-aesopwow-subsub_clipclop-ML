from flask import Blueprint, request, jsonify
from modules.openai.csv_agent_module import analyze_csv
from modules.openai.insight_service_module import extract_insight_and_recommendation

openai_bp = Blueprint('analyze', __name__)

@openai_bp.route('/analyze', methods=['POST'])
def analyze():
    """
    CSV 파일과 자연어 질문을 받아 분석 및 인사이트/추천을 반환하는 엔드포인트
    """
    file = request.files['file']
    question = request.form.get('question', '이 데이터에서 주요 인사이트와 행동 추천을 한국어로 알려줘')
    csv_path = f'/tmp/{file.filename}'
    file.save(csv_path)

    # 1. LangChain+OpenAI로 분석
    llm_response = analyze_csv(csv_path, question)

    # 2. 인사이트 및 행동 추천 추출
    insight = extract_insight_and_recommendation(llm_response)

    # 3. 결과 반환
    return jsonify({
        "summary": insight.summary,
        "recommendations": insight.recommendations
    })
