from models.insight import Insight

def extract_insight_and_recommendation(llm_response: str) -> Insight:
    """
    LLM의 자연어 응답에서 인사이트 요약과 행동 추천을 분리/정제합니다.
    (실제 서비스에서는 LLM 프롬프트를 설계해 json 등으로 받는 것도 추천)
    """
    # 예시: "요약: ... \n추천: 1. ... 2. ... "
    lines = llm_response.split('\n')
    summary = ""
    recommendations = []
    for line in lines:
        if line.startswith("요약:"):
            summary = line.replace("요약:", "").strip()
        elif line.startswith("추천:"):
            recs = line.replace("추천:", "").strip().split(';')
            recommendations = [r.strip() for r in recs if r.strip()]
        elif line and line[0].isdigit():
            recommendations.append(line[line.find('.')+1:].strip())
    return Insight(summary=summary, recommendations=recommendations)
