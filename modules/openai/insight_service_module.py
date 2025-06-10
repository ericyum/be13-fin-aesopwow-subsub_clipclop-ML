from models.insight import Insight

def extract_insight_and_recommendation(llm_response: str) -> Insight:
    """
    LLM의 자연어 응답에서 인사이트 요약과 행동 추천을 분리/정제합니다.
    (실제 서비스에서는 LLM 프롬프트를 설계해 json 등으로 받는 것도 추천)
    """
    if not llm_response or not llm_response.strip():
        return Insight(summary="", recommendations=[])

    lines = llm_response.split('\n')
    summary = ""
    recommendations = []
    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith("요약:"):
            summary = line.replace("요약:", "").strip()
        elif line.startswith("예측:"):
            prediction = line.replace("예측:", "").strip()
        elif line.startswith("추천:"):
            recs = line.replace("추천:", "").strip().split(';')
            recommendations = [r.strip() for r in recs if r.strip()]
        elif len(line) > 0 and line[0].isdigit() and '.' in line:
            dot_index = line.find('.')
            if dot_index != -1 and dot_index < len(line) - 1:
                recommendations.append(line[dot_index + 1:].strip())

    return Insight(summary=summary, recommendations=recommendations, prediction=prediction)