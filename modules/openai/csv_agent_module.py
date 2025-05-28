import os
from pathlib import Path
from langchain_experimental.agents import create_csv_agent
from langchain_openai import ChatOpenAI
from resources.config.openai_config import OPENAI_API_KEY

def analyze_csv(csv_path: str, question: str) -> str:
    """
    LangChain과 OpenAI LLM을 사용해 CSV 파일을 분석하고, 자연어 답변을 반환합니다.

    Args:
        csv_path: 분석할 CSV 파일 경로
        question: 분석에 대한 자연어 질문

    Returns:
        분석 결과 문자열

    Raises:
        FileNotFoundError: CSV 파일이 존재하지 않을 때
        ValueError: 잘못된 입력 파라미터일 때
    """
    if not csv_path or not question.strip():
        raise ValueError("CSV 파일 경로와 질문은 필수입니다.")
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_path}")

    llm = ChatOpenAI(
        model_name="gpt-4o",  # model → model_name
        api_key=OPENAI_API_KEY,
        temperature=0
    )
    agent = create_csv_agent(
        llm,
        csv_path,
        verbose=True,
        allow_dangerous_code=False,
        agent_executor_kwargs={"handle_parsing_errors": True}  # 옵션은 agent_executor_kwargs로 전달
    )
    response = agent.run(question)
    return response
