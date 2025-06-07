import io
from pathlib import Path
from langchain_experimental.agents import create_csv_agent
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_openai import ChatOpenAI
import pandas as pd
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
        model="gpt-4o",
        api_key=OPENAI_API_KEY,
        temperature=0
    )
    agent = create_csv_agent(
        llm,
        csv_path,
        verbose=True,
        allow_dangerous_code=True,
        agent_executor_kwargs={"handle_parsing_errors": True}
    )
    response = agent.run(question)
    return response

def analyze_csv_from_bytes(csv_bytes: bytes, question: str) -> str:
    if not csv_bytes or not question.strip():
        raise ValueError("CSV 데이터와 질문은 필수입니다.")

    # 1. CSV 내용을 DataFrame으로 변환
    df = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))

    # 2. LangChain agent 생성
    llm = ChatOpenAI(
        model="gpt-4o",
        api_key=OPENAI_API_KEY,
        temperature=0
    )

    agent = create_pandas_dataframe_agent(
        llm,
        df,
        verbose=True,
        allow_dangerous_code=True,
        agent_executor_kwargs={"handle_parsing_errors": True}
    )

    # 3. 질문 실행
    response = agent.run(question)
    return response
