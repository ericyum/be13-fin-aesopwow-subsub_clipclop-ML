from langchain_experimental.agents import create_csv_agent
from langchain_openai import ChatOpenAI
from resources.config.openai_config import OPENAI_API_KEY

def analyze_csv(csv_path: str, question: str) -> str:
    """
    LangChain과 OpenAI LLM을 사용해 CSV 파일을 분석하고, 자연어 답변을 반환합니다.
    """
    llm = ChatOpenAI(model="omni-moderation-latest", api_key=OPENAI_API_KEY)
    agent = create_csv_agent(llm, csv_path, verbose=False)
    return agent.run(question)
