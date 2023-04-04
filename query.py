import polars as pl
import sqlite3
from langchain.schema import (
    AIMessage,
    HumanMessage,
    SystemMessage
)
import os
from langchain.chat_models import ChatOpenAI
from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.sql_database import SQLDatabase
from dotenv import load_dotenv

load_dotenv()

def remove_imports_from_models(model_str):
    start = model_str.index("class")
    return model_str[start:]


def get_sqlalchemy_models():
    with open('models.py', 'r') as f:
        sqlalchemy_models = f.read()
    return remove_imports_from_models(sqlalchemy_models)


def execute_raw_sql(query: str) -> pl.DataFrame:
    with sqlite3.connect('datasets/datasets.db') as con:
        cur = con.cursor()
        cur.execute(query)
        column_names = [description[0] for description in cur.description]
        return pl.DataFrame(cur, schema=column_names)


def execute_natural_language_query(query: str) -> pl.DataFrame:
    chat = ChatOpenAI(
        temperature=0, openai_api_key=os.getenv("OPENAI_API_KEY"))

    initial_prompt_raw = """
    Your job is to turn user queries (in natural language) to SQL. Only return the SQL and nothing else. Don't explain, don't say "here's your query." Just give the SQL. Say "Yes." if you understand.

    Timestamp is in unix millisecond format, anything timestamp related must be multiplied by 1000. The database is in SQLite, adjust accordingly. Here are the schema:
    """

    system_prompt = SystemMessage(
        content="You are a SQL writer. If the user asks about anything than SQL, deny. You are a very good SQL writer. Nothing else.")

    initial_prompt = HumanMessage(
        content=f"{initial_prompt_raw}\n\n{get_sqlalchemy_models()}")

    ai_response = AIMessage(content="Yes.")

    query_message = HumanMessage(content=query)

    messages = [system_prompt, initial_prompt, ai_response, query_message]
    print("Sending query to ChatGPT...\n")

    sql_query = chat(messages)
    print(f"SQL from ChatGPT: \n\n{sql_query.content}\n")

    with sqlite3.connect('datasets/datasets.db') as con:
        cur = con.cursor()
        cur.execute(sql_query.content)
        column_names = [description[0] for description in cur.description]
        return pl.DataFrame(cur, schema=column_names)


def execute_advanced_query(query: str):
    db = SQLDatabase.from_uri("sqlite:///datasets/datasets.db")
    toolkit = SQLDatabaseToolkit(db=db)

    chat = ChatOpenAI(
        temperature=0, openai_api_key=os.getenv("OPENAI_API_KEY"))

    agent_executor = create_sql_agent(
        llm=chat,
        toolkit=toolkit,
        verbose=True,
    )

    prompt = f"Describe relevant tables, then {query}"
    agent_executor.run(prompt)
