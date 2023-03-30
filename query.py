from langchain.schema import (
    AIMessage,
    HumanMessage,
    SystemMessage
)
import os
import argparse
import duckdb
from langchain.chat_models import ChatOpenAI
from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.sql_database import SQLDatabase
from dotenv import load_dotenv
import time

load_dotenv()

parser = argparse.ArgumentParser()

parser.add_argument('query', nargs='?',
                    help='Query Farcaster data with natural language.')
parser.add_argument('--raw',
                    help='Query Farcaster data with SQL.')
parser.add_argument('--csv', nargs='?', const=int(time.time()), default=None, type=str,
                    help='Dump the query result to csv. If not specified, default to {unix_timestamp_in_second}.csv')
parser.add_argument('--advanced', nargs='?',
                    help='For testing purposes.')

args = parser.parse_args()


def remove_imports_from_models(model_str):
    start = model_str.index("class")
    return model_str[start:]


if args.query:
    chat = ChatOpenAI(
        temperature=0, openai_api_key=os.getenv("OPENAI_API_KEY"))

    with open('models.py', 'r') as f:
        sqlalchemy_models = f.read()

    initial_prompt_raw = """
    Your job is to turn user queries (in natural language) to SQL. Only return the SQL and nothing else. Don't explain, don't say "here's your query." Just give the SQL. Say "Yes." if you understand.

    Timestamp is in unix millisecond format, database is in SQLite, adjust accordingly. Here are the schema:
    """

    system_prompt = SystemMessage(
        content="You are a SQL writer. If the user asks about anything than SQL, deny. You are a very good SQL writer. Nothing else.")

    initial_prompt = HumanMessage(
        content=f"{initial_prompt_raw}\n\n{remove_imports_from_models(sqlalchemy_models)}")

    ai_response = AIMessage(content="Yes.")

    query_message = HumanMessage(content=args.query)

    messages = [system_prompt, initial_prompt, ai_response, query_message]
    print("Sending query to ChatGPT...\n")

    sql_query = chat(messages)
    print(f"SQL from ChatGPT: \n\n{sql_query.content}\n")

    with duckdb.connect('datasets/datasets.db') as con:
        df = con.sql(sql_query.content).pl()
        print(df)

        if args.csv:
            df.write_csv(f'{args.csv}.csv')


if args.advanced:
    db = SQLDatabase.from_uri("sqlite:///datasets/datasets.db")
    toolkit = SQLDatabaseToolkit(db=db)

    chat = ChatOpenAI(
        temperature=0, openai_api_key=os.getenv("OPENAI_API_KEY"))

    agent_executor = create_sql_agent(
        llm=chat,
        toolkit=toolkit,
        verbose=True,
    )

    prompt = f"Describe relevant tables, then {args.advanced}"
    agent_executor.run(prompt)


if args.raw:
    with duckdb.connect('datasets/datasets.db') as con:
        df = con.sql(args.raw).pl()
        print(df)

        if args.csv:
            df.write_csv(f'{args.csv}.csv')
