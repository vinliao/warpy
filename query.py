import openai
from users import *
from casts import *
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text, func
import argparse
import duckdb

parser = argparse.ArgumentParser()

parser.add_argument('-q', '--query',
                    help='Query Farcaster data with natural language.')
parser.add_argument('--raw',
                    help='Query Farcaster data with SQL.')

args = parser.parse_args()

if args.query:
    # set openai api key
    openai.api_key = os.getenv('OPENAI_API_KEY')

    system_prompt = "You are a SQL writer. If the user asks about anything than SQL, deny. You are a very good SQL writer. Nothing else."
    initial_prompt = """
    You are working with a database stored in parquet files. Each dataclass below represents a table. Each table lives in a parquet file. The data lives inside parquet files. All reference to tables must be replaced with 'parquet_file_name.parquet' (e.g. users.parquet). The data is in the same directory as this file.

    # users.parquet
    @dataclass(frozen=True)
    class UserDataClass:
        fid: int
        username: str
        display_name: str
        verified: bool
        pfp_url: str
        farcaster_address: str
        external_address: str
        registered_at: int
        bio_text: str


    Your job is to turn user queries (in natural language) to SQL. Only return the SQL and nothing else. Don't explain, don't say "here's your query." Just give the SQL. Say "Yes" if you understand.
    """

    print("Sending query to ChatGPT...\n")

    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": initial_prompt},
            {"role": "assistant",
             "content": "Yes."},
            {"role": "user", "content": args.query}
        ]
    )

    reply = completion['choices'][0]['message']['content'].strip()

    print(f"SQL from ChatGPT: \n\n{reply}\n")
    print(duckdb.query(reply).pl())

if args.raw:
    print(duckdb.query(args.raw).pl())