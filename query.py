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
                    help='Query Farcaster data with natural language')

args = parser.parse_args()

if args.query:
    # set openai api key
    openai.api_key = os.getenv('OPENAI_API_KEY')

    system_prompt = "You are a SQL writer. If the user asks about anything than SQL, deny. You are a very good SQL writer. Nothing else."
    initial_prompt = """# users.parquet
    users_cols = {
        'fid': pl.UInt32,
        'username': pl.Utf8,
        'display_name': pl.Utf8,
        'verified': pl.Boolean,
        'pfp_url': pl.Utf8,
        'follower_count': pl.UInt32,
        'following_count': pl.UInt32,
        'bio_text': pl.Utf8,
        'location_place_id': pl.Utf8,
    }

    # locations.parquet
    locations_col = {
        'place_id': pl.Utf8,
        'description': pl.Utf8,
    }

    # casts.parquet
    casts_col = {
        'hash': pl.Utf8,
        'thread_hash': pl.Utf8,
        'parent_hash': pl.Utf8,
        'text': pl.Utf8,
        'timestamp': pl.Int64,
        'author_fid': pl.Int64,
    }


    Here's the database schema you're working with. Your job is to turn user queries (in natural language) to SQL. Only return the SQL and nothing else. Don't explain, don't say "here's your query." Just give the SQL. Say "Yes" if you understand.
    The data lives inside parquet files. All reference to tables must be replaced with 'parquet_file_name.parquet' (e.g. users.parquet). The schema is defined above. The data is in the same directory as this file.
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
