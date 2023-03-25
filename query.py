import re
import openai
from users import *
from casts import *
import os
import argparse
import duckdb
from datetime import datetime
import pytz

parser = argparse.ArgumentParser()

parser.add_argument('query', nargs='?',
                    help='Query Farcaster data with natural language.')
parser.add_argument('--raw',
                    help='Query Farcaster data with SQL.')
parser.add_argument('--test',
                    help='For testing purposes.')

args = parser.parse_args()


def update_parquet_file_paths(query):
    # Find all occurrences of ".parquet" and "*.parquet" in the query
    parquet_pattern = r'\b\w+\*?\.parquet\b'
    parquet_matches = re.finditer(parquet_pattern, query)

    # Replace each occurrence of "filename.parquet" with "read_parquet('datasets/filename.parquet')"
    offset = 0
    for match in parquet_matches:
        start_index = match.start() + offset
        end_index = match.end() + offset
        file_path = query[start_index:end_index]

        # Check if read_parquet is already present
        read_parquet_start = query.rfind('read_parquet(', 0, start_index)
        if read_parquet_start == -1 or query.find(')', read_parquet_start, start_index) != -1:
            updated_file_path = f"read_parquet('datasets/{file_path}')"
        else:
            updated_file_path = f"datasets/{file_path}"

        query = query[:start_index] + updated_file_path + query[end_index:]
        offset += len(updated_file_path) - len(file_path)

    return query


if args.query:
    # set openai api key
    openai.api_key = os.getenv('OPENAI_API_KEY')

    system_prompt = "You are a SQL writer. If the user asks about anything than SQL, deny. You are a very good SQL writer. Nothing else."
    initial_prompt = """
    You are working with a database stored in parquet files. Each dataclass below represents a table. Each table lives in a parquet file. The data lives inside parquet files. All reference to tables must be replaced with 'parquet_file_name.parquet' (e.g. users.parquet). The data is in the same directory as this file.

    # Important note: if the query contains data that's not in the users.parquet schema, get the data from user_extra.parquet.
    # example: `SELECT u.username FROM users.parquet u JOIN user_extra.parquet ue ON u.fid = ue.fid WHERE ue.follower_count > 50`
    @dataclass(frozen=True)
    class UserDataClass:
        fid: int
        username: str
        display_name: str
        pfp_url: str
        bio_text: str

    # user_extra.parquet
    @dataclass(frozen=True)
    class UserExtraDataClass:
        fid: int
        following_count: int
        follower_count: int
        location_id: Optional[str] = None
        verified: bool = False
        farcaster_address: Optional[str] = None
        external_address: Optional[str] = None
        registered_at: int = -1

    # locations.parquet
    @dataclass(frozen=True)
    class LocationDataClass:
        id: str
        description: str

    # f"casts_{dt.year:04d}_{dt.month:02d}.parquet"
    @dataclass(frozen=True)
    class CastDataClass:
        hash: str
        thread_hash: str
        text: str
        timestamp: int
        author_fid: int
        parent_hash: str = None

    To query from multiple files, you can do `SELECT timestamp FROM read_parquet('casts*.parquet')`. You can also use read_parquet(['file1.parquet', 'file2.parquet']).

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
    reply = update_parquet_file_paths(reply)

    print(f"SQL from ChatGPT: \n\n{reply}\n")
    print(duckdb.query(reply).pl())

if args.raw:
    print(duckdb.query(args.raw).pl())
