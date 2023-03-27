import openai
from users import *
from casts import *
import os
import argparse
import duckdb

parser = argparse.ArgumentParser()

parser.add_argument('query', nargs='?',
                    help='Query Farcaster data with natural language.')
parser.add_argument('--raw',
                    help='Query Farcaster data with SQL.')
parser.add_argument('--test',
                    help='For testing purposes.')

args = parser.parse_args()


if args.query:
    # set openai api key
    openai.api_key = os.getenv('OPENAI_API_KEY')

    system_prompt = "You are a SQL writer. If the user asks about anything than SQL, deny. You are a very good SQL writer. Nothing else."
    initial_prompt = """
    Your job is to turn user queries (in natural language) to SQL. Only return the SQL and nothing else. Don't explain, don't say "here's your query." Just give the SQL. Say "Yes" if you understand.

    My database is in SQLite adjust accordingly.

    class User(Base):
        __tablename__ = 'users'
        fid = Column(Integer, primary_key=True)
        username = Column(String)
        display_name = Column(String)
        pfp_url = Column(String)
        bio_text = Column(String)
        following_count = Column(Integer)
        follower_count = Column(Integer)
        verified = Column(Boolean)
        farcaster_address = Column(String)
        external_address = Column(String, nullable=True)
        registered_at = Column(Integer)
        location_id = Column(String, ForeignKey('locations.id'), nullable=True)

        location = relationship("Location", back_populates="user_extras")


    class Location(Base):
        __tablename__ = 'locations'
        id = Column(String, primary_key=True)
        description = Column(String)

        user_extras = relationship("User", back_populates="location")


    class Cast(Base):
        __tablename__ = 'casts'

        hash = Column(String, primary_key=True)
        thread_hash = Column(String, nullable=False)
        text = Column(String, nullable=False)
        timestamp = Column(Integer, nullable=False)
        author_fid = Column(Integer, nullable=False)
        parent_hash = Column(String, ForeignKey('casts.hash'), nullable=True)
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
    with duckdb.connect('datasets/datasets.db') as con:
        print(con.sql(reply).pl())

if args.raw:
    with duckdb.connect('datasets/datasets.db') as con:
        print(con.sql(args.raw).pl())
