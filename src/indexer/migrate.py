# Throwaway code to migrate stuff from old DB to Planetscale.

import os
import sys

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sqlite_engine = create_engine("sqlite:///old.db")

load_dotenv()

PLANETSCALE_URL = os.getenv("PLANETSCALE_MYSQL_URL")
if not PLANETSCALE_URL:
    raise Exception("PLANETSCALE_MYSQL_URL is not set in .env")

mysql_engine = create_engine(PLANETSCALE_URL)

# Get the list of tables in the SQLite database
sqlite_tables_query = text("SELECT name FROM sqlite_master WHERE type='table';")
with sqlite_engine.connect() as con:
    tables = con.execute(sqlite_tables_query).fetchall()

my_array = ["ens_data", "eth_transactions", "erc1155_metadata", "user_eth_transactions"]

try:
    with sqlite_engine.connect() as sqlite_con:
        # Get the list of tables
        tables = sqlite_con.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()

    for table in tables:
        table_name = table[0]
        print(f"Processing {table_name}...")
        if table_name in my_array:
            print("skipping")
            continue

        # Read data from SQLite into a DataFrame
        with sqlite_engine.connect() as sqlite_con:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_con)

        # Write DataFrame to MySQL
        with mysql_engine.connect() as mysql_con:
            df.to_sql(
                table_name,
                mysql_con,
                if_exists="replace",
                index=False,
                chunksize=10000,
            )

        print(f"Finished processing {table_name}")

except Exception as e:
    print("An error occurred during the migration process:")
    print(str(e))
    sys.exit(1)

finally:
    sqlite_engine.dispose()
    mysql_engine.dispose()

print("Data migration completed successfully!")
