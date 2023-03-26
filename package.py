import os
import sqlite3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tarfile

# Open a connection to the SQLite database
conn = sqlite3.connect('./datasets/datasets.db')

# Get a list of tables in the database
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [x[0] for x in cursor.fetchall()]

# Create a temporary directory to store the Parquet files
tmpdir = 'temp_parquet_files'
if not os.path.exists(tmpdir):
    os.mkdir(tmpdir)

# Convert each table to a Parquet file and save it in the temporary directory
for table in tables:
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    pq.write_table(
        table=pa.Table.from_pandas(df),
        where=os.path.join(tmpdir, f"{table}.parquet")
    )

# Close the connection to the SQLite database
conn.close()

# Create a tar.gz archive of the Parquet files
with tarfile.open('datasets.tar.gz', 'w:gz') as tar:
    for root, dirs, files in os.walk(tmpdir):
        for file in files:
            path = os.path.join(root, file)
            tar.add(path, arcname=os.path.relpath(path, tmpdir))

# Delete the temporary directory and its contents
for root, dirs, files in os.walk(tmpdir, topdown=False):
    for file in files:
        os.remove(os.path.join(root, file))
    for dir in dirs:
        os.rmdir(os.path.join(root, dir))
os.rmdir(tmpdir)
