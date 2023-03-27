import os
import sqlite3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tarfile
from datetime import datetime
import hashlib

# Open a connection to the SQLite database
conn = sqlite3.connect('./datasets/datasets.db')

# Get the latest cast timestamp
cursor = conn.cursor()
cursor.execute("SELECT MAX(timestamp) FROM casts")
latest_timestamp = cursor.fetchone()[0]

# Get the highest fid
cursor.execute("SELECT MAX(author_fid) FROM casts")
highest_fid = cursor.fetchone()[0]

# Create a temporary directory to store the Parquet files
tmpdir = 'temp_parquet_files'
if not os.path.exists(tmpdir):
    os.mkdir(tmpdir)

# Convert each table to a Parquet file and save it in the temporary directory
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [x[0] for x in cursor.fetchall()]
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

# Create a hash of the tar.gz archive
with open('datasets.tar.gz', 'rb') as f:
    hash = hashlib.sha256(f.read()).hexdigest()

# Delete the temporary directory and its contents
for root, dirs, files in os.walk(tmpdir, topdown=False):
    for file in files:
        os.remove(os.path.join(root, file))
    for dir in dirs:
        os.rmdir(os.path.join(root, dir))
os.rmdir(tmpdir)

# Print the results
print(
    f"Latest cast timestamp: {latest_timestamp}")
print(f"Highest fid: {highest_fid}")
print(f"Hash of tar.gz: {hash}")