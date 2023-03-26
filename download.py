import pyarrow.parquet as pq
import pandas as pd
import sqlite3
import os
import requests
import tarfile
from tqdm import tqdm

url = "https://pub-3916d8c82abb435eb70175747fdc2119.r2.dev/datasets.tar.gz"
filename = "datasets.tar.gz"

# Download the file
response = requests.get(url, stream=True)
total_size = int(response.headers.get("content-length", 0))
block_size = 1024
progress_bar = tqdm(total=total_size, unit="iB", unit_scale=True)
with open(filename, "wb") as f:
    for data in response.iter_content(block_size):
        progress_bar.update(len(data))
        f.write(data)
progress_bar.close()

# Extract the contents of the file
with tarfile.open(filename, "r:gz") as tar:
    tar.extractall(path='./')


# Extract the tar.gz archive containing the Parquet files
archive_path = 'datasets.tar.gz'
extracted_dir = 'extracted_parquet_files'
if not os.path.exists(extracted_dir):
    os.mkdir(extracted_dir)

with tarfile.open(archive_path, 'r:gz') as tar:
    tar.extractall(path=extracted_dir)

# Create the SQLite database
db_path = 'datasets.db'
if os.path.exists(db_path):
    os.remove(db_path)

conn = sqlite3.connect(db_path)

# Iterate over the extracted Parquet files and write them to the SQLite database
for root, dirs, files in os.walk(extracted_dir):
    for file in files:
        if file.endswith('.parquet'):
            table_name = os.path.splitext(file)[0]
            file_path = os.path.join(root, file)

            # Read the Parquet file into a Pandas DataFrame
            df = pq.read_table(file_path).to_pandas()

            # Write the DataFrame to the SQLite database
            df.to_sql(table_name, conn, if_exists='replace', index=False)

# Close the connection to the SQLite database
conn.close()

# Remove the extracted Parquet files and the directory
for root, dirs, files in os.walk(extracted_dir, topdown=False):
    for file in files:
        os.remove(os.path.join(root, file))
    for dir in dirs:
        os.rmdir(os.path.join(root, dir))
os.rmdir(extracted_dir)
