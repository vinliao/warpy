import os
import sqlite3
import tarfile

import pyarrow.parquet as pq
import requests
from tqdm import tqdm


def main():
    filename = "1681979704072.tar.gz"
    url = f"https://pub-3916d8c82abb435eb70175747fdc2119.r2.dev/{filename}"

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

    # Get the parent directory of the current script
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    extracted_dir = os.path.join(parent_dir, "datasets")

    if not os.path.exists(extracted_dir):
        os.mkdir(extracted_dir)

    with tarfile.open(filename, "r:gz") as tar:
        tar.extractall(path=extracted_dir)

    # Create the SQLite database inside the same directory
    db_path = os.path.join(parent_dir, "datasets", "datasets.db")
    conn = sqlite3.connect(db_path)

    # Iterate over the extracted Parquet files and write them to the SQLite database
    for root, dirs, files in os.walk(extracted_dir):
        for file in files:
            if file.endswith(".parquet"):
                table_name = os.path.splitext(file)[0]
                file_path = os.path.join(root, file)

                # Read the Parquet file into a Pandas DataFrame
                df = pq.read_table(file_path).to_pandas()

                # Write the DataFrame to the SQLite database
                df.to_sql(table_name, conn, if_exists="replace", index=False)

    # Close the connection to the SQLite database
    conn.close()


if __name__ == "__main__":
    main()
