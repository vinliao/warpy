import os
import tarfile
import sqlite3
import requests
from tqdm import tqdm
import pyarrow.parquet as pq
from sqlalchemy import create_engine

from models import Base  # Make sure to import your models


def main():
    # url = "https://pub-3916d8c82abb435eb70175747fdc2119.r2.dev/datasets.tar.gz"
    # filename = "datasets.tar.gz"

    # # Download the file
    # response = requests.get(url, stream=True)
    # total_size = int(response.headers.get("content-length", 0))
    # block_size = 1024
    # progress_bar = tqdm(total=total_size, unit="iB", unit_scale=True)
    # with open(filename, "wb") as f:
    #     for data in response.iter_content(block_size):
    #         progress_bar.update(len(data))
    #         f.write(data)
    # progress_bar.close()

    # Extract the tar.gz archive containing the Parquet files
    archive_path = 'datasets.tar.gz'
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    extracted_dir = os.path.join(parent_dir, 'datasets')

    if not os.path.exists(extracted_dir):
        os.mkdir(extracted_dir)

    with tarfile.open(archive_path, 'r:gz') as tar:
        tar.extractall(path=extracted_dir)

    # Create the SQLite database inside the same directory
    db_path = os.path.join(parent_dir, 'datasets', 'datasets.db')

    with sqlite3.connect(db_path) as conn:
        # Create the schema using SQLAlchemy
        engine = create_engine(f'sqlite:///{db_path}')
        Base.metadata.create_all(engine)
        # Iterate over the extracted Parquet files and write them to the SQLite database
        for root, dirs, files in os.walk(extracted_dir):
            for file in files:
                if file.endswith('.parquet'):
                    table_name = os.path.splitext(file)[0]
                    file_path = os.path.join(root, file)

                    # Read the Parquet file into a Pandas DataFrame
                    df = pq.read_table(file_path).to_pandas()

                    # Write the DataFrame to the SQLite database
                    df.to_sql(table_name, conn,
                              if_exists='replace', index=False)


if __name__ == '__main__':
    main()
