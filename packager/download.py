from pyarrow import parquet as pq
import os
import tarfile
import sqlite3
import requests
from tqdm import tqdm
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import shutil
import hashlib

from models import Base


def hash_models_py(file_path: str) -> str:
    with open(file_path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()[:4]


def get_local_and_downloaded_hashes(parent_dir: str, extracted_dir: str) -> tuple:
    models_file_path = os.path.join(parent_dir, 'models.py')
    local_hash = hash_models_py(models_file_path)

    downloaded_metadata_path = os.path.join(
        extracted_dir, "warpy_metadata.parquet")
    downloaded_metadata_df = pq.read_table(
        downloaded_metadata_path).to_pandas()
    downloaded_hash = downloaded_metadata_df.loc[0, 'models_hash']

    return local_hash, downloaded_hash


def download_file(url: str, filename: str) -> None:
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024
    progress_bar = tqdm(total=total_size, unit="iB", unit_scale=True)
    with open(filename, "wb") as f:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            f.write(data)
    progress_bar.close()


def extract_archive(archive_path: str, extracted_dir: str) -> None:
    if not os.path.exists(extracted_dir):
        os.mkdir(extracted_dir)

    with tarfile.open(archive_path, 'r:gz') as tar:
        tar.extractall(path=extracted_dir)


def create_sqlite_database(db_path: str) -> None:
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)


def parquet_to_sqlite(extracted_dir: str, db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        for root, dirs, files in os.walk(extracted_dir):
            for file in files:
                if file.endswith('.parquet'):
                    table_name = os.path.splitext(file)[0]
                    file_path = os.path.join(root, file)
                    df = pq.read_table(file_path).to_pandas()
                    df.to_sql(table_name, conn,
                              if_exists='replace', index=False)


def main():
    url = "https://pub-3916d8c82abb435eb70175747fdc2119.r2.dev/datasets.tar.gz"
    filename = "datasets.tar.gz"
    archive_path = 'datasets.tar.gz'
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    extracted_dir = os.path.join(parent_dir, 'datasets')
    db_path = os.path.join(parent_dir, 'datasets', 'datasets.db')

    # download_file(url, filename)
    extract_archive(archive_path, extracted_dir)

    # Remove existing 'datasets' directory if it exists
    if os.path.exists(extracted_dir):
        shutil.rmtree(extracted_dir)

    extract_archive(archive_path, extracted_dir)
    create_sqlite_database(db_path)
    parquet_to_sqlite(extracted_dir, db_path)

    # activate these all later
    # local_hash, downloaded_hash = get_local_and_downloaded_hashes(
    #     parent_dir, extracted_dir)

    # if local_hash == downloaded_hash:
    #     print("Hash verification successful: The downloaded data matches the models.py.")

    #     # Remove existing 'datasets' directory if it exists
    #     if os.path.exists(extracted_dir):
    #         shutil.rmtree(extracted_dir)

    #     extract_archive(archive_path, extracted_dir)
    #     create_sqlite_database(db_path)
    #     parquet_to_sqlite(extracted_dir, db_path)
    # else:
    #     print("Oops, it seems like the branch you're working on is out of date. Please run `git pull` to get the latest updates.")


if __name__ == '__main__':
    main()
