import os
import sqlite3
import tarfile
import hashlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List


def get_db_connection(parent_dir: str) -> sqlite3.Connection:
    db_path = os.path.join(parent_dir, 'datasets', 'datasets.db')
    return sqlite3.connect(db_path)


def get_latest_timestamp(cursor: sqlite3.Cursor) -> int:
    cursor.execute("SELECT MAX(timestamp) FROM casts")
    return cursor.fetchone()[0]


def get_highest_fid(cursor: sqlite3.Cursor) -> int:
    cursor.execute("SELECT MAX(author_fid) FROM casts")
    return cursor.fetchone()[0]


def get_highest_block_num(cursor: sqlite3.Cursor) -> int:
    cursor.execute("SELECT MAX(block_num) FROM eth_transactions")
    return cursor.fetchone()[0]


def create_temporary_directory(tmpdir: str) -> None:
    if not os.path.exists(tmpdir):
        os.mkdir(tmpdir)


def get_all_tables(cursor: sqlite3.Cursor) -> List[str]:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [x[0] for x in cursor.fetchall()]


def convert_tables_to_parquet(conn: sqlite3.Connection, tables: List[str], tmpdir: str) -> None:
    for table in tables:
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        pq.write_table(
            table=pa.Table.from_pandas(df),
            where=os.path.join(tmpdir, f"{table}.parquet")
        )


def create_tar_gz_archive(tmpdir: str, archive_name: str) -> None:
    with tarfile.open(archive_name, 'w:gz') as tar:
        for root, dirs, files in os.walk(tmpdir):
            for file in files:
                path = os.path.join(root, file)
                tar.add(path, arcname=os.path.relpath(path, tmpdir))


def compute_hash_of_archive(archive_name: str) -> str:
    with open(archive_name, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()


def delete_temporary_directory(tmpdir: str) -> None:
    for root, dirs, files in os.walk(tmpdir, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            os.rmdir(os.path.join(root, dir))
    os.rmdir(tmpdir)


def main():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    conn = get_db_connection(parent_dir)

    cursor = conn.cursor()

    tmpdir = 'temp_parquet_files'
    create_temporary_directory(tmpdir)

    tables = get_all_tables(cursor)
    convert_tables_to_parquet(conn, tables, tmpdir)

    archive_name = 'datasets.tar.gz'
    create_tar_gz_archive(tmpdir, archive_name)

    delete_temporary_directory(tmpdir)

    print(
        f"Dataset latest cast timestamp: {get_latest_timestamp(cursor)}; dataset highest fid: {get_highest_fid(cursor)}; dataset highest block number: {get_highest_block_num(cursor)}; tar.gz shasum: {compute_hash_of_archive(archive_name)}")

    conn.close()


if __name__ == "__main__":
    main()
