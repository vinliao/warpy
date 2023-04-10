import os
import tempfile
import sqlite3
import pyarrow.parquet as pq
import tarfile
import hashlib
from packager.package import *


def test_create_temporary_directory():
    with tempfile.TemporaryDirectory() as tmpdirname:
        create_temporary_directory(os.path.join(tmpdirname, "temp"))
        assert os.path.exists(os.path.join(tmpdirname, "temp"))


def test_convert_tables_to_parquet_and_data_integrity():
    with tempfile.TemporaryDirectory() as tmpdirname:
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")
        cursor.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")
        cursor.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")

        convert_tables_to_parquet(conn, ["test"], tmpdirname)

        table_path = os.path.join(tmpdirname, "test.parquet")
        assert os.path.exists(table_path)

        table = pq.read_table(table_path)
        df = table.to_pandas()

        assert len(df) == 2
        assert df.loc[0, "id"] == 1
        assert df.loc[0, "name"] == "Alice"
        assert df.loc[1, "id"] == 2
        assert df.loc[1, "name"] == "Bob"

        conn.close()


def test_create_tar_gz_archive_and_data_integrity():
    with tempfile.TemporaryDirectory() as tmpdirname:
        test_file = os.path.join(tmpdirname, "test.txt")
        with open(test_file, "w") as f:
            f.write("Hello, World!")

        create_tar_gz_archive(tmpdirname, "archive.tar.gz")

        assert os.path.exists("archive.tar.gz")

        with tarfile.open("archive.tar.gz", "r:gz") as tar:
            tar.extractall(tmpdirname)

        with open(os.path.join(tmpdirname, "test.txt"), "r") as f:
            content = f.read()

        assert content == "Hello, World!"

        os.remove("archive.tar.gz")


def test_compute_hash_of_archive():
    with tempfile.TemporaryDirectory() as tmpdirname:
        test_file = os.path.join(tmpdirname, "test.txt")
        with open(test_file, "w") as f:
            f.write("Hello, World!")

        tar_path = os.path.join(tmpdirname, "archive.tar.gz")
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(test_file, arcname="test.txt")

        file_hash = compute_hash_of_archive(tar_path)

        with open(tar_path, "rb") as f:
            expected_hash = hashlib.sha256(f.read()).hexdigest()

        assert file_hash == expected_hash
