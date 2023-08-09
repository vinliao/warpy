import time
from datetime import datetime
from typing import List, Optional

import pytz


# Convert time units to milliseconds
def ms_now() -> int:
    return int(round(time.time() * 1000))


def minutes_to_ms(minutes: int) -> int:
    return minutes * 60 * 1000


def hours_to_ms(hours: int) -> int:
    return hours * 60 * 60 * 1000


def days_to_ms(days: int) -> int:
    return days * 24 * 60 * 60 * 1000


def weeks_to_ms(weeks: int) -> int:
    return weeks * 7 * 24 * 60 * 60 * 1000


# Convert milliseconds to time units
def ms_to_minutes(ms: int) -> float:
    return ms / (60 * 1000)


def ms_to_hours(ms: int) -> float:
    return ms / (60 * 60 * 1000)


def ms_to_days(ms: int) -> float:
    return ms / (24 * 60 * 60 * 1000)


def ms_to_weeks(ms: int) -> float:
    return ms / (7 * 24 * 60 * 60 * 1000)


# Convert time ago to Unix ms
def minutes_ago_to_unixms(minutes: int) -> int:
    return ms_now() - minutes_to_ms(minutes)


def hours_ago_to_unixms(hours: int) -> int:
    return ms_now() - hours_to_ms(hours)


def days_ago_to_unixms(days: int) -> int:
    return ms_now() - days_to_ms(days)


def weeks_ago_to_unixms(weeks: int) -> int:
    return ms_now() - weeks_to_ms(weeks)


# Convert Unix ms to time ago
def unixms_to_minutes_ago(ms: int) -> float:
    return ms_to_minutes(ms_now() - ms)


def unixms_to_hours_ago(ms: int) -> float:
    return ms_to_hours(ms_now() - ms)


def unixms_to_days_ago(ms: int) -> float:
    return ms_to_days(ms_now() - ms)


def unixms_to_weeks_ago(ms: int) -> float:
    return ms_to_weeks(ms_now() - ms)


def ymd_to_unixms(year: int, month: int, day: int = 1) -> int:
    dt = datetime(year, month, day, tzinfo=pytz.UTC)
    timestamp = dt.timestamp()
    return int(timestamp * 1000)


def dedupe() -> None:
    def dedupe_single(filename: str, pk: str) -> None:
        import duckdb

        con = duckdb.connect(database=":memory:")

        con.execute(f"CREATE TABLE temp AS SELECT * FROM parquet_scan('{filename}')")
        con.execute(f"CREATE TABLE deduped AS SELECT DISTINCT ON ({pk}) * FROM temp")
        con.execute(f"COPY deduped TO '{filename}' (FORMAT 'parquet')")
        con.execute("DROP TABLE temp")
        con.execute("DROP TABLE deduped")

    metadata = [
        {"filename": "data/reactions.parquet", "pk": "hash"},
        {"filename": "data/casts.parquet", "pk": "hash"},
        {"filename": "data/users.parquet", "pk": "fid"},
    ]

    map(lambda x: dedupe_single(**x), metadata)


def csv_to_parquet(csv_filepath: str) -> None:
    import pandas as pd

    parquet_filepath = csv_filepath.replace(".csv", ".parquet")
    dataframe = pd.read_csv(csv_filepath)
    dataframe.to_parquet(parquet_filepath, engine="pyarrow")


def get_fid_by_username(username: str) -> Optional[int]:
    import duckdb

    con = duckdb.connect(database=":memory:")
    query = "SELECT fid FROM read_parquet('data/users.parquet') "
    query += f"WHERE username = '{username}'"

    try:
        return list(filter(None, [x[0] for x in con.execute(query).fetchall()]))[0]
    except IndexError:
        return None


def get_username_by_fid(fid: int) -> Optional[int]:
    import duckdb

    con = duckdb.connect(database=":memory:")
    query = "SELECT username FROM read_parquet('data/users.parquet') "
    query += f"WHERE fid = '{fid}'"

    try:
        return list(filter(None, [x[0] for x in con.execute(query).fetchall()]))[0]
    except IndexError:
        return None


def extract_ethereum_address(s: str) -> Optional[str]:
    import re

    match = re.search(r"0x[a-fA-F0-9]{40}", s)
    return match.group() if match else None


def is_valid_ethereum_address(address: str) -> bool:
    import re

    return bool(re.match(r"^(0x)?[0-9a-f]{40}$", address.lower()))


def max_gap(xs: List[int]) -> Optional[int]:
    import numpy as np

    diffs = np.diff(np.sort(xs))
    return diffs.max() if len(xs) > 0 else None
