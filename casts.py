from dotenv import load_dotenv
import os
import requests
import time
from requests.exceptions import RequestException, JSONDecodeError
from dataclasses import dataclass
import requests
import time
import polars as pl
import pandas as pd
from dataclasses import dataclass
import duckdb
import datetime
from typing import List
import glob


load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


@dataclass(frozen=True)
class CastDataClass:
    hash: str
    thread_hash: str
    text: str
    timestamp: int
    author_fid: int
    parent_hash: str = None


# ============================================================
# ====================== WARPCAST ============================
# ============================================================


def get_casts_from_warpcast(key: str, cursor: str = None):
    try:
        url = f"https://api.warpcast.com/v2/recent-casts?cursor={cursor}&limit=1000" if cursor else "https://api.warpcast.com/v2/recent-casts?limit=1000"

        print(f"Fetching from {url}")

        result = requests.get(url, headers={"Authorization": "Bearer " + key})
        result.raise_for_status()  # Raises a RequestException if the request failed
        json_data = result.json()

        return {
            "casts": json_data["result"]['casts'],
            "cursor": json_data.get("next", {}).get('cursor') if json_data.get("next") else None
        }

    except (RequestException, JSONDecodeError) as e:
        print(f"Error occurred while fetching data: {e}")
        time.sleep(10)
        return {"casts": [], "cursor": cursor}


def get_all_casts_from_warpcast(key: str, timestamp: int):
    cursor = None
    cast_arr = []
    while True:
        data = get_casts_from_warpcast(key, cursor)

        casts = [extract_warpcast_cast_data(cast) for cast in data['casts']]

        time_diff = datetime.timedelta(
            milliseconds=casts[0]['timestamp'] - timestamp)

        time_diff_str = str(time_diff).split('.')[0]

        print(
            f"Processing casts with timestamp >= {timestamp}. Next cast timestamp: {casts[0]['timestamp']} ({time_diff_str} left)")

        if casts[0]['timestamp'] < timestamp:
            break

        cast_arr.extend(casts)

        cursor = data.get("cursor")

        if cursor is None:
            break
        else:
            time.sleep(1)  # add a delay to avoid hitting rate limit
            continue

    return cast_arr


def extract_warpcast_cast_data(cast):
    return {
        "hash": cast['hash'],
        "thread_hash": cast['threadHash'],
        "parent_hash": cast.get('parentHash', None),
        "text": cast['text'],
        "timestamp": cast['timestamp'],
        "author_fid": cast['author']['fid']
    }


def dump_casts_to_parquet_file(casts: List[CastDataClass], append: bool = True):
    casts = [CastDataClass(**cast) for cast in casts]
    new_df = pd.DataFrame(casts)
    new_df['year'] = pd.to_datetime(new_df['timestamp'], unit='ms').dt.year
    new_df['month'] = pd.to_datetime(new_df['timestamp'], unit='ms').dt.month

    for (year, month), group in new_df.groupby(['year', 'month']):
        filename = get_monthly_filename(group.timestamp.min())

        if os.path.exists(filename) and append:
            existing_df = pd.read_parquet(filename)

            # Removing duplicates by merging on a unique identifier, such as 'hash'
            # Assuming 'hash' is a unique identifier for each cast
            merged_df = pd.concat([existing_df, group],
                                  axis=0).drop_duplicates(subset=['hash'])

            merged_df.to_parquet(filename, index=False)
        else:
            # Saving only unique casts from the new data, in case the parquet file doesn't exist yet
            group.drop_duplicates(subset=['hash']).to_parquet(
                filename, index=False)


def main():
    file_pattern = './datasets/casts*.parquet'
    existing_files = glob.glob(file_pattern)

    if not existing_files:
        casts = get_all_casts_from_warpcast(warpcast_hub_key, 0)
        dump_casts_to_parquet_file(casts, './datasets/casts.parquet')
    else:
        latest_timestamp = duckdb.query(
            f"SELECT timestamp FROM read_parquet('{file_pattern}') ORDER BY timestamp DESC").fetchone()[0]
        timestamp = latest_timestamp if latest_timestamp else 0
        casts = get_all_casts_from_warpcast(warpcast_hub_key, timestamp)
        dump_casts_to_parquet_file(
            casts, append=bool(latest_timestamp))


def get_monthly_filename(timestamp: int) -> str:
    dt = pd.to_datetime(timestamp, unit='ms')
    return f"./datasets/casts_{dt.year:04d}_{dt.month:02d}.parquet"


def split_existing_parquet_to_monthly_files(existing_filename: str):
    if not os.path.exists(existing_filename):
        print(f"{existing_filename} does not exist!")
        return

    existing_df = pd.read_parquet(existing_filename)
    existing_df['year'] = pd.to_datetime(
        existing_df['timestamp'], unit='ms').dt.year
    existing_df['month'] = pd.to_datetime(
        existing_df['timestamp'], unit='ms').dt.month

    for (year, month), group in existing_df.groupby(['year', 'month']):
        filename = get_monthly_filename(group.timestamp.min())
        print(
            f"Processing group for year={year}, month={month}, filename={filename}")

        if os.path.exists(filename):
            print(f"{filename} already exists. Skipping.")
        else:
            print(f"Saving to {filename}")
            group.to_parquet(filename, index=False)


if __name__ == '__main__':
    main()
