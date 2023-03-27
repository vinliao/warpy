from datetime import datetime
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from dotenv import load_dotenv
import os
import requests
import time
from requests.exceptions import RequestException, JSONDecodeError
from dataclasses import dataclass
import requests
import time
import pandas as pd
from dataclasses import dataclass
import duckdb
import datetime
from typing import List
import glob
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Cast


load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


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


def get_casts_until_timestamp(key: str, timestamp: int):
    cursor = None
    cast_arr = []

    while True:
        data = get_casts_from_warpcast(key, cursor)
        casts = [extract_warpcast_cast_data(cast) for cast in data['casts']]

        next_timestamp = casts[0].timestamp
        time_diff = datetime.timedelta(milliseconds=next_timestamp - timestamp)
        time_diff_str = str(time_diff).split('.')[0]

        print(f"Processing casts with timestamp >= {timestamp}. "
              f"Next cast timestamp: {next_timestamp} ({time_diff_str} left)")

        if next_timestamp < timestamp:
            break

        cast_arr.extend(casts)
        cursor = data.get("cursor")

        if cursor is None:
            break
        else:
            time.sleep(1)  # add a delay to avoid hitting rate limit

    return cast_arr


def extract_warpcast_cast_data(cast):
    return Cast(
        hash=cast['hash'],
        thread_hash=cast['threadHash'],
        text=cast['text'],
        timestamp=cast['timestamp'],
        author_fid=cast['author']['fid'],
        parent_hash=cast.get('parentHash', None)
    )


def dump_casts_to_sqlite(engine, casts: List[dict], timestamp: int):
    with sessionmaker(bind=engine)() as session:
        three_days_before_latest_timestamp = timestamp - \
            datetime.timedelta(days=3).total_seconds() * 1000
        existing_hashes = {cast.hash for cast in session.query(Cast.hash).filter(
            Cast.timestamp >= timestamp - three_days_before_latest_timestamp).all()}

        new_casts = [cast
                     for cast in casts if cast.hash not in existing_hashes]

        # Bulk insert new casts
        if new_casts:
            session.bulk_save_objects(new_casts)
            session.commit()


def main():
    engine = create_engine('sqlite:///datasets/datasets.db')

    with sessionmaker(bind=engine)() as session:
        latest_cast = session.query(Cast).order_by(
            Cast.timestamp.desc()).first()
        latest_timestamp = latest_cast.timestamp if latest_cast else 0

        casts = get_casts_until_timestamp(warpcast_hub_key, latest_timestamp)
        dump_casts_to_sqlite(engine, casts, latest_timestamp)


if __name__ == '__main__':
    main()
