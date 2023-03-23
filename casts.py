from dotenv import load_dotenv
import os
import requests
from models import Cast
import time
from typing import Optional
from sqlalchemy import text
from requests.exceptions import RequestException, JSONDecodeError
from dataclasses import dataclass, asdict
import requests
import time
import polars as pl
from dataclasses import dataclass
from typing import List


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

        print(
            f"timestamp: {timestamp}, cast timestamp: {casts[0]['timestamp']}")

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


def dump_casts_to_parquet_file(casts, filename: str):
    casts = [CastDataClass(**cast) for cast in casts]
    print(casts)

    df = pl.DataFrame(casts)
    df.write_parquet(filename)


def main():
    casts = get_all_casts_from_warpcast(warpcast_hub_key, 1679513539000)
    dump_casts_to_parquet_file(casts, 'casts2.parquet')

# use the functions above to dump to parquet files


if __name__ == '__main__':
    main()
