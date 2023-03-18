from dotenv import load_dotenv
import os
import requests
from models import Cast
import time
from typing import Optional
from sqlalchemy import text
from requests.exceptions import RequestException, JSONDecodeError

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


def get_all_casts_from_warpcast(key: str, timestamp: int):
    cursor = None
    cast_arr = []
    while True:
        data = get_casts_from_warpcast(key, cursor)

        casts = [extract_warpcast_cast_data(cast) for cast in data['casts']]

        print(f"timestamp: {timestamp}, cast timestamp: {casts[0].timestamp}")

        if casts[0].timestamp < timestamp:
            break

        cast_arr.extend(casts)

        cursor = data.get("cursor")

        if cursor is None:
            break
        else:
            time.sleep(1)  # add a delay to avoid hitting rate limit
            continue

    return cast_arr


def bulk_index_casts(casts, session):
    cast_insert_query = text("""
    INSERT IGNORE INTO casts (hash, thread_hash, parent_hash, text, timestamp, author_fid)
    VALUES (:hash, :thread_hash, :parent_hash, :text, :timestamp, :author_fid)
    """)
    session.execute(cast_insert_query, casts)
    print(f"Indexed {len(casts)} casts")
    session.commit()


def extract_warpcast_cast_data(cast):
    return {
        "hash": cast['hash'],
        "thread_hash": cast['threadHash'],
        "parent_hash": cast.get('parentHash', ''),
        "text": cast['text'],
        "timestamp": cast['timestamp'],
        "author_fid": cast['author']['fid']
    }
