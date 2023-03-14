from dotenv import load_dotenv
import os
import requests
from models import Cast
import time
import asyncio
import aiohttp
from dataclasses import dataclass, asdict
from typing import Optional

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


@dataclass(frozen=True)
class CastDataClass:
    hash: str
    thread_hash: str
    parent_hash: str
    text: str
    timestamp: int
    author_fid: int


# ============================================================
# ====================== WARPCAST ============================
# ============================================================


def get_casts_from_warpcast(key: str, cursor: str = None):
    # have cursor in url if cursor exists, use ternary

    url = f"https://api.warpcast.com/v2/recent-casts?cursor={cursor}&limit=1000" if cursor else "https://api.warpcast.com/v2/recent-casts?limit=5"

    print(f"Fetching from {url}")

    # fetch to url with the bearer token
    result = requests.get(
        url, headers={"Authorization": "Bearer " + key})
    json_data = result.json()

    # cursor may be empty here so handle it
    return {"casts": json_data["result"]['casts'],
            "cursor": json_data.get("next", {}).get('cursor') if json_data.get("next") else None}


def get_all_casts_from_warpcast(key: str):
    cursor = None
    casts = []
    while True:
        data = get_casts_from_warpcast(key, cursor)
        casts += data["casts"]
        cursor = data.get("cursor")

        if cursor is None:
            break
        else:
            time.sleep(1)  # add a delay to avoid hitting rate limit
            continue

    return casts


def extract_warpcast_cast_data(cast):
    return CastDataClass(
        hash=cast['hash'],
        thread_hash=cast['threadHash'],
        parent_hash=cast.get('parentHash', ''),
        text=cast['text'],
        timestamp=cast['timestamp'],
        author_fid=cast['author']['fid']
    )


data = get_casts_from_warpcast(warpcast_hub_key)
cast_data = [extract_warpcast_cast_data(cast) for cast in data['casts']]
# turn cast to sqlalchemy model, use **
casts = [Cast(**asdict(cast)) for cast in cast_data]

print(casts)
# insert this to db or soemthing