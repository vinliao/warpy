from typing import List, Dict, Union
from datetime import datetime
from dotenv import load_dotenv
import os
import requests
import time
from requests.exceptions import RequestException, JSONDecodeError
import requests
import time
import datetime
from typing import List
from sqlalchemy.orm import sessionmaker
from utils.models import Cast
from sqlalchemy.engine import Engine


load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


def get_casts_from_warpcast(key: str, cursor: str = None):
    try:
        url = f"https://api.warpcast.com/v2/recent-casts?cursor={cursor}&limit=1000" if cursor else "https://api.warpcast.com/v2/recent-casts?limit=1000"

        print(f"Fetching from {url}")

        result = requests.get(url, headers={"Authorization": "Bearer " + key})
        result.raise_for_status()
        json_data = result.json()

        return {
            "casts": json_data["result"]['casts'],
            "cursor": json_data.get("next", {}).get('cursor') if json_data.get("next") else None
        }

    except (RequestException, JSONDecodeError) as e:
        print(f"Error occurred while fetching data: {e}")
        time.sleep(10)
        return {"casts": [], "cursor": cursor}


def extract_warpcast_cast_data(cast: Dict[str, Union[str, int]]) -> Cast:
    return Cast(
        hash=cast['hash'],
        thread_hash=cast['threadHash'],
        text=cast['text'],
        timestamp=cast['timestamp'],
        author_fid=cast['author']['fid'],
        parent_hash=cast.get('parentHash', None)
    )


def get_casts_until_timestamp(key: str, timestamp: int) -> List[Cast]:
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
            time.sleep(1)

    return cast_arr


def dump_casts_to_sqlite(engine, casts: List[Cast], timestamp: int) -> None:
    with sessionmaker(bind=engine)() as session:
        three_days_before_latest_timestamp = timestamp - \
            datetime.timedelta(days=3).total_seconds() * 1000
        existing_hashes = {cast.hash for cast in session.query(Cast.hash).filter(
            Cast.timestamp >= timestamp - three_days_before_latest_timestamp).all()}

        new_casts = [
            cast for cast in casts if cast.hash not in existing_hashes]

        if new_casts:
            session.bulk_save_objects(new_casts)
            session.commit()


def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        latest_cast = session.query(Cast).order_by(
            Cast.timestamp.desc()).first()
        latest_timestamp = latest_cast.timestamp if latest_cast else 0

        casts = get_casts_until_timestamp(warpcast_hub_key, latest_timestamp)
        dump_casts_to_sqlite(engine, casts, latest_timestamp)
