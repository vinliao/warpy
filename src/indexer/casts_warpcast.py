import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.app.logger import logger
from src.db.models import Cast

load_dotenv()


def make_request(
    url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 10
) -> Any:
    print(f"Fetching from {url}")
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_data(key: str, latest_timestamp: int) -> List[Dict[str, Any]]:
    all_data = []
    cursor = None

    while True:
        batch_data, cursor = fetch_batch(key, cursor)
        all_data.extend(batch_data)

        if all_data[-1]["timestamp"] < latest_timestamp:
            break

        if cursor is None:
            break
        else:
            time.sleep(1)

    all_data = [cast for cast in all_data if cast["timestamp"] >= latest_timestamp]
    return all_data


def fetch_batch(
    key: str, cursor: Optional[str] = None, limit: int = 1000
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    url = (
        f"https://api.warpcast.com/v2/recent-casts?cursor={cursor}&limit={limit}"
        if cursor
        else f"https://api.warpcast.com/v2/recent-casts?limit={limit}"
    )
    logger.info(f"Fetching {url}")
    json_data = make_request(url, headers={"Authorization": "Bearer " + key})
    return (
        json_data["result"]["casts"],
        json_data.get("next", {}).get("cursor") if json_data.get("next") else None,
    )


def extract_data(cast: Dict[str, Any]) -> Cast:
    return Cast(
        hash=cast["hash"],
        thread_hash=cast["threadHash"],
        text=cast["text"],
        timestamp=cast["timestamp"],
        author_fid=cast["author"]["fid"],
        parent_hash=cast.get("parentHash", None),
    )


def modelify(casts: List[Dict[str, Any]]) -> List[Cast]:
    return [extract_data(cast) for cast in casts]


def print_timestamp_info(timestamp: int):
    if timestamp:
        timestamp_dt = datetime.fromtimestamp(timestamp / 1000)
        days_since_cast = (datetime.now() - timestamp_dt).days
        print(f"Indexing casts since {days_since_cast} days ago")
    else:
        print("No casts found, indexing everything.")


def print_time_difference(models: List[Cast], latest_timestamp: int):
    if models:
        earliest_timestamp = min(model.timestamp for model in models)
        time_difference = datetime.utcfromtimestamp(
            latest_timestamp / 1000
        ) - datetime.utcfromtimestamp(earliest_timestamp / 1000)
        print(
            "Time difference between the latest timestamp and the earliest timestamp"
            f" in the fetched models: {time_difference}"
        )
    else:
        print("No new models were fetched.")


def to_pandas(models: List[Cast]) -> pd.DataFrame:
    logger.info(f"Converting {len(models)} models to pandas dataframe.")
    data = [
        {
            key: value
            for key, value in cast.__dict__.items()
            if key != "_sa_instance_state"
        }
        for cast in models
    ]
    return pd.DataFrame(data)


def save_casts(session: Session, casts: List[Cast], timestamp: int) -> None:
    TOTAL_DAYS = 3

    time_range_start = timestamp - timedelta(days=TOTAL_DAYS).total_seconds() * 1000
    existing_hashes = {
        cast.hash
        for cast in session.query(Cast.hash)
        .filter(Cast.timestamp >= time_range_start)
        .all()
    }

    new_casts = [cast for cast in casts if cast.hash not in existing_hashes]

    if new_casts:
        df = to_pandas(new_casts)

        logger.info(f"Saving {len(new_casts)} new casts to database.")
        df.to_sql(
            "casts",
            session.bind,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )

    logger.info(
        f"Total casts fetched: {len(casts)}; total casts inserted: {len(new_casts)}"
    )


def main(engine: Engine):
    warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")

    if not warpcast_hub_key:
        raise Exception("WARPCAST_HUB_KEY not found in .env file.")

    with sessionmaker(bind=engine)() as session:
        # get the latest cast timestamp
        latest_cast = session.query(Cast).order_by(Cast.timestamp.desc()).first()
        latest_timestamp = latest_cast.timestamp if latest_cast else 0
        print_timestamp_info(latest_timestamp)

        casts = fetch_data(warpcast_hub_key, latest_timestamp)
        models = modelify(casts)
        save_casts(session, models, latest_timestamp)
        print_time_difference(models, latest_timestamp)
