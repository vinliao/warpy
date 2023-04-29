import asyncio
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.app.logger import logger
from src.db.models import Cast, Reaction

load_dotenv()


async def make_async_request_with_retry(
    url: str, headers: Optional[Dict[str, str]] = None
) -> Any:
    retry_limit = 3
    retry_count = 0

    while retry_count < retry_limit:
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
        except (Exception, asyncio.TimeoutError):
            logging.error(f"Error occurred for {url}. Retrying...")

        retry_count += 1
        await asyncio.sleep(5)

    return None


async def fetch_reactions(url: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    reactions = []
    cursor = None
    request_count = 1

    while True:
        try:
            url_with_cursor = f"{url}&cursor={cursor}" if cursor else url
            logger.info(f"Fetching {url_with_cursor}")

            data = await make_async_request_with_retry(url_with_cursor, headers=headers)

            if data and "result" in data and "reactions" in data["result"]:
                reactions.extend(data["result"]["reactions"])
                logger.info(
                    f"Request {request_count}:"
                    f" {len(data['result']['reactions'])} reactions fetched."
                )
            else:
                logging.error(f"Error occurred for {url}: {data}. Retrying...")

            if data:
                cursor = data.get("next", {}).get("cursor")
                if not cursor or not data["result"]["reactions"]:
                    logger.info(
                        "Finished fetching reactions. Total reactions fetched:"
                        f" {len(reactions)}"
                    )
                    break
                request_count += 1
            else:
                logger.info("No data received. Retrying...")
                break

        except ValueError as e:
            logger.error(f"Error occurred for {url}: {e}. Retrying...")
            await asyncio.sleep(5)

    return reactions


async def fetch_data(
    cast_hashes: List[str], warpcast_hub_key: str, n: int
) -> Dict[str, List[Dict[str, Any]]]:
    headers = {"Authorization": f"Bearer {warpcast_hub_key}"}
    reactions = {}
    for i in range(0, len(cast_hashes), n):
        tasks = []
        for cast_hash in cast_hashes[i : i + n]:
            url = f"https://api.warpcast.com/v2/cast-reactions?castHash={cast_hash}&limit=100"
            tasks.append(fetch_reactions(url, headers))

        responses = await asyncio.gather(*tasks)
        for cast_hash, response_data in zip(cast_hashes[i : i + n], responses):
            if response_data is not None and response_data != []:
                reactions[cast_hash] = response_data

    return reactions


def extract_data(data: Dict[str, Any]) -> Reaction:
    return Reaction(
        reaction_type=data["type"],
        hash=data["hash"],
        timestamp=data["timestamp"],
        target_hash=data["castHash"],
        author_fid=data["reactor"]["fid"],
    )


def modelify(reactions: Dict[str, List[Dict[str, Any]]]) -> List[Reaction]:
    return [
        extract_data(reaction_data)
        for reaction_list in reactions.values()
        for reaction_data in reaction_list
    ]


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


def insert_reactions(session: Session, reactions: List[Reaction]):
    existing_hashes = (
        session.query(Reaction.hash)
        .filter(Reaction.hash.in_(tuple(reaction.hash for reaction in reactions)))
        .all()
    )

    existing_hashes = set([hash_[0] for hash_ in existing_hashes])

    new_reactions = [
        reaction for reaction in reactions if reaction.hash not in existing_hashes
    ]

    if new_reactions:
        df = to_pandas(new_reactions)
        df.to_sql(
            "reactions",
            session.bind,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )

    logger.info(
        f"Total reactions fetched: {len(reactions)}; total reactions inserted:"
        f" {len(new_reactions)}"
    )


async def main(engine: Engine):
    warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")

    if not warpcast_hub_key:
        raise Exception("WARPCAST_HUB_KEY not found in .env file.")

    with sessionmaker(engine)() as session:
        now = datetime.now()
        days_per_batch = 1

        for batch_ago in range(1, 7):
            start_timestamp = int(
                (now - timedelta(days=batch_ago * days_per_batch)).timestamp() * 1000
            )
            end_timestamp = int(
                (now - timedelta(days=(batch_ago - 1) * days_per_batch)).timestamp()
                * 1000
            )

            casts = (
                session.query(Cast)
                .filter(
                    and_(
                        Cast.timestamp > start_timestamp,
                        Cast.timestamp <= end_timestamp,
                    )
                )
                .all()
            )
            cast_hashes = [cast.hash for cast in casts]

            reactions = await fetch_data(cast_hashes, warpcast_hub_key, 1000)
            models = modelify(reactions)
            insert_reactions(session, models)

            time.sleep(10)
