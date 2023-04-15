import asyncio
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from utils.fetcher import AsyncFetcher
from utils.models import Cast, Reaction

load_dotenv()


class WarpcastReactionFetcher(AsyncFetcher):
    """
    WarpcastReactionFetcher is a concrete implementation of the BaseFetcher.
    It fetches reaction data from the Warpcast API.
    """

    def __init__(self, key: str, cast_hashes: List[str], limit: int = 10):
        """
        Initializes a WarpcastReactionFetcher object.
        """
        self.cast_hashes = cast_hashes
        self.warpcast_hub_key = key
        self.limit = limit
        self.json_data: Dict[str, List[Dict[str, Any]]] = {}

    async def _fetch_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetches reaction data from the Warpcast API for a list of cast hashes.
        """
        self.json_data = await self._get_cast_reactions_async(
            self.cast_hashes, self.warpcast_hub_key, self.limit
        )

    def _get_models(self) -> List[Reaction]:
        """
        Processes raw reaction data and returns a list of Reaction model objects.
        """
        extracted_reactions = [
            self._extract_data(reaction)
            for reaction_list in self.json_data.values()
            for reaction in reaction_list
        ]
        return extracted_reactions

    def _extract_data(self, data: Dict[str, Any]) -> Reaction:
        """
        Extracts relevant reaction data from a raw reaction dictionary.
        :param data: dict, Raw reaction data from the Warpcast API.
        :return: A Reaction model object.
        """
        return Reaction(
            reaction_type=data["type"],
            hash=data["hash"],
            timestamp=data["timestamp"],
            target_hash=data["castHash"],
            author_fid=data["reactor"]["fid"],
        )

    async def _get_cast_reactions_async(
        self, cast_hashes: List[str], warpcast_hub_key: str, n: int
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetches reaction data from the Warpcast API for a list of cast hashes.
        :param cast_hashes: list, A list of cast hashes to fetch data for.
        :param warpcast_hub_key: str, The Warpcast API key to use for fetching data.
        :param n: int, The number of reactions to fetch for each cast.
        :return: A dictionary containing a list of reactions for each cast hash.
        """
        headers = {"Authorization": f"Bearer {warpcast_hub_key}"}
        reactions = {}
        for i in range(0, len(cast_hashes), n):
            tasks = []
            for cast_hash in cast_hashes[i : i + n]:
                url = f"https://api.warpcast.com/v2/cast-reactions?castHash={cast_hash}&limit=100"
                tasks.append(self._fetch_reactions(url, headers))

            responses = await asyncio.gather(*tasks)
            for cast_hash, response_data in zip(cast_hashes[i : i + n], responses):
                if response_data is not None and response_data != []:
                    reactions[cast_hash] = response_data

        return reactions

    async def _fetch_reactions(
        self, url: str, headers: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """
        Fetches reaction data from the Warpcast API for a single cast hash.
        :param url: str, The URL to fetch data from.
        :param headers: dict, The headers to use for the request.
        :return: A list of reaction data.
        """
        reactions = []
        cursor = None
        while True:
            try:
                url_with_cursor = f"{url}&cursor={cursor}" if cursor else url

                data = await self._make_async_request_with_retry(
                    url_with_cursor, headers=headers
                )

                reactions.extend(data["result"]["reactions"])

                cursor = data.get("next", {}).get("cursor")
                if cursor is None:
                    break

            except ValueError as e:
                print(f"Error occurred for {url}: {e}. Retrying...")
                await asyncio.sleep(5)

        return reactions

    async def fetch(self):
        await self._fetch_data()
        return self._get_models()


def insert_reactions(session, reactions: List[Reaction]):
    # Get the list of existing reaction hashes
    existing_hashes = (
        session.query(Reaction.hash)
        .filter(Reaction.hash.in_(tuple(reaction.hash for reaction in reactions)))
        .all()
    )

    # Convert the list of tuples to a set for faster lookup
    existing_hashes = set([hash_[0] for hash_ in existing_hashes])

    # Insert only the new reactions into the database
    for reaction in reactions:
        if reaction.hash not in existing_hashes:
            session.add(reaction)

    # Commit the changes to the database
    session.commit()
    print(f"Inserted {len(reactions)} reactions")


async def main(engine: Engine):
    warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")

    if not warpcast_hub_key:
        raise Exception("WARPCAST_HUB_KEY not found in .env file.")

    with sessionmaker(engine)() as session:
        # One week because a cast that been around for a week
        # probably would have their reactions "solidified"
        one_week_ago = datetime.now() - timedelta(days=7)
        one_week_ago_unix_ms = int(one_week_ago.timestamp() * 1000)

        casts = (
            session.query(Cast)
            .filter(
                Cast.timestamp < one_week_ago_unix_ms,
                ~Cast.hash.in_(session.query(Reaction.target_hash).distinct()),
            )
            .order_by(Cast.timestamp.desc())
            .all()
        )

        print(f"Fetching reactions for {len(casts)} casts...")

        cast_hashes = [cast.hash for cast in casts]
        batch_size = 10
        for i in range(0, len(cast_hashes), batch_size):
            batch = cast_hashes[i : i + batch_size]  # noqa: E203
            fetcher = WarpcastReactionFetcher(
                key=warpcast_hub_key, cast_hashes=batch, limit=100
            )
            data = await fetcher.fetch()
            insert_reactions(session, data)
