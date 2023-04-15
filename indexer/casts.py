import os
import time
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from utils.models import Cast
from utils.new_fetcher import SyncFetcher
from utils.utils import save_casts_to_sqlite


class WarpcastCastFetcher(SyncFetcher):
    """
    WarpcastCastFetcher is a concrete implementation of the BaseFetcher.
    It fetches recent cast data from the Warpcast API.
    """

    def __init__(self, key: str, latest_timestamp: int):
        """
        Initializes WarpcastCastFetcher with an API key.

        :param key: str, API key to access the Warpcast API.
        """
        self.key: str = key
        self.casts: List[Dict[str, Any]]
        self.latest_timestamp = latest_timestamp

    def _fetch_data(self):
        """
        Fetches data for recent casts from the Warpcast API until a specific timestamp.

        :param timestamp: int, UNIX timestamp in milliseconds.
        :return: A list of dictionaries containing cast data.
        """
        all_data = []
        cursor = None

        while True:
            batch_data, cursor = self._fetch_batch(cursor)
            all_data.extend(batch_data)

            # Check if the last fetched cast timestamp is less than the given timestamp
            if all_data[-1]["timestamp"] < self.latest_timestamp:
                break

            if cursor is None:
                break
            else:
                time.sleep(1)  # add a delay to avoid hitting rate limit

        # Remove casts with a timestamp less than the given timestamp
        all_data = [
            cast for cast in all_data if cast["timestamp"] >= self.latest_timestamp
        ]
        self.casts = all_data
        # return all_data

    def _extract_data(self, cast: Dict[str, Any]) -> Cast:
        """
        Extracts relevant cast data from a raw cast dictionary.

        :param cast: dict, Raw cast data from the Warpcast API.
        :return: A Cast object.
        """
        return Cast(
            hash=cast["hash"],
            thread_hash=cast["threadHash"],
            text=cast["text"],
            timestamp=cast["timestamp"],
            author_fid=cast["author"]["fid"],
            parent_hash=cast.get("parentHash", None),
        )

    def _get_models(self) -> List[Cast]:
        """
        Processes raw cast data and returns a list of Cast model objects.

        :param casts: list, A list of dictionaries containing raw cast data.
        :return: A list of Cast objects.
        """
        return [self._extract_data(cast) for cast in self.casts]

    def _fetch_batch(
        self, cursor: Optional[str] = None, limit: int = 1000
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Fetches a batch of cast data from the Warpcast API with pagination.

        :param cursor: Optional, str, Cursor for pagination.
        :param limit: int, Maximum number of casts to fetch in the batch.
        :return: A tuple containing a list of dictionaries with cast data and the next cursor, if any.
        """
        url = (
            f"https://api.warpcast.com/v2/recent-casts?cursor={cursor}&limit={limit}"
            if cursor
            else f"https://api.warpcast.com/v2/recent-casts?limit={limit}"
        )
        json_data = self._make_request(
            url, headers={"Authorization": "Bearer " + self.key}
        )
        return (
            json_data["result"]["casts"],
            json_data.get("next", {}).get("cursor") if json_data.get("next") else None,
        )

    def fetch(self):
        """
        Fetches recent cast data from the Warpcast API and saves it to the SQLite database.
        """
        self._fetch_data()
        return self._get_models()


load_dotenv()


def main(engine: Engine):
    warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")

    if not warpcast_hub_key:
        raise Exception("WARPCAST_HUB_KEY not found in .env file.")

    with sessionmaker(bind=engine)() as session:
        # latest_cast = session.query(Cast).order_by(Cast.timestamp.desc()).first()
        # latest_timestamp = latest_cast.timestamp if latest_cast else 0

        # 7 days ago in unix ms
        latest_timestamp = int(time.time() * 1000) - 7 * 24 * 60 * 60 * 1000

        fetcher = WarpcastCastFetcher(
            key=warpcast_hub_key, latest_timestamp=latest_timestamp
        )
        data = fetcher.fetch()
        # data = fetcher.fetch_data(latest_timestamp)
        save_casts_to_sqlite(session, data, latest_timestamp)
