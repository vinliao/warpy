import asyncio
import os
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from utils.fetcher import AsyncFetcher, SyncFetcher
from utils.models import Location, User
from utils.utils import save_objects, update_users_warpcast


class WarpcastUserFetcher(SyncFetcher):
    def __init__(self, key: str):
        self.key = key
        self.users: List[Dict[str, Any]] = []

    def _fetch_data(self, partial: bool = False) -> None:
        """
        Fetch all users and store them in the self.users attribute.
        """
        cursor = None

        while True:
            batch_data, cursor = self._fetch_batch(
                cursor, limit=52 if partial else 1000
            )
            self.users.extend(batch_data)

            if partial or cursor is None:
                break
            else:
                time.sleep(1)

    def _fetch_batch(
        self, cursor: Optional[str] = None, limit: int = 1000
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Fetches a batch of user data from the Warpcast API with pagination.
        """
        url = (
            f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit={limit}"
            if cursor
            else f"https://api.warpcast.com/v2/recent-users?limit={limit}"
        )
        json_data = self._make_request(
            url, headers={"Authorization": "Bearer " + self.key}
        )
        return (
            json_data["result"]["users"],
            json_data.get("next", {}).get("cursor") if json_data.get("next") else None,
        )

    def _extract_data(self, user: Dict[str, Any]) -> Tuple[User, Optional[Location]]:
        """
        Turns JSON api result into SQLAlchemy models.
        """
        location_data = user.get("profile", {}).get("location", {})
        location = Location(
            id=location_data.get("placeId", ""),
            description=location_data.get("description", ""),
        )

        user_data = User(
            fid=user["fid"],
            username=user["username"],
            display_name=user["displayName"],
            pfp_url=user["pfp"]["url"] if "pfp" in user else "",
            bio_text=user.get("profile", {}).get("bio", {}).get("text", ""),
            following_count=user.get("followingCount", 0),
            follower_count=user.get("followerCount", 0),
            location_id=location.id if location.id else None,
            verified=int(user["pfp"]["verified"])
            if "pfp" in user and "verified" in user["pfp"]
            else 0,
            generated_farcaster_address="",  # Update this value as needed
            registered_at=-1,  # Update this value as needed
        )

        return user_data, location if location.id else None

    def _get_models(self) -> List[Union[User, Location]]:
        """
        Processes user data, returns a list of User and Location model objects.
        """
        user_data = [self._extract_data(user) for user in self.users]

        user_and_location_list = [
            item for sublist in user_data for item in sublist if item
        ]

        # Filter duplicates for locations
        location_dict = {}
        mixed_list = []

        for item in user_and_location_list:
            if isinstance(item, Location):
                if item.id not in location_dict:
                    location_dict[item.id] = item
                    mixed_list.append(item)
            else:
                mixed_list.append(item)

        return mixed_list

    def fetch(self, partial: bool = False) -> List[Union[User, Location]]:
        """
        Fetches user data from the Warpcast API.
        """
        self._fetch_data(partial=partial)
        return self._get_models()


class SearchcasterFetcher(AsyncFetcher):
    def __init__(self, users: List[User]):
        self.users = users
        self.json_data: List[Dict[str, Any]]

    async def _fetch_data(self, _=None) -> None:
        """
        Fetches user data from the Searchcaster API for a list of usernames.
        """
        usernames = [user.username for user in self.users]
        tasks = [
            asyncio.create_task(self._fetch_single_user(username))
            for username in usernames
        ]
        users = await asyncio.gather(*tasks)
        self.json_data = users

    async def _fetch_single_user(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Fetches data for a single user from the Searchcaster API by username.
        """
        url = f"https://searchcaster.xyz/api/profiles?username={username}"

        response_data = await self._make_async_request_with_retry(url)
        return response_data[0] if response_data else None

    def _extract_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Turn JSON API result into a dictionary containing relevant data.
        """
        return {
            "fid": data["body"]["id"],
            "generated_farcaster_address": data["body"]["address"],
            "address": data["connectedAddress"],
            "registered_at": data["body"]["registeredAt"],
        }

    def _get_models(self) -> List[User]:
        """
        Updates User model objects with additional data fetched from the Searchcaster API.
        """

        updated_users = []

        extracted_user_data = [
            self._extract_data(user_data) for user_data in self.json_data
        ]
        user_data_dict = {
            user_data["fid"]: user_data for user_data in extracted_user_data
        }

        for user in self.users:
            user_data = user_data_dict.get(user.fid)
            if (
                user_data
                and user_data["generated_farcaster_address"] is not None
                and user_data["registered_at"] != 0
            ):
                user.generated_farcaster_address = user_data[
                    "generated_farcaster_address"
                ]
                user.address = user_data["address"]
                user.registered_at = user_data["registered_at"]
                updated_users.append(user)

        return updated_users

    async def fetch(self) -> List[User]:
        """
        Fetches user data from the Searchcaster API.
        """
        await self._fetch_data()
        return self._get_models()


load_dotenv()


def delete_unregistered_users(session):
    """
    -1 means the user didn't register properly,
    need better way to handle this
    """
    session.query(User).filter(User.registered_at == -1).delete()
    session.commit()


def update_user_searchcaster(session, user_list):
    for user in user_list:
        session.merge(user)
    session.commit()


async def main(engine: Engine):
    fetch_warpcast = True

    with sessionmaker(bind=engine)() as session:
        if fetch_warpcast:
            warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")
            if warpcast_hub_key is None:
                raise ValueError("WARPCAST_HUB_KEY is not set")
            warpcast_fetcher = WarpcastUserFetcher(key=warpcast_hub_key)
            users_and_location = warpcast_fetcher.fetch(partial=True)

            location_list = [x for x in users_and_location if isinstance(x, Location)]
            user_list = [x for x in users_and_location if isinstance(x, User)]

            save_objects(session, location_list)
            update_users_warpcast(session, user_list)

        unprocessed_user = session.query(User).filter_by(registered_at=-1).all()
        batch_size = 50
        for i in range(0, len(unprocessed_user), batch_size):
            batch = unprocessed_user[i : i + batch_size]  # noqa: E203
            searchcaster_fetcher = SearchcasterFetcher(batch)
            updated_users = await searchcaster_fetcher.fetch()
            save_objects(session, updated_users)

        delete_unregistered_users(session)
