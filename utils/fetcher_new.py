from abc import ABC, abstractmethod
from typing import Tuple, List
import time
import requests
from utils.models import Reaction, Location, User, Cast
import asyncio
import aiohttp
from typing import List, Dict, Optional, Any


class BaseFetcher(ABC):
    """
    BaseFetcher is an abstract base class for all Fetcher classes.
    It provides the basic structure for all fetchers including synchronous and asynchronous request functions.
    """

    @abstractmethod
    def fetch_data(self):
        """
        Abstract method to be implemented by child classes to fetch data from the respective sources.
        """
        pass

    @abstractmethod
    def get_models(self):
        """
        Abstract method to be implemented by child classes to return clean data models ready for insertion into the database.
        """
        pass

    @abstractmethod
    def _extract_data(self):
        """
        Abstract method to be implemented by child classes to extract relevant data from the raw data fetched.
        """
        pass

    def _make_request(url: str, headers: Dict[str, str] = None, timeout: int = 10) -> Any:
        """
        Makes a synchronous GET request to the specified URL, and returns the JSON response.

        :param url: The URL to send the request to.
        :param headers: Optional dictionary of headers to include in the request.
        :param timeout: Optional time limit in seconds for the request to complete.
        :return: Parsed JSON response.
        """
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()

    async def _make_async_request(url: str, headers: Dict[str, str] = None, timeout: int = 10) -> Any:
        """
        Makes an asynchronous GET request to the specified URL, and returns the JSON response.

        :param url: The URL to send the request to.
        :param headers: Optional dictionary of headers to include in the request.
        :param timeout: Optional time limit in seconds for the request to complete.
        :return: Parsed JSON response.
        """
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.json()

    async def _make_async_request_with_retry(url: str, max_retries: int = 3, delay: int = 5, headers: Dict[str, str] = None, timeout: int = 10) -> Any:
        """
        Makes an asynchronous GET request to the specified URL with a retry mechanism.

        :param url: The URL to send the request to.
        :param max_retries: Maximum number of times to retry the request in case of a failure.
        :param delay: Time delay in seconds between retries.
        :param headers: Optional dictionary of headers to include in the request.
        :param timeout: Optional time limit in seconds for the request to complete.
        :return: Parsed JSON response.
        """
        retries = 0
        while retries < max_retries:
            try:
                response_data = await self._make_async_request(url, headers=headers, timeout=timeout)
                if response_data:
                    return response_data
                else:
                    raise ValueError(f"No results found for the URL: {url}")
            except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                print(f"Error occurred for URL {url}. Retrying...")
                await asyncio.sleep(delay)
                retries += 1

        raise RuntimeError(
            f"Failed to fetch data from URL {url} after {max_retries} attempts.")


class WarpcastUserFetcher(BaseFetcher):
    """
    WarpcastUserFetcher is a concrete implementation of the BaseFetcher.
    It fetches recent user data from the Warpcast API.
    """

    def __init__(self, key: str):
        """
        Initializes WarpcastUserFetcher with an API key.

        :param key: str, API key to access the Warpcast API.
        """
        self.key = key

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetches data for recent users from the Warpcast API.

        :return: A list of dictionaries containing user data.
        """
        all_data = []
        cursor = None

        while True:
            batch_data, cursor = self._fetch_batch(cursor)
            all_data.extend(batch_data)

            if cursor is None:
                break
            else:
                time.sleep(1)  # add a delay to avoid hitting rate limit

        return all_data

    def _fetch_batch(self, cursor: Optional[str] = None, limit: int = 1000) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Fetches a batch of user data from the Warpcast API with pagination.

        :param cursor: Optional, str, Cursor for pagination.
        :param limit: int, Maximum number of users to fetch in the batch.
        :return: A tuple containing a list of dictionaries with user data and the next cursor, if any.
        """
        url = f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit={limit}" if cursor else f"https://api.warpcast.com/v2/recent-users?limit={limit}"
        print(f"Fetching from {url}")
        json_data = self._make_request(
            url, headers={"Authorization": "Bearer " + self.key})
        return json_data["result"]['users'], json_data.get("next", {}).get('cursor') if json_data.get("next") else None

    def _extract_data(self, user: Dict[str, Any]) -> Tuple[User, Optional[Location]]:
        """
        Extracts relevant user data and location from a raw user dictionary.

        :param user: dict, Raw user data from the Warpcast API.
        :return: A tuple containing a User object and an optional Location object.
        """
        location_data = user.get('profile', {}).get('location', {})
        location = Location(
            id=location_data.get('placeId', ''),
            description=location_data.get('description', '')
        )

        user_data = User(
            fid=user['fid'],
            username=user['username'],
            display_name=user['displayName'],
            pfp_url=user['pfp']['url'] if 'pfp' in user else '',
            bio_text=user.get('profile', {}).get('bio', {}).get('text', ''),
            following_count=user.get('followingCount', 0),
            follower_count=user.get('followerCount', 0),
            location_id=location.id,
            verified=int(user['pfp']['verified']
                         ) if 'pfp' in user and 'verified' in user['pfp'] else 0,
            farcaster_address="",  # Update this value as needed
            registered_at=-1  # Update this value as needed
        )

        return user_data, location if location.id else None

    def get_models(self, users: List[Dict[str, Any]]) -> Tuple[List[User], List[Location]]:
        """
        Processes raw user data and returns lists of User and Location model objects.

        :param users: list, A list of dictionaries containing raw user data.
        :return: A tuple containing two lists: one of User objects and one of Location objects.
        """
        user_data = [self._extract_data(user) for user in users]

        user_list = [data[0] for data in user_data]
        location_list = [data[1] for data in user_data if data[1]]

        # Filter duplicates and remove None for locations
        location_list = list(
            {location.id: location for location in location_list}.values())

        return user_list, location_list


class SearchcasterFetcher(BaseFetcher):
    """
    SearchcasterFetcher is a concrete implementation of the BaseFetcher.
    It fetches user data from the Searchcaster API.
    """

    async def fetch_data(self, usernames: List[str]) -> List[Dict[str, Any]]:
        """
        Fetches user data from the Searchcaster API for a list of usernames.

        :param usernames: list, A list of usernames to fetch data for.
        :return: A list of dictionaries containing user data.
        """
        tasks = [asyncio.create_task(self._fetch_single_user(
            username)) for username in usernames]
        users = await asyncio.gather(*tasks)
        return users

    async def _fetch_single_user(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Fetches data for a single user from the Searchcaster API by username.

        :param username: str, Username to fetch data for.
        :return: A dictionary containing user data, or None if not found.
        """
        url = f'https://searchcaster.xyz/api/profiles?username={username}'
        print(f"Fetching {username} from {url}")

        response_data = await self._make_async_request_with_retry(url)
        return response_data[0] if response_data else None

    def _extract_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extracts relevant user data from a raw user dictionary.

        :param data: dict, Raw user data from the Searchcaster API.
        :return: A dictionary with the extracted user data.
        """
        return {
            'fid': data['body']['id'],
            'farcaster_address': data['body']['address'],
            'external_address': data['connectedAddress'],
            'registered_at': data['body']['registeredAt']
        }

    def get_models(self, users: List[User], user_data_list: List[Dict[str, Any]]) -> List[User]:
        """
        Updates User model objects with additional data fetched from the Searchcaster API.

        :param users: list, A list of User model objects to update.
        :param user_data_list: list, A list of dictionaries containing raw user data.
        :return: A list of updated User model objects.
        """

        updated_users = []

        extracted_user_data = [self._extract_data(
            user_data) for user_data in user_data_list]
        user_data_dict = {
            user_data['fid']: user_data for user_data in extracted_user_data}

        for user in users:
            user_data = user_data_dict.get(user.fid)
            if user_data and user_data['farcaster_address'] is not None and user_data['registered_at'] != 0:
                user.farcaster_address = user_data['farcaster_address']
                user.external_address = user_data['external_address']
                user.registered_at = user_data['registered_at']
                updated_users.append(user)

        return updated_users
