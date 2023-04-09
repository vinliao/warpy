from abc import ABC, abstractmethod
from typing import Tuple, List
import time
import requests
from utils.models import Reaction, Location, User, Cast
import asyncio
import aiohttp
from typing import List, Dict, Optional


class BaseFetcher(ABC):

    @abstractmethod
    def fetch_data(self):
        pass

    @abstractmethod
    def get_models(self):
        pass

    @abstractmethod
    def _extract_data(self):
        pass

    def _make_request(self, url, headers=None, timeout=10):
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()

    async def _make_async_request(self, url, headers=None, timeout=10):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.json()

    async def _make_async_request_with_retry(self, url, max_retries=3, delay=5, headers=None, timeout=10):
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
    def __init__(self, key):
        self.key = key

    def fetch_data(self):
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

    def _fetch_batch(self, cursor=None, limit=1000):
        url = f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit={limit}" if cursor else f"https://api.warpcast.com/v2/recent-users?limit={limit}"
        print(f"Fetching from {url}")
        json_data = self._make_request(
            url, headers={"Authorization": "Bearer " + self.key})
        return json_data["result"]['users'], json_data.get("next", {}).get('cursor') if json_data.get("next") else None

    # extract data of individual user
    def _extract_data(self, user):
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

    # loop through the json data, return clean sqlalchemy models
    def get_models(self, users) -> Tuple[List[User], List[Location]]:
        user_data = [self._extract_data(user) for user in users]

        user_list = [data[0] for data in user_data]
        location_list = [data[1] for data in user_data if data[1]]

        # Filter duplicates and remove None for locations
        location_list = list(
            {location.id: location for location in location_list}.values())

        return user_list, location_list


class SearchcasterFetcher(BaseFetcher):
    async def fetch_data(self, usernames):
        tasks = [asyncio.create_task(self._fetch_single_user(
            username)) for username in usernames]
        users = await asyncio.gather(*tasks)
        return users

    async def _fetch_single_user(self, username: str) -> dict:
        url = f'https://searchcaster.xyz/api/profiles?username={username}'
        print(f"Fetching {username} from {url}")

        response_data = await self._make_async_request_with_retry(url)
        return response_data[0] if response_data else None

    def _extract_data(self, data):
        return {
            'fid': data['body']['id'],
            'farcaster_address': data['body']['address'],
            'external_address': data['connectedAddress'],
            'registered_at': data['body']['registeredAt']
        }

    def get_models(self, users: List[User], user_data_list) -> List[User]:
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
