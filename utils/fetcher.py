from typing import Optional, Dict
import asyncio
import requests
import aiohttp
import abc
from abc import ABC, abstractmethod
from typing import Any, Dict
from utils.models import Reaction, Location, User, Cast
import time
from typing import Tuple, List


class AbstractFetcher(abc.ABC):

    @abc.abstractmethod
    def fetch_data(self):
        pass

    @abc.abstractmethod
    def extract_data(self, data):
        pass


class SyncFetcher(ABC):
    def fetch_data(self, url, headers=None, timeout=10):
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def fetch_all_data(self):
        all_data = []
        cursor = None

        while True:
            batch_data, cursor = self.fetch_batch(cursor)
            all_data.extend(batch_data)

            if cursor is None:
                break
            else:
                time.sleep(1)  # add a delay to avoid hitting rate limit

        return all_data

    @abstractmethod
    def fetch_batch(self, cursor):
        pass

    @abstractmethod
    def extract_data(self, data):
        pass


class AsyncFetcher(ABC):
    @abstractmethod
    async def fetch_data(self, url: str, headers: Optional[Dict[str, str]] = None):
        pass

    @abstractmethod
    async def extract_data(self, data: Any):
        pass


class SearchcasterFetcher(AsyncFetcher):
    async def fetch_data(self, url: str, headers: Optional[Dict[str, str]] = None):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.json()

    async def fetch_single_user(self, username: str) -> dict:
        url = f'https://searchcaster.xyz/api/profiles?username={username}'
        print(f"Fetching {username} from {url}")

        while True:
            try:
                response_data = await self.fetch_data(url)
                if response_data:
                    return response_data[0]
                else:
                    raise ValueError(
                        f"No results found for username {username}")
            except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                print(f"Error occurred for {username}: {e}. Retrying...")
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying

    async def fetch_users(self, usernames):
        tasks = [asyncio.create_task(self.fetch_single_user(
            username)) for username in usernames]
        users = await asyncio.gather(*tasks)
        return users

    async def extract_data(self, data):
        return {
            'fid': data['body']['id'],
            'farcaster_address': data['body']['address'],
            'external_address': data['connectedAddress'],
            'registered_at': data['body']['registeredAt']
        }

    def get_clean_models(self, user_data) -> List[User]:
        update_data = []
        for user_data in user_data:
            # if farcaster address is not None
            # if farcaster address is not None and registered_at is not 0
            if user_data['farcaster_address'] is not None and user_data['registered_at'] != 0:
                update_data.append({
                    'fid': user_data['fid'],
                    'farcaster_address': user_data['farcaster_address'],
                    'external_address': user_data['external_address'],
                    'registered_at': user_data['registered_at'],
                })
        return update_data


class WarpcastUserFetcher(SyncFetcher):
    def __init__(self, key):
        self.key = key

    def fetch_batch(self, cursor=None, limit=1000):
        url = f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit={limit}" if cursor else f"https://api.warpcast.com/v2/recent-users?limit={limit}"
        print(f"Fetching from {url}")
        json_data = self.fetch_data(
            url, headers={"Authorization": "Bearer " + self.key})
        return json_data["result"]['users'], json_data.get("next", {}).get('cursor') if json_data.get("next") else None

    def extract_data(self, user):
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

    def get_clean_models(self, users) -> Tuple[List[User], List[Location]]:
        user_data = [self.extract_data(user) for user in users]

        user_list = [data[0] for data in user_data]
        location_list = [data[1] for data in user_data if data[1]]

        # Filter duplicates and remove None for locations
        location_list = list(
            {location.id: location for location in location_list}.values())

        return user_list, location_list


# class WarpcastCastFetcher(SyncFetcher):
#     def __init__(self, key):
#         self.key = key

#     def fetch_batch(self, cursor=None, limit=1000):
#         url = f"https://api.warpcast.com/v2/recent-casts?cursor={cursor}&limit={limit}" if cursor else f"https://api.warpcast.com/v2/recent-casts?limit={limit}"
#         print(f"Fetching from {url}")
#         json_data = self.fetch_data(
#             url, headers={"Authorization": "Bearer " + self.key})
#         return json_data["result"]['casts'], json_data.get("next", {}).get('cursor') if json_data.get("next") else None

#     def extract_data(self, cast):
#         return Cast(
#             hash=cast['hash'],
#             thread_hash=cast['threadHash'],
#             text=cast['text'],
#             timestamp=cast['timestamp'],
#             author_fid=cast['author']['fid'],
#             parent_hash=cast.get('parentHash', None)
#         )
