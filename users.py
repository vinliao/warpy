from dotenv import load_dotenv
import os
import requests
from models import Location
import time
import asyncio
import aiohttp
from dataclasses import dataclass, asdict, field
import pandas as pd
from typing import List, Optional
import polars as pl
import sys

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


"""
@dataclass(frozen=True)
class UserEnsDataClass
    address: str
    ens: str
    # other info...
"""


@dataclass(frozen=True)
class UserExtraDataClass:
    fid: int
    following_count: int
    follower_count: int
    location_id: Optional[str] = None
    verified: bool = False
    farcaster_address: Optional[str] = None
    external_address: Optional[str] = None
    registered_at: int = -1


@dataclass(frozen=True)
class LocationDataClass:
    id: str
    description: str


@dataclass(frozen=True)
class UserDataClass:
    fid: int
    username: str
    display_name: str
    pfp_url: str
    bio_text: str


# ============================================================
# ====================== WARPCAST ============================
# ============================================================


def get_users_from_warpcast(key: str, cursor: str = None, limit: int = 1000):
    url = f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit={limit}" if cursor else f"https://api.warpcast.com/v2/recent-users?limit={limit}"

    print(f"Fetching from {url}")

    # fetch to url with the bearer token
    result = requests.get(
        url, headers={"Authorization": "Bearer " + key})
    json_data = result.json()

    # cursor may be empty here so handle it
    return {"users": json_data["result"]['users'],
            "cursor": json_data.get("next", {}).get('cursor') if json_data.get("next") else None}


def get_all_users_from_warpcast(key: str, cursor: str = None):
    users = []
    while True:
        data = get_users_from_warpcast(key, cursor)
        users += data["users"]
        cursor = data.get("cursor")

        if cursor is None:
            break
        else:
            time.sleep(1)  # add a delay to avoid hitting rate limit
            continue

    return users


def extract_warpcast_user_data(user):
    location_data = user.get('profile', {}).get('location', {})
    location = LocationDataClass(
        id=location_data.get('placeId', ''),
        description=location_data.get('description', '')
    )

    user_extra_data = UserExtraDataClass(
        fid=user['fid'],
        following_count=user.get('followingCount', 0),
        follower_count=user.get('followerCount', 0),
        location_id=location.id,
        verified=user['pfp']['verified'] if 'pfp' in user else False,
        farcaster_address=None,  # Update this value as needed
        registered_at=-1  # Update this value as needed
    )

    user_data = UserDataClass(
        fid=user['fid'],
        username=user['username'],
        display_name=user['displayName'],
        pfp_url=user['pfp']['url'] if 'pfp' in user else '',
        bio_text=user.get('profile', {}).get('bio', {}).get('text', '')
    )

    return user_data, user_extra_data, location if location.id else None


async def get_single_user_from_searchcaster(username):
    url = f'https://searchcaster.xyz/api/profiles?username={username}'

    print(f"Fetching {username} from {url}")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.get(url, timeout=timeout) as response:
                    # Sleep between requests to avoid rate limiting
                    await asyncio.sleep(1)

                    response.raise_for_status()
                    json_data = await response.json()
                    if json_data:
                        return json_data[0]
                    else:
                        raise ValueError(
                            f"No results found for username {username}")
            except aiohttp.ClientResponseError as e:
                if e.status == 504:
                    print(f"Timeout occurred for {username}. Retrying...")
                else:
                    raise


async def get_users_from_searchcaster(usernames):
    tasks = [asyncio.create_task(get_single_user_from_searchcaster(username))
             for username in usernames]
    users = await asyncio.gather(*tasks)
    return users


def extract_searchcaster_user_data(data):
    return {
        'fid': data['body']['id'],
        'farcaster_address': data['body']['address'],
        'external_address': data['connectedAddress'],
        'registered_at': data['body']['registeredAt']
    }


def merge_user_data(warpcast_data: List[dict], searchcaster_data: List[dict]) -> List[UserDataClass]:
    merged_data = []

    for warpcast_user in warpcast_data:
        searchcaster_user = next((user for user in searchcaster_data if user.get(
            'fid') == warpcast_user.get('fid')), {})

        user_data_dict = {**warpcast_user, **searchcaster_user}
        merged_data.append(UserDataClass(**user_data_dict))

    return merged_data


async def update_unregistered_users():
    # Read data from user_extra.parquet and filter rows where registered_at is -1
    users_extra_df = pd.read_parquet('user_extra.parquet')
    unregistered_users = users_extra_df[users_extra_df['registered_at'] == -1]

    # get fids from unregistered_users, then get the usernames from users.parquet
    unregistered_fids = unregistered_users['fid'].tolist()
    users_df = pd.read_parquet('users.parquet')

    unregistered_usernames = users_df[users_df['fid'].isin(
        unregistered_fids)]['username'].tolist()

    if len(unregistered_usernames) > 0:
        batch_size = 50
        updated_users_extra_df = users_extra_df.set_index('fid')

        for i in range(0, len(unregistered_usernames), batch_size):
            batch_usernames = unregistered_usernames[i:i + batch_size]
            searchcaster_users = await get_users_from_searchcaster(batch_usernames)
            searchcaster_user_data = [extract_searchcaster_user_data(
                user) for user in searchcaster_users]

            # Create a Pandas DataFrame from the searchcaster user data
            searchcaster_user_data_df = pd.DataFrame(searchcaster_user_data)

            # Update updated_users_extra_df with data from searchcaster_user_data_df
            updated_users_extra_df.update(
                searchcaster_user_data_df.set_index('fid'))

        updated_users_extra_df.reset_index(inplace=True)
        updated_users_extra_df.to_parquet('user_extra.parquet')


async def main():
    if '--extra' in sys.argv:
        await update_unregistered_users()
    else:
        warpcast_users = get_all_users_from_warpcast(warpcast_hub_key)

        warpcast_user_data = [extract_warpcast_user_data(
            user) for user in warpcast_users]

        # Extract the user_data, user_extra_data, and location lists
        user_data_list = [data[0] for data in warpcast_user_data]
        user_extra_data_list = [data[1] for data in warpcast_user_data]
        location_list = [data[2]
                         for data in warpcast_user_data if data[2]]

        # filter dupliate and remove None for locations
        location_list = list(
            {location.id: location for location in location_list}.values())

        pl.DataFrame(user_data_list).write_parquet('users.parquet')
        pl.DataFrame(user_extra_data_list).write_parquet(
            'user_extra.parquet')
        pl.DataFrame(location_list).write_parquet('locations.parquet')


if __name__ == '__main__':
    asyncio.run(main())
