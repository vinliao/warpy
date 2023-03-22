from dotenv import load_dotenv
import os
import requests
from models import Location
import time
import asyncio
import aiohttp
from dataclasses import dataclass, asdict
from typing import Optional
import polars as pl
from typing import List

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


# @dataclass(frozen=True)
# class EthereumAddressDataClass:
#     address: str
#     ens: str
#     url: str
#     github: str
#     twitter: str
#     discord: str
#     email: str
#     telegram: str


@dataclass(frozen=True)
class UserDataClass:
    fid: int
    username: str
    display_name: str
    verified: bool
    pfp_url: str
    farcaster_address: str
    external_address: str
    registered_at: int
    bio_text: str

# ============================================================
# ====================== WARPCAST ============================
# ============================================================


def get_users_from_warpcast(key: str, cursor: str = None, limit: int = None):
    url = f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit={limit}" if cursor else f"https://api.warpcast.com/v2/recent-users?limit={limit}"

    print(f"Fetching from {url}")

    # fetch to url with the bearer token
    result = requests.get(
        url, headers={"Authorization": "Bearer " + key})
    json_data = result.json()

    # cursor may be empty here so handle it
    return {"users": json_data["result"]['users'],
            "cursor": json_data.get("next", {}).get('cursor') if json_data.get("next") else None}


def get_all_users_from_warpcast(key: str):
    cursor = None
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


def get_warpcast_location(user) -> Optional[dict]:
    if 'location' in user['profile']:
        place_id = user['profile']['location'].get('placeId')
        if place_id:
            description = user['profile']['location'].get('description')
            return {'place_id': place_id, 'description': description}
    return None


def extract_warpcast_user_data(user):
    return {
        'fid': user['fid'],
        'username': user['username'],
        'display_name': user['displayName'],
        'verified': user['pfp']['verified'] if 'pfp' in user else False,
        'pfp_url': user['pfp']['url'] if 'pfp' in user else '',
        'bio_text': user['profile']['bio']['text'] if 'bio' in user['profile'] else None,
    }


async def get_single_user_from_searchcaster(username):
    url = f'https://searchcaster.xyz/api/profiles?username={username}'

    print(f"Fetching {username} from {url}")

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            # Sleep between requests to avoid rate limiting
            await asyncio.sleep(1)

            json_data = await response.json()
            return json_data[0]


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


async def main():
    warpcast_users = get_users_from_warpcast(warpcast_hub_key, None, 50)
    warpcast_user_data = [extract_warpcast_user_data(
        user) for user in warpcast_users['users']]

    usernames = [user['username'] for user in warpcast_users['users']]

    # get users from searchcaster in batches of 50
    batch_size = 20
    searchcaster_users = []
    for i in range(0, len(usernames), batch_size):
        batch = usernames[i:i+batch_size]
        searchcaster_users += await get_users_from_searchcaster(batch)
        time.sleep(1)

    # # get users from searchcaster in batches of 50, extract data, merge to df, write to parquet
    # searchcaster_users = await get_users_from_searchcaster(usernames)
    searchcaster_user_data = [extract_searchcaster_user_data(
        user) for user in searchcaster_users]

    merged_user_data = merge_user_data(
        warpcast_user_data, searchcaster_user_data)
    users_df = pl.DataFrame([asdict(user) for user in merged_user_data])
    users_df.write_parquet('users.parquet')


if __name__ == '__main__':
    asyncio.run(main())
