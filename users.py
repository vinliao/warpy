from dotenv import load_dotenv
import os
import requests
from models import Location
import time
import asyncio
import aiohttp
from dataclasses import dataclass
from typing import Optional

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


@dataclass(frozen=True)
class SearchcasterDataClass:
    fid: int
    farcaster_address: str
    external_address: str
    registered_at: int


# this is for the external address table
@dataclass(frozen=True)
class EnsdataDataClass:
    address: str
    ens: str
    url: str
    github: str
    twitter: str
    discord: str
    email: str
    telegram: str


@dataclass
class UserClass:
    fid: int
    username: str
    display_name: str
    verified: bool
    pfp_url: str
    follower_count: int
    following_count: int
    location_place_id: str = None
    bio_text: str = None
    farcaster_address: str = None
    external_address: str = None
    registered_at: int = None

# ============================================================
# ====================== WARPCAST ============================
# ============================================================


def get_users_from_warpcast(key: str, cursor: str = None):
    # have cursor in url if cursor exists, use ternary

    url = f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit=1000" if cursor else "https://api.warpcast.com/v2/recent-users?limit=1000"

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


def get_warpcast_location(user) -> Optional[Location]:
    if 'location' in user['profile']:
        place_id = user['profile']['location'].get('placeId')
        if place_id:
            description = user['profile']['location'].get('description')
            return Location(place_id=place_id, description=description)
    return None


def extract_warpcast_user_data(user):
    return {
        'fid': user['fid'],
        'username': user['username'],
        'display_name': user['displayName'],
        'verified': user['pfp']['verified'] if 'pfp' in user else False,
        'pfp_url': user['pfp']['url'] if 'pfp' in user else '',
        'follower_count': user['followerCount'],
        'following_count': user['followingCount'],
        'bio_text': user['profile']['bio']['text'] if 'bio' in user['profile'] else None,
        'location_place_id': user['profile']['location']['placeId'] if 'location' in user['profile'] else None
    }


# ============================================================
# ====================== SEARCHCASTER ========================
# ============================================================


async def get_single_user_from_searchcaster(username):
    url = f'https://searchcaster.xyz/api/profiles?username={username}'

    print(f"Fetching {username} from {url}")

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            json_data = await response.json()
            return json_data[0]


async def get_users_from_searchcaster(usernames):
    tasks = [asyncio.create_task(get_single_user_from_searchcaster(username))
             for username in usernames]
    users = await asyncio.gather(*tasks)
    return users


def extract_searchcaster_user_data(data) -> SearchcasterDataClass:
    return SearchcasterDataClass(
        fid=data['body']['id'],
        farcaster_address=data['body']['address'],
        external_address=data['connectedAddress'],
        registered_at=data['body']['registeredAt']
    )


# ============================================================
# ====================== ENSDATA =============================
# ============================================================


async def get_single_user_from_ensdata(address: str):
    url = f'https://ensdata.net/{address}'

    print(f"Fetching {address} from {url}")

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            json_data = await response.json()
            return json_data


async def get_users_from_ensdata(addresses: list[str]):
    tasks = [asyncio.create_task(get_single_user_from_ensdata(address))
             for address in addresses]
    users = await asyncio.gather(*tasks)
    return users


def extract_ensdata_user_data(raw_data) -> EnsdataDataClass:
    return EnsdataDataClass(
        address=raw_data.get('address'),
        ens=raw_data.get('ens'),
        url=raw_data.get('url'),
        github=raw_data.get('github'),
        twitter=raw_data.get('twitter'),
        telegram=raw_data.get('telegram'),
        email=raw_data.get('email'),
        discord=raw_data.get('discord')
    )
