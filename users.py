from dotenv import load_dotenv
import os
import requests
from sqlalchemy.orm import sessionmaker, Session
from models import Base, User, Location
from sqlalchemy import create_engine, and_, Engine
import time
import asyncio
import aiohttp
import argparse
import sys
from dataclasses import dataclass, asdict
from utils import not_none
from typing import Union, Optional
import csv

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


# ============================================================
# ====================== MAIN ================================
# ============================================================

parser = argparse.ArgumentParser()

parser.add_argument('-a', '--all', action='store_true',
                    help='Refresh user data from Warpcast, Searchcaster, and ENSData')
parser.add_argument('--farcaster', action='store_true',
                    help='Refresh user data from Warpcast and Searchcaster')
parser.add_argument('--ens', action='store_true',
                    help='Refresh user data from Ensdata')

args = parser.parse_args()


def fix_user_types(user: UserClass) -> UserClass:
    user.fid = int(user.fid)
    user.verified = bool(user.verified)
    user.follower_count = int(user.follower_count)
    user.following_count = int(user.following_count)
    user.registered_at = int(
        user.registered_at) if user.registered_at else None
    return user


def set_searchcaster_data(user: UserClass, data: list[SearchcasterDataClass]) -> UserClass:
    if len(data) == 0:
        return user

    for d in data:
        if user.fid == d.fid:
            user.external_address = d.external_address
            user.farcaster_address = d.farcaster_address
            user.registered_at = d.registered_at
            break

    return user


if args.farcaster:
    users_filename = 'users.csv'
    if os.path.exists(users_filename):
        with open(users_filename, mode='r') as csv_file:
            reader = csv.reader(csv_file)
            next(reader)
            users = [UserClass(*u) for u in list(reader)]

        # Fix types of UserClass attributes
        users = [fix_user_types(u) for u in users]

        batch_size = 2
        start_index = 0
        end_index = batch_size
        while start_index < len(users):
            current_users = users[start_index:end_index]
            if len(current_users) == 0:
                break

            usernames = [u.username for u in current_users]
            searchcaster_users = asyncio.run(
                get_users_from_searchcaster(usernames))
            searchcaster_data = [extract_searchcaster_user_data(
                u) for u in searchcaster_users]

            current_users = [set_searchcaster_data(
                u, searchcaster_data) for u in current_users]
            engine = create_engine(os.getenv('PLANETSCALE_URL'))
            with sessionmaker(bind=engine)() as session:
                user_models = list(
                    map(lambda u: User(**asdict(u)), current_users))
                session.bulk_save_objects(user_models)
                session.commit()

            # open the CSV file for reading and writing
            with open(users_filename, mode='r') as csv_file:
                reader = csv.DictReader(csv_file)
                fieldnames = reader.fieldnames

                # filter out the current_users and get the remaining users
                remaining_users = [row for row in reader if row['username'] not in [
                    u.username for u in current_users]]

            # open the CSV file for writing and write the remaining users
            with open(users_filename, mode='w', newline='') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(remaining_users)

            start_index = end_index
            end_index += batch_size

    else:
        all_users = get_users_from_warpcast(warpcast_hub_key)
        all_users = all_users['users']
        # all_users = get_all_users_from_warpcast(warpcast_hub_key)
        warpcast_data = [extract_warpcast_user_data(u) for u in all_users]
        users = [UserClass(**data) for data in warpcast_data]

        warpcast_locations = [get_warpcast_location(u) for u in all_users]

        engine = create_engine(os.getenv('PLANETSCALE_URL'), echo=True)
        with sessionmaker(bind=engine)() as session:
            all_fids_in_db = [u.fid for u in session.query(User).all()]
            users = [u for u in users if u.fid not in all_fids_in_db]

            # filter out locations that are already in the database,
            # then save to db
            db_locations = session.query(Location).all()
            locations = [l for l in warpcast_locations if l and l.place_id not in [
                db_l.place_id for db_l in db_locations]]
            session.bulk_save_objects(locations)

        with open(users_filename, mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(UserClass.__annotations__.keys())
            for user in users:
                writer.writerow(asdict(user).values())

if args.ens:
    pass
