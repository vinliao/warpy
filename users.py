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
from typing import Union
import csv

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


@dataclass(frozen=True)
class SearchcasterDataClass:
    fid: int
    farcaster_address: str
    external_address: str
    registered_at: int


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
    ens: str = None
    url: str = None
    github: str = None
    twitter: str = None
    discord: str = None
    email: str = None
    telegram: str = None

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


def get_location(session: Session, user: User):
    if 'location' in user['profile']:
        place_id = user['profile']['location'].get('placeId')
        if place_id:
            l = session.query(Location).filter_by(
                place_id=place_id).first()
            if not l:
                l = Location(place_id=place_id,
                             description=user['profile']['location']['description'])
                session.merge(l)

            return l

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
        'location_place_id': None  # figure it out later
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
parser.add_argument('--test', action='store_true',
                    help='For testing purposes')
parser.add_argument('--test2', action='store_true',
                    help='For testing purposes')
parser.add_argument('--test3', action='store_true',
                    help='For testing purposes')
parser.add_argument('--test4', action='store_true',
                    help='For testing purposes')

args = parser.parse_args()

# if args.all or len(sys.argv) == 1:
#     print("Fetching new users from Warpcast, ENSData, and Searchcaster, and updating the DB...")
#     engine = create_engine('sqlite:///data.db')

#     # warpcast adds new users (new fids)
#     # searchcaster adds registered_at, external_address, and farcaster_address
#     # ensdata adds ens, url, github, twitter, telegram, email, and discord
#     refresh_user_data_warpcast(engine, warpcast_hub_key)
#     make_ensdata_fids(engine)
#     refresh_user_data_searchcaster(engine)
#     refresh_user_data_ensdata(engine)


def no_registered_at(u):
    return u.registered_at is None


if args.test:
    warpcast_users = get_users_from_warpcast(warpcast_hub_key)
    warpcast_data = list(
        map(extract_warpcast_user_data, warpcast_users['users']))
    users = list(
        map(lambda data: UserClass(**data), warpcast_data))

    usernames = [u.username for u in users]
    searchcaster_users = asyncio.run(get_users_from_searchcaster(usernames))
    searchcaster_data = list(
        map(extract_searchcaster_user_data, searchcaster_users))
    registered_ats = list(
        map(lambda data: SearchcasterDataClass(**data), searchcaster_data))
    print(registered_ats)

    addresses = filter(not_none, [u.external_address for u in registered_ats])
    print(addresses)

    ensdata_users = asyncio.run(get_users_from_ensdata(addresses))
    ensdata_data = list(map(extract_ensdata_user_data, ensdata_users))
    ens = list(map(lambda data: EnsdataDataClass(**data), ensdata_data))

    print(ens)

    for user in users:
        for searchcaster_user in registered_ats:
            if user.fid == searchcaster_user.fid:
                user.external_address = searchcaster_user.external_address
                user.farcaster_address = searchcaster_user.farcaster_address
                user.registered_at = searchcaster_user.registered_at
                break

        for ensdata_user in ens:
            if user.external_address == ensdata_user.address:
                user.ens = ensdata_user.ens
                user.url = ensdata_user.url
                user.github = ensdata_user.github
                user.twitter = ensdata_user.twitter
                user.telegram = ensdata_user.telegram
                user.email = ensdata_user.email
                user.discord = ensdata_user.discord
                break

    users = list(filter(no_registered_at, users))
    print(users)

    # insert this into db

    # todo: somehow combine all these things, then insert to db
    # do all these in batch of 50, insert to db, then sleep for 1 second


if args.test2:
    users_filename = 'users.csv'
    if os.path.exists(users_filename):
        with open(users_filename, mode='r') as csv_file:
            reader = csv.reader(csv_file)
            next(reader)
            users = [UserClass(*u) for u in list(reader)]

        # Fix types of UserClass attributes
        for user in users:
            user.fid = int(user.fid)
            user.verified = bool(user.verified)
            user.follower_count = int(user.follower_count)
            user.following_count = int(user.following_count)
            user.registered_at = int(
                user.registered_at) if user.registered_at else None

        # loop through users in batches of 50
        while True:
            current_users = users[:3]
            if len(current_users) == 0:
                break

            usernames = [u.username for u in current_users]
            searchcaster_users = asyncio.run(
                get_users_from_searchcaster(usernames))
            searchcaster_data = list(
                map(extract_searchcaster_user_data, searchcaster_users))

            addresses = filter(
                not_none, [u.external_address for u in searchcaster_data])
            ensdata_users = asyncio.run(get_users_from_ensdata(addresses))
            ensdata_data = list(
                map(extract_ensdata_user_data, ensdata_users))

            # seems like setting it like this is not setting the value properly
            for user in current_users:
                for searchcaster_user in searchcaster_data:
                    if user.fid == searchcaster_user.fid:
                        print(searchcaster_user.external_address)
                        user.external_address = searchcaster_user.external_address
                        user.farcaster_address = searchcaster_user.farcaster_address
                        user.registered_at = searchcaster_user.registered_at
                        break

                for ensdata_user in ensdata_data:
                    if user.external_address == ensdata_user.address:
                        user.ens = ensdata_user.ens
                        user.url = ensdata_user.url
                        user.github = ensdata_user.github
                        user.twitter = ensdata_user.twitter
                        user.telegram = ensdata_user.telegram
                        user.email = ensdata_user.email
                        user.discord = ensdata_user.discord
                        break

            engine = create_engine(os.getenv('PLANETSCALE_URL'))
            with sessionmaker(bind=engine)() as session:
                user_models = list(
                    map(lambda u: User(**asdict(u)), current_users))
                session.bulk_save_objects(user_models)
                session.commit()

            break

    else:
        all_users = get_all_users_from_warpcast(warpcast_hub_key)
        warpcast_data = list(
            map(extract_warpcast_user_data, all_users))
        users = list(
            map(lambda data: UserClass(**data), warpcast_data))

        engine = create_engine(os.getenv('PLANETSCALE_URL'))
        with sessionmaker(bind=engine)() as session:
            all_fids_in_db = [u.fid for u in session.query(User).all()]
            users = list(filter(lambda u: u.fid not in all_fids_in_db, users))

        with open(users_filename, mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(UserClass.__annotations__.keys())
            for user in users:
                writer.writerow(asdict(user).values())
