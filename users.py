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
from dataclasses import dataclass
from utils import not_none
from typing import Union

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


@dataclass(frozen=True)
class WarpcastUserClass:
    fid: int
    username: str
    display_name: str
    verified: bool
    pfp_url: str
    follower_count: int
    following_count: int
    location_place_id: Union[str, None]
    bio_text: str


@dataclass(frozen=True)
class SearchcasterUserClass:
    fid: int
    farcaster_address: str
    external_address: str
    registered_at: int


@dataclass(frozen=True)
class EnsdataUserClass:
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
    address: str = None
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

    url = f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit=1000" if cursor else "https://api.warpcast.com/v2/recent-users?limit=10"

    print(f"Fetching from {url}")

    # fetch to url with the bearer token
    result = requests.get(
        url, headers={"Authorization": "Bearer " + key})
    json_data = result.json()

    # cursor may be empty here so handle it
    return {"users": json_data["result"]['users'],
            "cursor": json_data.get("next", {}).get('cursor') if json_data.get("next") else None}


def refresh_user_data_warpcast(engine: Engine, key: str, start_cursor: str = None):
    with sessionmaker(bind=engine)() as session:
        cursor = start_cursor
        while True:
            data = get_users_from_warpcast(key, cursor)
            users = data["users"]
            cursor = data.get("cursor")

            for user in users:
                u = User(**extract_warpcast_user_data(user))
                location = get_location(session, user)
                u.location = location
                session.merge(u)
            session.commit()

            if cursor is None:
                break
            else:
                time.sleep(1)  # add a delay to avoid hitting rate limit
                continue


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
        'verified': user['pfp']['verified'] if 'pfp' in user else 0,
        'pfp_url': user['pfp']['url'] if 'pfp' in user else '',
        'follower_count': user['followerCount'],
        'following_count': user['followingCount'],
        'bio_text': user['profile']['bio']['text'] if 'bio' in user['profile'] else None,
        'location_place_id': None  # figure it out later
    }


def update_user_data(user, data):
    for attr, value in data.items():
        setattr(user, attr, value)
    return user


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


def extract_searchcaster_user_data(raw_data):
    return {
        'fid': raw_data['body']['id'],
        'farcaster_address': raw_data['body']['address'],
        'external_address': raw_data['connectedAddress'],
        'registered_at': raw_data['body']['registeredAt']
    }


# def refresh_user_data_searchcaster(engine):
#     url = 'https://searchcaster.xyz/api/profiles?username='

#     with sessionmaker(bind=engine)() as session:
#         all_usernames = [u.username for u in session.query(
#             User).filter(User.registered_at == None).all()]

#         for username in all_usernames:
#             success = False  # flag to indicate whether the update was successful
#             while not success:
#                 try:
#                     result = requests.get(url + username, timeout=10)
#                     data = result.json()
#                     item = data[0]

#                     user = session.query(User).filter_by(
#                         username=username).first()
#                     user = update_user_data(
#                         user, extract_searchcaster_user_data(item))
#                     session.merge(user)
#                     session.commit()

#                     print(f"Updated {username} with {data}")

#                     success = True  # set flag to indicate success

#                 except requests.exceptions.Timeout:
#                     print(f"Request timed out for {username}. Retrying...")
#                     continue


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


def make_ensdata_fids(engine):
    with sessionmaker(bind=engine)() as session:
        all_fids = [u.fid for u in session.query(User).filter(
            User.registered_at == None).all()]

        if len(all_fids) == 0:
            all_fids = [u.fid for u in session.query(User).filter(
                and_(User.external_address != None, User.ens == None)).all()]

        with open('ensdata_fids.csv', 'w') as f:
            f.write(','.join(str(fid) for fid in all_fids))

        return all_fids


def get_ensdata_fids():
    with open('ensdata_fids.csv', 'r') as f:
        all_fids = f.read().split(',')
        all_fids = [int(fid.strip()) for fid in all_fids]
    return all_fids


def extract_ensdata_user_data(raw_data):
    return {
        'address': raw_data.get('address'),
        'ens': raw_data.get('ens'),
        'url': raw_data.get('url'),
        'github': raw_data.get('github'),
        'twitter': raw_data.get('twitter'),
        'telegram': raw_data.get('telegram'),
        'email': raw_data.get('email'),
        'discord': raw_data.get('discord')
    }


# def refresh_user_data_ensdata(engine):
#     url = 'https://ensdata.net/'

#     with sessionmaker(bind=engine)() as session:
#         if os.path.exists('ensdata_fids.csv'):
#             all_fids = get_ensdata_fids()
#         else:
#             all_fids = make_ensdata_fids()

#         all_addresses = [u.external_address for u in session.query(User).filter(
#             User.fid.in_(all_fids), User.external_address.isnot(None)).all()]

#         async def fetch(session, address):
#             try:
#                 async with session.get(url + address, timeout=10) as response:
#                     return await response.json()
#             except:
#                 print(f"Timeout: {address}")
#                 return None

#         async def run(addresses):
#             tasks = []
#             async with aiohttp.ClientSession() as session:
#                 for address in addresses:
#                     task = asyncio.ensure_future(fetch(session, address))
#                     tasks.append(task)

#                 responses = await asyncio.gather(*tasks)
#                 return [r for r in responses if r is not None]

#         def update_user(address, data):
#             user = session.query(User).filter_by(
#                 external_address=address).first()
#             user = update_user_data(user, extract_ensdata_user_data(data))
#             session.merge(user)
#             session.commit()

#         def remove_fid_from_csv(fid: str):
#             if os.path.exists('ensdata_fids.csv'):
#                 with open('ensdata_fids.csv', 'r') as f:
#                     all_fids = f.read().split(',')
#                     all_fids = [fid.strip() for fid in all_fids]
#                 if fid in all_fids:
#                     print(
#                         f"{len(all_fids)} fids left to process...")
#                     all_fids.remove(fid)
#                     with open('ensdata_fids.csv', 'w') as f:
#                         f.write(','.join(all_fids))

#         batch_size = 50
#         for i in range(0, len(all_addresses), batch_size):
#             batch = all_addresses[i:i + batch_size]
#             print(f"Processing batch {i} to {i + batch_size}...")
#             loop = asyncio.get_event_loop()
#             future = asyncio.ensure_future(run(batch))
#             responses = loop.run_until_complete(future)

#             for response in responses:
#                 current_address = response.get('address')
#                 if current_address:
#                     update_user(current_address, response)
#                     fid = session.query(User).filter_by(
#                         external_address=current_address).first().fid
#                     remove_fid_from_csv(str(fid))

#             time.sleep(1)


parser = argparse.ArgumentParser()

parser.add_argument('-a', '--all', action='store_true',
                    help='Refresh user data from Warpcast, Searchcaster, and ENSData')
parser.add_argument('--test', action='store_true',
                    help='For testing purposes')
parser.add_argument('--test2', action='store_true',
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
        map(lambda data: SearchcasterUserClass(**data), searchcaster_data))
    print(registered_ats)

    addresses = filter(not_none, [u.external_address for u in registered_ats])
    print(addresses)

    ensdata_users = asyncio.run(get_users_from_ensdata(addresses))
    ensdata_data = list(map(extract_ensdata_user_data, ensdata_users))
    ens = list(map(lambda data: EnsdataUserClass(**data), ensdata_data))

    print(ens)

    # make user data class, some users may not have ensdata data or searchcaster data,
    # so make sure the fid in searchcaster matches the fid in users, and the
    # address in ensdata matches the external_address in users

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

    print(users)

    # insert this into db

    # todo: somehow combine all these things, then insert to db
    # do all these in batch of 50, insert to db, then sleep for 1 second
