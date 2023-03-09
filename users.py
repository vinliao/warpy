from dotenv import load_dotenv
import os
import requests
from sqlalchemy.orm import sessionmaker
from models import Base, User, Location
from sqlalchemy import create_engine
import time
import re
import asyncio
import aiohttp

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


def get_recent_users(cursor: str = None):
    # have cursor in url if cursor exists, use ternary
    url = f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit=1000" if cursor else "https://api.warpcast.com/v2/recent-users?limit=1000"

    print(f"Fetching from {url}")

    # fetch to url with the bearer token
    result = requests.get(
        url, headers={"Authorization": "Bearer " + warpcast_hub_key})
    json_data = result.json()

    # cursor may be empty here so handle it
    return {"users": json_data["result"]['users'],
            "cursor": json_data.get("next", {}).get('cursor') if json_data.get("next") else None}


def get_location(session, user):
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


def get_warpcast_data(user):
    return {
        'fid': user['fid'],
        'username': user['username'],
        'display_name': user['displayName'],
        'verified': user['pfp']['verified'] if 'pfp' in user else 0,
        'pfp_url': user['pfp']['url'] if 'pfp' in user else '',
        'follower_count': user['followerCount'],
        'following_count': user['followingCount'],
        'bio_text': user['profile']['bio']['text'] if 'bio' in user['profile'] else None,
    }


def insert_users_to_db(engine, start_cursor=None):
    Session = sessionmaker(bind=engine)
    session = Session()

    cursor = start_cursor
    while True:
        data = get_recent_users(cursor=cursor)
        users = data["users"]
        cursor = data.get("cursor")

        for user in users:
            u = User(**get_warpcast_data(user))
            location = get_location(session, user)
            u.location = location
            session.merge(u)
        session.commit()

        if cursor is None:
            break
        else:
            time.sleep(1)  # add a delay to avoid hitting rate limit
            continue

    session.close()


def update_user_data(user, data):
    for attr, value in data.items():
        setattr(user, attr, value)
    return user


def get_searchcaster_data(raw_data):
    return {
        'farcaster_address': raw_data['body']['address'],
        'external_address': raw_data['connectedAddress'],
        'registered_at': raw_data['body']['registeredAt']
    }


def get_ensdata_data(raw_data):
    return {
        'ens': raw_data.get('ens'),
        'url': raw_data.get('url'),
        'github': raw_data.get('github'),
        'twitter': raw_data.get('twitter'),
        'telegram': raw_data.get('telegram'),
        'email': raw_data.get('email'),
        'discord': raw_data.get('discord')
    }


def insert_data_from_searchcaster(engine):
    url = 'https://searchcaster.xyz/api/profiles?username='

    session = sessionmaker(bind=engine)()

    all_usernames = [u.username for u in session.query(
        User).filter(User.registered_at == None).all()]

    for username in all_usernames:
        success = False  # flag to indicate whether the update was successful
        while not success:
            try:
                result = requests.get(url + username, timeout=10)
                data = result.json()
                item = data[0]

                user = session.query(User).filter_by(username=username).first()
                user = update_user_data(user, get_searchcaster_data(item))
                session.merge(user)
                session.commit()

                print(f"Updated {username} with {data}")

                success = True  # set flag to indicate success

            except requests.exceptions.Timeout:
                print(f"Request timed out for {username}. Retrying...")
                continue

    session.close()


def insert_data_from_ensdata_async(engine):
    url = 'https://ensdata.net/'

    session = sessionmaker(bind=engine)()

    if os.path.exists('ensdata_addresses.csv'):
        with open('ensdata_addresses.csv', 'r') as f:
            all_addresses = f.read().split(',')
            all_addresses = [address.strip() for address in all_addresses]
    else:
        all_addresses = [u.external_address for u in session.query(
            User).filter(User.external_address != None).all()]
        with open('ensdata_addresses.csv', 'w') as f:
            f.write(','.join(all_addresses))

    async def fetch(session, address):
        try:
            async with session.get(url + address, timeout=10) as response:
                return await response.json()
        except:
            print(f"Timeout: {address}")
            return None

    async def run(addresses):
        tasks = []
        async with aiohttp.ClientSession() as session:
            for address in addresses:
                task = asyncio.ensure_future(fetch(session, address))
                tasks.append(task)

            responses = await asyncio.gather(*tasks)
            return [r for r in responses if r is not None]

    def update_user(address, data):
        user = session.query(User).filter_by(
            external_address=address).first()
        user = update_user_data(user, get_ensdata_data(data))
        session.merge(user)
        session.commit()

    def remove_address_from_csv(address):
        if os.path.exists('ensdata_addresses.csv'):
            with open('ensdata_addresses.csv', 'r') as f:
                all_addresses = f.read().split(',')
                all_addresses = [address.strip()
                                 for address in all_addresses]
            if address in all_addresses:
                print(
                    f"{len(all_addresses)} addresses left to process...")
                all_addresses.remove(address)
                with open('ensdata_addresses.csv', 'w') as f:
                    f.write(','.join(all_addresses))

    batch_size = 50
    for i in range(0, len(all_addresses), batch_size):
        batch = all_addresses[i:i + batch_size]
        print(f"Processing batch {i} to {i + batch_size}...")
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(batch))
        responses = loop.run_until_complete(future)

        for address, data in zip(batch, responses):
            if data:
                update_user(address, data)
                remove_address_from_csv(address)

        time.sleep(1)

    session.close()


def set_more_info_from_bio(engine):
    session = sessionmaker(bind=engine)()
    all_user = session.query(User).all()

    for user in all_user:
        if user.bio_text:
            bio_text = user.bio_text.lower()

            if 'twitter' in bio_text:
                twitter_username = re.search(r'(\S+)\.twitter', bio_text)
                if twitter_username:
                    twitter_username = twitter_username.group(1)
                    twitter_username = re.sub(
                        r'[^\w\s]|_', '', twitter_username)
                    # print(twitter_username)
                    user.twitter = twitter_username

            if 'telegram' in bio_text:
                telegram_username = re.search(r'(\S+)\.telegram', bio_text)
                if telegram_username:
                    telegram_username = telegram_username.group(1)
                    # print(telegram_username)
                    user.telegram = telegram_username

            session.merge(user)
    session.commit()
    session.close()


def create_schema(engine):
    Base.metadata.create_all(engine)


engine = create_engine('sqlite:///data.db')

# create_schema(engine)
# insert_users_to_db(engine)
# insert_data_from_searchcaster(engine)
insert_data_from_ensdata_async(engine)

# todo:
# 1. go through user, if it contains xxx.twitter or yyy.telegram, add it to the respective table
# 2. modularize the code even further
# 3. type safety
# 4. write test (lol)
