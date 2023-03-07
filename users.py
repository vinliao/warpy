from dotenv import load_dotenv
import os
import requests
from sqlalchemy.orm import sessionmaker
from models import Base, User, Location
from sqlalchemy import create_engine
import time

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


def insert_users_to_db(engine, start_cursor=None):
    Session = sessionmaker(bind=engine)
    session = Session()

    cursor = start_cursor
    while True:
        data = get_recent_users(cursor=cursor)
        users = data["users"]
        cursor = data.get("cursor")

        for user in users:
            u = User(
                fid=user['fid'],
                username=user['username'],
                display_name=user['displayName'],
                verified=user['pfp']['verified'] if 'pfp' in user else 0,
                pfp_url=user['pfp']['url'] if 'pfp' in user else '',
                follower_count=user['followerCount'],
                following_count=user['followingCount'],
                bio_text=user['profile']['bio']['text'] if 'bio' in user['profile'] else None,
                registered_at=-1,  # will fetch this data from searchcaster
                farcaster_address='',  # will fetch this data from searchcaster
            )

            if 'location' in user['profile']:
                place_id = user['profile']['location'].get('placeId')
                if place_id:
                    l = session.query(Location).filter_by(
                        place_id=place_id).first()
                    if not l:
                        l = Location(place_id=place_id,
                                     description=user['profile']['location']['description'])
                        session.merge(l)

                    u.location = l

            session.merge(u)
        session.commit()

        if cursor is None:
            break
        else:
            time.sleep(1)  # add a delay to avoid hitting rate limit
            continue

    session.close()


def insert_data_from_searchcaster(engine):
    url = 'https://searchcaster.xyz/api/profiles?username='

    session = sessionmaker(bind=engine)()

    all_usernames = [u.username for u in session.query(
        User).filter_by(registered_at=-1).all()]

    for username in all_usernames:
        result = requests.get(url + username)
        data = result.json()
        item = data[0]

        farcaster_address = item['body']['address']
        external_address = item['connectedAddress']
        registered_at = item['body']['registeredAt']

        # update user

        user = session.query(User).filter_by(username=username).first()
        user.farcaster_address = farcaster_address
        user.external_address = external_address
        user.registered_at = registered_at

        session.merge(user)
        session.commit()

        print(
            f"Updated {username} with {farcaster_address} {external_address} {registered_at}")

        time.sleep(0.5)


def insert_data_from_ensdata(engine):
    url = 'https://ensdata.net/'

    session = sessionmaker(bind=engine)()

    # get all users where ens is null but external address is not null
    all_addresses = [u.external_address for u in session.query(
        User).filter_by(ens=None).filter(User.external_address != None).all()]

    for address in all_addresses:
        result = requests.get(url + address)
        data = result.json()

        if data:
            # user_data = {
            #     'ens': data.get('ens'),
            #     'url': data.get('url'),
            #     'github': data.get('github'),
            #     'twitter': data.get('twitter'),
            #     'telegram': data.get('telegram'),
            #     'email': data.get('email'),
            #     'discord': data.get('discord')
            # }

            user = session.query(User).filter_by(
                external_address=address).first()
            user.ens = data.get('ens')
            user.url = data.get('url')
            user.github = data.get('github')
            user.twitter = data.get('twitter')
            user.telegram = data.get('telegram')
            user.email = data.get('email')
            user.discord = data.get('discord')

            session.merge(user)
            session.commit()

            print(f"Updated {address} with {data}")

        time.sleep(0.5)


def create_schema(engine):
    Base.metadata.create_all(engine)


engine = create_engine('sqlite:///data.db')

# create_schema(engine)
# insert_users_to_db(engine)
insert_data_from_searchcaster(engine)
# insert_data_from_ensdata(engine)
