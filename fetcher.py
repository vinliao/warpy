from dotenv import load_dotenv
import os
import requests
import json
from sqlalchemy.orm import sessionmaker
from models import Base, Cast, User, Reaction, Location, parent_association
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
                bio_text=user['profile']['bio']['text'] if 'bio' in user['profile'] else None
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


def get_recent_casts(cursor: str = None):
    # have cursor in url if cursor exists, use ternary
    url = f"https://api.warpcast.com/v2/recent-casts?cursor={cursor}&limit=1000" if cursor else "https://api.warpcast.com/v2/recent-casts?limit=1000"

    print(f"Fetching from {url}")

    # fetch to url with the bearer token
    result = requests.get(
        url, headers={"Authorization": "Bearer " + warpcast_hub_key})
    json_data = result.json()

    data = {"casts": json_data["result"]['casts'],
            "cursor": json_data["next"]['cursor']}

    return data


def insert_casts_to_db(engine, start_cursor=None):
    Session = sessionmaker(bind=engine)
    session = Session()

    cursor = start_cursor
    while True:
        data = get_recent_casts(cursor=cursor)
        casts = data["casts"]
        cursor = data.get("cursor")

        for cast in casts:
            c = Cast(
                hash=cast['_hashV2'],
                text=cast['text'],
                timestamp=cast['timestamp'],
                author_fid=cast['author']['fid'],
                parent_hash=cast['_parentHashV2'] if '_parentHashV2' in cast else None,
                thread_hash=cast['_threadHashV2'] if '_threadHashV2' in cast else None,
            )

            session.merge(c)

        session.commit()

        one_hash = casts[0]['_hashV2']
        if session.query(Cast).filter_by(hash=one_hash).first():
            break
        elif cursor is None:
            break
        else:
            time.sleep(1)  # add a delay to avoid hitting rate limit
            continue

    session.close()




engine = create_engine('sqlite:///new.db')