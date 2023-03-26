from dotenv import load_dotenv
import os
import requests
from models import Location
import time
import asyncio
import aiohttp
from dataclasses import dataclass
import pandas as pd
from typing import List, Optional
import sys

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    fid = Column(Integer, primary_key=True)
    username = Column(String)
    display_name = Column(String)
    pfp_url = Column(String)
    bio_text = Column(String)

    user_extra = relationship("UserExtra", back_populates="user")


class UserExtra(Base):
    __tablename__ = 'user_extra'
    fid = Column(Integer, ForeignKey('users.fid'), primary_key=True)
    following_count = Column(Integer)
    follower_count = Column(Integer)
    location_id = Column(String, ForeignKey('locations.id'), nullable=True)
    verified = Column(Boolean)
    farcaster_address = Column(String)
    external_address = Column(String, nullable=True)
    registered_at = Column(Integer)

    user = relationship("User", back_populates="user_extra")
    location = relationship("Location", back_populates="user_extras")


class Location(Base):
    __tablename__ = 'locations'
    id = Column(String, primary_key=True)
    description = Column(String)

    user_extras = relationship("UserExtra", back_populates="location")


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
    location = Location(
        id=location_data.get('placeId', ''),
        description=location_data.get('description', '')
    )

    user_extra_data = UserExtra(
        fid=user['fid'],
        following_count=user.get('followingCount', 0),
        follower_count=user.get('followerCount', 0),
        location_id=location.id,
        verified=user['pfp']['verified'] if 'pfp' in user else False,
        farcaster_address=None,  # Update this value as needed
        registered_at=-1  # Update this value as needed
    )

    user_data = User(
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
            except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                print(f"Error occurred for {username}: {e}. Retrying...")
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying


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


async def update_unregistered_users(engine):
    with sessionmaker(bind=engine)() as session:
        # Read data from the user_extra table and filter rows where registered_at is -1
        users_extra_df = pd.read_sql(
            session.query(UserExtra).statement, session.bind)
        unregistered_users = users_extra_df[users_extra_df['registered_at'] == -1]

        # Get fids from unregistered_users, then get the usernames from the users table
        unregistered_fids = unregistered_users['fid'].tolist()
        users_df = pd.read_sql(session.query(User).statement, session.bind)

        unregistered_usernames = users_df[users_df['fid'].isin(
            unregistered_fids)]['username'].tolist()

        if len(unregistered_usernames) > 0:
            batch_size = 50

            for i in range(0, len(unregistered_usernames), batch_size):
                # Read the dataframe from the user_extra table
                users_extra_df = pd.read_sql(
                    session.query(UserExtra).statement, session.bind)
                updated_users_extra_df = users_extra_df.set_index('fid')

                batch_usernames = unregistered_usernames[i:i + batch_size]
                searchcaster_users = await get_users_from_searchcaster(batch_usernames)
                searchcaster_user_data = [extract_searchcaster_user_data(
                    user) for user in searchcaster_users]

                # Create a Pandas DataFrame from the searchcaster user data
                searchcaster_user_data_df = pd.DataFrame(
                    searchcaster_user_data)

                # Update updated_users_extra_df with data from searchcaster_user_data_df
                updated_users_extra_df.update(
                    searchcaster_user_data_df.set_index('fid'))

                # Reset the index and update the user_extra table
                updated_users_extra_df.reset_index(inplace=True)

                # Save updated user_extra data to the database
                with sessionmaker(bind=engine)() as session:
                    for idx, row in updated_users_extra_df.iterrows():
                        user_extra = session.query(UserExtra).get(row['fid'])
                        user_extra.following_count = row['following_count']
                        user_extra.follower_count = row['follower_count']
                        user_extra.location_id = row['location_id']
                        user_extra.verified = row['verified']
                        user_extra.farcaster_address = row['farcaster_address']
                        user_extra.external_address = row['external_address']
                        user_extra.registered_at = row['registered_at']

                    session.commit()


def create_tables():
    engine = create_engine('sqlite:///datasets/datasets.db')
    Base.metadata.create_all(engine)


def save_data_to_db(engine, all_data):
    with sessionmaker(bind=engine)() as session:
        for data_list in all_data:
            session.bulk_save_objects(data_list)
        session.commit()


async def main():
    if '--extra' in sys.argv:
        engine = create_engine('sqlite:///datasets/datasets.db')
        await update_unregistered_users(engine)
    else:
        # warpcast_users = get_all_users_from_warpcast(warpcast_hub_key)
        warpcast_users = get_users_from_warpcast(
            warpcast_hub_key, None, 50)['users']

        warpcast_user_data = [extract_warpcast_user_data(
            user) for user in warpcast_users]

        # Extract the user_data, user_extra_data, and location lists
        user_data_list = [data[0] for data in warpcast_user_data]
        user_extra_data_list = [data[1] for data in warpcast_user_data]
        location_list = [data[2]
                         for data in warpcast_user_data if data[2]]

        # # filter dupliate and remove None for locations
        location_list = list(
            {location.id: location for location in location_list}.values())

        create_tables()

        engine = create_engine('sqlite:///datasets/datasets.db')
        save_data_to_db(engine, zip(
            user_data_list, user_extra_data_list, location_list))


if __name__ == '__main__':
    asyncio.run(main())
