from sqlalchemy.orm import aliased
from dotenv import load_dotenv
import os
import requests
import time
from utils.models import User, Location, Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from utils.fetcher import WarpcastUserFetcher, SearchcasterFetcher

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


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

    user_data = User(
        fid=user['fid'],
        username=user['username'],
        display_name=user['displayName'],
        pfp_url=user['pfp']['url'] if 'pfp' in user else '',
        bio_text=user.get('profile', {}).get('bio', {}).get('text', ''),
        following_count=user.get('followingCount', 0),
        follower_count=user.get('followerCount', 0),
        location_id=location.id,
        verified=int(user['pfp']['verified']
                     ) if 'pfp' in user and 'verified' in user['pfp'] else 0,
        farcaster_address="",  # Update this value as needed
        registered_at=-1  # Update this value as needed
    )

    return user_data, location if location.id else None


# # New function to get users from Searchcaster using the SearchcasterFetcher class
# async def get_users_from_searchcaster(usernames: List[str]):
#     base_url = 'https://searchcaster.xyz/api/profiles'
#     fetcher = SearchcasterFetcher(base_url)
#     results = await fetcher.fetch(usernames)
#     return results.values()


def extract_searchcaster_user_data(data):
    return {
        'fid': data['body']['id'],
        'farcaster_address': data['body']['address'],
        'external_address': data['connectedAddress'],
        'registered_at': data['body']['registeredAt']
    }


async def update_unregistered_users(engine):
    with sessionmaker(bind=engine)() as session:
        # Read data from the user table and filter rows where registered_at is -1
        unregistered_users = session.query(
            User).filter(User.registered_at == -1).all()

        # Get usernames from unregistered_users
        unregistered_usernames = [user.username for user in unregistered_users]
        print(unregistered_usernames)

        if unregistered_usernames:
            batch_size = 50

            for i in range(0, len(unregistered_usernames), batch_size):
                batch_usernames = unregistered_usernames[i:i + batch_size]
                searchcaster_users = await get_users_from_searchcaster(batch_usernames)
                searchcaster_user_data = [extract_searchcaster_user_data(
                    user) for user in searchcaster_users]

                # Bulk update User table with searchcaster_user_data
                update_data = []
                for user_data in searchcaster_user_data:
                    # if farcaster address is not None
                    # if farcaster address is not None and registered_at is not 0
                    if user_data['farcaster_address'] is not None and user_data['registered_at'] != 0:
                        update_data.append({
                            'fid': user_data['fid'],
                            'farcaster_address': user_data['farcaster_address'],
                            'external_address': user_data['external_address'],
                            'registered_at': user_data['registered_at'],
                        })

                session.bulk_update_mappings(User, update_data)
                session.commit()


def delete_unregistered_users(engine):
    # delete users where registered_at is -1
    # -1 means the user didn't register properly, need better way to handle this
    with sessionmaker(bind=engine)() as session:
        session.query(User).filter(User.registered_at == -1).delete()
        session.commit()


def create_tables(engine):
    Base.metadata.create_all(engine)


def save_bulk_data(engine, user_list, location_list):
    with sessionmaker(bind=engine)() as session:
        for location in location_list:
            session.merge(location)
        session.commit()

        for user in user_list:
            # Create an aliased version of the User model to prevent field overwrites
            user_aliased = aliased(user.__class__)

            # Retrieve existing user from the database, if any
            existing_user = session.query(user_aliased).filter_by(
                fid=user.fid).one_or_none()

            if existing_user:
                # Update the existing user while preserving the specified fields
                for attr, value in user.__dict__.items():
                    if attr not in ['farcaster_address', 'registered_at', 'external_address', '_sa_instance_state']:
                        setattr(existing_user, attr, value)
            else:
                # If user is not in the database, add it
                session.add(user)

        session.commit()


async def main(engine: Engine):
    warpcast_user_fetcher = WarpcastUserFetcher(warpcast_hub_key)
    warpcast_users = warpcast_user_fetcher.fetch_all_data()
    user_list, location_list = warpcast_user_fetcher.process_users(
        warpcast_users)

    create_tables(engine)
    save_bulk_data(engine, user_list, location_list)

    # # with sessionmaker(bind=engine)() as session:
    # #     new_users = session.query(User).filter(
    # #         User.registered_at == -1).all()
    # #     new_usernames = [user.username for user in new_users]

    # #     batch_size = 50
    # #     for i in range(0, len(new_usernames), batch_size):
    # #         batch_usernames = new_usernames[i:i + batch_size]

    # #         # continue

    # searchcaster_fetcher = SearchcasterFetcher()
    # searchcaster_data = await searchcaster_fetcher.fetch_users(
    #     ['farcaster', 'v', 'dwr'])
    # users = searchcaster_fetcher.process_user_data(searchcaster_data)
    # print(users)
    # # print(searchcaster_data)
