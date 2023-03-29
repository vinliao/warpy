from sqlalchemy.orm import aliased
from dotenv import load_dotenv
import os
import requests
import time
import asyncio
import aiohttp
from models import User, Location, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


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
        farcaster_address=None,  # Update this value as needed
        registered_at=-1  # Update this value as needed
    )

    return user_data, location if location.id else None


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
    session = sessionmaker(bind=engine)()

    # Read data from the user table and filter rows where registered_at is -1
    unregistered_users = session.query(
        User).filter(User.registered_at == -1).all()

    # Get usernames from unregistered_users
    unregistered_usernames = [user.username for user in unregistered_users]

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
                update_data.append({
                    'fid': user_data['fid'],
                    'farcaster_address': user_data['farcaster_address'],
                    'external_address': user_data['external_address'],
                    'registered_at': user_data['registered_at'],
                })

            session.bulk_update_mappings(User, update_data)
            session.commit()

    session.close()


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


async def main():
    warpcast_users = get_all_users_from_warpcast(warpcast_hub_key)

    warpcast_user_data = [extract_warpcast_user_data(
        user) for user in warpcast_users]

    # Extract the user_data, user_extra_data, and location lists
    user_list = [data[0] for data in warpcast_user_data]
    location_list = [data[1]
                     for data in warpcast_user_data if data[1]]

    # # filter dupliate and remove None for locations
    location_list = list(
        {location.id: location for location in location_list}.values())

    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    db_path = os.path.join(parent_dir, 'datasets', 'datasets.db')
    engine = create_engine('sqlite:///' + db_path)

    create_tables(engine)
    save_bulk_data(engine, user_list, location_list)
    await update_unregistered_users(engine)


if __name__ == '__main__':
    asyncio.run(main())
