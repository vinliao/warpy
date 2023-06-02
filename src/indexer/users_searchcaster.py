# TODO: use tenacity

import asyncio
from typing import Any, Dict, List, Optional

import aiohttp
from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from src.app.logger import logger
from src.db.models import User

load_dotenv()


async def fetch_single_user(username: str) -> Optional[Dict[str, Any]]:
    url = f"https://searchcaster.xyz/api/profiles?username={username}"
    logger.info(f"Fetching {url}")
    response_data = await make_async_request_with_retry(url)
    return response_data[0] if response_data else None


async def make_async_request_with_retry(url: str) -> Any:
    retry_limit = 3
    retry_count = 0

    while retry_count < retry_limit:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.json()
        except Exception:
            pass

        retry_count += 1

    return None


async def fetch_data(users: List[User]) -> List[Dict[str, Any]]:
    usernames = [user.username for user in users]
    tasks = [asyncio.create_task(fetch_single_user(username)) for username in usernames]
    return await asyncio.gather(*tasks)


def extract_data(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fid": data["body"]["id"],
        "generated_farcaster_address": data["body"]["address"],
        "address": data["connectedAddress"],
        "registered_at": data["body"]["registeredAt"],
    }


def update_users(users: List[User], json_data: List[Dict[str, Any]]):
    extracted_user_data = [extract_data(user_data) for user_data in json_data]
    user_data_dict = {user_data["fid"]: user_data for user_data in extracted_user_data}

    for user in users:
        user_data = user_data_dict.get(user.fid)
        if user_data:
            user.generated_farcaster_address = user_data["generated_farcaster_address"]
            user.address = user_data["address"]
            user.registered_at = user_data["registered_at"]


async def main(engine: Engine):
    INCOMPLETE_USER_SENTINELS = [0, -1]

    with sessionmaker(bind=engine)() as session:
        incomplete_users = (
            session.query(User)
            .filter(User.registered_at.in_(INCOMPLETE_USER_SENTINELS))
            .all()
        )

        BATCH_SIZE = 50

        for i in range(0, len(incomplete_users), BATCH_SIZE):
            batch_users = incomplete_users[i : i + BATCH_SIZE]

            json_data = await fetch_data(batch_users)
            update_users(batch_users, json_data)
            session.commit()

        # Delete unregistered users after updating the users
        session.query(User).filter(
            User.registered_at.in_(INCOMPLETE_USER_SENTINELS)
        ).delete()
        session.commit()
