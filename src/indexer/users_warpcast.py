import os
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.app.logger import logger
from src.db.models import Location, User

load_dotenv()


def fetch_data(key: str, partial: bool = False):
    users = []
    cursor = None

    while True:
        batch_data, cursor = fetch_batch(key, cursor, limit=52 if partial else 1000)
        users.extend(batch_data)
        if partial or cursor is None:
            break
        else:
            time.sleep(1)
    return users


def fetch_batch(
    key: str, cursor: Optional[str] = None, limit: int = 1000
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    url = (
        f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit={limit}"
        if cursor
        else f"https://api.warpcast.com/v2/recent-users?limit={limit}"
    )
    logger.info(f"Fetching {url}")
    json_data = make_request(url, headers={"Authorization": "Bearer " + key})
    return (
        json_data["result"]["users"],
        json_data.get("next", {}).get("cursor") if json_data.get("next") else None,
    )


def make_request(url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def extract_data(user: Dict[str, Any]) -> Tuple[User, Optional[Location]]:
    location_data = user.get("profile", {}).get("location", {})
    location = None
    if location_data.get("placeId"):
        location = Location(
            id=location_data["placeId"],
            description=location_data["description"],
        )

    user_data = User(
        fid=user["fid"],
        username=user["username"],
        display_name=user["displayName"],
        pfp_url=user.get("pfp", {}).get("url", ""),
        bio_text=user.get("profile", {}).get("bio", {}).get("text", ""),
        following_count=user.get("followingCount", 0),
        follower_count=user.get("followerCount", 0),
        location_id=location.id if location else None,
        verified=user.get("pfp", {}).get("verified", False),
        generated_farcaster_address="",  # Update this value as needed
        registered_at=-1,  # Update this value as needed
    )

    return user_data, location


def modelify(users: List[Dict[str, Any]]) -> List[Union[User, Location]]:
    user_and_location_data = [extract_data(user) for user in users]
    user_and_location_list = [
        item for sublist in user_and_location_data for item in sublist if item
    ]

    # Filter duplicates for locations
    location_dict = {}
    mixed_list = []

    for item in user_and_location_list:
        if isinstance(item, Location):
            if item.id not in location_dict:
                location_dict[item.id] = item
                mixed_list.append(item)
        else:
            mixed_list.append(item)

    return mixed_list


def update_users_warpcast(
    session: Session,
    db_users: List[User],
    new_users: List[User],
) -> None:
    preserved_fields_list = [
        "farcaster_address",
        "registered_at",
        "external_address",
        "_sa_instance_state",
    ]

    db_users_dict = {user.fid: user for user in db_users}

    for new_user in new_users:
        existing_user = db_users_dict.get(new_user.fid)
        if existing_user:
            update_existing_user(existing_user, new_user, preserved_fields_list)
            # No need to "update" because users is a part of the session
        else:
            session.add(new_user)
    session.commit()


def update_existing_user(
    existing_user: User, new_user: User, preserved_fields_list: List[str]
) -> None:
    preserved_fields = {
        key: value
        for key, value in new_user.__dict__.items()
        if key not in preserved_fields_list
    }
    existing_user.__dict__.update(preserved_fields)


def main(engine: Engine):
    WARPCAST_HUB_KEY = os.getenv("WARPCAST_HUB_KEY")
    if not WARPCAST_HUB_KEY:
        raise ValueError("WARPCAST_HUB_KEY is not set")
    users = fetch_data(WARPCAST_HUB_KEY, False)
    models = modelify(users=users)
    location_models = [model for model in models if isinstance(model, Location)]

    with sessionmaker(bind=engine)() as session:
        session.bulk_save_objects(location_models)
        session.commit()

        users_in_db = session.query(User).all()
        user_models = [model for model in models if isinstance(model, User)]
        update_users_warpcast(session, users_in_db, user_models)
