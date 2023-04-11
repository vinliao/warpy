import datetime
from typing import List, Optional, TypeVar

from sqlalchemy.orm import Session

from utils.models import Cast, User

T = TypeVar("T")


def save_objects(session: Session, objects: List[T]):
    if not objects:
        return

    [session.merge(obj) for obj in objects]
    session.commit()


def save_casts_to_sqlite(session, casts: List[Cast], timestamp: int) -> None:
    TOTAL_DAYS = 3

    time_range_start = (
        timestamp - datetime.timedelta(days=TOTAL_DAYS).total_seconds() * 1000
    )
    existing_hashes = {
        cast.hash
        for cast in session.query(Cast.hash)
        .filter(Cast.timestamp >= time_range_start)
        .all()
    }

    new_casts = [cast for cast in casts if cast.hash not in existing_hashes]

    if new_casts:
        session.bulk_save_objects(new_casts)
        session.commit()


def get_user_by_fid(session: Session, fid: int) -> Optional[User]:
    return session.query(User).filter_by(fid=fid).one_or_none()


def update_existing_user(
    existing_user: User, user: User, preserved_fields_list: List[str]
) -> None:
    preserved_fields = {
        key: value
        for key, value in user.__dict__.items()
        if key not in preserved_fields_list
    }
    existing_user.__dict__.update(preserved_fields)


def update_users_warpcast(session: Session, user_list: List[User]) -> None:
    preserved_fields_list = [
        "farcaster_address",
        "registered_at",
        "external_address",
        "_sa_instance_state",
    ]

    for user in user_list:
        existing_user = get_user_by_fid(session, user.fid)
        if existing_user:
            update_existing_user(existing_user, user, preserved_fields_list)
        else:
            session.add(user)

    session.commit()
