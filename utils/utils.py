from typing import List, TypeVar
from sqlalchemy.orm import Session
import datetime
from utils.models import Cast

T = TypeVar('T')


def save_objects(session: Session, objects: List[T]):
    if not objects:
        return

    [session.merge(obj) for obj in objects]
    session.commit()


def save_casts_to_sqlite(session, casts: List[Cast], timestamp: int) -> None:
    TOTAL_DAYS = 3

    time_range_start = timestamp - \
        datetime.timedelta(days=TOTAL_DAYS).total_seconds() * 1000
    existing_hashes = {cast.hash for cast in session.query(Cast.hash).filter(
        Cast.timestamp >= time_range_start).all()}

    new_casts = [cast for cast in casts if cast.hash not in existing_hashes]

    if new_casts:
        session.bulk_save_objects(new_casts)
        session.commit()
