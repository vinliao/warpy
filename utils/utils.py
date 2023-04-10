from typing import List, TypeVar
from sqlalchemy.orm import Session

T = TypeVar('T')


def save_objects(session: Session, objects: List[T]):
    if not objects:
        return

    [session.merge(obj) for obj in objects]
    session.commit()
