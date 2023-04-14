from typing import List

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from utils.fetcher import EnsdataFetcher
from utils.models import ENSData, User
from utils.utils import save_objects


def get_users(session: Session) -> List[User]:
    """Get all users with an external address that is not in the database."""
    return (
        session.query(User)
        .filter(
            User.external_address.isnot(None),
            ~User.external_address.in_(session.query(ENSData.address)),
        )
        .all()
    )


async def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        addresses = [user.external_address for user in get_users(session)]

        fetcher = EnsdataFetcher()
        batch_size = 50
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i : i + batch_size]  # noqa: E203
            data = await fetcher.fetch_data(batch)
            save_objects(session, fetcher.get_models(data))
