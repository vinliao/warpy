from typing import List
from sqlalchemy.orm import Session
from utils.models import ExternalAddress, User
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from utils.fetcher import EnsdataFetcher
from utils.utils import save_objects


def get_farcaster_users(session: Session) -> List[User]:
    return session.query(User).filter(
        User.external_address != None,
        ~User.external_address.in_(
            session.query(ExternalAddress.address)
        )
    ).all()


async def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        addresses = [
            user.external_address for user in get_farcaster_users(session)]

        fetcher = EnsdataFetcher()
        batch_size = 50
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i:i+batch_size]
            data = await fetcher.fetch_data(batch)
            save_objects(session, fetcher.get_models(data))
