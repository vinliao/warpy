from typing import List
from sqlalchemy.orm import Session
from utils.models import ExternalAddress, User
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from utils.utils import batch_fetcher
from typing import Any


class EnsDataFetcher:
    @staticmethod
    def build_url(address: str, _: Any) -> str:
        return f'https://ensdata.net/{address}'

    @staticmethod
    def process_response(data):
        return ExternalAddress(
            address=data.get('address'),
            ens=data.get('ens'),
            url=data.get('url'),
            github=data.get('github'),
            twitter=data.get('twitter'),
            telegram=data.get('telegram'),
            email=data.get('email'),
            discord=data.get('discord')
        )

    @staticmethod
    def error_handler(address, error):
        print(f"Error occurred for {address}: {error}. Retrying...")

    @staticmethod
    def get_next_page_token(_):
        return None

    @classmethod
    async def get_users_from_ensdata(cls, addresses: List[str]):
        users = await batch_fetcher(
            batch_elements=addresses,
            build_url=cls.build_url,
            process_response=cls.process_response,
            error_handler=cls.error_handler,
            get_next_page_token=cls.get_next_page_token,
        )

        # flatten list, it's 2d because some fetches contain cursor
        # (like eth and reaction batch fetches)
        users = [user for sublist in users for user in sublist]
        return users


def save_users_to_db(session: Session, users: List[ExternalAddress]):
    for user in users:
        session.merge(user)
    session.commit()


async def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        users = session.query(User).filter(
            User.external_address != None,
            ~User.external_address.in_(
                session.query(ExternalAddress.address)
            )
        ).all()
        addresses = [user.external_address for user in users]

        ensdata_fetcher = EnsDataFetcher()
        batch_size = 5
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i:i+batch_size]
            users = await ensdata_fetcher.get_users_from_ensdata(batch)
            save_users_to_db(session, users)
