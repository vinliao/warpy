import os
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from utils.models import ExternalAddress
from sqlalchemy import func
from sqlalchemy.engine import Engine
from utils.fetcher import AlchemyTransactionFetcher
from utils.utils import save_objects

load_dotenv()


async def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        # get three random external addresses
        addresses = (
            session.query(ExternalAddress).order_by(func.random()).limit(3).all()
        )

        addresses_string = [address.address for address in addresses]

        fetcher = AlchemyTransactionFetcher(key=os.getenv("ALCHEMY_API_KEY"))
        data = await fetcher.fetch_data(addresses_string)
        transactions = fetcher.get_models(data)
        save_objects(session, transactions)
