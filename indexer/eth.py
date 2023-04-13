import os

from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from utils.fetcher import AlchemyTransactionFetcher
from utils.models import ExternalAddress
from utils.utils import save_objects

load_dotenv()


async def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        # TODO: figure out a more robust algorithm for this
        addresses = session.query(ExternalAddress).all()
        addresses_string = [address.address for address in addresses]

        alchemy_api_key = os.getenv("ALCHEMY_API_KEY")
        if not alchemy_api_key:
            raise ValueError("Missing ALCHEMY_API_KEY")

        fetcher = AlchemyTransactionFetcher(key=alchemy_api_key)

        batch_size = 5
        for i in range(0, len(addresses_string), batch_size):
            batch = addresses_string[i : i + batch_size]  # noqa: E203

            data = await fetcher.fetch_data(batch)
            transactions = fetcher.get_models(data)
            save_objects(session, transactions)
