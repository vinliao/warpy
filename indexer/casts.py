from dotenv import load_dotenv
import os
from sqlalchemy.orm import sessionmaker
from utils.models import Cast
from sqlalchemy.engine import Engine
from utils.fetcher import WarpcastCastFetcher
from utils.utils import save_casts_to_sqlite

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        latest_cast = session.query(Cast).order_by(
            Cast.timestamp.desc()).first()
        latest_timestamp = latest_cast.timestamp if latest_cast else 0

        fetcher = WarpcastCastFetcher(warpcast_hub_key)
        data = fetcher.fetch_data(latest_timestamp)
        save_casts_to_sqlite(
            session, fetcher.get_models(data), latest_timestamp)
