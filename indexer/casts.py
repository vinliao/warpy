from typing import List
from datetime import datetime
from dotenv import load_dotenv
import os
import datetime
from typing import List
from sqlalchemy.orm import sessionmaker
from utils.models import Cast
from sqlalchemy.engine import Engine
from utils.fetcher import WarpcastCastFetcher


load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


def dump_casts_to_sqlite(engine, casts: List[Cast], timestamp: int) -> None:
    with sessionmaker(bind=engine)() as session:
        three_days_before_latest_timestamp = timestamp - \
            datetime.timedelta(days=3).total_seconds() * 1000
        existing_hashes = {cast.hash for cast in session.query(Cast.hash).filter(
            Cast.timestamp >= timestamp - three_days_before_latest_timestamp).all()}

        new_casts = [
            cast for cast in casts if cast.hash not in existing_hashes]

        if new_casts:
            session.bulk_save_objects(new_casts)
            session.commit()


def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        latest_cast = session.query(Cast).order_by(
            Cast.timestamp.desc()).first()
        latest_timestamp = latest_cast.timestamp if latest_cast else 0

        fetcher = WarpcastCastFetcher(warpcast_hub_key)
        data = fetcher.fetch_data(latest_timestamp)
        # save_objects(session, fetcher.get_models(data))
        dump_casts_to_sqlite(
            engine, fetcher.get_models(data), latest_timestamp)
