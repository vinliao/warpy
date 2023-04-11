import os
from datetime import datetime, timedelta
from typing import List

from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from utils.fetcher import WarpcastReactionFetcher
from utils.models import Cast, Reaction

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


def insert_reactions(session, reactions: List[Reaction]):
    # Get the list of existing reaction hashes
    existing_hashes = (
        session.query(Reaction.hash)
        .filter(Reaction.hash.in_(tuple(reaction.hash for reaction in reactions)))
        .all()
    )

    # Convert the list of tuples to a set for faster lookup
    existing_hashes = set([hash_[0] for hash_ in existing_hashes])

    # Insert only the new reactions into the database
    for reaction in reactions:
        if reaction.hash not in existing_hashes:
            session.add(reaction)

    # Commit the changes to the database
    session.commit()
    print(f"Inserted {len(reactions)} reactions")


async def main(engine: Engine):
    with sessionmaker(engine)() as session:
        one_week_ago = datetime.now() - timedelta(days=7)
        one_week_ago_unix_ms = int(one_week_ago.timestamp() * 1000)

        casts = (
            session.query(Cast)
            .filter(
                Cast.timestamp < one_week_ago_unix_ms,
                ~Cast.hash.in_(session.query(Reaction.target_hash).distinct()),
            )
            .order_by(Cast.timestamp.desc())
            .all()
        )

        print(f"Fetching reactions for {len(casts)} casts...")

        cast_hashes = [cast.hash for cast in casts]
        batch_size = 10
        for i in range(0, len(cast_hashes), batch_size):
            fetcher = WarpcastReactionFetcher(key=warpcast_hub_key)
            data = await fetcher.fetch_data(cast_hashes[i : i + batch_size])
            reactions = fetcher.get_models(data)
            insert_reactions(session, reactions)
