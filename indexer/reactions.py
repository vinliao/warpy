from dotenv import load_dotenv
import os
from sqlalchemy.orm import sessionmaker
from utils.models import Cast, Reaction
from datetime import datetime, timedelta
from sqlalchemy.engine import Engine
# from utils.fetcher_old import WarpcastReactionFetcher


load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


def extract_reactions(data):
    return Reaction(
        reaction_type=data['type'],
        hash=data['hash'],
        timestamp=data['timestamp'],
        target_hash=data['castHash'],
        author_fid=data['reactor']['fid'],
    )


def insert_reactions(session, reactions):
    # Get the list of existing reaction hashes
    existing_hashes = session.query(Reaction.hash).filter(
        Reaction.hash.in_(tuple(reaction.hash for reaction_list in reactions.values(
        ) for reaction in reaction_list))
    ).all()

    # Convert the list of tuples to a set for faster lookup
    existing_hashes = set([hash_[0] for hash_ in existing_hashes])

    # Insert only the new reactions into the database
    for reaction_list in reactions.values():
        for reaction in reaction_list:
            if reaction.hash not in existing_hashes:
                session.add(reaction)

    # Commit the changes to the database
    session.commit()
    print(f"Inserted reactions for {len(reactions)} casts")


async def main(engine: Engine):
    print('Hello')
    # with sessionmaker(engine)() as session:
    #     one_week_ago = datetime.now() - timedelta(days=7)
    #     one_week_ago_unix_ms = int(one_week_ago.timestamp() * 1000)

    #     casts = session.query(Cast).filter(
    #         Cast.timestamp < one_week_ago_unix_ms,
    #         ~Cast.hash.in_(
    #             session.query(Reaction.target_hash).distinct()
    #         )
    #     ).order_by(Cast.timestamp.desc()).all()

    #     print(f"Fetching reactions for {len(casts)} casts...")

    #     cast_hashes = [cast.hash for cast in casts]
    #     batch_size = 1000
    #     for i in range(0, len(cast_hashes), batch_size):
    #         warpcast_reaction_fetcher = WarpcastReactionFetcher(
    #             "https://api.warpcast.com/v2/cast-reactions", warpcast_hub_key
    #         )
    #         reactions = await warpcast_reaction_fetcher.fetch(
    #             cast_hashes[i:i + batch_size]
    #         )

    #         reactions = {
    #             cast_hash: [extract_reactions(reaction)
    #                         for reaction in reaction_list]
    #             for cast_hash, reaction_list in reactions.items()
    #         }

    #         insert_reactions(session, reactions)
