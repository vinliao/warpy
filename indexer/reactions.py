from sqlalchemy import tuple_
from datetime import datetime
from dotenv import load_dotenv
import os
from requests.exceptions import RequestException, JSONDecodeError
from typing import List
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Cast, Reaction
import asyncio
import aiohttp


load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


# ============================================================
# ====================== WARPCAST ============================
# ============================================================


async def fetch_reactions(session, url, headers):
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with session.get(url, headers=headers, timeout=timeout) as response:
                # Sleep between requests to avoid rate limiting
                await asyncio.sleep(1)
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
            print(f"Error occurred for {url}: {e}. Retrying...")
            await asyncio.sleep(5)  # Wait for 5 seconds before retrying


async def get_cast_reactions_async(cast_hashes, warpcast_hub_key, n):
    async with aiohttp.ClientSession() as session:
        headers = {"Authorization": f"Bearer {warpcast_hub_key}"}
        reactions = {}
        for i in range(0, len(cast_hashes), n):
            tasks = []
            for cast_hash in cast_hashes[i:i + n]:
                url = f"https://api.warpcast.com/v2/cast-reactions?castHash={cast_hash}&limit=100"
                tasks.append(fetch_reactions(session, url, headers))

            responses = await asyncio.gather(*tasks)
            for cast_hash, response in zip(cast_hashes[i:i + n], responses):
                data = response['result']['reactions']
                if data is not None and data != []:
                    print(data)
                    reactions[cast_hash] = [extract_reactions(
                        item) for item in data if item]

        return reactions


async def get_cast_reactions(cast_hashes, bearer_token, n=10):
    reactions = await get_cast_reactions_async(cast_hashes, bearer_token, n)
    return reactions


def extract_reactions(data):
    return Reaction(
        reaction_type=data['type'],
        hash=data['hash'],
        timestamp=data['timestamp'],
        target_hash=data['castHash'],
        author_fid=data['reactor']['fid'],
    )


# def insert_reactions(session, reactions):
#     for reaction_list in reactions.values():
#         for reaction in reaction_list:
#             session.add(reaction)
#     session.commit()


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


# create the file path relative to the parent directory
async def main():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    db_path = os.path.join(parent_dir, 'datasets', 'datasets.db')
    engine = create_engine('sqlite:///' + db_path)

    with sessionmaker(engine)() as session:
        casts = session.query(Cast).filter(
            Cast.timestamp > datetime.now().timestamp() * 1000 - 7 * 24 * 60 * 60 * 1000).all()
        cast_hashes = [cast.hash for cast in casts]

        reactions = await get_cast_reactions(cast_hashes, warpcast_hub_key, n=35)

        # dump to db
        insert_reactions(session, reactions)


if __name__ == "__main__":
    asyncio.run(main())
