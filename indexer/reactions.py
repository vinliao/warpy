from dotenv import load_dotenv
import os
from sqlalchemy.orm import sessionmaker
from utils.models import Cast, Reaction
import asyncio
import aiohttp
from datetime import datetime, timedelta
from sqlalchemy.engine import Engine


load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


async def fetch_reactions(session, base_url, headers):
    reactions = []
    cursor = None
    while True:
        try:
            # Add the cursor query parameter to the URL if it exists
            url = f"{base_url}&cursor={cursor}" if cursor else base_url

            timeout = aiohttp.ClientTimeout(total=10)
            async with session.get(url, headers=headers, timeout=timeout) as response:
                # Sleep between requests to avoid rate limiting
                await asyncio.sleep(1)
                response.raise_for_status()
                data = await response.json()

                # Append reactions to the list
                reactions.extend(data['result']['reactions'])

                # Check if there's a next page
                cursor = data.get('next', {}).get('cursor')
                if cursor is None:
                    break

        except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
            print(f"Error occurred for {url}: {e}. Retrying...")
            await asyncio.sleep(5)  # Wait for 5 seconds before retrying

    return reactions


async def get_cast_reactions_async(cast_hashes, warpcast_hub_key, n):
    async with aiohttp.ClientSession() as aiohttp_session:
        headers = {"Authorization": f"Bearer {warpcast_hub_key}"}
        reactions = {}
        for i in range(0, len(cast_hashes), n):
            tasks = []
            for cast_hash in cast_hashes[i:i + n]:
                url = f"https://api.warpcast.com/v2/cast-reactions?castHash={cast_hash}&limit=100"
                tasks.append(fetch_reactions(aiohttp_session, url, headers))

            responses = await asyncio.gather(*tasks)
            for cast_hash, response_data in zip(cast_hashes[i:i + n], responses):
                if response_data is not None and response_data != []:
                    reactions[cast_hash] = [extract_reactions(
                        reaction) for reaction in response_data if reaction]

        return reactions


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
    with sessionmaker(engine)() as session:
        one_week_ago = datetime.now() - timedelta(days=7)
        one_week_ago_unix_ms = int(one_week_ago.timestamp() * 1000)

        casts = session.query(Cast).filter(
            Cast.timestamp < one_week_ago_unix_ms,
            ~Cast.hash.in_(
                session.query(Reaction.target_hash).distinct()
            )
        ).order_by(Cast.timestamp.desc()).all()

        print(f"Fetching reactions for {len(casts)} casts...")

        cast_hashes = [cast.hash for cast in casts]
        batch_size = 1000
        for i in range(0, len(cast_hashes), batch_size):
            reactions = await get_cast_reactions_async(
                cast_hashes[i:i + batch_size], warpcast_hub_key, n=1000)
            insert_reactions(session, reactions)
