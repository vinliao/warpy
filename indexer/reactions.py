from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Cast, Reaction
import asyncio
import aiohttp
from datetime import datetime, timedelta


load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


# ============================================================
# ====================== WARPCAST ============================
# ============================================================

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


async def get_cast_reactions_async(cast_hashes, warpcast_hub_key, n, session):
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

            # Dump to the database after fetching reactions for each batch of cast hashes
            insert_reactions(session, reactions)
            reactions.clear()  # Clear the reactions dictionary for the next batch

        return reactions


async def get_cast_reactions(cast_hashes, bearer_token, n=10):
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    db_path = os.path.join(parent_dir, 'datasets', 'datasets.db')
    engine = create_engine('sqlite:///' + db_path)

    with sessionmaker(engine)() as session:
        reactions = await get_cast_reactions_async(cast_hashes, bearer_token, n, session)
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
    print(f"Inserted {len(reactions)} reactions into the database.")


# create the file path relative to the parent directory
async def main():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    db_path = os.path.join(parent_dir, 'datasets', 'datasets.db')
    engine = create_engine('sqlite:///' + db_path)

    with sessionmaker(engine)() as session:
        # calculate datetime object for one month ago
        one_month_ago = datetime.now() - timedelta(days=30)

        # convert datetime object to unix timestamp format
        timestamp = int(one_month_ago.strftime('%s')) * 1000

        # query casts within the last month
        casts = session.query(Cast).filter(Cast.timestamp > timestamp).all()
        cast_hashes = [cast.hash for cast in casts]

        print(f"Fetching reactions for {len(cast_hashes)} casts...")

        await get_cast_reactions(cast_hashes, warpcast_hub_key, n=50)


if __name__ == "__main__":
    asyncio.run(main())
