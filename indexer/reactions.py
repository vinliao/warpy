from datetime import datetime
from dotenv import load_dotenv
import os
import requests
import time
from requests.exceptions import RequestException, JSONDecodeError
import requests
import time
import datetime
from typing import List
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Cast


load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


# ============================================================
# ====================== WARPCAST ============================
# ============================================================


def get_cast_reactions(cast_hashes, bearer_token):
    reactions = {}
    for cast_hash in cast_hashes:
        cursor = None
        reactions[cast_hash] = []
        while True:
            url = f"https://api.warpcast.com/v2/cast-reactions?castHash={cast_hash}&limit=100"
            if cursor:
                url += f"&cursor={cursor}"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {bearer_token}"})
            response.raise_for_status()
            data = response.json()
            reactions[cast_hash].extend(data["result"])
            cursor = data.get("next", {}).get("cursor")
            if not cursor:
                break
    return reactions


# create the file path relative to the parent directory
def main():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    db_path = os.path.join(parent_dir, 'datasets', 'datasets.db')
    engine = create_engine('sqlite:///' + db_path)

    with sessionmaker(engine)() as session:
        casts = session.query(Cast).limit(10).all()
        cast_hashes = [cast.hash for cast in casts]
        print(cast_hashes)
        # reactions = get_cast_reactions(cast_hashes, warpcast_hub_key)
        # for cast in casts:
        #     cast.reactions = reactions[cast.hash]
        # session.commit()
