from dotenv import load_dotenv
import os
import requests

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


def get_recent_users(cursor: str = None):
    # have cursor in url if cursor exists, use ternary
    url = f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit=1000" if cursor else "https://api.warpcast.com/v2/recent-users?limit=1000"

    # fetch to url with the bearer token
    result = requests.get(
        url, headers={"Authorization": "Bearer " + warpcast_hub_key})
    json = result.json()
    return {"users": json["result"]['users'], "cursor": json["next"]['cursor']}


print(get_recent_users()['users'])
