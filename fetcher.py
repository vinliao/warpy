from dotenv import load_dotenv
import os
import requests
import json

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


def get_recent_users(cursor: str = None):
    # if users.json exists, read it and return it
    if os.path.exists('users.json'):
        with open('users.json', 'r') as f:
            data = json.load(f)
    else:
        # have cursor in url if cursor exists, use ternary
        url = f"https://api.warpcast.com/v2/recent-users?cursor={cursor}&limit=1000" if cursor else "https://api.warpcast.com/v2/recent-users?limit=1000"

        # fetch to url with the bearer token
        result = requests.get(
            url, headers={"Authorization": "Bearer " + warpcast_hub_key})
        json_data = result.json()

        data = {"users": json_data["result"]['users'],
                "cursor": json_data["next"]['cursor']}

        # write the fetched data to a file
        with open('users.json', 'w') as f:
            json.dump(data, f, indent=4)

    return data


def get_recent_casts(cursor: str = None):
    # if casts.json exists, read it and return it
    if os.path.exists('casts.json'):
        with open('casts.json', 'r') as f:
            data = json.load(f)
    else:
        # have cursor in url if cursor exists, use ternary
        url = f"https://api.warpcast.com/v2/recent-casts?cursor={cursor}&limit=1000" if cursor else "https://api.warpcast.com/v2/recent-casts?limit=1000"

        # fetch to url with the bearer token
        result = requests.get(
            url, headers={"Authorization": "Bearer " + warpcast_hub_key})
        json_data = result.json()

        data = {"casts": json_data["result"]['casts'],
                "cursor": json_data["next"]['cursor']}

        # write the fetched data to a file
        with open('casts.json', 'w') as f:
            json.dump(data, f, indent=4)

    return data