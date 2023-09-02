import functools
import json
import os
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
import pydantic
import requests
from dotenv import load_dotenv

import utils

load_dotenv()


# ======================================================================================
# indexer utils
# ======================================================================================


def execute_query(query: str) -> List[Any]:
    con = duckdb.connect(database=":memory:")
    return list(filter(None, [x[0] for x in con.execute(query).fetchall()]))


def execute_query_df(query: str) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    return con.execute(query).fetchdf()


def make_warpcast_request(url: str) -> Any:
    api_key = os.getenv("PICTURE_WARPCAST_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(url, headers=headers)
    return response.json()


def json_append(file_path: str, data: List[Any]) -> None:
    with open(file_path, "a") as f:
        for item in data:
            item = item.model_dump() if isinstance(item, pydantic.BaseModel) else item
            json.dump(item, f)
            f.write("\n")


# ======================================================================================
# indexer
# ======================================================================================


def extract_cast(cast: Any) -> Dict[str, Any]:
    def _get_in(data_dict: Any, map_list: List[Any], default: Any = None) -> Any:
        for key in map_list:
            try:
                data_dict = data_dict[key]
            except KeyError:
                return default
        return data_dict

    cast_getter = functools.partial(_get_in, cast)
    images = cast_getter(["embeds", "images"], [])
    tags = cast_getter(["tags"], [])
    mentions = cast_getter(["mentions"], [])
    channel_tag = next((tag for tag in tags if tag["type"] == "channel"), None)

    class CastWarpcast(pydantic.BaseModel):
        hash: str
        thread_hash: str
        text: str
        timestamp: int
        author_fid: int
        parent_hash: Optional[str]
        images: List[str]
        mentions: List[int]
        parent_url: Optional[str]
        channel_id: Optional[str]
        channel_description: Optional[str]

    c = CastWarpcast(
        hash=cast_getter(["hash"]),
        thread_hash=cast_getter(["threadHash"]),
        text=cast_getter(["text"]),
        timestamp=cast_getter(["timestamp"]),
        author_fid=cast_getter(["author", "fid"]),
        parent_hash=cast_getter(["parentHash"]),
        images=[image["sourceUrl"] for image in images],
        channel_id=channel_tag["id"] if channel_tag else None,
        channel_description=channel_tag["name"] if channel_tag else None,
        parent_url=cast_getter(["parentSource", "url"]),
        mentions=[mention["fid"] for mention in mentions],
    )

    # NOTE pydantic: to verify the incoming data is correct, but should return dict
    return c.model_dump()


def main() -> None:
    def _make_url(limit: int = 1000, cursor: Optional[str] = None) -> str:
        warpcast_url = "https://api.warpcast.com/v2/recent-casts"
        if cursor:
            return f"{warpcast_url}?limit={limit}&cursor={cursor}"
        return f"{warpcast_url}?limit={limit}"

    t = utils.TimeConverter.ymd_to_unixms(2023, 7, 28)
    cursor = "eyJsaW1pdCI6MTAwMCwiYmVmb3JlIjoxNjkzMjI3ODg3MDAwfQ"

    while True:
        url = _make_url(limit=1000, cursor=cursor)
        data = make_warpcast_request(url)
        casts = [extract_cast(cast) for cast in data["result"]["casts"]]
        cursor = data.get("next", {}).get("cursor")
        new_t = casts[-1]["timestamp"]

        json_append("data/cast_warpcast.ndjson", casts)
        days_left = utils.TimeConverter.from_ms(factor="days", ms=new_t - t)
        print(f"cast_warpcast: fetching {url}; {days_left} days left")

        if new_t < t or cursor is None:
            print("Fetching complete. Terminating.")
            break


if __name__ == "__main__":
    main()
