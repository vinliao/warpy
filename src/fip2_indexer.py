import functools
import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

import utils


def make_warpcast_request(url: str) -> Any:
    api_key = os.getenv("PICTURE_WARPCAST_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(url, headers=headers)
    return response.json()


def url_maker(limit: int = 1000, cursor: Optional[str] = None) -> str:
    warpcast_url = "https://api.warpcast.com/v2/recent-casts"
    if cursor:
        return f"{warpcast_url}?limit={limit}&cursor={cursor}"
    return f"{warpcast_url}?limit={limit}"


def extractor(cast: Any) -> Any:
    def _get_in(data_dict: Any, map_list: List[Any], default: Any = None) -> Any:
        for key in map_list:
            try:
                data_dict = data_dict[key]
            except KeyError:
                return default
        return data_dict

    cast_getter = functools.partial(_get_in, cast)
    tags = cast_getter(["tags"], [])
    channel_tag = next((tag for tag in tags if tag["type"] == "channel"), None)

    return {
        "parent_url": cast_getter(["parentSource", "url"]),
        "channel_id": channel_tag["id"] if channel_tag else None,
        "channel_description": channel_tag["name"] if channel_tag else None,
    }


def not_fip2(cast: Any) -> Any:
    return cast.get("parentSource") is not None


def json_append(file_path: str, data: List[Dict[str, Any]]) -> None:
    with open(file_path, "a") as f:
        for item in data:
            json.dump(item, f)
            f.write("\n")


# because 2023/06/01 is ~roughly the start of fip2
def pull_fip2s(t: int = utils.TimeConverter.ymd_to_unixms(2023, 6, 1)) -> None:
    cursor = None
    while True:
        url = url_maker(limit=1000, cursor=cursor)
        data = make_warpcast_request(url)
        casts = data["result"]["casts"]
        c_t = casts[-1]["timestamp"]
        if c_t < t:
            break
        next_data = data.get("next")
        cursor = next_data["cursor"] if next_data else None
        casts = filter(not_fip2, casts)
        fip2 = list(map(extractor, casts))
        with open("data.ndjson", "a") as f:
            for item in fip2:
                json.dump(item, f)
                f.write("\n")

        if cursor is None:
            break
        print(utils.TimeConverter.unixms_to_ago("days", t - c_t))


def unique_fip2s(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=["parent_url", "channel_id"])


df = pd.read_json("data.ndjson", lines=True, dtype_backend="pyarrow")
df = df[df["channel_id"].notnull()]
df = unique_fip2s(df)
df.to_json("data/fip2.ndjson", orient="records", lines=True)
