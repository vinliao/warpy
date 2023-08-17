import asyncio
import functools
import json
import os
import time
from typing import Any, List, Optional, Tuple, TypedDict, Union
from datetime import datetime

import aiohttp
import duckdb
import pandas as pd
import pydantic
import requests
from dotenv import load_dotenv

load_dotenv()


class UserWarpcast(pydantic.BaseModel):
    fid: int
    username: str
    display_name: str
    pfp_url: Optional[str]
    bio_text: Optional[str]
    following_count: int
    follower_count: int
    location_id: Optional[str]
    location_description: Optional[str]
    verified: bool
    is_active: bool
    inviter_fid: Optional[int]
    onchain_collections: List[str]


class UserSearchcaster(pydantic.BaseModel):
    fid: int
    generated_farcaster_address: str
    address: Optional[str]
    registered_at: int


class UserEnsdata(pydantic.BaseModel):
    address: str
    discord: Optional[str] = None
    email: Optional[str] = None
    ens: Optional[str] = None
    github: Optional[str] = None
    telegram: Optional[str] = None
    twitter: Optional[str] = None
    url: Optional[str] = None


class User(UserWarpcast, UserSearchcaster):
    pass


class CastWarpcast(pydantic.BaseModel):
    hash: str
    thread_hash: str
    text: str
    timestamp: int
    author_fid: int
    parent_hash: Optional[str]
    images: List[str]
    mentions: List[int]
    channel_id: Optional[str]
    channel_description: Optional[str]


class ReactionWarpcast(pydantic.BaseModel):
    type: str
    hash: str
    timestamp: int
    target_hash: str
    reactor_fid: int


class FetcherUserResponse(TypedDict):
    users: List[Union[UserWarpcast, UserSearchcaster, UserEnsdata]]


class FetcherCastWarpcastResponse(TypedDict):
    casts: List[CastWarpcast]
    next_cursor: Optional[str]


class FetcherReactionWarpcastResponse(TypedDict):
    reactions: List[ReactionWarpcast]
    next_cursor: Optional[str]
    target_hash: str  # single cast hash, multiple reactions, hence list above


# ======================================================================================
# indexer utils
# ======================================================================================


def execute_query(query: str) -> List[Any]:
    con = duckdb.connect(database=":memory:")
    return list(filter(None, [x[0] for x in con.execute(query).fetchall()]))


def execute_query_df(query: str) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    return con.execute(query).fetchdf()


def read_ndjson(file_path: str) -> pd.DataFrame:
    # wrapper exist because i want pyarrow by default
    # pyarrow because it preserves dtypes
    return pd.read_json(
        file_path, lines=True, dtype_backend="pyarrow", convert_dates=False
    )


def read_parquet(file_path: str) -> pd.DataFrame:
    # wrapper exist because i want pyarrow by default
    # pyarrow because it preserves dtypes
    return pd.read_parquet(file_path, dtype_backend="pyarrow")


def make_warpcast_request(url: str) -> Any:
    api_key = os.getenv("PICTURE_WARPCAST_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(url, headers=headers)
    return response.json()


def fetch_highest_fid() -> int:
    try:
        url = "https://api.warpcast.com/v2/recent-users?limit=1"
        response = make_warpcast_request(url)
        fid = response["result"]["users"][0]["fid"]
        assert isinstance(fid, int)
        return fid
    except Exception as e:
        print(f"Error fetching highest fid: {e}")
        return 10000


def get_property(property: str, file_path: str) -> List[Any]:
    file_format = file_path.split(".")[-1]
    read_fn = "read_json_auto" if file_format in ("ndjson", "json") else "read_parquet"
    query = f"SELECT {property} FROM {read_fn}('{file_path}')"

    try:
        return execute_query(query)
    except Exception as e:
        print(e)
        return []


get_fids = functools.partial(get_property, "fid")
get_addresses = functools.partial(get_property, "address")
get_hashes = functools.partial(get_property, "hash")
get_target_hashes = functools.partial(get_property, "target_hash")


def json_append(file_path: str, data: List[Any]) -> None:
    with open(file_path, "a") as f:
        for item in data:
            item = item.model_dump() if isinstance(item, pydantic.BaseModel) else item
            json.dump(item, f)
            f.write("\n")


def get_fid_by_username(username: str) -> Optional[int]:
    query = "SELECT fid FROM read_parquet('data/users.parquet') "
    query += f"WHERE username = '{username}'"
    result = execute_query(query)
    return result[0] if result else None


def get_username_by_fid(fid: int) -> Optional[str]:
    query = "SELECT username FROM read_parquet('data/users.parquet') "
    query += f"WHERE fid = {fid}"
    result = execute_query(query)
    return result[0] if result else None


class TimeConverter:
    FACTORS = {
        "minutes": 60 * 1000,
        "hours": 60 * 60 * 1000,
        "days": 24 * 60 * 60 * 1000,
        "weeks": 7 * 24 * 60 * 60 * 1000,
        "months": 30 * 24 * 60 * 60 * 1000,
        "years": 365 * 24 * 60 * 60 * 1000,
    }

    @staticmethod
    def ms_now() -> int:
        return int(round(time.time() * 1000))

    @staticmethod
    def to_ms(factor: str, units: int) -> int:
        return units * TimeConverter.FACTORS[factor]

    @staticmethod
    def from_ms(factor: str, ms: int) -> float:
        return ms / TimeConverter.FACTORS[factor]

    @staticmethod
    def ago_to_unixms(factor: str, units: int) -> int:
        return TimeConverter.ms_now() - TimeConverter.to_ms(factor, units)

    @staticmethod
    def unixms_to_ago(factor: str, ms: int) -> float:
        return TimeConverter.from_ms(factor, TimeConverter.ms_now() - ms)
    
    @staticmethod
    def ymd_to_unixms(year: int, month: int, day: int) -> int:
        return int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)) * 1000)
    
    @staticmethod
    def datetime_to_unixms(dt: datetime) -> int:
        return int(time.mktime(dt.timetuple()) * 1000)


def is_valid_ethereum_address(address: str) -> bool:
    import re

    return bool(re.match(r"^(0x)?[0-9a-f]{40}$", address.lower()))


def max_gap(xs: List[int]) -> Optional[int]:
    import numpy as np

    diffs = np.diff(np.sort(xs))
    return diffs.max() if len(xs) > 0 else None


# ======================================================================================
# indexer
# ======================================================================================


class UrlMaker:
    @staticmethod
    def make_url(base_url: str, endpoint: str, **params: dict[str, Any]) -> str:
        query_params = "&".join(f"{key}={value}" for key, value in params.items())
        return f"{base_url}{endpoint}?{query_params}"

    warpcast_url = "https://api.warpcast.com/v2"
    searchcaster_url = "https://searchcaster.xyz/api"

    user_warpcast = functools.partial(make_url, warpcast_url, "/user")
    user_searchcaster = functools.partial(make_url, searchcaster_url, "/profiles")
    user_ensdata = functools.partial(lambda address: f"https://ensdata.net/{address}")
    cast_warpcast = functools.partial(make_url, warpcast_url, "/recent-casts")
    reaction_warpcast = functools.partial(make_url, warpcast_url, "/cast-reactions")


class Extractor:
    @staticmethod
    def get_in(data_dict: Any, map_list: List[Any], default: Any = None) -> Any:
        for key in map_list:
            try:
                data_dict = data_dict[key]
            except KeyError:
                return default
        return data_dict

    @staticmethod
    def user_warpcast(user: Any) -> Optional[UserWarpcast]:
        user_getter = functools.partial(Extractor.get_in, user)
        try:
            location = user_getter(["user", "profile", "location"], {})
            location_id = location.get("placeId") or None
            location_description = location.get("description") or None

            collections = user.get("collectionsOwned", [])
            collections = [collection.get("id") for collection in collections]

            return UserWarpcast(
                fid=user_getter(["user", "fid"]),
                username=user_getter(["user", "username"]),
                display_name=user_getter(["user", "displayName"]),
                pfp_url=user_getter(["user", "pfp", "url"]),
                bio_text=user_getter(["user", "profile", "bio", "text"]),
                following_count=user_getter(["user", "followingCount"], 0),
                follower_count=user_getter(["user", "followerCount"], 0),
                location_id=location_id,
                location_description=location_description,
                verified=user_getter(["user", "pfp", "verified"], False),
                is_active=user_getter(["user", "activeOnFcNetwork"], False),
                inviter_fid=user_getter(["inviter", "fid"]),
                onchain_collections=collections,
            )
        except Exception as e:
            print(f"Failed to create UserWarpcast due to {str(e)}")
            return None

    @staticmethod
    def user_searchcaster(user: Any) -> Optional[UserSearchcaster]:
        user_getter = functools.partial(Extractor.get_in, user)
        try:
            return UserSearchcaster(
                fid=user_getter(["body", "id"]),
                generated_farcaster_address=user_getter(["body", "address"]),
                address=user_getter(["connectedAddress"]),
                registered_at=user_getter(["body", "registeredAt"]),
            )
        except Exception as e:
            print(f"Failed to create UserSearchcaster due to {str(e)}")
            return None

    @staticmethod
    def user_ensdata(user: Any) -> Optional[UserEnsdata]:
        user_getter = functools.partial(Extractor.get_in, user)

        try:
            return UserEnsdata(
                address=user_getter(["address"]),
                discord=user_getter(["discord"]),
                email=user_getter(["email"]),
                ens=user_getter(["ens"]),
                github=user_getter(["github"]),
                telegram=user_getter(["telegram"]),
                twitter=user_getter(["twitter"]),
                url=user_getter(["url"]),
            )
        except Exception as e:
            import re

            print(f"Failed to create UserEnsdata due to {str(e)}")

            address = re.search(r"0x[a-fA-F0-9]{40}", user.get("message"))
            if address is None:
                return None

            return UserEnsdata(address=address.group())

    @staticmethod
    def cast_warpcast(cast: Any) -> CastWarpcast:
        cast_getter = functools.partial(Extractor.get_in, cast)
        images = cast_getter(["embeds", "images"], [])
        tags = cast_getter(["tags"], [])
        mentions = cast_getter(["mentions"], [])
        channel_tag = next((tag for tag in tags if tag["type"] == "channel"), None)

        return CastWarpcast(
            hash=cast_getter(["hash"]),
            thread_hash=cast_getter(["threadHash"]),
            text=cast_getter(["text"]),
            timestamp=cast_getter(["timestamp"]),
            author_fid=cast_getter(["author", "fid"]),
            parent_hash=cast_getter(["parentHash"]),
            images=[image["sourceUrl"] for image in images],
            channel_id=channel_tag["id"] if channel_tag else None,
            channel_description=channel_tag["name"] if channel_tag else None,
            mentions=[mention["fid"] for mention in mentions],
        )

    @staticmethod
    def reaction_warpcast(reaction: Any) -> ReactionWarpcast:
        reaction_getter = functools.partial(Extractor.get_in, reaction)
        return ReactionWarpcast(
            type=reaction_getter(["type"]),
            hash=reaction_getter(["hash"]),
            timestamp=reaction_getter(["timestamp"]),
            target_hash=reaction_getter(["castHash"]),
            reactor_fid=reaction_getter(["reactor", "fid"]),
        )


class Fetcher:
    api_key = os.getenv("PICTURE_WARPCAST_API_KEY")

    @staticmethod
    async def make_request(url: str, key: Optional[str] = None) -> Any:
        headers = {"Authorization": f"Bearer {key}"} if key else None
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                return await response.json()

    @staticmethod
    async def user_warpcast(urls: List[str]) -> FetcherUserResponse:
        async def _fetch(url: str) -> Optional[UserWarpcast]:
            data = await Fetcher.make_request(url, Fetcher.api_key)
            return Extractor.user_warpcast(data["result"])

        users = await asyncio.gather(*[_fetch(url) for url in urls])
        users = list(filter(lambda user: user is not None, users))
        return {"users": users}

    @staticmethod
    async def user_searchcaster(urls: List[str]) -> FetcherUserResponse:
        async def _fetch(url: str) -> Optional[UserSearchcaster]:
            data = await Fetcher.make_request(url)
            return Extractor.user_searchcaster(data[0])

        users = await asyncio.gather(*[_fetch(url) for url in urls])
        users = list(filter(lambda user: user is not None, users))
        return {"users": users}

    @staticmethod
    async def user_ensdata(urls: List[str]) -> FetcherUserResponse:
        async def _fetch(url: str) -> Optional[UserEnsdata]:
            data = await Fetcher.make_request(url)
            return Extractor.user_ensdata(data)

        users = await asyncio.gather(*[_fetch(url) for url in urls])
        users = list(filter(lambda user: user is not None, users))
        return {"users": users}

    @staticmethod
    async def cast_warpcast(url: str) -> FetcherCastWarpcastResponse:
        data = await Fetcher.make_request(url, Fetcher.api_key)
        next_data = data.get("next")
        return {
            "casts": list(map(Extractor.cast_warpcast, data["result"]["casts"])),
            "next_cursor": next_data["cursor"] if next_data else None,
        }

    @staticmethod
    async def reaction_warpcast(
        urls: List[str],
    ) -> List[FetcherReactionWarpcastResponse]:
        async def _fetch(url: str) -> Optional[FetcherReactionWarpcastResponse]:
            data = await Fetcher.make_request(url, Fetcher.api_key)
            next_data = data.get("next")
            reactions = data["result"]["reactions"]
            return {
                "reactions": list(map(Extractor.reaction_warpcast, reactions)),
                "next_cursor": next_data["cursor"] if next_data else None,
                "target_hash": reactions[0]["castHash"] if len(reactions) > 0 else None,
            }

        reactions = await asyncio.gather(*[_fetch(url) for url in urls])
        return list(filter(lambda reaction: reaction is not None, reactions))


class QueueProducer:
    @staticmethod
    def user_warpcast(
        queued_file: str = "queue/user_warpcast.ndjson",
        data_file: str = "data/users.parquet",
    ) -> List[int]:
        # TODO: have a file that saves user without usernames
        local_fids = set.union(*map(set, map(get_fids, [queued_file, data_file])))
        network_fids = set(range(1, fetch_highest_fid() + 1))
        return list(set.difference(network_fids, local_fids))

    @staticmethod
    def user_searchcaster(
        warpcast_queue_file: str = "queue/user_warpcast.ndjson",
        searchcaster_queue_file: str = "queue/user_searchcaster.ndjson",
    ) -> List[int]:
        # TODO: have a file that saves user without usernames
        w_fids = set(get_fids(warpcast_queue_file))
        s_fids = set(get_fids(searchcaster_queue_file))
        return list(set.difference(w_fids, s_fids))

    # TODO: this code still untested
    @staticmethod
    def user_ensdata(
        searchcaster_queue_file: str = "queue/user_searchcaster.ndjson",
        ensdata_queue_file: str = "queue/user_ensdata.ndjson",
    ) -> List[str]:
        #  ensdata.net doesn't return these addresses
        forbidden_addresses = [
            "0x947caf5ada865ace0c8de0ffd55de0c02e5f6b54",
            "0xaee33d473c68f9b4946020d79021416ff0587005",
            "0x2d3fe453caaa7cd2c5475a50b06630dd75f67377",
            "0xc6735e557cb2c5850708cf00a2dec05da2aa6490",
        ]

        s_addrs = set(get_addresses(searchcaster_queue_file))
        e_addrs = set(get_addresses(ensdata_queue_file))
        e_addrs = set.union(e_addrs, set(forbidden_addresses))
        return list(set.difference(s_addrs, e_addrs))

    @staticmethod
    def cast_warpcast(filepath: str = "data/casts.parquet") -> int:
        try:
            query = f"SELECT MAX(timestamp) FROM read_parquet('{filepath}')"
            time: List[int] = execute_query(query)  # a list of one element
            return time[0] if time else 0
        except Exception:
            return 0

    @staticmethod
    def reaction_warpcast(
        t_from: int = TimeConverter.ago_to_unixms(factor="days", units=1),
        t_until: int = TimeConverter.ms_now(),
        data_file: str = "data/casts.parquet",
    ) -> List[Tuple[str, Optional[str]]]:
        query = f"SELECT hash FROM read_parquet('{data_file}') "
        query += f"WHERE timestamp >= {t_from} AND timestamp < {t_until}"
        hashes = set(execute_query(query))
        return list(map(lambda hash: (hash, None), hashes))


class BatchFetcher:
    @staticmethod
    async def user_warpcast(
        fids: List[int], n: int = 100, out: str = "queue/user_warpcast.ndjson"
    ) -> None:
        for i in range(0, len(fids), n):
            batch = fids[i : i + n]
            urls = [UrlMaker.user_warpcast(fid=fid) for fid in batch]
            print(f"user_warpcast: {len(fids) - i - n} left; fetching: {n}")
            data = await Fetcher.user_warpcast(urls)
            json_append(out, data["users"])
            await asyncio.sleep(0.5)

    @staticmethod
    async def user_searchcaster(
        fids: List[int], n: int = 125, out: str = "queue/user_searchcaster.ndjson"
    ) -> None:
        for i in range(0, len(fids), n):
            batch = fids[i : i + n]
            urls = [UrlMaker.user_searchcaster(fid=fid) for fid in batch]
            print(f"user_searchcaster: {len(fids) - i - n} left; fetching: {n}")
            data = await Fetcher.user_searchcaster(urls)
            json_append(out, data["users"])
            await asyncio.sleep(0.5)

    @staticmethod
    async def user_ensdata(
        addrs: List[str], n: int = 50, out: str = "queue/user_ensdata.ndjson"
    ) -> None:
        for i in range(0, len(addrs), n):
            batch = addrs[i : i + n]
            urls = [UrlMaker.user_ensdata(addr) for addr in batch]
            print(f"user_ensdata: {len(addrs) - i - n} left; fetching: {n}")
            data = await Fetcher.user_ensdata(urls)
            json_append(out, data["users"])
            await asyncio.sleep(0.5)

    @staticmethod
    async def cast_warpcast(
        cursor: Optional[str] = None,
        n: int = 1000,
        out: str = "queue/cast_warpcast.ndjson",
    ) -> None:
        local_t = QueueProducer.cast_warpcast()
        new_t = local_t + 1
        while new_t > local_t:
            url = UrlMaker.cast_warpcast(limit=n)
            url = UrlMaker.cast_warpcast(limit=n, cursor=cursor) if cursor else url
            result = await Fetcher.cast_warpcast(url)
            cursor = result["next_cursor"]
            new_t = result["casts"][-1].timestamp
            days_left = TimeConverter.from_ms(factor="days", ms=new_t - local_t)
            print(f"cast_warpcast: fetching {url}; {days_left} days left")
            json_append(out, result["casts"])
            await asyncio.sleep(0.5)
            if cursor is None:
                break

    @staticmethod
    async def reaction_warpcast(
        hashes: List[Tuple[str, Optional[str]]],  # tuple of cast hash and cursors
        n: int = 1000,
        out: str = "queue/reaction_warpcast.ndjson",
    ) -> None:
        def _make_url(hash: str, cursor: Optional[str]) -> str:
            if cursor is None:
                return UrlMaker.reaction_warpcast(castHash=hash)
            return UrlMaker.reaction_warpcast(castHash=hash, cursor=cursor)

        while hashes:
            batch = hashes[:n]
            urls = [_make_url(item[0], item[1]) for item in batch]
            print(f"reaction_warpcast: {len(hashes) - n} left; fetching: {n}")
            data = await Fetcher.reaction_warpcast(urls)
            for cast in data:
                json_append(out, cast["reactions"])
                if cast["next_cursor"]:
                    hashes.append((cast["target_hash"], cast["next_cursor"]))
            await asyncio.sleep(1)


class Merger:
    @staticmethod
    def user(
        warpcast_file: str = "queue/user_warpcast.ndjson",
        searchcaster_file: str = "queue/user_searchcaster.ndjson",
        user_file: str = "data/users.parquet",
    ) -> pd.DataFrame:
        def make_queued_df(warpcast_file: str, searchcaster_file: str) -> pd.DataFrame:
            w_df = read_ndjson(warpcast_file)
            s_df = read_ndjson(searchcaster_file)
            w_df = w_df.drop_duplicates(subset=["fid"])
            s_df = s_df.drop_duplicates(subset=["fid"])
            return pd.merge(w_df, s_df, on="fid", how="inner")

        try:
            df = read_parquet(user_file)
        except Exception:
            df = pd.DataFrame()

        df = pd.concat([df, make_queued_df(warpcast_file, searchcaster_file)])
        df = df.drop_duplicates(subset=["fid"])
        return df

    @staticmethod
    def cast(queued_file: str, data_file: str) -> pd.DataFrame:
        queued_df = read_ndjson(queued_file)

        try:
            df = read_parquet(data_file)
        except Exception:
            df = pd.DataFrame()

        df = pd.concat([df, queued_df])
        df = df.drop_duplicates(subset=["hash"])
        return df

    @staticmethod
    def reaction(queued_file: str, data_file: str) -> pd.DataFrame:
        return Merger.cast(queued_file, data_file)
