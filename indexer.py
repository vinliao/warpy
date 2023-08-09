import asyncio
import functools
import json
import os
import time
from typing import Any, List, Optional, Tuple

import aiohttp
import duckdb
import pandas as pd
import pydantic
import requests
from dotenv import load_dotenv

import utils

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
    discord: Optional[str]
    email: Optional[str]
    ens: Optional[str]
    github: Optional[str]
    telegram: Optional[str]
    twitter: Optional[str]
    url: Optional[str]


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
    channel: Optional[str]


class ReactionWarpcast(pydantic.BaseModel):
    type: str
    hash: str
    timestamp: int
    target_hash: str
    reactor_fid: int


# ======================================================================================
# indexer utils
# ======================================================================================


def execute_query(query: str) -> List[Any]:
    con = duckdb.connect(database=":memory:")
    return list(filter(None, [x[0] for x in con.execute(query).fetchall()]))


def execute_query_df(query: str) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    return con.execute(query).fetchdf()


def make_request(url: str) -> dict:
    response = requests.get(url)
    return response.json()


def make_warpcast_request(url: str) -> dict:
    api_key = os.getenv("PICTURE_WARPCAST_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(url, headers=headers)
    return response.json()


def get_property(property: str, file_path: str) -> List[Any]:
    file_format = file_path.split(".")[-1]
    if file_format == "ndjson" or file_format == "json":
        query = f"SELECT {property} FROM read_json_auto('{file_path}')"
    else:
        query = f"SELECT {property} FROM read_parquet('{file_path}')"

    try:
        return execute_query(query)
    except Exception as e:
        print(e)
        return []


get_fids = functools.partial(get_property, "fid")
get_addresses = functools.partial(get_property, "address")
get_hashes = functools.partial(get_property, "hash")
get_target_hashes = functools.partial(get_property, "target_hash")


def json_append(file_path: str, data: list[pydantic.BaseModel]):
    with open(file_path, "a") as f:
        for item in data:
            json.dump(item.model_dump(), f)  # type: ignore
            f.write("\n")


# ======================================================================================
# indexer
# ======================================================================================


class UrlMaker:
    warpcast_url = "https://api.warpcast.com/v2"
    searchcaster_url = "https://searchcaster.xyz/api"

    @staticmethod
    def user_warpcast(fid: int) -> str:
        return f"{UrlMaker.warpcast_url}/user?fid={fid}"

    @staticmethod
    def user_searchcaster(fid: int) -> str:
        return f"{UrlMaker.searchcaster_url}/profiles?fid={fid}"

    @staticmethod
    def user_ensdata(address: str) -> str:
        return f"https://ensdata.net/{address}"

    @staticmethod
    def cast_warpcast(limit=1000, cursor: Optional[str] = None) -> str:
        url = f"{UrlMaker.warpcast_url}/recent-casts?limit={limit}"
        return f"{url}&cursor={cursor}" if cursor else url

    @staticmethod
    def reaction_warpcast(hash: str, cursor: Optional[str] = None) -> str:
        url = f"{UrlMaker.warpcast_url}/cast-reactions?castHash={hash}&limit=100"
        return f"{url}&cursor={cursor}" if cursor else url


class Extractor:
    @staticmethod
    def get_in(data_dict, map_list, default=None):
        for key in map_list:
            try:
                data_dict = data_dict[key]
            except KeyError:
                return default
        return data_dict

    @staticmethod
    def user_warpcast(user: dict) -> Optional[UserWarpcast]:
        try:
            location = Extractor.get_in(user, ["user", "profile", "location"], {})
            location_id = location.get("placeId") or None
            location_description = location.get("description") or None

            collections = user.get("collectionsOwned", [])
            collections = [collection.get("id") for collection in collections]

            return UserWarpcast(
                fid=Extractor.get_in(user, ["user", "fid"]),
                username=Extractor.get_in(user, ["user", "username"]),
                display_name=Extractor.get_in(user, ["user", "displayName"]),
                pfp_url=Extractor.get_in(user, ["user", "pfp", "url"]),
                bio_text=Extractor.get_in(user, ["user", "profile", "bio", "text"]),
                following_count=Extractor.get_in(user, ["user", "followingCount"], 0),
                follower_count=Extractor.get_in(user, ["user", "followerCount"], 0),
                location_id=location_id,
                location_description=location_description,
                verified=Extractor.get_in(user, ["user", "pfp", "verified"], False),
                is_active=Extractor.get_in(user, ["user", "activeOnFcNetwork"], False),
                inviter_fid=Extractor.get_in(user, ["inviter", "fid"]),
                onchain_collections=collections,
            )
        except Exception as e:
            print(f"Failed to create UserWarpcast due to {str(e)}")
            return None

    @staticmethod
    def user_searchcaster(user: dict) -> UserSearchcaster:
        return UserSearchcaster(
            fid=Extractor.get_in(user, ["body", "id"]),
            generated_farcaster_address=Extractor.get_in(user, ["body", "address"]),
            address=Extractor.get_in(user, ["connectedAddress"]),
            registered_at=Extractor.get_in(user, ["body", "registeredAt"]),
        )

    @staticmethod
    def user_ensdata(user: dict) -> Optional[UserEnsdata]:
        if Extractor.get_in(user, ["error"]) is True:
            address = utils.extract_ethereum_address(user["message"])
            if address is None:
                return None
            return UserEnsdata(
                address=address,
                discord=None,
                email=None,
                ens=None,
                github=None,
                telegram=None,
                twitter=None,
                url=None,
            )

        try:
            return UserEnsdata(
                address=Extractor.get_in(user, ["address"]),
                discord=Extractor.get_in(user, ["discord"]),
                email=Extractor.get_in(user, ["email"]),
                ens=Extractor.get_in(user, ["ens"]),
                github=Extractor.get_in(user, ["github"]),
                telegram=Extractor.get_in(user, ["telegram"]),
                twitter=Extractor.get_in(user, ["twitter"]),
                url=Extractor.get_in(user, ["url"]),
            )
        except Exception as e:
            print(f"Failed to create UserEnsdata due to {str(e)}")
            return None

    @staticmethod
    def cast_warpcast(cast: dict) -> CastWarpcast:
        images = Extractor.get_in(cast, ["embeds", "images"], [])
        tags: List[dict] = Extractor.get_in(cast, ["tags"], [])
        mentions = Extractor.get_in(cast, ["mentions"], [])

        return CastWarpcast(
            hash=Extractor.get_in(cast, ["hash"]),
            thread_hash=Extractor.get_in(cast, ["threadHash"]),
            text=Extractor.get_in(cast, ["text"]),
            timestamp=Extractor.get_in(cast, ["timestamp"]),
            author_fid=Extractor.get_in(cast, ["author", "fid"]),
            parent_hash=Extractor.get_in(cast, ["parentHash"]),
            images=[image["sourceUrl"] for image in images],
            channel=next(  # out of the tag list, get the first channel tag
                (tag["name"] for tag in tags if tag["type"] == "channel"), None
            ),
            mentions=[mention["fid"] for mention in mentions],
        )

    @staticmethod
    def reaction_warpcast(reaction: dict) -> ReactionWarpcast:
        return ReactionWarpcast(
            type=Extractor.get_in(reaction, ["type"]),
            hash=Extractor.get_in(reaction, ["hash"]),
            timestamp=Extractor.get_in(reaction, ["timestamp"]),
            target_hash=Extractor.get_in(reaction, ["castHash"]),
            reactor_fid=Extractor.get_in(reaction, ["reactor", "fid"]),
        )


class Fetcher:
    api_key = os.getenv("PICTURE_WARPCAST_API_KEY")

    @staticmethod
    async def make_request(url: str, key: Optional[str] = None) -> dict:
        headers = {"Authorization": f"Bearer {key}"} if key else None
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                return await response.json()

    @staticmethod
    async def user_warpcast(urls: str) -> List[UserWarpcast]:
        async def _fetch(url: str):
            data = await Fetcher.make_request(url, Fetcher.api_key)
            return Extractor.user_warpcast(data["result"])

        return await asyncio.gather(*[_fetch(url) for url in urls])

    @staticmethod
    async def user_searchcaster(urls: str) -> List[UserSearchcaster]:
        async def _fetch(url: str):
            data = await Fetcher.make_request(url)
            return Extractor.user_searchcaster(data[0])

        return await asyncio.gather(*[_fetch(url) for url in urls])

    @staticmethod
    async def user_ensdata(urls: str) -> List[UserEnsdata]:
        async def _fetch(url: str):
            data = await Fetcher.make_request(url)
            return Extractor.user_ensdata(data)

        return await asyncio.gather(*[_fetch(url) for url in urls])

    @staticmethod
    async def cast_warpcast(url: str) -> dict:
        data = await Fetcher.make_request(url, Fetcher.api_key)
        next_data = data.get("next")
        return {
            "casts": list(map(Extractor.cast_warpcast, data["result"]["casts"])),
            "next_cursor": next_data["cursor"] if next_data else None,
        }

    @staticmethod
    async def reaction_warpcast(urls: List[str]) -> List[dict]:
        async def _fetch(url: str):
            data = await Fetcher.make_request(url, Fetcher.api_key)
            next_data = data.get("next")
            reactions = data["result"]["reactions"]
            return {
                "reactions": list(map(Extractor.reaction_warpcast, reactions)),
                "next_cursor": next_data["cursor"] if next_data else None,
                "target_hash": reactions[0]["castHash"] if len(reactions) > 0 else None,
            }

        return await asyncio.gather(*[_fetch(url) for url in urls])


class QueueProducer:
    @staticmethod
    def user_warpcast(
        queued_file="queue/user_warpcast.ndjson", data_file="data/users.parquet"
    ):
        def _get_local_fids(queued_file: str, data_file: str) -> List[int]:
            fids1 = get_fids(queued_file)
            fids2 = get_fids(data_file)
            return list(set(fids1).union(set(fids2)))

        def _fetch_highest_fid() -> int:
            url = "https://api.warpcast.com/v2/recent-users?limit=1"
            api_key = os.getenv("PICTURE_WARPCAST_API_KEY")
            headers = {"Authorization": f"Bearer {api_key}"}
            response = requests.get(url, headers=headers)
            return json.loads(response.text)["result"]["users"][0]["fid"]

        network_fids = range(1, _fetch_highest_fid() + 1)
        local_fids = _get_local_fids(queued_file, data_file)
        return list(set(network_fids) - set(local_fids))  # set difference

    @staticmethod
    def user_searchcaster(
        warpcast_queue_file="queue/user_warpcast.ndjson",
        searchcaster_queue_file="queue/user_searchcaster.ndjson",
    ) -> List[int]:
        # NOTE: not including parquet users because it's gonna be be merged with queue
        w_fids = get_fids(warpcast_queue_file)
        s_fids = get_fids(searchcaster_queue_file)
        return list(set(w_fids) - set(s_fids))  # set difference

    # TODO: this code still untested
    @staticmethod
    def user_ensdata() -> List[str]:
        searchcaster_data_path = "queue/user_searchcaster.ndjson"
        ensdata_data_path = "queue/user_ensdata.ndjson"
        searchcaster_addresses = get_addresses(searchcaster_data_path)
        ensdata_addresses = get_addresses(ensdata_data_path)
        # NOTE: ensdata doesn't return these addresses
        forbidden_addresses = [
            "0x947caf5ada865ace0c8de0ffd55de0c02e5f6b54",
            "0xaee33d473c68f9b4946020d79021416ff0587005",
            "0x2d3fe453caaa7cd2c5475a50b06630dd75f67377",
            "0xc6735e557cb2c5850708cf00a2dec05da2aa6490",
        ]
        return list(
            set(searchcaster_addresses) - set(ensdata_addresses + forbidden_addresses)
        )

    @staticmethod
    def cast_warpcast(filepath="data/casts.parquet") -> int:
        # NOTE: once queue is consuming, can't stop and resume
        query = f"SELECT MAX(timestamp) FROM read_parquet('{filepath}')"
        try:
            time: List[int] = execute_query(query)  # a list of one element
            return time[0] if time else 0
        except Exception as e:
            print(f"Error: {e}")
            return 0  # no item, means get all casts in network

    # TODO: simplify-able
    @staticmethod
    def reaction_warpcast(
        t_from=utils.days_ago_to_unixms(32),  # more than 1 month
        t_until=utils.ms_now(),
        data_file="data/casts.parquet",
    ) -> List[Tuple[str, Optional[str]]]:
        query = f"SELECT hash FROM read_parquet('{data_file}') "
        query += f"WHERE timestamp >= {t_from} AND timestamp < {t_until}"
        try:
            hashes = execute_query(query)
            hashes = list(set(hashes))
            return [(hash, None) for hash in hashes]
        except Exception as e:
            print(f"Error: {e}")
            return []


class QueueConsumer:
    @staticmethod
    async def user_queue_consumer(
        url_maker_fn,
        queue_producer_fn,
        fetcher_fn,
        n=100,
    ) -> None:
        queue = queue_producer_fn()
        while queue:
            current_batch = queue[:n]
            queue = queue[n:]
            print(f"source_type: {len(queue)} left; fetching: {n}")
            urls = [url_maker_fn(fid) for fid in current_batch]
            users = await fetcher_fn(urls)
            json_append(f"queue/{url_maker_fn.__name__}", list(filter(None, users)))
            time.sleep(0.5)

    @staticmethod
    async def cast_warpcast(limit=1000) -> None:
        max_timestamp = QueueProducer.cast_warpcast()
        new_timestamp = max_timestamp + 1
        cursor = None
        while new_timestamp > max_timestamp:
            url = UrlMaker.cast_warpcast(limit, cursor)
            result = await Fetcher.cast_warpcast(url)
            casts: List[CastWarpcast] = result["casts"]
            new_timestamp = casts[-1].timestamp
            cursor = result["next_cursor"]
            json_append("queue/cast_warpcast.ndjson", casts)  # type: ignore
            timestamp_diff = new_timestamp - max_timestamp
            print(f"cast_warpcast: {utils.ms_to_days(timestamp_diff):.2f} days left")
            time.sleep(0.5)

    @staticmethod
    async def reaction_warpcast(n=1000) -> None:
        queue = QueueProducer.reaction_warpcast()
        while queue:
            batch = queue[:n]
            queue = queue[n:]
            print(f"reaction_warpcast: {len(queue)} left; fetching: {n}")
            urls = [UrlMaker.reaction_warpcast(*item) for item in batch]
            data = await Fetcher.reaction_warpcast(urls)
            for cast in data:
                json_append("queue/reaction_warpcast.ndjson", cast["reactions"])
                if cast["next_cursor"]:
                    queue.append((cast["target_hash"], cast["next_cursor"]))
            time.sleep(1)


class Merger:
    # NOTE: ideally DataFrame operations should only happen here,
    # queries should use duckdb+values and set function on PKs

    @staticmethod
    def user(
        warpcast_file="queue/user_warpcast.ndjson",
        searchcaster_file="queue/user_searchcaster.ndjson",
        user_file="data/users.parquet",
    ) -> pd.DataFrame:
        def make_queued_df(warpcast_file: str, searchcaster_file: str):
            df1 = pd.read_json(
                warpcast_file,
                lines=True,
                dtype_backend="pyarrow",
            )
            df2 = pd.read_json(
                searchcaster_file,
                lines=True,
                dtype_backend="pyarrow",
                convert_dates=False,
            )

            df1 = df1.drop_duplicates(subset=["fid"])
            df2 = df2.drop_duplicates(subset=["fid"])
            return pd.merge(df1, df2, on="fid", how="left")

        try:
            df = pd.read_parquet(user_file, dtype_backend="pyarrow")
        except Exception:
            df = pd.DataFrame()

        df = pd.concat([df, make_queued_df(warpcast_file, searchcaster_file)])
        df = df.drop_duplicates(subset=["fid"])
        return df

    @staticmethod
    def cast(queued_file, data_file) -> pd.DataFrame:
        queued_df = pd.read_json(
            queued_file, lines=True, dtype_backend="pyarrow", convert_dates=False
        )

        try:
            df = pd.read_parquet(data_file, dtype_backend="pyarrow")
        except Exception:
            df = pd.DataFrame()

        df = pd.concat([df, queued_df])
        df = df.drop_duplicates(subset=["hash"])
        return df

    @staticmethod
    def reaction(queued_file, data_file) -> pd.DataFrame:
        return Merger.cast(queued_file, data_file)


async def refresh_user() -> None:
    quwf = "queue/user_warpcast.ndjson"
    qusf = "queue/user_searchcaster.ndjson"
    uf = "data/users.parquet"

    # NOTE: to refresh from scratch, delete the queue and the parquet
    refresh_everything = False
    if refresh_everything:
        os.remove(quwf)
        os.remove(uf)

    function_groups = [
        (
            UrlMaker.user_warpcast,
            QueueProducer.user_warpcast,
            Fetcher.user_warpcast,
            1000,
        ),
        (
            UrlMaker.user_searchcaster,
            QueueProducer.user_searchcaster,
            Fetcher.user_searchcaster,
            125,
        ),
        (UrlMaker.user_ensdata, QueueProducer.user_ensdata, Fetcher.user_ensdata, 25),
    ]

    for function_group in function_groups:
        await QueueConsumer.user_queue_consumer(*function_group)

    df = Merger.user(quwf, qusf, uf)
    df.to_parquet(uf, index=False)


# # TODO: refresh casts and reactions
