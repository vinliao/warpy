import asyncio
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


# ======================================================================================
# indexer
# ======================================================================================


def url_maker(source_type: str):
    warpcast_url = "https://api.warpcast.com/v2"
    searchcaster_url = "https://searchcaster.xyz/api"

    def user_warpcast(fid: int) -> str:
        return f"{warpcast_url}/user?fid={fid}"

    def user_searchcaster(fid: int) -> str:
        return f"{searchcaster_url}/profiles?fid={fid}"

    def user_ensdata(address: str) -> str:
        return f"https://ensdata.net/{address}"

    def cast_warpcast(cursor: Optional[str] = None, limit=1000) -> str:
        url = f"{warpcast_url}/recent-casts?limit={limit}"
        return f"{url}&cursor={cursor}" if cursor else url

    def reaction_warpcast(hash: str, cursor: Optional[str] = None) -> str:
        url = f"{warpcast_url}/cast-reactions?castHash={hash}&limit=100"
        return f"{url}&cursor={cursor}" if cursor else url

    fn_map = {
        "user_warpcast": user_warpcast,
        "user_searchcaster": user_searchcaster,
        "user_ensdata": user_ensdata,
        "cast_warpcast": cast_warpcast,
        "reaction_warpcast": reaction_warpcast,
    }

    return fn_map[source_type]


def extractor(source_type):
    def get_in(data_dict, map_list, default=None):
        for key in map_list:
            try:
                data_dict = data_dict[key]
            except KeyError:
                return default
        return data_dict

    def user_warpcast(user: dict) -> Optional[UserWarpcast]:
        try:
            location_id = get_in(user, ["user", "profile", "location", "placeId"])
            location_description = get_in(
                user, ["user", "profile", "location", "description"]
            )

            location_id = None if location_id == "" else location_id
            location_description = (
                None if location_description == "" else location_description
            )

            return UserWarpcast(
                fid=get_in(user, ["user", "fid"]),
                username=get_in(user, ["user", "username"]),
                display_name=get_in(user, ["user", "displayName"]),
                pfp_url=get_in(user, ["user", "pfp", "url"]),
                bio_text=get_in(user, ["user", "profile", "bio", "text"]),
                following_count=get_in(user, ["user", "followingCount"], 0),
                follower_count=get_in(user, ["user", "followerCount"], 0),
                location_id=location_id,
                location_description=location_description,
                verified=get_in(user, ["user", "pfp", "verified"], False),
                is_active=get_in(user, ["user", "activeOnFcNetwork"], False),
                inviter_fid=get_in(user, ["inviter", "fid"]),
                onchain_collections=[
                    collection.get("id")
                    for collection in user.get("collectionsOwned", [])
                ],
            )
        except Exception as e:
            print(f"Failed to create UserWarpcast due to {str(e)}")
            return None

    def user_searchcaster(user: dict) -> UserSearchcaster:
        return UserSearchcaster(
            fid=get_in(user, ["body", "id"]),
            generated_farcaster_address=get_in(user, ["body", "address"]),
            address=get_in(user, ["connectedAddress"]),
            registered_at=get_in(user, ["body", "registeredAt"]),
        )

    def user_ensdata(user: dict) -> Optional[UserEnsdata]:
        if get_in(user, ["error"]) is True:
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
                address=get_in(user, ["address"]),
                discord=get_in(user, ["discord"]),
                email=get_in(user, ["email"]),
                ens=get_in(user, ["ens"]),
                github=get_in(user, ["github"]),
                telegram=get_in(user, ["telegram"]),
                twitter=get_in(user, ["twitter"]),
                url=get_in(user, ["url"]),
            )
        except Exception as e:
            print(f"Failed to create UserEnsdata due to {str(e)}")
            return None

    def cast_warpcast(cast: dict) -> CastWarpcast:
        images = get_in(cast, ["embeds", "images"], [])
        tags: List[dict] = get_in(cast, ["tags"], [])
        mentions = get_in(cast, ["mentions"], [])

        return CastWarpcast(
            hash=get_in(cast, ["hash"]),
            thread_hash=get_in(cast, ["threadHash"]),
            text=get_in(cast, ["text"]),
            timestamp=get_in(cast, ["timestamp"]),
            author_fid=get_in(cast, ["author", "fid"]),
            parent_hash=get_in(cast, ["parentHash"]),
            images=[image["sourceUrl"] for image in images],
            channel=next(  # out of the tag list, get the first channel tag
                (tag["name"] for tag in tags if tag["type"] == "channel"), None
            ),
            mentions=[mention["fid"] for mention in mentions],
        )

    def reaction_warpcast(reaction: dict) -> ReactionWarpcast:
        return ReactionWarpcast(
            type=get_in(reaction, ["type"]),
            hash=get_in(reaction, ["hash"]),
            timestamp=get_in(reaction, ["timestamp"]),
            target_hash=get_in(reaction, ["castHash"]),
            reactor_fid=get_in(reaction, ["reactor", "fid"]),
        )

    fn_map = {
        "user_warpcast": user_warpcast,
        "user_searchcaster": user_searchcaster,
        "user_ensdata": user_ensdata,
        "cast_warpcast": cast_warpcast,
        "reaction_warpcast": reaction_warpcast,
    }

    return fn_map[source_type]


def fetcher(source_type):
    api_key = os.getenv("PICTURE_WARPCAST_API_KEY")

    async def _make_request(
        session: aiohttp.ClientSession, url: str, key: Optional[str] = None
    ) -> dict:
        headers = {"Authorization": f"Bearer {key}"} if key else None
        async with session.get(url, headers=headers) as response:
            return await response.json()

    async def user_warpcast(
        session: aiohttp.ClientSession, urls: str
    ) -> List[UserWarpcast]:
        async def _fetch(url: str):
            data = await _make_request(session, url, api_key)
            return extractor(source_type)(data["result"])

        return await asyncio.gather(*[_fetch(url) for url in urls])

    async def user_searchcaster(
        session: aiohttp.ClientSession, urls: str
    ) -> List[UserSearchcaster]:
        async def _fetch(url: str):
            data = await _make_request(session, url)
            return extractor(source_type)(data[0])

        return await asyncio.gather(*[_fetch(url) for url in urls])

    async def user_ensdata(
        session: aiohttp.ClientSession, urls: str
    ) -> List[UserEnsdata]:
        async def _fetch(url: str):
            data = await _make_request(session, url)
            return extractor(source_type)(data)

        return await asyncio.gather(*[_fetch(url) for url in urls])

    async def cast_warpcast(session: aiohttp.ClientSession, url: str) -> dict:
        data = await _make_request(session, url, api_key)
        next_data = data.get("next")
        return {
            "casts": list(map(extractor(source_type), data["result"]["casts"])),
            "next_cursor": next_data["cursor"] if next_data else None,
        }

    async def reaction_warpcast(
        session: aiohttp.ClientSession, urls: str
    ) -> List[dict]:
        async def _fetch(url: str):
            data = await _make_request(session, url, api_key)
            next_data = data.get("next")
            reactions = data["result"]["reactions"]
            return {
                "reactions": list(map(extractor(source_type), reactions)),
                "next_cursor": next_data["cursor"] if next_data else None,
                "target_hash": reactions[0]["castHash"] if len(reactions) > 0 else None,
            }

        return await asyncio.gather(*[_fetch(url) for url in urls])

    fn_map = {
        "user_warpcast": user_warpcast,
        "user_searchcaster": user_searchcaster,
        "user_ensdata": user_ensdata,
        "cast_warpcast": cast_warpcast,
        "reaction_warpcast": reaction_warpcast,
    }

    return fn_map[source_type]


def queue_producer(source_type):
    def _get_fids(file_path: str) -> List[int]:
        file_format = file_path.split(".")[-1]
        if file_format == "ndjson" or file_format == "json":
            query = f"SELECT fid FROM read_json_auto('{file_path}')"
        else:
            query = f"SELECT fid FROM read_parquet('{file_path}')"

        try:
            return execute_query(query)
        except Exception as e:
            print(e)
            return []

    def _get_addresses(file_path: str) -> List[str]:
        file_format = file_path.split(".")[-1]
        if file_format == "ndjson" or file_format == "json":
            query = f"SELECT address FROM read_json_auto('{file_path}')"
        else:
            query = f"SELECT address FROM read_parquet('{file_path}')"

        try:
            return execute_query(query)
        except Exception as e:
            print(e)
            return []

    def _get_hashes(file_path: str) -> List[str]:
        file_format = file_path.split(".")[-1]
        if file_format == "ndjson" or file_format == "json":
            query = f"SELECT hash FROM read_json_auto('{file_path}')"
        else:
            query = f"SELECT hash FROM read_parquet('{file_path}')"

        try:
            return execute_query(query)
        except Exception as e:
            print(e)
            return []

    def user_warpcast(
        queued_file="queue/user_warpcast.ndjson", data_file="data/users.parquet"
    ):
        def _get_local_fids(queued_file: str, data_file: str) -> List[int]:
            fids1 = _get_fids(queued_file)
            fids2 = _get_fids(data_file)
            return list(set(fids1).union(set(fids2)))

        def _fetch_highest_fid() -> int:
            url = "https://api.warpcast.com/v2/recent-users?limit=1"
            api_key = os.getenv("PICTURE_WARPCAST_API_KEY")
            headers = {"Authorization": f"Bearer {api_key}"}
            response = requests.get(url, headers=headers)
            return json.loads(response.text)["result"]["users"][0]["fid"]

        network_fids = range(1, _fetch_highest_fid() + 1)
        local_fids = _get_local_fids(queued_file, data_file)
        return list(set(network_fids) - set(local_fids))

    def user_searchcaster(
        warpcast_queue_file="queue/user_warpcast.ndjson",
        searchcaster_queue_file="queue/user_searchcaster.ndjson",
    ) -> List[int]:
        # NOTE: not including parquet users because it's gonna be be merged with queue
        warpcast_fids = _get_fids(warpcast_queue_file)
        searchcaster_fids = _get_fids(searchcaster_queue_file)
        return list(set(warpcast_fids) - set(searchcaster_fids))  # set difference

    # TODO: this code still untested
    def user_ensdata() -> List[str]:
        searchcaster_data_path = "queue/user_searchcaster.ndjson"
        ensdata_data_path = "queue/user_ensdata.ndjson"
        searchcaster_addresses = _get_addresses(searchcaster_data_path)
        ensdata_addresses = _get_addresses(ensdata_data_path)
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

    def cast_warpcast(filepath="data/casts.parquet") -> int:
        # NOTE: once queue is consuming, can't stop and resume
        query = f"SELECT MAX(timestamp) FROM read_parquet('{filepath}')"
        try:
            r: List[int] = execute_query(query)  # a list of one element
            return r[0] if r else 0
        except Exception as e:
            print(f"Error: {e}")
            return 0

    # TODO: simplify-able
    def reaction_warpcast(
        t_from=utils.days_ago_to_unixms(32),  # more than 1 month
        t_until=utils.ms_now(),
        data_file="data/casts.parquet",
    ) -> List[Tuple[str, Optional[str]]]:
        query = f"SELECT hash FROM read_parquet('{data_file}') "
        query += f"WHERE timestamp >= {t_from} AND timestamp < {t_until}"
        try:
            cast_hashes = execute_query(query)
            cast_hashes = list(set(cast_hashes))
            return [(hash, None) for hash in cast_hashes]
        except Exception as e:
            print(f"Error: {e}")
            return []

    fn_map = {
        "user_warpcast": user_warpcast,
        "user_searchcaster": user_searchcaster,
        "user_ensdata": user_ensdata,
        "cast_warpcast": cast_warpcast,
        "reaction_warpcast": reaction_warpcast,
    }

    return fn_map[source_type]


def queue_consumer(source_type):
    def json_append(file_path: str, data: list[pydantic.BaseModel]):
        with open(file_path, "a") as f:
            for item in data:
                json.dump(item.model_dump(), f)  # type: ignore
                f.write("\n")

    async def user_queue_consumer(n=100):
        queue = queue_producer(source_type)()
        while queue:
            current_batch = queue[:n]
            queue = queue[n:]
            print(f"{source_type}: {len(queue)} left; fetching: {n}")
            urls = [url_maker(source_type)(fid) for fid in current_batch]
            async with aiohttp.ClientSession() as session:
                users = await fetcher(source_type)(session, urls)

            json_append(f"queue/{source_type}.ndjson", list(filter(None, users)))
            time.sleep(0.5)

    async def cast_warpcast() -> None:
        max_timestamp = queue_producer(source_type)()
        incoming_cast_timestamp = max_timestamp + 1
        cursor = None
        while incoming_cast_timestamp > max_timestamp:
            url = url_maker(source_type)(cursor)
            async with aiohttp.ClientSession() as session:
                result = await fetcher(source_type)(session, url)
                casts = result["casts"]
                incoming_cast_timestamp = casts[-1].timestamp
                cursor = result["next_cursor"]

            json_append(f"queue/{source_type}.ndjson", casts)
            timestamp_diff = incoming_cast_timestamp - max_timestamp
            print(f"{source_type}: {utils.ms_to_days(timestamp_diff):.2f} days left")
            time.sleep(0.5)

    async def reaction_warpcast(n=1000):
        queue = queue_producer(source_type)()
        while queue:
            current_batch = queue[:n]
            queue = queue[n:]
            print(f"{source_type}: {len(queue)} left; fetching: {n}")
            urls = [url_maker(source_type)(item[0], item[1]) for item in current_batch]
            async with aiohttp.ClientSession() as session:
                data = await fetcher(source_type)(session, urls)

            for cast in data:
                json_append(f"queue/{source_type}.ndjson", cast["reactions"])
                if cast["next_cursor"]:
                    queue.append([cast["target_hash"], cast["next_cursor"]])
            time.sleep(1)

    fn_map = {
        "user_warpcast": user_queue_consumer,
        "user_searchcaster": user_queue_consumer,
        "user_ensdata": user_queue_consumer,
        "cast_warpcast": cast_warpcast,
        "reaction_warpcast": reaction_warpcast,
    }

    return fn_map[source_type]


def merger(source_type):
    # NOTE: ideally DataFrame operations should only happen here,
    # queries should use duckdb+values and set function on PKs

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

    def cast_reaction(queued_file, data_file) -> pd.DataFrame:
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

    fn_map = {
        "user": user,
        "cast": cast_reaction,
        "reaction": cast_reaction,
        "user_warpcast": user,
        "user_searchcaster": user,
        "cast_warpcast": cast_reaction,
        "reaction_warpcast": cast_reaction,
    }

    return fn_map[source_type]


async def refresh_user() -> None:
    # NOTE: to refresh from scratch, delete the queue and the parquet
    refresh_everything = False
    if refresh_everything:
        os.remove("data/user_warpcast.ndjson")
        os.remove("data/users.parquet")

    await queue_consumer("user_warpcast")(1000)
    await queue_consumer("user_searchcaster")(125)
    await queue_consumer("user_ensdata")(100)
    df: pd.DataFrame = merger("user")()
    df.to_parquet("data/users.parquet", index=False)


# TODO: refresh casts and reactions

# ======================================================================================
# evals
# ======================================================================================

# asyncio.run(refresh_user()
