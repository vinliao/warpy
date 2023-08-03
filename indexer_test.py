import glob
import json
import os
import random
from typing import List

import aiohttp
import pandas as pd
import pydantic
import pytest

import indexer


def json_append(file_path: str, data: list[pydantic.BaseModel]):
    with open(file_path, "a") as f:
        for item in data:
            json.dump(item.model_dump(), f)  # type: ignore
            f.write("\n")


@pytest.fixture(autouse=True)
def setup_and_teardown():
    # setup
    files = glob.glob("testdata/*")
    for file in files:
        if os.path.isfile(file):
            os.remove(file)

    yield  # This is where your tests are executed

    # teardown
    # ...


@pytest.mark.asyncio
async def test_user_integration():
    def r_ints(n: int) -> List[int]:
        return [random.randint(1, 10000) for i in range(n)]

    def make_fids(df1, df2) -> List[int]:
        return list(set(df1["fid"].tolist() + df2["fid"].tolist()))

    async def fetch_write(st: str, fids: List[int]):
        urls = [indexer.url_maker(st)(fid) for fid in fids]
        async with aiohttp.ClientSession() as session:
            users = await indexer.fetcher(st)(session, urls)
            json_append(f"testdata/{st}.ndjson", users)

    wf = "testdata/user_warpcast.ndjson"
    sf = "testdata/user_searchcaster.ndjson"
    uf = "testdata/users.parquet"

    # test merge warpcast with searchcaster
    ints1 = r_ints(10) + [3]
    await fetch_write("user_warpcast", ints1)
    await fetch_write("user_searchcaster", ints1)
    w_df = pd.read_json(wf, lines=True, dtype_backend="pyarrow")
    s_df = pd.read_json(sf, lines=True, dtype_backend="pyarrow")
    fids = make_fids(w_df, s_df)
    df = indexer.merger("user")(wf, sf, uf)
    assert isinstance(w_df, pd.DataFrame)
    assert isinstance(s_df, pd.DataFrame)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(w_df) == len(s_df)
    assert set(df["fid"]).issubset(set(fids))
    assert len(df.columns) == len(w_df.columns) + len(s_df.columns) - 1

    # test merge local parquet with incoming data
    os.remove("testdata/user_warpcast.ndjson")
    os.remove("testdata/user_searchcaster.ndjson")
    df.to_parquet("testdata/users.parquet")
    ints2 = r_ints(5)
    await fetch_write("user_warpcast", ints2)
    await fetch_write("user_searchcaster", ints2)

    new_w_df = pd.read_json(wf, lines=True, dtype_backend="pyarrow")
    new_s_df = pd.read_json(
        sf, lines=True, dtype_backend="pyarrow", convert_dates=False
    )
    new_fids = make_fids(new_w_df, new_s_df)
    assert isinstance(new_w_df, pd.DataFrame)
    assert isinstance(new_s_df, pd.DataFrame)
    assert len(new_w_df) == len(new_s_df)

    new_df = indexer.merger("user")(wf, sf, uf)
    assert sorted(new_df["fid"].tolist()) == sorted(fids + new_fids)
    users = list(map(lambda x: indexer.User(**x), new_df.to_dict("records")))
    assert all(isinstance(user, indexer.User) for user in users)


# WIP
@pytest.mark.asyncio
async def test_cast_reaction_integration():
    cast_limit = 100

    async with aiohttp.ClientSession() as session:
        st = "cast_warpcast"
        url = indexer.url_maker(st)(limit=cast_limit)
        data = await indexer.fetcher(st)(session, url)
        json_append(f"testdata/{st}.ndjson", data["casts"])

        url = indexer.url_maker(st)(data["next_cursor"], limit=cast_limit)
        data = await indexer.fetcher(st)(session, url)
        json_append(f"testdata/{st}.ndjson", data["casts"])

        hash_query = "SELECT hash FROM read_json_auto('testdata/cast_warpcast.ndjson')"
        hashes = indexer.execute_query(hash_query)

        st = "reaction_warpcast"
        urls = [indexer.url_maker(st)(hash) for hash in hashes]
        data = await indexer.fetcher(st)(session, urls)
        for cast in data:
            json_append(f"testdata/{st}.ndjson", cast["reactions"])

    # make a bunch of asserts
    cast_df = pd.read_json(
        "testdata/cast_warpcast.ndjson", lines=True, dtype_backend="pyarrow"
    )
    reaction_df = pd.read_json(
        "testdata/reaction_warpcast.ndjson", lines=True, dtype_backend="pyarrow"
    )

    assert len(cast_df) == cast_limit * 2
    assert reaction_df["hash"].is_unique
    assert set(reaction_df["target_hash"]).issubset(set(cast_df["hash"]))

    # TODO: more asserts
    # TODO: re-fetch again and must merge with parquets


# # # TODO: test for queue producer
# # # 1. if no cast, must return 0
# # # 2. then for all users and stuff, must return the right fid set
# # # 3. reactions too, must return the correct hash set


# # async def test_queue_producer():
# #     indexer.queue_producer("cast_warpcast")
