import glob
import json
import os
import random
import string
from typing import List

import aiohttp
import pandas as pd
import pydantic
import pytest

import indexer
import utils


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
    files = glob.glob("testdata/*")
    for file in files:
        if os.path.isfile(file):
            os.remove(file)


@pytest.mark.asyncio
async def test_user_integration():
    """
    What's tested:
    - both user fetcher fetches and extracts properly (returns pydantic model)
    - merger merges properly (warpcast+searchcaster and parquet+ndjson)
    """

    def r_ints(n: int) -> List[int]:
        return [random.randint(1, 10000) for i in range(n)]

    def make_fids(df1, df2) -> List[int]:
        return list(set(df1["fid"].tolist() + df2["fid"].tolist()))

    async def fetch_write(st: str, fids: List[int]):
        urls = [indexer.url_maker(st)(fid) for fid in fids]
        async with aiohttp.ClientSession() as session:
            users = await indexer.fetcher(st)(session, urls)
            json_append(f"testdata/{st}.ndjson", users)

    warpcast_file = "testdata/user_warpcast.ndjson"
    searchcaster_file = "testdata/user_searchcaster.ndjson"
    user_file = "testdata/users.parquet"

    # test merge warpcast with searchcaster
    ints1 = r_ints(10) + [3]
    await fetch_write("user_warpcast", ints1)
    await fetch_write("user_searchcaster", ints1)
    w_df = pd.read_json(warpcast_file, lines=True, dtype_backend="pyarrow")
    s_df = pd.read_json(searchcaster_file, lines=True, dtype_backend="pyarrow")
    fids = make_fids(w_df, s_df)
    df = indexer.merger("user")(warpcast_file, searchcaster_file, user_file)
    assert isinstance(w_df, pd.DataFrame)
    assert isinstance(s_df, pd.DataFrame)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(w_df) == len(s_df)
    assert set(df["fid"]).issubset(set(fids))
    assert len(df.columns) == len(w_df.columns) + len(s_df.columns) - 1

    # test merge local parquet with incoming data
    os.remove(warpcast_file)
    os.remove(searchcaster_file)
    df.to_parquet(user_file)
    ints2 = r_ints(5)
    await fetch_write("user_warpcast", ints2)
    await fetch_write("user_searchcaster", ints2)

    new_w_df = pd.read_json(warpcast_file, lines=True, dtype_backend="pyarrow")
    new_s_df = pd.read_json(
        searchcaster_file, lines=True, dtype_backend="pyarrow", convert_dates=False
    )
    new_fids = make_fids(new_w_df, new_s_df)
    assert isinstance(new_w_df, pd.DataFrame)
    assert isinstance(new_s_df, pd.DataFrame)
    assert len(new_w_df) == len(new_s_df)

    new_df = indexer.merger("user")(warpcast_file, searchcaster_file, user_file)
    assert sorted(new_df["fid"].tolist()) == sorted(fids + new_fids)
    users = list(map(lambda x: indexer.User(**x), new_df.to_dict("records")))
    assert all(isinstance(user, indexer.User) for user in users)


@pytest.mark.asyncio
async def test_cast_reaction_integration():
    """
    What's tested:
    - cast fetcher, merger
    - reaction fetcher (from fetched cast hashes), merger
    """

    cast_limit = 10

    async with aiohttp.ClientSession() as session:
        c_queued_file = "testdata/cast_warpcast.ndjson"
        c_data_file = "testdata/casts.parquet"

        url = indexer.url_maker("cast_warpcast")(limit=cast_limit)
        data = await indexer.fetcher("cast_warpcast")(session, url)
        json_append(c_queued_file, data["casts"])

        df = indexer.merger("cast")(c_queued_file, c_data_file)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == cast_limit
        # TODO: maybe more asserts here

        os.remove(c_queued_file)
        df.to_parquet(c_data_file)

        # test merge with local parquet
        url = indexer.url_maker("cast_warpcast")(data["next_cursor"], cast_limit)
        data = await indexer.fetcher("cast_warpcast")(session, url)
        json_append(c_queued_file, data["casts"])
        df = indexer.merger("cast")(c_queued_file, c_data_file)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == cast_limit * 2
        assert len(set(list(df["hash"]))) == cast_limit * 2

        casts = list(map(lambda x: indexer.CastWarpcast(**x), df.to_dict("records")))
        assert all(isinstance(cast, indexer.CastWarpcast) for cast in casts)

        # test fetching reactions
        r_queued_file = "testdata/reaction_warpcast.ndjson"
        r_data_file = "testdata/reactions.parquet"

        hashes = list(df["hash"])
        urls = [indexer.url_maker("reaction_warpcast")(hash) for hash in hashes]
        data = await indexer.fetcher("reaction_warpcast")(session, urls)
        for cast in data:
            json_append(r_queued_file, cast["reactions"])

        df = indexer.merger("reaction")(r_queued_file, r_data_file)
        assert isinstance(df, pd.DataFrame)
        assert set(df["target_hash"]).issubset(set(hashes))
        reactions = list(
            map(lambda x: indexer.ReactionWarpcast(**x), df.to_dict("records"))
        )
        assert all(
            isinstance(reaction, indexer.ReactionWarpcast) for reaction in reactions
        )
        # TODO: maybe more asserts here


def test_queue_producer():
    """
    What's tested:
    - queue_producer (user, cast, reaction)
    """

    # ==================================================================================
    # users
    # ==================================================================================

    fids = [random.randint(1, 10000) for i in range(1000)]
    fids_dict = [{"fid": fid} for fid in fids]
    uw_file = "testdata/user_warpcast.ndjson"
    us_file = "testdata/user_searchcaster.ndjson"
    u_file = "testdata/users.parquet"

    with open(uw_file, "w") as f:
        for fid in fids_dict:
            f.write(json.dumps(fid) + "\n")

    w_queued_fids = indexer.queue_producer("user_warpcast")(uw_file, u_file)
    highest_network_fid = max(w_queued_fids)
    all_fids = list(range(1, highest_network_fid + 1))
    assert sorted(set(w_queued_fids)) == sorted(set(all_fids) - set(fids))

    half_fids = random.sample(fids, len(fids) // 2)
    half_fids_dict = [{"fid": fid} for fid in half_fids]

    with open(us_file, "w") as f:
        for fid in half_fids_dict:
            f.write(json.dumps(fid) + "\n")

    s_queued_fids = indexer.queue_producer("user_searchcaster")(uw_file, us_file)
    assert sorted(set(s_queued_fids)) == sorted(set(fids) - set(half_fids))

    # ==================================================================================
    # cast and reactions
    # ==================================================================================

    def random_hash(n):
        return "".join(random.choices(string.ascii_letters + string.digits, k=n))

    one_week_ago = utils.weeks_ago_to_unixms(1)
    expected_hash_1 = random_hash(6)
    expected_hash_2 = random_hash(6)
    dummy_casts = [
        {"hash": expected_hash_1, "timestamp": one_week_ago},
        {"hash": expected_hash_2, "timestamp": utils.weeks_ago_to_unixms(2)},
        {"hash": random_hash(6), "timestamp": utils.weeks_ago_to_unixms(3)},
        {"hash": random_hash(6), "timestamp": utils.weeks_ago_to_unixms(4)},
    ]

    c_file = "testdata/casts.parquet"

    c_queued_max_timestamp = indexer.queue_producer("cast_warpcast")(c_file)
    assert c_queued_max_timestamp == 0

    df = pd.DataFrame(dummy_casts)
    df.to_parquet(c_file)

    c_queued_max_timestamp = indexer.queue_producer("cast_warpcast")(c_file)
    assert c_queued_max_timestamp == one_week_ago

    reaction_queue_producer = indexer.queue_producer("reaction_warpcast")
    t = utils.weeks_ago_to_unixms(2) - utils.days_to_ms(1)
    r_queued_hashes = reaction_queue_producer(t_from=t, data_file=c_file)
    expected_hashes_tuple = [(expected_hash_1, None), (expected_hash_2, None)]
    assert set(r_queued_hashes) == set(expected_hashes_tuple)
