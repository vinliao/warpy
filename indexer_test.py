import glob
import json
import os
import random
import string
from typing import List

import pandas as pd
import pytest

import indexer
import utils


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

    async def fetch_write(url_maker_fn, fetcher_fn, fids: List[int]) -> None:
        urls = [url_maker_fn(fid) for fid in fids]
        users = await fetcher_fn(urls)
        indexer.json_append(f"testdata/{url_maker_fn.__name__}.ndjson", users)

    async def fetch_fids(fids: List[int]) -> None:
        functions = [
            (indexer.UrlMaker.user_warpcast, indexer.Fetcher.user_warpcast),
            (indexer.UrlMaker.user_searchcaster, indexer.Fetcher.user_searchcaster),
        ]

        for url_maker, fetcher in functions:
            await fetch_write(url_maker, fetcher, fids)

    warpcast_file = "testdata/user_warpcast.ndjson"
    searchcaster_file = "testdata/user_searchcaster.ndjson"
    user_file = "testdata/users.parquet"

    # test merge warpcast with searchcaster
    ints1 = r_ints(10) + [3]
    await fetch_fids(ints1)
    w_df = pd.read_json(warpcast_file, lines=True, dtype_backend="pyarrow")
    s_df = pd.read_json(searchcaster_file, lines=True, dtype_backend="pyarrow")
    fids = make_fids(w_df, s_df)
    df = indexer.Merger.user(warpcast_file, searchcaster_file, user_file)
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
    await fetch_fids(ints2)
    new_w_df = pd.read_json(warpcast_file, lines=True, dtype_backend="pyarrow")
    new_s_df = pd.read_json(
        searchcaster_file, lines=True, dtype_backend="pyarrow", convert_dates=False
    )
    new_fids = make_fids(new_w_df, new_s_df)
    assert isinstance(new_w_df, pd.DataFrame)
    assert isinstance(new_s_df, pd.DataFrame)
    assert len(new_w_df) == len(new_s_df)

    new_df = indexer.Merger.user(warpcast_file, searchcaster_file, user_file)
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

    c_queued_file = "testdata/cast_warpcast.ndjson"
    c_data_file = "testdata/casts.parquet"
    r_queued_file = "testdata/reaction_warpcast.ndjson"
    r_data_file = "testdata/reactions.parquet"

    url = indexer.UrlMaker.cast_warpcast(cast_limit)
    data = await indexer.Fetcher.cast_warpcast(url)
    indexer.json_append(c_queued_file, data["casts"])

    df = indexer.Merger.cast(c_queued_file, c_data_file)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == cast_limit
    # TODO: maybe more asserts here

    os.remove(c_queued_file)
    df.to_parquet(c_data_file)

    # test merge with local parquet
    url = indexer.UrlMaker.cast_warpcast(cast_limit, data["next_cursor"])
    data = await indexer.Fetcher.cast_warpcast(url)
    indexer.json_append(c_queued_file, data["casts"])
    df = indexer.Merger.cast(c_queued_file, c_data_file)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == cast_limit * 2
    assert len(set(list(df["hash"]))) == cast_limit * 2

    casts = list(map(lambda x: indexer.CastWarpcast(**x), df.to_dict("records")))
    assert all(isinstance(cast, indexer.CastWarpcast) for cast in casts)

    # test fetching reactions
    hashes = list(df["hash"])
    urls = [indexer.UrlMaker.reaction_warpcast(hash) for hash in hashes]
    data = await indexer.Fetcher.reaction_warpcast(urls)
    for cast in data:
        indexer.json_append(r_queued_file, cast["reactions"])

    df = indexer.Merger.reaction(r_queued_file, r_data_file)
    assert isinstance(df, pd.DataFrame)
    assert set(df["target_hash"]).issubset(set(hashes))
    reactions = list(
        map(lambda x: indexer.ReactionWarpcast(**x), df.to_dict("records"))
    )
    assert all(isinstance(reaction, indexer.ReactionWarpcast) for reaction in reactions)
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

    w_queued_fids = indexer.QueueProducer.user_warpcast(uw_file, u_file)
    highest_network_fid = max(w_queued_fids)
    all_fids = list(range(1, highest_network_fid + 1))
    assert sorted(set(w_queued_fids)) == sorted(set(all_fids) - set(fids))

    half_fids = random.sample(fids, len(fids) // 2)
    half_fids_dict = [{"fid": fid} for fid in half_fids]

    with open(us_file, "w") as f:
        for fid in half_fids_dict:
            f.write(json.dumps(fid) + "\n")

    s_queued_fids = indexer.QueueProducer.user_searchcaster(uw_file, us_file)
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

    c_queued_max_timestamp = indexer.QueueProducer.cast_warpcast(c_file)
    assert c_queued_max_timestamp == 0

    df = pd.DataFrame(dummy_casts)
    df.to_parquet(c_file)

    c_queued_max_timestamp = indexer.QueueProducer.cast_warpcast(c_file)
    assert c_queued_max_timestamp == one_week_ago

    reaction_queue_producer = indexer.QueueProducer.reaction_warpcast
    t = utils.weeks_ago_to_unixms(2) - utils.days_to_ms(1)
    r_queued_hashes = reaction_queue_producer(t_from=t, data_file=c_file)
    expected_hashes_tuple = [(expected_hash_1, None), (expected_hash_2, None)]
    assert set(r_queued_hashes) == set(expected_hashes_tuple)
