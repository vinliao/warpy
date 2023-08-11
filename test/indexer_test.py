import glob
import json
import os
import random
import string
from typing import Any, Dict, Generator, Hashable, List

import pandas as pd
import pytest

from src import indexer, utils


@pytest.fixture(autouse=True)
def setup_and_teardown() -> Generator[None, None, None]:
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


def stringify_keys(d: Dict[Hashable, Any]) -> Dict[str, Any]:
    return {str(key): value for key, value in d.items()}


@pytest.mark.asyncio
async def test_user_integration() -> None:
    """
    What's tested:
    - both user fetcher fetches and extracts properly (returns pydantic model)
    - merger merges properly (warpcast+searchcaster and parquet+ndjson)
    """

    def r_ints(n: int) -> List[int]:
        return [random.randint(1, 10000) for i in range(n)]

    def make_fids(df1: pd.DataFrame, df2: pd.DataFrame) -> List[int]:
        return list(set(df1["fid"].tolist() + df2["fid"].tolist()))

    async def fetch_write(url_maker_fn, fetcher_fn, fids: List[int]) -> None:  # type: ignore
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

    # Test merge warpcast with searchcaster (first batch)
    await fetch_fids(r_ints(10) + [3])  # add dwr for "complete profile"
    w_df_1 = indexer.read_ndjson(warpcast_file)
    s_df_1 = indexer.read_ndjson(searchcaster_file)
    fids_1 = make_fids(w_df_1, s_df_1)
    df_1 = indexer.Merger.user(warpcast_file, searchcaster_file, user_file)
    assert isinstance(w_df_1, pd.DataFrame)
    assert isinstance(s_df_1, pd.DataFrame)
    assert isinstance(df_1, pd.DataFrame)
    assert len(df_1) == len(w_df_1) == len(s_df_1)
    assert set(df_1["fid"]).issubset(set(fids_1))
    assert len(df_1.columns) == len(w_df_1.columns) + len(s_df_1.columns) - 1

    # Test merge local parquet with incoming data (second batch)
    os.remove(warpcast_file)
    os.remove(searchcaster_file)
    df_1.to_parquet(user_file)
    await fetch_fids(r_ints(5))
    w_df_2 = indexer.read_ndjson(warpcast_file)
    s_df_2 = indexer.read_ndjson(searchcaster_file)
    fids_2 = make_fids(w_df_2, s_df_2)
    assert isinstance(w_df_2, pd.DataFrame)
    assert isinstance(s_df_2, pd.DataFrame)
    assert len(w_df_2) == len(s_df_2)

    new_df = indexer.Merger.user(warpcast_file, searchcaster_file, user_file)
    assert sorted(new_df["fid"].tolist()) == sorted(fids_1 + fids_2)
    users_2 = [indexer.User(**stringify_keys(x)) for x in new_df.to_dict("records")]
    assert all(isinstance(user, indexer.User) for user in users_2)


@pytest.mark.asyncio
async def test_cast_reaction_integration() -> None:
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

    c_url_1 = indexer.UrlMaker.cast_warpcast(cast_limit)
    c_data_1 = await indexer.Fetcher.cast_warpcast(c_url_1)
    indexer.json_append(c_queued_file, c_data_1["casts"])

    c_df_1 = indexer.Merger.cast(c_queued_file, c_data_file)
    assert isinstance(c_df_1, pd.DataFrame)
    assert len(c_df_1) == cast_limit
    # TODO: maybe more asserts here

    c_df_1.to_parquet(c_data_file)
    os.remove(c_queued_file)

    # test merge with local parquet (second batch)
    c_url_2 = indexer.UrlMaker.cast_warpcast(cast_limit, c_data_1["next_cursor"])
    c_data_2 = await indexer.Fetcher.cast_warpcast(c_url_2)
    indexer.json_append(c_queued_file, c_data_2["casts"])
    c_df_2 = indexer.Merger.cast(c_queued_file, c_data_file)
    assert isinstance(c_df_2, pd.DataFrame)
    assert len(c_df_2) == cast_limit * 2
    assert len(set(list(c_df_2["hash"]))) == cast_limit * 2
    cs = [indexer.CastWarpcast(**stringify_keys(x)) for x in c_df_2.to_dict("records")]
    assert all(isinstance(cast, indexer.CastWarpcast) for cast in cs)

    # test fetching reactions (first batch)
    r_hashes = list(c_df_1["hash"])
    r_urls = [indexer.UrlMaker.reaction_warpcast(hash) for hash in r_hashes]
    r_data = await indexer.Fetcher.reaction_warpcast(r_urls)
    for cast in r_data:
        indexer.json_append(r_queued_file, cast["reactions"])
    r_df = indexer.Merger.reaction(r_queued_file, r_data_file)
    assert isinstance(r_df, pd.DataFrame)
    assert set(r_df["target_hash"]).issubset(set(r_hashes))
    rs = [
        indexer.ReactionWarpcast(**stringify_keys(x)) for x in r_df.to_dict("records")
    ]
    assert all(isinstance(reaction, indexer.ReactionWarpcast) for reaction in rs)
    # TODO: maybe more asserts here


def test_queue_producer() -> None:
    """
    What's tested:
    - queue_producer (user, cast, reaction)
    """

    # ==================================================================================
    # users
    # ==================================================================================

    u_fids = [random.randint(1, 10000) for i in range(1000)]
    u_fids_dict = [{"fid": fid} for fid in u_fids]
    u_wf = "testdata/user_warpcast.ndjson"
    u_sf = "testdata/user_searchcaster.ndjson"
    u_f = "testdata/users.parquet"

    with open(u_wf, "w") as f:
        for fid in u_fids_dict:
            f.write(json.dumps(fid) + "\n")

    u_w_queued_fids = indexer.QueueProducer.user_warpcast(u_wf, u_f)
    u_highest_network_fid = max(u_w_queued_fids)
    u_all_fids = list(range(1, u_highest_network_fid + 1))
    assert sorted(set(u_w_queued_fids)) == sorted(set(u_all_fids) - set(u_fids))

    u_half_fids = random.sample(u_fids, len(u_fids) // 2)
    u_half_fids_dict = [{"fid": fid} for fid in u_half_fids]

    with open(u_sf, "w") as f:
        for fid in u_half_fids_dict:
            f.write(json.dumps(fid) + "\n")

    u_s_queued_fids = indexer.QueueProducer.user_searchcaster(u_wf, u_sf)
    assert sorted(set(u_s_queued_fids)) == sorted(set(u_fids) - set(u_half_fids))

    # ==================================================================================
    # cast and reactions
    # ==================================================================================

    def random_hash(n: int) -> str:
        return "".join(random.choices(string.ascii_letters + string.digits, k=n))

    c_one_week_ago = utils.weeks_ago_to_unixms(1)
    c_expected_hash_1 = random_hash(6)
    c_expected_hash_2 = random_hash(6)
    c_dummy_casts = [
        {"hash": c_expected_hash_1, "timestamp": c_one_week_ago},
        {"hash": c_expected_hash_2, "timestamp": utils.weeks_ago_to_unixms(2)},
        {"hash": random_hash(6), "timestamp": utils.weeks_ago_to_unixms(3)},
        {"hash": random_hash(6), "timestamp": utils.weeks_ago_to_unixms(4)},
    ]

    c_file = "testdata/casts.parquet"

    c_queued_max_timestamp_1 = indexer.QueueProducer.cast_warpcast(c_file)
    assert c_queued_max_timestamp_1 == 0

    c_df = pd.DataFrame(c_dummy_casts)
    c_df.to_parquet(c_file)

    c_queued_max_timestamp_2 = indexer.QueueProducer.cast_warpcast(c_file)
    assert c_queued_max_timestamp_2 == c_one_week_ago

    r_t = utils.weeks_ago_to_unixms(2) - utils.days_to_ms(1)
    r_queue_producer = indexer.QueueProducer.reaction_warpcast
    r_queued_hashes = r_queue_producer(t_from=r_t, data_file=c_file)
    r_expected_hashes_tuple = [(c_expected_hash_1, None), (c_expected_hash_2, None)]
    assert set(r_queued_hashes) == set(r_expected_hashes_tuple)
