import glob
import os
import random
import time
from typing import Any, Dict, Generator, Hashable

import pandas as pd
import pytest

from src import indexer


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


# ======================================================================================
# integration tests
# ======================================================================================


# @pytest.mark.asyncio
# async def test_user_integration() -> None:
#     """
#     What's tested:
#     - both user fetcher fetches and extracts properly (returns pydantic model)
#     - merger merges properly (warpcast+searchcaster and parquet+ndjson)
#     """

#     def r_ints(n: int) -> List[int]:
#         return [random.randint(1, 10000) for i in range(n)]

#     def make_fids(df1: pd.DataFrame, df2: pd.DataFrame) -> List[int]:
#         return list(set(df1["fid"].tolist() + df2["fid"].tolist()))

#     async def fetch_fids(fids: List[int]) -> None:
#         w_urls = [indexer.UrlMaker.user_warpcast(fid=fid) for fid in fids]
#         w_data = await indexer.Fetcher.user_warpcast(w_urls)
#         indexer.json_append("testdata/user_warpcast.ndjson", w_data["users"])

#         s_urls = [indexer.UrlMaker.user_searchcaster(fid=fid) for fid in fids]
#         s_data = await indexer.Fetcher.user_searchcaster(s_urls)
#         indexer.json_append("testdata/user_searchcaster.ndjson", s_data["users"])

#     warpcast_file = "testdata/user_warpcast.ndjson"
#     searchcaster_file = "testdata/user_searchcaster.ndjson"
#     user_file = "testdata/users.parquet"

#     # Test merge warpcast with searchcaster (first batch)
#     await fetch_fids(r_ints(10) + [3])  # add dwr for "complete profile"
#     w_df_1 = indexer.read_ndjson(warpcast_file)
#     s_df_1 = indexer.read_ndjson(searchcaster_file)
#     fids_1 = make_fids(w_df_1, s_df_1)
#     df_1 = indexer.Merger.user(warpcast_file, searchcaster_file, user_file)
#     assert isinstance(w_df_1, pd.DataFrame)
#     assert isinstance(s_df_1, pd.DataFrame)
#     assert isinstance(df_1, pd.DataFrame)
#     assert len(df_1) == len(w_df_1) == len(s_df_1)
#     assert set(df_1["fid"]).issubset(set(fids_1))
#     assert len(df_1.columns) == len(w_df_1.columns) + len(s_df_1.columns) - 1

#     # Test merge local parquet with incoming data (second batch)
#     os.remove(warpcast_file)
#     os.remove(searchcaster_file)
#     df_1.to_parquet(user_file)
#     await fetch_fids(r_ints(5))
#     w_df_2 = indexer.read_ndjson(warpcast_file)
#     s_df_2 = indexer.read_ndjson(searchcaster_file)
#     fids_2 = make_fids(w_df_2, s_df_2)
#     assert isinstance(w_df_2, pd.DataFrame)
#     assert isinstance(s_df_2, pd.DataFrame)
#     assert len(w_df_2) == len(s_df_2)

#     new_df = indexer.Merger.user(warpcast_file, searchcaster_file, user_file)
#     assert sorted(new_df["fid"].tolist()) == sorted(fids_1 + fids_2)
#     users_2 = [indexer.User(**stringify_keys(x)) for x in new_df.to_dict("records")]
#     assert all(isinstance(user, indexer.User) for user in users_2)


# @pytest.mark.asyncio
# async def test_cast_reaction_integration() -> None:
#     """
#     What's tested:
#     - cast fetcher, merger
#     - reaction fetcher (from fetched cast hashes), merger
#     """

#     cast_limit = 50

#     c_queued_file = "testdata/cast_warpcast.ndjson"
#     c_data_file = "testdata/casts.parquet"
#     r_queued_file = "testdata/reaction_warpcast.ndjson"
#     r_data_file = "testdata/reactions.parquet"

#     c_url_1 = indexer.UrlMaker.cast_warpcast(limit=cast_limit)
#     c_data_1 = await indexer.Fetcher.cast_warpcast(c_url_1)
#     indexer.json_append(c_queued_file, c_data_1["casts"])

#     c_df_1 = indexer.Merger.cast(c_queued_file, c_data_file)
#     assert isinstance(c_df_1, pd.DataFrame)
#     assert len(c_df_1) == cast_limit
#     # TODO: maybe more asserts here

#     c_df_1.to_parquet(c_data_file)
#     os.remove(c_queued_file)

#     # test merge with local parquet (second batch)
#     c_url_2 = indexer.UrlMaker.cast_warpcast(
#         limit=cast_limit, cursor=c_data_1["next_cursor"]
#     )
#     c_data_2 = await indexer.Fetcher.cast_warpcast(c_url_2)
#     indexer.json_append(c_queued_file, c_data_2["casts"])
#     c_df_2 = indexer.Merger.cast(c_queued_file, c_data_file)
#     assert isinstance(c_df_2, pd.DataFrame)
#     assert len(c_df_2) == cast_limit * 2
#     assert len(set(list(c_df_2["hash"]))) == cast_limit * 2
#     cs = [indexer.CastWarpcast(**stringify_keys(x)) for x in c_df_2.to_dict("records")]
#     assert all(isinstance(cast, indexer.CastWarpcast) for cast in cs)

#     # test fetching reactions (first batch)
#     r_hashes = list(c_df_1["hash"])
#     r_urls = [indexer.UrlMaker.reaction_warpcast(castHash=hash) for hash in r_hashes]
#     r_data = await indexer.Fetcher.reaction_warpcast(r_urls)
#     for cast in r_data:
#         indexer.json_append(r_queued_file, cast["reactions"])
#     r_df = indexer.Merger.reaction(r_queued_file, r_data_file)
#     assert isinstance(r_df, pd.DataFrame)
#     assert set(r_df["target_hash"]).issubset(set(r_hashes))
#     rs = [
#         indexer.ReactionWarpcast(**stringify_keys(x)) for x in r_df.to_dict("records")
#     ]
#     assert all(isinstance(reaction, indexer.ReactionWarpcast) for reaction in rs)
#     # TODO: maybe more asserts here


@pytest.mark.asyncio
async def test_user_integration() -> None:
    """
    What's tested:
    - queue_producer (users)
    - batch_fetcher (users)
    """

    # ==================================================================================
    # users queue producer
    # ==================================================================================

    fids = [random.randint(1, 10000) for i in range(1000)]
    fids = list(set(fids))

    wf = "testdata/user_warpcast.ndjson"
    sf = "testdata/user_searchcaster.ndjson"
    ef = "testdata/user_ensdata.ndjson"
    f = "testdata/users.parquet"

    fids_dict = [{"fid": fid} for fid in fids]
    indexer.json_append(wf, fids_dict)
    w_queued_fids = indexer.QueueProducer.user_warpcast(wf, f)
    highest_network_fid = max(w_queued_fids)
    all_fids = set(range(1, highest_network_fid + 1))
    assert set(w_queued_fids) == set.difference(all_fids, set(fids))

    half_fids = random.sample(fids, len(fids) // 2)
    half_fids_dict = [{"fid": fid} for fid in half_fids]
    indexer.json_append(sf, half_fids_dict)
    s_queued_fids = indexer.QueueProducer.user_searchcaster(wf, sf)
    assert set(s_queued_fids) == set.difference(set(fids), set(half_fids))

    # ==================================================================================
    # users batch fetcher
    # ==================================================================================

    os.remove(wf)
    os.remove(sf)
    batch_fids = random.sample(fids, 10) + [3]  # add dwr for non-empty address field
    batch_fids = list(set(batch_fids))
    await indexer.BatchFetcher.user_warpcast(fids=batch_fids, n=5, out=wf)
    await indexer.BatchFetcher.user_searchcaster(fids=batch_fids, n=5, out=sf)
    addrs = indexer.get_addresses(sf)
    await indexer.BatchFetcher.user_ensdata(addrs=addrs, n=5, out=ef)
    w_df = indexer.read_ndjson(wf)
    s_df = indexer.read_ndjson(sf)
    e_df = indexer.read_ndjson(ef)
    assert abs(len(w_df) - len(s_df)) <= 2  # 2 item fetch error tolerance
    s_with_address = s_df[s_df["address"].notna()]
    assert abs(len(s_with_address) - len(e_df)) <= 2  # 2 item fetch error tolerance

    # ==================================================================================
    # user merger
    # ==================================================================================

    ws_fids = set.intersection(set(w_df["fid"]), set(s_df["fid"]))
    ws_fids_half = random.sample(list(ws_fids), len(ws_fids) // 2)
    ws_df = indexer.Merger.user(warpcast_file=wf, searchcaster_file=sf, user_file=f)
    assert isinstance(ws_df, pd.DataFrame)
    assert set(ws_df["fid"]) == ws_fids
    ws_df = ws_df[ws_df["fid"].isin(ws_fids_half)]
    ws_df.to_parquet(f)
    ws_df = indexer.read_parquet(f)
    assert set(ws_df["fid"]) == set(ws_fids_half)
    ws_df = indexer.Merger.user(warpcast_file=wf, searchcaster_file=sf, user_file=f)
    assert isinstance(ws_df, pd.DataFrame)
    assert set(ws_df["fid"]) == ws_fids
    assert len(ws_df.columns) == len(w_df.columns) + len(s_df.columns) - 1  # fid dedup


@pytest.mark.asyncio
async def test_cast_reaction_integration() -> None:
    pass

    # ==================================================================================
    # cast and reactions
    # ==================================================================================

    # def random_hash(n: int) -> str:
    #     return "".join(random.choices(string.ascii_letters + string.digits, k=n))

    # c_one_week_ago = indexer.TimeConverter.ago_to_unixms(factor="weeks", units=1)
    # c_expected_hash_1 = random_hash(6)
    # c_expected_hash_2 = random_hash(6)
    # c_dummy_casts = [
    #     {"hash": c_expected_hash_1, "timestamp": c_one_week_ago},
    #     {
    #         "hash": c_expected_hash_2,
    #         "timestamp": indexer.TimeConverter.ago_to_unixms(factor="weeks", units=2),
    #     },
    #     {
    #         "hash": random_hash(6),
    #         "timestamp": indexer.TimeConverter.ago_to_unixms(factor="weeks", units=3),
    #     },
    #     {
    #         "hash": random_hash(6),
    #         "timestamp": indexer.TimeConverter.ago_to_unixms(factor="weeks", units=4),
    #     },
    # ]

    # c_df = pd.DataFrame(c_dummy_casts)
    # c_df.to_parquet(c_file)

    # c_queued_max_timestamp_1 = indexer.QueueProducer.cast_warpcast(c_file)
    # assert c_queued_max_timestamp_1 == 0

    # c_queued_max_timestamp_2 = indexer.QueueProducer.cast_warpcast(c_file)
    # assert c_queued_max_timestamp_2 == c_one_week_ago

    # r_one_day_t = indexer.TimeConverter.to_ms(factor="days", units=1)
    # r_t = indexer.TimeConverter.ago_to_unixms(factor="weeks", units=2) - r_one_day_t
    # r_queue_producer = indexer.QueueProducer.reaction_warpcast
    # r_queued_hashes = r_queue_producer(t_from=r_t, data_file=c_file)
    # r_expected_hashes_tuple = [(c_expected_hash_1, None), (c_expected_hash_2, None)]
    # assert set(r_queued_hashes) == set(r_expected_hashes_tuple)


# ======================================================================================
# unit tests
# ======================================================================================


def test_time_converter() -> None:
    assert abs(indexer.TimeConverter.ms_now() - int(round(time.time() * 1000))) < 10

    # Test the conversion to milliseconds for all factors
    assert indexer.TimeConverter.to_ms("minutes", 1) == 60 * 1000
    assert indexer.TimeConverter.to_ms("hours", 2) == 2 * 60 * 60 * 1000
    assert indexer.TimeConverter.to_ms("days", 1) == 24 * 60 * 60 * 1000
    assert indexer.TimeConverter.to_ms("weeks", 1) == 7 * 24 * 60 * 60 * 1000
    assert indexer.TimeConverter.to_ms("months", 1) == 30 * 24 * 60 * 60 * 1000
    assert indexer.TimeConverter.to_ms("years", 1) == 365 * 24 * 60 * 60 * 1000

    # Test the conversion from milliseconds for all factors
    assert indexer.TimeConverter.from_ms("minutes", 60 * 1000) == 1
    assert indexer.TimeConverter.from_ms("hours", 2 * 60 * 60 * 1000) == 2
    assert indexer.TimeConverter.from_ms("days", 24 * 60 * 60 * 1000) == 1
    assert indexer.TimeConverter.from_ms("weeks", 7 * 24 * 60 * 60 * 1000) == 1
    assert indexer.TimeConverter.from_ms("months", 30 * 24 * 60 * 60 * 1000) == 1
    assert indexer.TimeConverter.from_ms("years", 365 * 24 * 60 * 60 * 1000) == 1

    # Test the conversion of time ago to UNIX milliseconds for all factors
    factors = ["minutes", "hours", "days", "weeks", "months", "years"]
    for factor in factors:
        ms_ago = indexer.TimeConverter.ago_to_unixms(factor, 1)
        now = indexer.TimeConverter.ms_now()
        ms = indexer.TimeConverter.to_ms(factor, 1)
        assert abs(ms_ago - (now - ms)) < 10

    # Test the conversion of UNIX milliseconds to time ago for all factors
    for factor in factors:
        ms = indexer.TimeConverter.ms_now() - indexer.TimeConverter.to_ms(factor, 1)
        assert abs(indexer.TimeConverter.unixms_to_ago(factor, ms) - 1) < 0.01
