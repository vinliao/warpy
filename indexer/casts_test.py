# import datetime
# import os

# import pytest
# import vcr
# from dotenv import load_dotenv
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker

# from utils.fetcher import WarpcastCastFetcher
# from utils.models import Base, Cast
# from utils.utils import save_casts_to_sqlite

# load_dotenv()
# warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")
# yaml_filename = "casts_test.yaml"

# # Define a cassette library directory to store API response recordings
# my_vcr = vcr.VCR(
#     cassette_library_dir="cassettes",
#     record_mode="once",  # Record the API call once and reuse the response
#     filter_headers=["Authorization"],  # Filter out any sensitive headers
# )


# @pytest.fixture(scope="module")
# def test_engine():
#     """
#     Creates and returns a SQLAlchemy database engine for testing.
#     """
#     test_engine = create_engine("sqlite:///:memory:")
#     Base.metadata.create_all(test_engine)
#     return test_engine


# @pytest.fixture(scope="module")
# def test_sessionmaker(test_engine):
#     """
#     Returns a SQLAlchemy session maker for testing.

#     :param test_engine: The database engine to use for the session maker.
#     :return: A SQLAlchemy session maker for testing.
#     """
#     return sessionmaker(bind=test_engine)


# @pytest.mark.asyncio
# @my_vcr.use_cassette(yaml_filename)
# async def test_fetch_and_insert_casts(test_sessionmaker):
#     # Create an instance of the WarpcastCastFetcher and fetch data from the API
#     warpcast_fetcher = WarpcastCastFetcher(key=warpcast_hub_key)

#     # Replace this with your desired timestamp for testing
#     now = datetime.datetime.now()
#     one_day = datetime.timedelta(days=1)
#     latest_timestamp = int((now + one_day).timestamp() * 1000)
#     fetched_data = warpcast_fetcher.fetch_data(latest_timestamp)

#     # Check if the fetched data is correctly formed
#     models = warpcast_fetcher.get_models(fetched_data)

#     # Check if data is inserted correctly
#     with test_sessionmaker() as session:
#         initial_count = session.query(Cast).count()

#         save_casts_to_sqlite(session, models, latest_timestamp)

#         after_insert_count = session.query(Cast).count()
#         assert after_insert_count == initial_count + len(models)

#         inserted_casts = (
#             session.query(Cast).filter(Cast.timestamp >= latest_timestamp).all()
#         )
#         assert len(inserted_casts) == len(models)


# @pytest.mark.asyncio
# @my_vcr.use_cassette(yaml_filename)
# async def test_no_replacement_of_old_casts(test_sessionmaker):
#     # Create an instance of the WarpcastCastFetcher and fetch data from the API
#     warpcast_fetcher = WarpcastCastFetcher(key=warpcast_hub_key)

#     # Replace this with your desired timestamp for testing
#     now = datetime.datetime.now()
#     one_day = datetime.timedelta(days=1)
#     latest_timestamp = int((now + one_day).timestamp() * 1000)
#     fetched_data = warpcast_fetcher.fetch_data(latest_timestamp)

#     # Insert a test cast with an older timestamp into the database
#     old_cast = Cast(
#         hash="0xd3f34f3fef9f1a01679f85da438aa2ef88088629",
#         thread_hash="0xc9ab9cdc5ebd317eeeaedad71c0dfc066fbe68bd",
#         text="gm pedro!",
#         timestamp=latest_timestamp - 2000,
#         author_fid=5777,
#         parent_hash="0xed0cb3fee3aedf7da7360963684e583c233a5c8f",
#     )
#     with test_sessionmaker() as session:
#         session.add(old_cast)
#         session.commit()

#         models = warpcast_fetcher.get_models(fetched_data)
#         save_casts_to_sqlite(session, models, latest_timestamp)

#         retrieved_old_cast = (
#             session.query(Cast).filter(Cast.hash == old_cast.hash).one()
#         )
#         assert retrieved_old_cast.hash == old_cast.hash


# @pytest.mark.asyncio
# @my_vcr.use_cassette(yaml_filename)
# async def test_only_insert_newer_data(test_sessionmaker):
#     # Create an instance of the WarpcastCastFetcher and fetch data from the API
#     warpcast_fetcher = WarpcastCastFetcher(key=warpcast_hub_key)

#     # Replace this with your desired timestamp for testing
#     now = datetime.datetime.now()
#     one_day = datetime.timedelta(days=1)
#     latest_timestamp = int((now + one_day).timestamp() * 1000)
#     fetched_data = warpcast_fetcher.fetch_data(latest_timestamp)

#     # Insert a test cast with the latest timestamp into the database
#     latest_cast = Cast(
#         hash="0x0f095772c6d37d231efc6ea0259361a196a37dbb",
#         thread_hash="0xc9ab9cdc5ebd317eeeaedad71c0dfc066fbe68bd",
#         text="gm pedro!",
#         timestamp=latest_timestamp,
#         author_fid=5777,
#         parent_hash="0xed0cb3fee3aedf7da7360963684e583c233a5c8f",
#     )
#     with test_sessionmaker() as session:
#         session.add(latest_cast)
#         session.commit()

#         models = warpcast_fetcher.get_models(fetched_data)
#         save_casts_to_sqlite(session, models, latest_timestamp)

#         inserted_casts = (
#             session.query(Cast).filter(Cast.timestamp > latest_timestamp).all()
#         )
#         assert len(inserted_casts) == len(
#             [cast for cast in models if cast.timestamp > latest_timestamp]
#         )
