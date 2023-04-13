import os

import pytest
import vcr
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from utils.fetcher import SearchcasterFetcher, WarpcastUserFetcher
from utils.models import Base, Location, User
from utils.utils import get_user_by_fid, save_objects, update_users_warpcast

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")
yaml_filename = "users_test.yaml"

# Define a cassette library directory to store API response recordings
my_vcr = vcr.VCR(
    cassette_library_dir="cassettes",
    record_mode="once",  # Record the API call once and reuse the response
    filter_headers=["Authorization"],  # Filter out any sensitive headers
)


@pytest.fixture(scope="module")
def test_engine():
    """
    Creates and returns a SQLAlchemy database engine for testing.
    """
    test_engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(test_engine)
    return test_engine


@pytest.fixture(scope="module")
def test_sessionmaker(test_engine):
    """
    Returns a SQLAlchemy session maker for testing.

    :param test_engine: The database engine to use for the session maker.
    :return: A SQLAlchemy session maker for testing.
    """
    return sessionmaker(bind=test_engine)


@pytest.mark.asyncio
@my_vcr.use_cassette(yaml_filename)
async def test_warpcast_and_searchcaster_integration(test_sessionmaker):
    # Create an instance of the WarpcastUserFetcher and fetch data from the API
    warpcast_user_fetcher = WarpcastUserFetcher(key=warpcast_hub_key)
    fetched_users = warpcast_user_fetcher.fetch_data(partial=True)

    # # Check if the fetched data is correctly formed
    data = warpcast_user_fetcher.get_models(fetched_users)
    location_list = [x for x in data if isinstance(x, Location)]
    user_list = [x for x in data if isinstance(x, User)]

    # Insert data
    with test_sessionmaker() as session:
        initial_user_count = session.query(User).count()
        initial_location_count = session.query(Location).count()

        save_objects(session, location_list)
        update_users_warpcast(session, user_list)

        # Assert len(location_list) == len(fetched_users["locations"])
        after_insert_user_count = session.query(User).count()
        after_insert_location_count = session.query(Location).count()

        assert after_insert_user_count - initial_user_count == len(user_list)
        assert after_insert_location_count - initial_location_count == len(
            location_list
        )

        # TODO: assert the fields are actually correct

        # Select 10 users, highest fid
        searchcaster_test_users = (
            session.query(User).order_by(User.fid.desc()).limit(10).all()
        )

        test_usernames = [user.username for user in searchcaster_test_users]

    # Create an instance of the SearchcasterFetcher and fetch data from the API
    searchcaster_fetcher = SearchcasterFetcher()

    # Fetch user data from the Searchcaster API
    user_data_list = await searchcaster_fetcher.fetch_data(test_usernames)

    # Check if the transformation occurred
    with test_sessionmaker() as session:
        users = session.query(User).filter(User.username.in_(test_usernames)).all()
        updated_users = searchcaster_fetcher.get_models(users, user_data_list)

        for updated_user in updated_users:
            user = get_user_by_fid(session, updated_user.fid)
            assert user.farcaster_address == updated_user.farcaster_address
            assert user.external_address == updated_user.external_address
            assert user.registered_at == updated_user.registered_at

            # Check if the transformation occurred
            assert user.farcaster_address is not None
            assert user.registered_at >= 0

        # Additional checks for robust testing
        assert len(user_data_list) == len(test_usernames)
        assert len(updated_users) == len(users)
        assert all(user.username in test_usernames for user in updated_users)
