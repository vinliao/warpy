import os

import pytest
import vcr
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from utils.fetcher import (
    SearchcasterFetcher,
    WarpcastUserFetcher,
    EnsdataFetcher,
    AlchemyTransactionFetcher,
)
from utils.models import Base, Location, User, ExternalAddress, EthTransaction
from utils.utils import get_user_by_fid, save_objects, update_users_warpcast

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")

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
@my_vcr.use_cassette("user_integration.yaml")
async def test_integration_users(test_sessionmaker):
    """
    Integration test for the user fetcher.
    - Fetches from Warpcast, assert not empty
    - Fetches additional data from Searchcaster, assert data is updated
    - Fetches ENS data data from Ensdata, assert length of ens data is
        equal to the number of users with external address
    - Fetches Ethereum transactions from Alchemy, assert length of
        unique external address is equal to the number
        of users with external address, this means users with empty transaction
        must contain empty Eth transaction

    TODO: currently, asserts are asserting the length, it should also be
    asserting the fielsd of the fetched data against the db
    """

    # ==============================================================
    # ========================== Warpcast ==========================
    # ==============================================================

    warpcast_user_fetcher = WarpcastUserFetcher(key=warpcast_hub_key)
    fetched_users = warpcast_user_fetcher.fetch_data(partial=True)

    data = warpcast_user_fetcher.get_models(fetched_users)
    location_list = [x for x in data if isinstance(x, Location)]
    user_list = [x for x in data if isinstance(x, User)]

    # Insert data
    with test_sessionmaker() as session:
        initial_user_count = session.query(User).count()
        initial_location_count = session.query(Location).count()

        save_objects(session, location_list)
        update_users_warpcast(session, user_list)

        after_insert_user_count = session.query(User).count()
        after_insert_location_count = session.query(Location).count()

        # asserting whether the initial user is successful
        assert after_insert_user_count - initial_user_count == len(user_list)
        assert after_insert_location_count - initial_location_count == len(
            location_list
        )

        # TODO: assert fields value

    # ==============================================================
    # ======================== Searchcaster ========================
    # ==============================================================
    searchcaster_fetcher = SearchcasterFetcher()

    # Check if the transformation occurred
    with test_sessionmaker() as session:
        users = session.query(User).all()
        usernames = [user.username for user in users]

        # Fetch user data from the Searchcaster API
        user_data_list = await searchcaster_fetcher.fetch_data(usernames)
        updated_users = searchcaster_fetcher.get_models(users, user_data_list)

        save_objects(session, updated_users)

        # assert whether the values from searchcaster is updated
        for updated_user in updated_users:
            user = get_user_by_fid(session, updated_user.fid)
            assert user.farcaster_address == updated_user.farcaster_address
            assert user.external_address == updated_user.external_address
            assert user.registered_at == updated_user.registered_at

            # Check if the transformation occurred
            assert user.farcaster_address is not None
            assert user.registered_at >= 0

    # ==============================================================
    # =========================== Ensdata ==========================
    # ==============================================================
    ensdata_fetcher = EnsdataFetcher()
    with test_sessionmaker() as session:
        users = session.query(User).all()

        addresses = [user.external_address for user in users]
        addresses = [address for address in addresses if address is not None]

        data = await ensdata_fetcher.fetch_data(addresses)
        models = ensdata_fetcher.get_models(data)
        save_objects(session, models)

        users_with_external_address = session.query(User).filter(
            User.external_address.isnot(None)
        )

        external_addresses = session.query(ExternalAddress).all()

        # assert whether the ens information of all external address is fetched
        # TODO: probably there's a more robust way to do it
        assert len(external_addresses) == users_with_external_address.count()

    # ==============================================================
    # ==================== Ethereum transactions ===================
    # ==============================================================
    alchemy_api_key = os.getenv("ALCHEMY_API_KEY")
    alchemy_fetcher = AlchemyTransactionFetcher(key=alchemy_api_key)

    with test_sessionmaker() as session:
        addresses_object = session.query(ExternalAddress).all()
        addresses = [address.address for address in addresses_object]

        batch_size = 3
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i : i + batch_size]  # noqa: E203
            data = await alchemy_fetcher.fetch_data(batch)
            models = alchemy_fetcher.get_models(data)

            save_objects(session, models)

        all_transactions = session.query(EthTransaction).all()
        unique_addresses = set(
            [transaction.address_external for transaction in all_transactions]
        )

        # assert whether the eth transaction of all external address is fetched
        # TODO: probably there's a more robust way to do it
        assert len(unique_addresses) == len(addresses)
