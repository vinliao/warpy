import os

import pytest
import vcr
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from indexer.eth import AlchemyTransactionFetcher
from indexer.ensdata import EnsdataFetcher
from indexer.users import SearchcasterFetcher, WarpcastUserFetcher

from utils.models import Base, ENSData, EthTransaction, Location, User
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
    data = warpcast_user_fetcher.fetch(partial=True)
    location_list = [x for x in data if isinstance(x, Location)]
    user_list = [x for x in data if isinstance(x, User)]

    for user in user_list:
        assert user.fid is not None
        assert user.registered_at is -1
        assert user.generated_farcaster_address == ""
        # generated_farcaster_address it shouldn't be null, but it is empty
        # that's why it's non-nullable with empty value for nwo

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

    # Check if the transformation occurred
    with test_sessionmaker() as session:
        users = session.query(User).all()
        searchcaster_fetcher = SearchcasterFetcher(users)

        # Fetch user data from the Searchcaster API
        updated_users = await searchcaster_fetcher.fetch()
        save_objects(session, updated_users)

        # Assert whether the values from searchcaster are updated
        for updated_user in updated_users:
            user = get_user_by_fid(session, updated_user.fid)
            assert (
                user.generated_farcaster_address
                == updated_user.generated_farcaster_address
            )
            assert user.address == updated_user.address
            assert user.registered_at == updated_user.registered_at

            # Check if the transformation occurred
            assert user.generated_farcaster_address is not None
            assert user.registered_at >= 0

    # ==============================================================
    # =========================== Ensdata ==========================
    # ==============================================================
    with test_sessionmaker() as session:
        users = session.query(User).all()

        addresses = [user.address for user in users]
        addresses = [address for address in addresses if address is not None]
        ensdata_fetcher = EnsdataFetcher(addresses=addresses)
        data = await ensdata_fetcher.fetch()
        save_objects(session, data)

        users_with_external_address = session.query(User).filter(
            User.address.isnot(None)
        )

        external_addresses = session.query(ENSData).all()

        # assert whether the ens information of all external address is fetched
        # TODO: probably there's a more robust way to do it
        assert len(external_addresses) == users_with_external_address.count()

    # ==============================================================
    # ==================== Ethereum transactions ===================
    # ==============================================================
    alchemy_api_key = os.getenv("ALCHEMY_API_KEY")

    with test_sessionmaker() as session:
        addresses_object = session.query(ENSData).all()
        # addresses = [address.address for address in addresses_object]
        # addresses_blocknum = [(address, 0) for address in addresses]
        addresses_blocknum = [(address.address, 0) for address in addresses_object]

        batch_size = 3
        for i in range(0, len(addresses_blocknum), batch_size):
            batch = addresses_blocknum[i : i + batch_size]  # noqa: E203
            alchemy_fetcher = AlchemyTransactionFetcher(
                key=alchemy_api_key, addresses_blocknum=batch
            )
            data = await alchemy_fetcher.fetch()
            save_objects(session, data)

        all_transactions = session.query(EthTransaction).all()
        # unique_addresses = set(
        #     [transaction.address_external for transaction in all_transactions]
        # )

        # assert whether the eth transaction of all external address is fetched
        # TODO: assert whether the eth fetcher only fetches stuff above the
        #   block number of the latest transaction of user
        # TODO: probably there's a more robust way to do it
        # assert len(unique_addresses) == len(addresses)
        # TOOD: need better asserts!
        assert len(all_transactions) > 1

    # TODO: test the user-transaction association table
