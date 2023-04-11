import os

import pytest
import vcr
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_utils import create_database, database_exists

from utils.fetcher import AlchemyTransactionFetcher, ExternalAddress
from utils.models import Base, ERC1155Metadata, EthTransaction
from utils.utils import save_objects

# Define a cassette library directory to store API response recordings
my_vcr = vcr.VCR(
    cassette_library_dir="cassettes",
    record_mode="once",  # Record the API call once and reuse the response
    filter_headers=["Authorization"],  # Filter out any sensitive headers
)
yaml_filename = "eth_test.yaml"


@pytest.fixture(scope="module")
def test_database_url():
    """
    Returns the URL for an in-memory SQLite database.

    :return: The URL for an in-memory SQLite database.
    """
    return "sqlite://"


@pytest.fixture(scope="module")
def test_engine(test_database_url):
    """
    Creates and returns a SQLAlchemy database engine for testing.

    :param test_database_url: The URL of the test database.
    :return: A SQLAlchemy database engine for testing.
    """
    # Create the database and engine if they don't exist
    if not database_exists(test_database_url):
        create_database(test_database_url)
    test_engine = create_engine(test_database_url)
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


def count_external_address_rows(session: Session) -> int:
    """
    Counts the number of rows in the ExternalAddress table.

    :param session: A SQLAlchemy session.
    :return: The number of rows in the ExternalAddress table.
    """
    return session.query(ExternalAddress).count()


@pytest.mark.asyncio
@my_vcr.use_cassette(yaml_filename)
async def test_alchemy_transaction(test_sessionmaker):
    # Setup
    fetcher = AlchemyTransactionFetcher(key=os.getenv("ALCHEMY_API_KEY"))

    addresses_string = [
        "0x0fd5c2cf64fe2d02f309f616fa362355c91470e7",
        "0xd7029bdea1c17493893aafe29aad69ef892b8ff2",
        "0xd26aca62da2564e591a58105c4431aa5edf2ad8a",
    ]

    # Fetch data
    data = await fetcher.fetch_data(addresses_string)

    # Test fetch_data results
    assert isinstance(data, list)
    assert len(data) > 0
    for transaction in data:
        assert "hash" in transaction

    # Get models and test results
    transactions = fetcher.get_models(data)

    txs = [tx for tx in transactions if isinstance(tx, EthTransaction)]
    metadata = [tx for tx in transactions if isinstance(tx, ERC1155Metadata)]

    with test_sessionmaker() as session:
        # Test if data is saved correctly to the database
        save_objects(session, transactions)
        assert len(txs) == session.query(EthTransaction).count()
        assert len(metadata) == session.query(ERC1155Metadata).count()

        # Test EthTransaction objects
        for tx in txs:
            db_transaction = (
                session.query(EthTransaction)
                .filter_by(unique_id=tx.unique_id)
                .one_or_none()
            )
            assert db_transaction is not None
            assert db_transaction.hash == tx.hash

        # Test ERC1155Metadata objects
        for md in metadata:
            print(md.eth_transaction_hash, md.token_id, md.value)
            db_metadata = (
                session.query(ERC1155Metadata)
                .filter_by(
                    eth_transaction_hash=md.eth_transaction_hash,
                    token_id=md.token_id,
                    value=md.value,
                )
                .one_or_none()
            )

            assert db_metadata is not None
            assert db_metadata.eth_transaction_hash == md.eth_transaction_hash
            assert db_metadata.token_id == md.token_id
            assert db_metadata.value == md.value
