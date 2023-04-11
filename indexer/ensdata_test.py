import pytest
import vcr
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_utils import create_database, database_exists

from utils.fetcher import EnsdataFetcher, ExternalAddress
from utils.models import Base, ExternalAddress
from utils.utils import save_objects

# Define a cassette library directory to store API response recordings
my_vcr = vcr.VCR(
    cassette_library_dir="cassettes",
    record_mode="once",  # Record the API call once and reuse the response
    filter_headers=["Authorization"],  # Filter out any sensitive headers
)


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


# Define the test function for inserting data and checking for merges
@pytest.mark.asyncio
# Use a cassette to record and replay the API response
@my_vcr.use_cassette("ensdata_test.yaml")
async def test_insert_data_and_check_merge(test_sessionmaker):
    # Create an instance of the EnsdataFetcher and fetch data from the API
    ensdata_fetcher = EnsdataFetcher()
    addresses = [
        "0x0fd5c2cf64fe2d02f309f616fa362355c91470e7",
        "0xd7029bdea1c17493893aafe29aad69ef892b8ff2",
        "0xd26aca62da2564e591a58105c4431aa5edf2ad8a",
        "0x4685bb0077179b45095a3c2a175acd9b3cd59d1c",
        "0x888b4bee0f8dff4e7269811e066a1de6438cbf7a",
    ]
    fetched_data = await ensdata_fetcher.fetch_data(addresses)

    # Convert the fetched data into model instances and save them to the database
    models = ensdata_fetcher.get_models(fetched_data)
    session = test_sessionmaker()
    initial_count = count_external_address_rows(session)
    save_objects(session, models)
    after_insert_count = count_external_address_rows(session)

    # Assert that the number of rows in the ExternalAddress table increased by the number of models inserted
    assert after_insert_count >= initial_count + len(models)

    # Re-insert the same data and ensure the count doesn't change
    save_objects(session, models)
    after_second_insert_count = count_external_address_rows(session)
    assert after_second_insert_count == after_insert_count

    # Close the session
    session.close()
