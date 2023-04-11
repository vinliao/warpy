from utils.utils import save_objects
from utils.models import Base, ExternalAddress, User, Reaction
from utils.fetcher import EnsdataFetcher
from sqlalchemy_utils import create_database, database_exists
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
import pytest
import vcr
from utils.fetcher import EnsdataFetcher, WarpcastReactionFetcher
from sqlalchemy.orm import Session, sessionmaker
from dotenv import load_dotenv
import os


load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")
yaml_filename = "reactions_test.yaml"
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


@pytest.mark.asyncio
@my_vcr.use_cassette("test_fetch_data_and_get_models.yaml")
async def test_fetch_data_and_get_models(test_sessionmaker):
    # Initialize the WarpcastReactionFetcher object
    fetcher = WarpcastReactionFetcher(key=warpcast_hub_key)

    # Sample input for the test
    cast_hashes = [
        "0xb18ffb5f38e3cf147c41f6f0ab2bb885c0b31bd5",
        "0xdf816f709a91295505defdba12a9f264ebfde392",
        "0x8a332194ba502f85d07daf598d2a2eac1dd397a0",
        "0x8e90845f90923eeff28007dcd83f3027c554387a",
    ]

    # Call the fetch_data method
    data = await fetcher.fetch_data(cast_hashes)

    # Call the get_models method
    reactions = fetcher.get_models(data)

    # Check if the resulting reactions are of the correct type
    assert all(isinstance(reaction, Reaction) for reaction in reactions)

    with test_sessionmaker() as session:
        save_objects(session, reactions)
        assert len(reactions) == session.query(Reaction).count()

    # TODO: add more specific assertions about the resulting reactions
