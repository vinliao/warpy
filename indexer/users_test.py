# Note: not done

import pytest
import os
import sqlalchemy
from indexer.users import (
    save_locations,
    update_users_warpcast,
    update_user_searchcaster,
    delete_unregistered_users,
    WarpcastUserFetcher,
    SearchcasterFetcher,
)
from utils.models import User, Location, Base
from sqlalchemy.orm import sessionmaker, Session

# Set up testing database
TEST_DATABASE_URL = "sqlite:///test.db"
test_engine = sqlalchemy.create_engine(TEST_DATABASE_URL)
SessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=test_engine)
Base.metadata.create_all(bind=test_engine)


@pytest.fixture(scope="session")
def db_session():
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture(scope="session")
def warpcast_user_fetcher():
    warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")
    return WarpcastUserFetcher(key=warpcast_hub_key)


@pytest.fixture(scope="session")
def searchcaster_fetcher():
    return SearchcasterFetcher()


def test_fetch_data_warpcast(warpcast_user_fetcher):
    data = warpcast_user_fetcher.fetch_data()
    assert isinstance(data, list)
    assert len(data) > 0


def test_fetch_data_searchcaster(searchcaster_fetcher):
    data = searchcaster_fetcher.fetch_data(["farcaster"])
    assert isinstance(data, list)
    assert len(data) > 0


def test_data_insertion_and_update(db_session, warpcast_user_fetcher, searchcaster_fetcher):
    warpcast_users = warpcast_user_fetcher.fetch_data()
    user_list, location_list = warpcast_user_fetcher.get_models(warpcast_users)

    save_locations(db_session, location_list)
    update_users_warpcast(db_session, user_list)

    users = db_session.query(User).filter_by(registered_at=-1).all()
    usernames = [user.username for user in users]
    user_data_list = searchcaster_fetcher.fetch_data(usernames)
    new_users = searchcaster_fetcher.get_models(users, user_data_list)
    update_user_searchcaster(db_session, new_users)

    delete_unregistered_users(db_session)

    total_users_in_db = db_session.query(User).count()
    assert total_users_in_db == len(user_list)

    random_user = user_list[0]
    db_user = db_session.query(User).filter_by(fid=random_user.fid).first()
    assert db_user.fid == random_user.fid
    assert db_user.username == random_user.username
    assert db_user.display_name == random_user.display_name
    assert db_user.registered_at != -1
