from sqlalchemy.orm import aliased
from dotenv import load_dotenv
import os
import requests
import time
from utils.models import User, Location, Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from utils.fetcher_new import WarpcastUserFetcher, SearchcasterFetcher

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


def delete_unregistered_users(session):
    # delete users where registered_at is -1
    # -1 means the user didn't register properly, need better way to handle this
    session.query(User).filter(User.registered_at == -1).delete()
    session.commit()


def save_locations(session, location_list):
    for location in location_list:
        session.merge(location)
    session.commit()


def update_users_warpcast(session, user_list):
    preserved_fields_list = [
        'farcaster_address', 'registered_at', 'external_address', '_sa_instance_state']
    for user in user_list:
        existing_user = session.query(User).filter_by(
            fid=user.fid).one_or_none()

        if existing_user:
            preserved_fields = {
                key: value for key, value in user.__dict__.items() if key not in preserved_fields_list}
            session.query(User).filter_by(
                fid=user.fid).update(preserved_fields)
        else:
            session.add(user)
    session.commit()


def update_user_searchcaster(session, user_list):
    for user in user_list:
        session.merge(user)
    session.commit()


async def main(engine: Engine):
    warpcast_user_fetcher = WarpcastUserFetcher(warpcast_hub_key)
    warpcast_users = warpcast_user_fetcher.fetch_data()
    user_list, location_list = warpcast_user_fetcher.get_models(
        warpcast_users)

    with sessionmaker(bind=engine)() as session:
        save_locations(session, location_list)
        update_users_warpcast(session, user_list)

        users = session.query(User).filter_by(registered_at=-1).all()
        usernames = [user.username for user in users]
        searchcaster_fetcher = SearchcasterFetcher()
        user_data_list = await searchcaster_fetcher.fetch_data(usernames)
        new_users = searchcaster_fetcher.get_models(users, user_data_list)
        update_user_searchcaster(session, new_users)
        delete_unregistered_users(session)
