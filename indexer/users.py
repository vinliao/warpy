from dotenv import load_dotenv
import os
from utils.models import User
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from utils.fetcher import WarpcastUserFetcher, SearchcasterFetcher
from utils.utils import save_objects
from utils.utils import update_users_warpcast

load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")


def delete_unregistered_users(session):
    """
    -1 means the user didn't register properly,
    need better way to handle this
    """
    session.query(User).filter(User.registered_at == -1).delete()
    session.commit()


def save_locations(session, location_list):
    for location in location_list:
        session.merge(location)
    session.commit()


def update_user_searchcaster(session, user_list):
    for user in user_list:
        session.merge(user)
    session.commit()


async def main(engine: Engine):
    warpcast_user_fetcher = WarpcastUserFetcher(key=warpcast_hub_key)
    warpcast_users = warpcast_user_fetcher.fetch_data()

    # TODO:
    # figure out a ways to update the user without overwriting registered_at
    # then figure out a way to return a single list instead of tuple
    user_list, location_list = warpcast_user_fetcher.get_models(warpcast_users)

    with sessionmaker(bind=engine)() as session:
        save_objects(session, location_list)
        update_users_warpcast(session, user_list)

        users = session.query(User).filter_by(registered_at=-1).all()
        usernames = [user.username for user in users]
        searchcaster_fetcher = SearchcasterFetcher()
        batch_size = 50
        for i in range(0, len(usernames), batch_size):
            batch = usernames[i : i + batch_size]
            user_data_list = await searchcaster_fetcher.fetch_data(batch)
            new_users = searchcaster_fetcher.get_models(users, user_data_list)
            save_objects(session, new_users)

        delete_unregistered_users(session)
