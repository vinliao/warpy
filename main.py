from users import *
import argparse
import os
from sqlalchemy.orm import sessionmaker
from models import Base, User, Location
from sqlalchemy import create_engine
import asyncio
import argparse
from dataclasses import asdict
import csv
from dotenv import load_dotenv

load_dotenv()
warpcast_hub_key = os.getenv('WARPCAST_HUB_KEY')

parser = argparse.ArgumentParser()

parser.add_argument('-a', '--all', action='store_true',
                    help='Refresh user data from Warpcast, Searchcaster, and ENSData')
parser.add_argument('--farcaster', action='store_true',
                    help='Refresh user data from Warpcast and Searchcaster')
parser.add_argument('--ens', action='store_true',
                    help='Refresh user data from Ensdata')

args = parser.parse_args()


def fix_user_types(user: UserClass) -> UserClass:
    user.fid = int(user.fid)
    user.verified = bool(user.verified)
    user.follower_count = int(user.follower_count)
    user.following_count = int(user.following_count)
    user.registered_at = int(
        user.registered_at) if user.registered_at else None
    return user


def set_searchcaster_data(user: UserClass, data: list[SearchcasterDataClass]) -> UserClass:
    if len(data) == 0:
        return user

    for d in data:
        if user.fid == d.fid:
            user.external_address = d.external_address
            user.farcaster_address = d.farcaster_address
            user.registered_at = d.registered_at
            break

    return user


if args.farcaster:
    users_filename = 'users.csv'
    if os.path.exists(users_filename):
        with open(users_filename, mode='r') as csv_file:
            reader = csv.reader(csv_file)
            next(reader)
            users = [UserClass(*u) for u in list(reader)]

        # Fix types of UserClass attributes
        users = [fix_user_types(u) for u in users]

        batch_size = 2
        start_index = 0
        end_index = batch_size
        while start_index < len(users):
            current_users = users[start_index:end_index]
            if len(current_users) == 0:
                break

            usernames = [u.username for u in current_users]
            searchcaster_users = asyncio.run(
                get_users_from_searchcaster(usernames))
            searchcaster_data = [extract_searchcaster_user_data(
                u) for u in searchcaster_users]

            current_users = [set_searchcaster_data(
                u, searchcaster_data) for u in current_users]
            engine = create_engine(os.getenv('PLANETSCALE_URL'))
            with sessionmaker(bind=engine)() as session:
                user_models = list(
                    map(lambda u: User(**asdict(u)), current_users))
                session.bulk_save_objects(user_models)
                session.commit()

            # open the CSV file for reading and writing
            with open(users_filename, mode='r') as csv_file:
                reader = csv.DictReader(csv_file)
                fieldnames = reader.fieldnames

                # filter out the current_users and get the remaining users
                remaining_users = [row for row in reader if row['username'] not in [
                    u.username for u in current_users]]

            # open the CSV file for writing and write the remaining users
            with open(users_filename, mode='w', newline='') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(remaining_users)

            start_index = end_index
            end_index += batch_size

    else:
        all_users = get_all_users_from_warpcast(warpcast_hub_key)
        warpcast_data = [extract_warpcast_user_data(u) for u in all_users]
        users = [UserClass(**data) for data in warpcast_data]

        warpcast_locations = [get_warpcast_location(u) for u in all_users]

        engine = create_engine(os.getenv('PLANETSCALE_URL'), echo=True)
        with sessionmaker(bind=engine)() as session:
            all_fids_in_db = [u.fid for u in session.query(User).all()]
            users = [u for u in users if u.fid not in all_fids_in_db]

            # filter out locations that are already in the database,
            # then save to db
            db_locations = session.query(Location).all()
            locations = [l for l in warpcast_locations if l and l.place_id not in [
                db_l.place_id for db_l in db_locations]]
            session.bulk_save_objects(locations)

        with open(users_filename, mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(UserClass.__annotations__.keys())
            for user in users:
                writer.writerow(asdict(user).values())

if args.ens:
    pass
