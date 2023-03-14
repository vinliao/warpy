from users import *
from casts import *
import argparse
import os
from sqlalchemy.orm import sessionmaker
from models import Base, User, Location, Cast
from sqlalchemy import create_engine, text, func
import asyncio
import argparse
from models import Base, User, Location, ExternalAddress
from dataclasses import asdict
import csv
from dotenv import load_dotenv

load_dotenv()
warpcast_hub_key = os.getenv('WARPCAST_HUB_KEY')

parser = argparse.ArgumentParser()

parser.add_argument('-a', '--all', action='store_true',
                    help='Refresh user data from Warpcast, Searchcaster, and ENSData')
parser.add_argument('--user', action='store_true',
                    help='Refresh user data from Warpcast and Searchcaster')
parser.add_argument('--ens', action='store_true',
                    help='Refresh user data from Ensdata')
parser.add_argument('--cast', action='store_true',
                    help='Refresh cast data from Warpcast')
parser.add_argument('--query', nargs='+', type=str,
                    help='Run query with the help of ChatGPT (ex: "give me all users with fid below 100")')

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


"""
Index users from Warpcast and Searchcaster:

1. get all users from Warpcast
2. get all locations, save to db
3. check all user against db, filter out users that are already in the db
4. save users to csv
5. for each user, get registered_at, external_address, farcaster_address from Searchcaster
6. save users to db
"""
if args.user:
    users_filename = 'users.csv'
    engine = create_engine(os.getenv('PLANETSCALE_URL'), echo=True)

    # if the users.csv file doesn't exist, get all users from warpcast,
    # save the unindexed users in csv, then work through it
    if not os.path.exists(users_filename):
        all_users = get_all_users_from_warpcast(warpcast_hub_key)
        warpcast_data = [extract_warpcast_user_data(u) for u in all_users]
        users = [UserClass(**data) for data in warpcast_data]

        warpcast_locations = [get_warpcast_location(
            u) for u in all_users if get_warpcast_location(u) is not None]
        # filter duplicates
        warpcast_locations = list(
            {v.place_id: v for v in warpcast_locations}.values())

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

    # index users from csv
    with open(users_filename, mode='r') as csv_file:
        reader = csv.reader(csv_file)
        next(reader)
        users = [UserClass(*u) for u in list(reader)]

    users = [fix_user_types(u) for u in users]

    batch_size = 10
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

    # delete the csv file when it's all indexed
    os.remove(users_filename)

"""
Index ENS data from ensdata.net
1. get all users from db where external_address is not null 
    and external_address is not in external addresstable 
2. get all data from ensdata.net
3. save to db
"""
if args.ens:
    engine = create_engine(os.getenv('PLANETSCALE_URL'))

    with sessionmaker(bind=engine)() as session:
        addresses = session.query(User.external_address).filter(
            User.external_address != None).filter(~User.external_address.in_(session.query(ExternalAddress.address))).all()

        external_addresses = [ExternalAddress(address=a[0]) for a in addresses]

        print(f'Found {len(external_addresses)} addresses to check')

        batch_size = 5
        start_index = 0
        end_index = batch_size
        while start_index < len(external_addresses):
            current_addresses = external_addresses[start_index:end_index]
            if len(current_addresses) == 0:
                break

            addresses = [a.address for a in current_addresses]
            ensdata_users = asyncio.run(get_users_from_ensdata(addresses))
            ensdata_data = [extract_ensdata_user_data(
                u) for u in ensdata_users]

            for address in current_addresses:
                for d in ensdata_data:
                    if address.address == d.address:
                        address.ens = d.ens
                        address.url = d.url
                        address.github = d.github
                        address.twitter = d.twitter
                        address.telegram = d.telegram
                        address.email = d.email
                        address.discord = d.discord

                        session.merge(address)
                        break
            session.commit()

            start_index = end_index
            end_index += batch_size

if args.cast:
    # get cast with the latest timestmap
    engine = create_engine(os.getenv('PLANETSCALE_URL'))
    with sessionmaker(bind=engine)() as session:
        db_latest_cast = session.query(Cast).order_by(
            Cast.timestamp.desc()).first()

        casts = get_all_casts_from_warpcast(
            warpcast_hub_key, db_latest_cast.timestamp)

        print(f'Found {len(casts)} casts to index')

        existing_cast_hashes = {
            c.hash for c in session.query(Cast.hash).all()}

        casts = [Cast(**asdict(cast))
                 for cast in casts if cast.hash not in existing_cast_hashes]
        session.bulk_save_objects(casts)


if args.query:
    import openai

    # set openai api key
    openai.api_key = os.getenv('OPENAI_API_KEY')

    """
    Note: to generate the table, run:

    for table_name in Base.metadata.tables.keys():
        print(f"Table: {table_name}")
        table = Base.metadata.tables[table_name]
        for column in table.columns:
            print(f" - {column.name} ({column.type})")

    """

    system_prompt = "You are a SQL writer. If the user asks about anything than SQL, deny. You are a very good SQL writer. Nothing else."
    initial_prompt = """Table: locations
    - place_id (VARCHAR(255)) # example: ChIJyc_U0TTDQUcRYBEeDCnEAA, ChIJYeZuBI9YwokRjMDs_IEyCwo, ChIJYTN9T-plUjoRM9RjaAunYW4	
    - description (VARCHAR(255)) # example: Budapest, Hungary; Manhattan, New York, NY, USA; Chennai, Tamil Nadu, India; Red Hook, NY, USA
    Table: users
    - fid (BIGINT)
    - username (VARCHAR(50))
    - display_name (VARCHAR(255))
    - verified (BOOLEAN)
    - pfp_url (VARCHAR(255))
    - follower_count (BIGINT)
    - following_count (BIGINT)
    - bio_text (VARCHAR(255))
    - location_place_id (VARCHAR(255)) # example: ChIJyc_U0TTDQUcRYBEeDCnEAA, ChIJYeZuBI9YwokRjMDs_IEyCwo, ChIJYTN9T-plUjoRM9RjaAunYW4	
    - farcaster_address (VARCHAR(63)) # ethereum address, example: 0xaff2ab518ba962bd19f67e75ceb9de4da350b327, 0xdcb3beb907745e7ec9e3632508baf927b5950f67, 0x19793902549ac230d25fe5b5688cb30e0535fbed,
    - external_address (VARCHAR(63)) # ethereum address, example: 0xaff2ab518ba962bd19f67e75ceb9de4da350b327, 0xdcb3beb907745e7ec9e3632508baf927b5950f67, 0x19793902549ac230d25fe5b5688cb30e0535fbed,
    - registered_at (BIGINT)
    Table: external_addresses
    - address (VARCHAR(63)) # ethereum address, example: 0xaff2ab518ba962bd19f67e75ceb9de4da350b327, 0xdcb3beb907745e7ec9e3632508baf927b5950f67, 0x19793902549ac230d25fe5b5688cb30e0535fbed,
    - ens (VARCHAR(255)) # string that ends with .eth, example: whatever.eth, something.eth, else.eth
    - url (VARCHAR(255))
    - github (VARCHAR(255))
    - twitter (VARCHAR(63))
    - telegram (VARCHAR(63))
    - email (VARCHAR(255))
    - discord (VARCHAR(63))
    Table: casts
    - hash (VARCHAR(127))
    - thread_hash (VARCHAR(127))
    - parent_hash (VARCHAR(127))
    - text (VARCHAR(511))
    - timestamp (BIGINT)
    - author_fid (BIGINT)

    Description: User table contains data for Farcaster user, casts is the post user makes, external address are ethereum address connected to Farcaster account (one external address can have have multiple Farcaster account).

    Here's the database schema you're working with. Your job is to turn user queries (in natural language) to SQL. The database is MySQL, adjust your query accordingly. Only return the SQL and nothing else. Don't explain, don't say "here's your query." Just give the SQL. Say "Yes" if you understand.
    """

    print("Sending query to ChatGPT...")

    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": initial_prompt},
            {"role": "assistant",
                "content": "Yes."},
            {"role": "user", "content": args.query[0]}
        ]
    )

    reply = completion['choices'][0]['message']['content'].strip()

    print(f"SQL from ChatGPT: \n\n{reply}\n")
    print(f"Running SQL...")

    # take the raw sql query, then run it against the db
    engine = create_engine(os.getenv('PLANETSCALE_URL'))
    with sessionmaker(bind=engine)() as session:
        result = session.execute(text(reply))

        # if result returns row
        rows = result.fetchall()
        for row in rows:
            print(row)
