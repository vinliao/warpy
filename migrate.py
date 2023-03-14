import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker, Session
from models import Base, User, Location, ExternalAddress, Cast
from sqlalchemy import create_engine, and_, Engine, inspect, func, text, select
import os
from dotenv import load_dotenv
load_dotenv()

engine = create_engine('sqlite:///data.db')
mysql_engine = create_engine(os.getenv("PLANETSCALE_URL"), echo=True)


def inspect_db(engine):
    # create an inspector object
    inspector = inspect(engine)

    # get a list of all table names in the database
    table_names = inspector.get_table_names()

    # iterate over each table and print its columns
    for table_name in table_names:
        print(f"Table name: {table_name}")
        columns = inspector.get_columns(table_name)
        for column in columns:
            print(f"\tColumn name: {column['name']}\tType: {column['type']}")


def get_all(engine, model):
    with sessionmaker(bind=engine)() as session:
        return session.query(model).all()


def make_sqlalchemy_object(obj_dict, model):
    obj_dict.pop('_sa_instance_state', None)
    return model(**obj_dict)


def migrate_objects(engine, mysql_engine, model_old, model):
    things = list(map(lambda x: make_sqlalchemy_object(
        x.__dict__, model), get_all(engine, model_old)))

    all_things_mysql = get_all(mysql_engine, model)
    all_things_mysql_ids = list(map(lambda x: x.fid, all_things_mysql))

    things = list(filter(lambda x: x.fid not in all_things_mysql_ids, things))

    with sessionmaker(bind=mysql_engine)() as session:
        session.bulk_save_objects(things)
        session.commit()


def migrate_casts():
    engine = create_engine('sqlite:///data-with-casts.db')
    mysql_engine = create_engine(os.getenv("PLANETSCALE_URL"))

    with sessionmaker(bind=mysql_engine)() as mysql_session:
        existing_cast_hashes = {
            c.hash for c in mysql_session.query(Cast.hash).all()}

    with sessionmaker(bind=engine)() as session:
        batch_size = 5000
        offset = 0

        while True:
            result = session.execute(text("SELECT * FROM casts LIMIT :batch_size OFFSET :offset;"),
                                     {"batch_size": batch_size, "offset": offset})

            print(f"Currently at offset {offset}...")

            rows = result.fetchall()
            if not rows:
                break

            # dump to df
            df = pd.DataFrame(rows)
            df = df.drop(
                columns=['replies_count', 'reactions_count', 'recasts_count', 'watches_count'])

            casts = [Cast(**row.to_dict()) for index, row in df.iterrows()]

            with sessionmaker(bind=mysql_engine)() as mysql_session:
                # existing_cast_hashes = {
                #     c.hash for c in mysql_session.query(Cast.hash).all()}

                # filter out existing casts
                new_casts = [
                    c for c in casts if c.hash not in existing_cast_hashes]

                mysql_session.bulk_insert_mappings(
                    Cast, [c.__dict__ for c in new_casts]
                )
                # mysql_session.bulk_save_objects(new_casts)
                # # mysql_session.commit()

                existing_cast_hashes = existing_cast_hashes.union(
                    {c.hash for c in new_casts})

            offset += batch_size


migrate_casts()
