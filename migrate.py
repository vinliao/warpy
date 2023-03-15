import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker, Session
from models import Base, User, Location, ExternalAddress, Cast, EthTransaction, ERC1155Metadata, OldEthTransaction
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
        return session.query(model).limit(100).all()


def make_sqlalchemy_object(obj_dict, model):
    obj_dict.pop('_sa_instance_state', None)
    return model(**obj_dict)


# def migrate_objects(engine, mysql_engine, model_old, model):
#     rows_to_migrate = [make_sqlalchemy_object(
#         x.__dict__, model) for x in get_all(engine, model_old)]

#     all_rows_in_db = get_all(mysql_engine, model)
#     all_primary_keys_in_db = [getattr(r, model.__table__.primary_key.columns.keys()[
#                                       0]) for r in all_rows_in_db]
#     filtered_rows = [r for r in rows_to_migrate if getattr(
#         r, model.__table__.primary_key.columns.keys()[0]) not in all_primary_keys_in_db]

#     BATCH_SIZE = 1000
#     for i in range(0, len(filtered_rows), BATCH_SIZE):
#         batch = filtered_rows[i:i+BATCH_SIZE]
#         with sessionmaker(bind=mysql_engine)() as session:
#             session.bulk_save_objects(batch)
#             session.commit()

# def migrate_objects(engine, mysql_engine, model_old, model):
#     rows_to_migrate = [make_sqlalchemy_object(
#         x.__dict__, model) for x in get_all(engine, model_old)]

#     BATCH_SIZE = 1000
#     for i in range(0, len(rows_to_migrate), BATCH_SIZE):
#         batch = rows_to_migrate[i:i+BATCH_SIZE]
#         with sessionmaker(bind=mysql_engine)() as session:
#             for obj in batch:
#                 erc1155_metadata = obj.pop('erc1155_metadata', None)
#                 session.add(model(**obj))
#                 if erc1155_metadata:
#                     for token in erc1155_metadata:
#                         session.add(ERC1155Metadata(
#                             eth_transaction_hash=obj['hash'],
#                             token_id=token['tokenId'],
#                             value=token['value']
#                         ))
#             session.commit()
def migrate_objects(engine, mysql_engine, model_old, model):
    rows_to_migrate = [make_sqlalchemy_object(
        x.__dict__, model) for x in get_all(engine, model_old)]

    BATCH_SIZE = 1000
    for i in range(0, len(rows_to_migrate), BATCH_SIZE):
        batch = rows_to_migrate[i:i+BATCH_SIZE]

        with sessionmaker(bind=mysql_engine)() as session:
            for obj in batch:
                print(obj.__dict__)
                metadata = obj.erc1155_metadata
                obj.erc1155_metadata = None
                session.add(obj)
            session.commit()


# # Get all the existing EthTransactions from the source database
# with sessionmaker(bind=engine)() as src_session:
#     eth_transactions = get_all(src_session.bind, OldEthTransaction)

# Migrate the EthTransactions to the destination database
migrate_objects(engine, mysql_engine, OldEthTransaction, EthTransaction)


# TODO: inefficient code
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

            casts = []
            for row in result:
                cast_dict = {
                    'hash': row['hash'],
                    'thread_hash': row['thread_hash'],
                    'parent_hash': row['parent_hash'],
                    'text': row['text'],
                    'timestamp': row['timestamp'],
                    'author_fid': row['author_fid']
                }

                cast = Cast(**cast_dict)
                casts.append(cast)

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
