# temporary file in moving data from sqlite to mysql


from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker, Session
from modelsold import Base as BaseOld, User as UserOld, Location as LocationOld
from models import Base, User, Location, ExternalAddress
from sqlalchemy import create_engine, and_, Engine, inspect
import os
import pymysql
from dotenv import load_dotenv
load_dotenv()

engine = create_engine('sqlite:///data.db')
mysql_engine = create_engine(os.getenv("PLANETSCALE_URL"))


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


# migrate_objects(engine, mysql_engine, UserOld, User)

users = get_all(mysql_engine, User)
print(users)

with sessionmaker(bind=mysql_engine)() as session:
    # get all users where exteral address is not null
    users = session.query(User).filter(
        and_(User.external_address != None, User.external_address != '')).all()
    
    all_external_addresses = session.query(ExternalAddress).all()

    # filter out users where external address is already in external address table
    users = list(filter(lambda x: x.external_address not in list(map(lambda x: x.address, all_external_addresses)), users))

    # fill the external address table from users

    external_addresses = list(map(lambda x: ExternalAddress(
        address=x.external_address,
        ens=x.ens,
        url=x.url,
        github=x.github,
        twitter=x.twitter,
        telegram=x.telegram,
        email=x.email,
        discord=x.discord
    ), users))

    session.bulk_save_objects(external_addresses)
    session.commit()
