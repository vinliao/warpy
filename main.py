from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table, text
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from models import Base, Cast, User, Reaction, Location
import json
from fetcher import get_recent_users


def create_schema(Base, engine):
    Base.metadata.create_all(engine)


def drop_schema(Base, engine):
    Base.metadata.drop_all(engine)


def recreate_schema(Base, engine):
    drop_schema(Base, engine)
    create_schema(Base, engine)


engine = create_engine('sqlite:///test.db', echo=True)
# create_schema(Base, engine)


def insert_users_to_db(engine, users):
    Session = sessionmaker(bind=engine)
    session = Session()

    for user in users:
        u = User(
            fid=user['fid'],
            username=user['username'],
            display_name=user['displayName'],
            verified=user['pfp']['verified'] if 'pfp' in user else 0,
            pfp_url=user['pfp']['url'] if 'pfp' in user else '',
            follower_count=user['followerCount'],
            following_count=user['followingCount'],
            bio_text=user['profile']['bio']['text'] if 'bio' in user['profile'] else None
        )

        if 'location' in user['profile']:
            place_id = user['profile']['location'].get('placeId')
            if place_id:
                l = session.query(Location).filter_by(
                    place_id=place_id).first()
                if not l:
                    l = Location(place_id=place_id,
                                 description=user['profile']['location']['description'])
                    session.add(l)

                u.location = l

        session.add(u)

    session.commit()
    session.close()


recreate_schema(Base, engine)
insert_users_to_db(engine, get_recent_users()['users'])
