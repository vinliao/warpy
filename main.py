from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import Base, Cast, User, Reaction, Location, parent_association
from fetcher import get_recent_users, get_recent_casts
import os
import dotenv

dotenv.load_dotenv()


def create_schema(Base, engine):
    Base.metadata.create_all(engine)


def drop_schema(Base, engine):
    Base.metadata.drop_all(engine)


def recreate_schema(Base, engine):
    drop_schema(Base, engine)
    create_schema(Base, engine)


def postgres_disable_referential_integrity(engine):
    with engine.connect() as conn:
        conn.execute(text('SET CONSTRAINTS ALL DEFERRED'))


engine = create_engine('sqlite:///test.db', echo=True)


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


def insert_casts_to_db(engine, casts):
    Session = sessionmaker(bind=engine)
    session = Session()

    for cast in casts:
        c = Cast(
            hash=cast['_hashV2'],
            text=cast['text'],
            timestamp=cast['timestamp'],
            author_fid=cast['author']['fid'],
            parent_hash=cast['_parentHashV2'] if '_parentHashV2' in cast else None,
            thread_hash=cast['_threadHashV2'] if '_threadHashV2' in cast else None,
        )

        session.add(c)

    session.commit()
    session.close()


def create_association(engine):
    Session = sessionmaker(bind=engine)
    session = Session()

    for cast in session.query(Cast).all():
        if cast.parent_hash:
            stmt = parent_association.insert().values(
                parent_hash=cast.parent_hash,
                cast_hash=cast.hash
            )
            session.execute(stmt)

    session.commit()
    session.close()

