from models import Base as NewBase, Cast as NewCast, User as NewUser, Reaction as NewReaction, Location as NewLocation, parent_association
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import Base, Cast, User, Reaction, Location, parent_association
from fetcher import get_recent_users, get_recent_casts
import os


Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    fid = Column(Integer, primary_key=True)
    username = Column(String)
    display_name = Column(String)
    verified = Column(Integer, default=0)
    pfp_url = Column(String, nullable=True)
    follower_count = Column(Integer)
    following_count = Column(Integer)
    bio_text = Column(String, nullable=True)
    location_place_id = Column(String, nullable=True)
    location_description = Column(String, nullable=True)
    casts = relationship('Cast', back_populates='author')


class Cast(Base):
    __tablename__ = 'casts'
    hash = Column(String, primary_key=True)
    thread_hash = Column(String)
    parent_hash = Column(String, ForeignKey('casts.hash'))
    text = Column(String)
    timestamp = Column(Integer)
    author_fid = Column(Integer, ForeignKey('users.fid'))
    author = relationship('User', back_populates='casts')
    parent = relationship('Cast', remote_side=[hash])


class Reaction(Base):
    __tablename__ = 'reactions'
    hash = Column(String, primary_key=True)
    type = Column(String)  # like & recast
    timestamp = Column(Integer)
    target_hash = Column(String, ForeignKey('casts.hash'))
    author_fid = Column(Integer, ForeignKey('users.fid'))


# create engine and session
engine = create_engine('sqlite:///old.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()


new_engine = create_engine('sqlite:///new.db', echo=True)
NewBase.metadata.drop_all(new_engine)
NewBase.metadata.create_all(new_engine)
NewSession = sessionmaker(bind=new_engine)
new_session = NewSession()


def migrate_users():
    all_users = session.query(User).all()
    for user in all_users:
        new_user = NewUser(fid=user.fid, username=user.username,
                           display_name=user.display_name, verified=user.verified,
                           pfp_url=user.pfp_url, follower_count=user.follower_count,
                           following_count=user.following_count, bio_text=user.bio_text,
                           location_place_id=user.location_place_id)
        new_session.add(new_user)

        if user.location_place_id:
            l = new_session.query(NewLocation).filter_by(
                place_id=user.location_place_id).first()
            if not l:
                l = NewLocation(place_id=user.location_place_id,
                                description=user.location_description)
                new_session.add(l)

    new_session.commit()


def migrate_casts():
    all_casts = session.query(Cast).all()
    for cast in all_casts:
        new_cast = NewCast(hash=cast.hash, thread_hash=cast.thread_hash,
                           parent_hash=cast.parent_hash, text=cast.text,
                           timestamp=cast.timestamp,
                           author_fid=cast.author_fid)
        new_session.add(new_cast)

    new_session.commit()


def create_association():
    for cast in new_session.query(Cast).all():
        if cast.parent_hash:
            stmt = parent_association.insert().values(
                parent_hash=cast.parent_hash,
                cast_hash=cast.hash
            )
            new_session.execute(stmt)

    new_session.commit()
    new_session.close()


def migrate_reactions():
    all_reactions = session.query(Reaction).all()
    for reaction in all_reactions:
        new_reaction = NewReaction(hash=reaction.hash, reaction_type=reaction.type,
                                   timestamp=reaction.timestamp, target_hash=reaction.target_hash,
                                   author_fid=reaction.author_fid)
        new_session.add(new_reaction)

    new_session.commit()


migrate_users()
migrate_casts()
create_association()
migrate_reactions()