from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    fid = Column(Integer, primary_key=True)
    username = Column(String)
    display_name = Column(String)
    pfp_url = Column(String)
    bio_text = Column(String)
    following_count = Column(Integer)
    follower_count = Column(Integer)
    verified = Column(Integer)
    farcaster_address = Column(String)
    external_address = Column(String, nullable=True)
    registered_at = Column(Integer)
    location_id = Column(String, ForeignKey('locations.id'), nullable=True)

    location = relationship("Location", back_populates="user_extras")


class Location(Base):
    __tablename__ = 'locations'
    id = Column(String, primary_key=True)
    description = Column(String)

    user_extras = relationship("User", back_populates="location")


class Cast(Base):
    __tablename__ = 'casts'

    hash = Column(String, primary_key=True)
    thread_hash = Column(String, nullable=False)
    text = Column(String, nullable=False)
    timestamp = Column(Integer, nullable=False)
    author_fid = Column(Integer, nullable=False)
    parent_hash = Column(String, ForeignKey('casts.hash'), nullable=True)
