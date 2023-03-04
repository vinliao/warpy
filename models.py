from sqlalchemy import Column, BigInteger, String, ForeignKey, Table
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

association_table = Table('parent_association', Base.metadata,
                          Column('parent_hash', String,
                                 ForeignKey('casts.hash')),
                          Column('cast_hash', String,
                                 ForeignKey('casts.hash'))
                          )


class Cast(Base):
    __tablename__ = 'casts'
    hash = Column(String, primary_key=True)
    thread_hash = Column(String, ForeignKey('casts.hash'))
    parent_hash = Column(String, ForeignKey('casts.hash'), nullable=True)
    text = Column(String)
    timestamp = Column(BigInteger)
    author_fid = Column(BigInteger, ForeignKey('users.fid'))
    author = relationship('User', back_populates='casts')
    reactions = relationship('Reaction', back_populates='target')
    children_hashes = relationship("Cast", secondary=association_table,
                                   primaryjoin=(
                                       hash == association_table.c.parent_hash),
                                   secondaryjoin=(
                                       hash == association_table.c.cast_hash),
                                   backref="parent_casts")
    ancestor_hashes = relationship("Cast", secondary=association_table,
                                   primaryjoin=(
                                       hash == association_table.c.cast_hash),
                                   secondaryjoin=(
                                       hash == association_table.c.parent_hash),
                                   backref="thread_casts")


class Reaction(Base):
    __tablename__ = 'reactions'
    hash = Column(String, primary_key=True)
    type = Column(String)  # like & recast
    timestamp = Column(BigInteger)
    target_hash = Column(String, ForeignKey('casts.hash'))
    author_fid = Column(BigInteger, ForeignKey('users.fid'))
    target = relationship('Cast', back_populates='reactions')


class Location(Base):
    __tablename__ = 'locations'
    place_id = Column(String, primary_key=True)
    description = Column(String)
    users = relationship('User', backref='location')


class User(Base):
    __tablename__ = 'users'
    fid = Column(BigInteger, primary_key=True)
    username = Column(String)
    display_name = Column(String)
    verified = Column(BigInteger, default=0)
    pfp_url = Column(String, nullable=True)
    follower_count = Column(BigInteger)
    following_count = Column(BigInteger)
    bio_text = Column(String, nullable=True)
    location_place_id = Column(String, ForeignKey(
        'locations.place_id'), nullable=True)
    casts = relationship('Cast', back_populates='author')
