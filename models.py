from sqlalchemy import Column, BigInteger, String, ForeignKey, Table, Integer, Float
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

parent_association = Table('parent_association', Base.metadata,
                           Column('parent_hash', String,
                                  ForeignKey('casts.hash')),
                           Column('cast_hash', String,
                                  ForeignKey('casts.hash'))
                           )


# figure out ancestor hashes later
class Cast(Base):
    __tablename__ = 'casts'
    hash = Column(String, primary_key=True)
    thread_hash = Column(String, ForeignKey(
        'casts.hash'))
    parent_hash = Column(String, ForeignKey(
        'casts.hash'), nullable=True)
    text = Column(String)
    timestamp = Column(BigInteger)
    author_fid = Column(BigInteger, ForeignKey(
        'users.fid'))
    author = relationship('User', back_populates='casts')
    reactions = relationship('Reaction', back_populates='target')
    children_hashes = relationship("Cast", secondary=parent_association,
                                   primaryjoin=(
                                       hash == parent_association.c.parent_hash),
                                   secondaryjoin=(
                                       hash == parent_association.c.cast_hash),
                                   backref="parent_casts")


class Reaction(Base):
    __tablename__ = 'reactions'
    hash = Column(String, primary_key=True)
    reaction_type = Column(String)  # like & recast
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
    verified = Column(Integer, default=0)
    pfp_url = Column(String, nullable=True)
    follower_count = Column(BigInteger)
    following_count = Column(BigInteger)
    bio_text = Column(String, nullable=True)
    location_place_id = Column(String, ForeignKey(
        'locations.place_id'), nullable=True)
    farcaster_address = Column(String, nullable=True)
    external_address = Column(String, nullable=True)
    registered_at = Column(BigInteger, nullable=True)
    ens = Column(String, nullable=True)
    url = Column(String, nullable=True)
    github = Column(String, nullable=True)
    twitter = Column(String, nullable=True)
    telegram = Column(String, nullable=True)
    email = Column(String, nullable=True)
    discord = Column(String, nullable=True)

    casts = relationship('Cast', back_populates='author')


class EthTransaction(Base):
    __tablename__ = 'eth_transactions'

    hash = Column(String, primary_key=True)
    address_fid = Column(BigInteger, ForeignKey('users.fid'), nullable=False)
    address_external = Column(String, ForeignKey('users.external_address'), nullable=False)
    address = relationship('User', backref='eth_transactions',
                           foreign_keys=[address_external])
    timestamp = Column(BigInteger)
    block_num = Column(BigInteger)
    from_address = Column(String)
    to_address = Column(String)
    value = Column(Float, nullable=True)
    erc721_token_id = Column(String, nullable=True)
    erc1155_metadata = Column(String, nullable=True)
    token_id = Column(String, nullable=True)
    asset = Column(String, nullable=True)
    category = Column(String)

