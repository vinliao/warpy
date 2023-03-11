from sqlalchemy import Column, BigInteger, String, ForeignKey, Table, Integer, Float
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

# parent_association = Table('parent_association', Base.metadata,
#                            Column('parent_hash', String,
#                                   ForeignKey('casts.hash')),
#                            Column('cast_hash', String,
#                                   ForeignKey('casts.hash'))
#                            )


# class Cast(Base):
#     __tablename__ = 'casts'
#     hash = Column(String, primary_key=True)
#     thread_hash = Column(String, ForeignKey(
#         'casts.hash'))
#     parent_hash = Column(String, ForeignKey(
#         'casts.hash'), nullable=True)
#     text = Column(String)
#     timestamp = Column(BigInteger)
#     author_fid = Column(BigInteger, ForeignKey(
#         'users.fid'))
#     author = relationship('User', back_populates='casts')
#     reactions = relationship('Reaction', back_populates='target')
#     children_hashes = relationship("Cast", secondary=parent_association,
#                                    primaryjoin=(
#                                        hash == parent_association.c.parent_hash),
#                                    secondaryjoin=(
#                                        hash == parent_association.c.cast_hash),
#                                    backref="parent_casts")


# class Reaction(Base):
#     __tablename__ = 'reactions'
#     hash = Column(String, primary_key=True)
#     reaction_type = Column(String)  # like & recast
#     timestamp = Column(BigInteger)
#     target_hash = Column(String, ForeignKey('casts.hash'))
#     author_fid = Column(BigInteger, ForeignKey('users.fid'))
#     target = relationship('Cast', back_populates='reactions')

class Location(Base):
    __tablename__ = 'locations'
    place_id = Column(String(255), primary_key=True)
    description = Column(String(255))
    users = relationship('User', backref='location')


class User(Base):
    __tablename__ = 'users'
    fid = Column(BigInteger, primary_key=True)
    username = Column(String(50))
    display_name = Column(String(255))
    verified = Column(Integer, default=0)
    pfp_url = Column(String(255), nullable=True)
    follower_count = Column(BigInteger)
    following_count = Column(BigInteger)
    bio_text = Column(String(255), nullable=True)
    location_place_id = Column(String(255), nullable=True)
    farcaster_address = Column(String(63), nullable=True)
    external_address = Column(String(63), nullable=True)
    registered_at = Column(BigInteger, nullable=True)
    ens = Column(String(255), nullable=True)
    url = Column(String(255), nullable=True)
    github = Column(String(255), nullable=True)
    twitter = Column(String(63), nullable=True)
    telegram = Column(String(63), nullable=True)
    email = Column(String(255), nullable=True)
    discord = Column(String(63), nullable=True)

    casts = relationship('Cast', back_populates='author')

# class EthTransaction(Base):
#     __tablename__ = 'eth_transactions'

#     hash = Column(String(127), primary_key=True)
#     address_fid = Column(BigInteger, ForeignKey('users.fid'), nullable=False)
#     address_external = Column(String(63), ForeignKey(
#         'users.external_address'), nullable=False)
#     address = relationship('User', backref='eth_transactions',
#                            foreign_keys=[address_external])
#     timestamp = Column(BigInteger)
#     block_num = Column(BigInteger)
#     from_address = Column(String(63))
#     to_address = Column(String(63))
#     value = Column(Float, nullable=True)
#     erc721_token_id = Column(String(127), nullable=True)
#     erc1155_metadata = Column(String(255), nullable=True)
#     token_id = Column(String(255), nullable=True)
#     asset = Column(String(255), nullable=True)
#     category = Column(String(255))
