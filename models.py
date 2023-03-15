from sqlalchemy import Column, BigInteger, String, Boolean, Float, Integer
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

# parent_association = Table('parent_association', Base.metadata,
#                            Column('parent_hash', String,
#                                   ForeignKey('casts.hash')),
#                            Column('cast_hash', String,
#                                   ForeignKey('casts.hash'))
#                            )


# class Reaction(Base):
#     __tablename__ = 'reactions'
#     hash = Column(String, primary_key=True)
#     reaction_type = Column(String)  # like & recast
#     timestamp = Column(BigInteger)
#     target_hash = Column(String, ForeignKey('casts.hash'))
#     author_fid = Column(BigInteger, ForeignKey('users.fid'))
#     target = relationship('Cast', back_populates='reactions')

class Cast(Base):
    __tablename__ = 'casts'
    hash = Column(String(127), primary_key=True)
    thread_hash = Column(String(127), nullable=False)
    parent_hash = Column(String(127), nullable=True)
    text = Column(String(511), nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    author_fid = Column(BigInteger, nullable=False)
    author = relationship(
        'User', primaryjoin='Cast.author_fid == foreign(User.fid)', remote_side='User.fid')
    # parent = relationship('Cast', primaryjoin='Cast.parent_hash == foreign(Cast.hash)', remote_side=[
    #                       hash], backref='children')
    # thread = relationship('Cast', primaryjoin='Cast.thread_hash == foreign(Cast.hash)', remote_side=[
    #                       hash], backref='descendant')


class User(Base):
    __tablename__ = 'users'
    fid = Column(BigInteger, primary_key=True)
    username = Column(String(50))
    display_name = Column(String(255))
    verified = Column(Boolean, default=False)
    pfp_url = Column(String(255), nullable=True)
    follower_count = Column(BigInteger)
    following_count = Column(BigInteger)
    bio_text = Column(String(255), nullable=True)
    location_place_id = Column(String(255), nullable=True)
    farcaster_address = Column(String(63), nullable=True)
    external_address = Column(String(63), nullable=True)
    registered_at = Column(BigInteger, nullable=True)
    casts = relationship(
        'Cast', primaryjoin='User.fid == foreign(Cast.author_fid)', backref='user')


class Location(Base):
    __tablename__ = 'locations'
    place_id = Column(String(255), primary_key=True)
    description = Column(String(255))
    users = relationship(
        'User', primaryjoin='foreign(User.location_place_id) == Location.place_id', backref='location')


class ExternalAddress(Base):
    __tablename__ = 'external_addresses'
    address = Column(String(63), primary_key=True)
    ens = Column(String(255), nullable=True)
    url = Column(String(255), nullable=True)
    github = Column(String(255), nullable=True)
    twitter = Column(String(63), nullable=True)
    telegram = Column(String(63), nullable=True)
    email = Column(String(255), nullable=True)
    discord = Column(String(63), nullable=True)

    user = relationship(
        'User', primaryjoin='foreign(User.external_address) == ExternalAddress.address', backref='external_addresses')


class EthTransaction(Base):
    __tablename__ = 'eth_transactions'

    hash = Column(String(127), primary_key=True)
    address_fid = Column(BigInteger, nullable=False)
    address_external = Column(String(63), nullable=False)
    timestamp = Column(BigInteger)
    block_num = Column(BigInteger)
    from_address = Column(String(63))
    to_address = Column(String(63))
    value = Column(Float, nullable=True)
    erc721_token_id = Column(String(127), nullable=True)
    token_id = Column(String(255), nullable=True)
    asset = Column(String(255), nullable=True)
    category = Column(String(255))

    user = relationship(
        'User', primaryjoin='User.fid == foreign(EthTransaction.address_fid)', backref='eth_transactions')
    address_obj = relationship(
        'ExternalAddress', primaryjoin='ExternalAddress.address == foreign(EthTransaction.address_external)', backref='eth_transactions')
    erc1155_metadata = relationship(
        'ERC1155Metadata', primaryjoin='ERC1155Metadata.eth_transaction_hash == foreign(EthTransaction.hash)', back_populates='eth_transaction')


class ERC1155Metadata(Base):
    __tablename__ = 'erc1155_metadata'

    id = Column(Integer, primary_key=True, autoincrement=True)
    eth_transaction_hash = Column(
        String(127), nullable=False)
    token_id = Column(String(255), nullable=False)
    value = Column(String(255), nullable=False)

    eth_transaction = relationship(
        'EthTransaction', primaryjoin='ERC1155Metadata.eth_transaction_hash == foreign(EthTransaction.hash)', back_populates='erc1155_metadata')
