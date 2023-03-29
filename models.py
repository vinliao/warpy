from sqlalchemy import Column, String, Integer, ForeignKey, Float, Integer
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
    external_address = Column(String, ForeignKey(
        'external_addresses.address'), nullable=True)
    registered_at = Column(Integer)
    location_id = Column(String, ForeignKey('locations.id'), nullable=True)

    location = relationship("Location", back_populates="users")
    external_address_rel = relationship(
        "ExternalAddress", back_populates="users")
    eth_transactions = relationship("EthTransaction", back_populates="user")


class Location(Base):
    __tablename__ = 'locations'
    id = Column(String, primary_key=True)
    description = Column(String)

    users = relationship("User", back_populates="location")


class Cast(Base):
    __tablename__ = 'casts'

    hash = Column(String, primary_key=True)
    thread_hash = Column(String, nullable=False)
    text = Column(String, nullable=False)
    timestamp = Column(Integer, nullable=False)
    author_fid = Column(Integer, nullable=False)
    parent_hash = Column(String, ForeignKey('casts.hash'), nullable=True)


class ExternalAddress(Base):
    __tablename__ = 'external_addresses'
    address = Column(String, primary_key=True)
    ens = Column(String, nullable=True)
    url = Column(String, nullable=True)
    github = Column(String, nullable=True)
    twitter = Column(String, nullable=True)
    telegram = Column(String, nullable=True)
    email = Column(String, nullable=True)
    discord = Column(String, nullable=True)

    users = relationship("User", back_populates="external_address_rel")
    eth_transactions = relationship(
        "EthTransaction", back_populates="address_obj")


class EthTransaction(Base):
    __tablename__ = 'eth_transactions'

    hash = Column(String, primary_key=True)
    address_fid = Column(Integer, ForeignKey('users.fid'), nullable=False)
    address_external = Column(String, ForeignKey(
        'external_addresses.address'), nullable=False)
    timestamp = Column(Integer)
    block_num = Column(Integer)
    from_address = Column(String)
    to_address = Column(String)
    value = Column(Float, nullable=True)
    erc721_token_id = Column(String, nullable=True)
    token_id = Column(String, nullable=True)
    asset = Column(String, nullable=True)
    category = Column(String)

    user = relationship("User", back_populates="eth_transactions")
    address_obj = relationship(
        "ExternalAddress", back_populates="eth_transactions")
    erc1155_metadata = relationship(
        "ERC1155Metadata", back_populates="eth_transaction")


class ERC1155Metadata(Base):
    __tablename__ = 'erc1155_metadata'

    id = Column(Integer, primary_key=True, autoincrement=True)
    eth_transaction_hash = Column(String, ForeignKey(
        'eth_transactions.hash'), nullable=False)
    token_id = Column(String, nullable=False)
    value = Column(String, nullable=False)

    eth_transaction = relationship(
        "EthTransaction", back_populates="erc1155_metadata")
