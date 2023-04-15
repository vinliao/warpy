from sqlalchemy import Column, Float, ForeignKey, Integer, String, Table
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

# Association table: many-to-many relationship between User and EthTransaction
user_eth_transactions_association = Table(
    "user_eth_transactions",
    Base.metadata,
    Column("user_fid", Integer, ForeignKey("users.fid")),
    Column(
        "eth_transaction_unique_id", String, ForeignKey("eth_transactions.unique_id")
    ),
)


class User(Base):
    __tablename__ = "users"
    fid = Column(Integer, primary_key=True, nullable=False)
    username = Column(String, nullable=True)
    display_name = Column(String, nullable=False)
    pfp_url = Column(String, nullable=True)
    bio_text = Column(String, nullable=True)
    following_count = Column(Integer, nullable=False)
    follower_count = Column(Integer, nullable=False)
    verified = Column(Integer, nullable=False)
    generated_farcaster_address = Column(String, nullable=False)
    address = Column(String, ForeignKey("ens_data.address"), nullable=True)
    registered_at = Column(Integer, nullable=True)
    location_id = Column(String, ForeignKey("locations.id"), nullable=True)

    location = relationship("Location", back_populates="users")
    ens_data_rel = relationship("ENSData", back_populates="users")
    eth_transactions = relationship(
        "EthTransaction",
        secondary=user_eth_transactions_association,
        back_populates="users",
    )


class Location(Base):
    __tablename__ = "locations"
    id = Column(String, primary_key=True, nullable=False)
    description = Column(String, nullable=False)

    users = relationship("User", back_populates="location")


class Cast(Base):
    __tablename__ = "casts"

    hash = Column(String, primary_key=True, nullable=False)
    thread_hash = Column(String, nullable=False)
    text = Column(String, nullable=False)
    timestamp = Column(Integer, nullable=False)
    author_fid = Column(Integer, ForeignKey("users.fid"), nullable=False)
    parent_hash = Column(String, ForeignKey("casts.hash"), nullable=True)

    reactions = relationship("Reaction", back_populates="target")


class Reaction(Base):
    __tablename__ = "reactions"
    hash = Column(String, primary_key=True)
    reaction_type = Column(String)  # like & recast
    timestamp = Column(Integer)
    target_hash = Column(String, ForeignKey("casts.hash"))
    author_fid = Column(Integer, ForeignKey("users.fid"))
    target = relationship("Cast", back_populates="reactions")


class ENSData(Base):
    __tablename__ = "ens_data"
    address = Column(String, primary_key=True, nullable=False)
    ens = Column(String, nullable=True)
    url = Column(String, nullable=True)
    github = Column(String, nullable=True)
    twitter = Column(String, nullable=True)
    telegram = Column(String, nullable=True)
    email = Column(String, nullable=True)
    discord = Column(String, nullable=True)

    users = relationship("User", back_populates="ens_data_rel")


class EthTransaction(Base):
    __tablename__ = "eth_transactions"

    unique_id = Column(String, primary_key=True, nullable=False)
    hash = Column(String, primary_key=False, nullable=False)
    timestamp = Column(Integer, nullable=False)
    block_num = Column(Integer, nullable=False)
    from_address = Column(String, nullable=True)
    to_address = Column(String, nullable=True)
    value = Column(Float, nullable=True)
    erc721_token_id = Column(String, nullable=True)
    token_id = Column(String, nullable=True)
    asset = Column(String, nullable=True)
    category = Column(String, nullable=False)

    users = relationship(
        "User",
        secondary=user_eth_transactions_association,
        back_populates="eth_transactions",
    )
    erc1155_metadata = relationship("ERC1155Metadata", back_populates="eth_transaction")


class ERC1155Metadata(Base):
    __tablename__ = "erc1155_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    eth_transaction_hash = Column(
        String, ForeignKey("eth_transactions.hash"), nullable=False
    )
    token_id = Column(String, nullable=False)
    value = Column(String, nullable=False)

    eth_transaction = relationship("EthTransaction", back_populates="erc1155_metadata")
