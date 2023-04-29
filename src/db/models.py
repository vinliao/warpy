from sqlalchemy import BigInteger, Boolean, Column, Index, Integer, String
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class User(Base):
    """
    Information about users in the Farcaster network.
    """

    __tablename__ = "users"
    fid = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    username = Column(String(63), nullable=True)  # prioritize this over display_name
    display_name = Column(String(127), nullable=False)
    pfp_url = Column(String(255), nullable=True)
    bio_text = Column(String(255), nullable=True)
    following_count = Column(Integer, nullable=False)
    follower_count = Column(Integer, nullable=False)
    verified = Column(Boolean, nullable=False)
    generated_farcaster_address = Column(String(63), nullable=False)  # ethereum address
    address = Column(String(63), nullable=True)  # ethereum addresss
    registered_at = Column(BigInteger, nullable=True)  # unix ms
    location_id = Column(String(63), nullable=True)

    location = relationship(
        "Location",
        primaryjoin="User.location_id == foreign(Location.id)",
        back_populates="users",
    )


class Location(Base):
    """
    Locations of the users in the Farcaster network.
    """

    __tablename__ = "locations"
    id = Column(String(63), primary_key=True, nullable=False)
    description = Column(
        String(255), nullable=False
    )  # example: "San Francisco, CA, USA"

    users = relationship(
        "User",
        primaryjoin="User.location_id == foreign(Location.id)",
        back_populates="location",
    )


class Cast(Base):
    """
    Casts are the main content of the Farcaster network.
    """

    __tablename__ = "casts"
    hash = Column(String(255), primary_key=True, nullable=False)
    thread_hash = Column(String(255), nullable=False)
    text = Column(String(255), nullable=False)
    timestamp = Column(BigInteger, nullable=False)  # unix ms
    author_fid = Column(Integer, nullable=False)
    parent_hash = Column(String(255), nullable=True)

    reactions = relationship(
        "Reaction",
        primaryjoin="Cast.hash == foreign(Reaction.target_hash)",
        back_populates="target",
    )


class Reaction(Base):
    """
    Reactions can be applied to casts.
    """

    __tablename__ = "reactions"
    hash = Column(String(255), primary_key=True)
    reaction_type = Column(String(255))  # Literal["like", "recast"]
    timestamp = Column(BigInteger)  # unix ms
    target_hash = Column(String(255))
    author_fid = Column(Integer)

    target = relationship(
        "Cast",
        primaryjoin="Cast.hash == foreign(Reaction.target_hash)",
        back_populates="reactions",
    )


# User table indexes
Index("ix_users_username", User.username, mysql_length=63)
Index("ix_users_address", User.address, mysql_length=63)

# Cast table indexes
Index("ix_casts_thread_hash", Cast.thread_hash, mysql_length=63)
Index("ix_casts_author_fid", Cast.author_fid)
Index("ix_casts_timestamp", Cast.timestamp)

# Reaction table indexes
Index("ix_reactions_target_hash", Reaction.target_hash, mysql_length=63)
Index("ix_reactions_author_fid", Reaction.author_fid)
Index("ix_reactions_timestamp", Reaction.timestamp)
