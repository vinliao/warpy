from dotenv import load_dotenv
import os
import requests
import json
from sqlalchemy.orm import sessionmaker
from models import Base, Cast, User, Reaction, Location, parent_association
from sqlalchemy import create_engine
import time

# get all casts that starts with recast:farcaster://
def get_recast(engine):
    session = sessionmaker(bind=engine)()
    casts = session.query(Cast).filter(Cast.text.like('recast:farcaster://%')).all()

    for cast in casts:
        reaction = extract_recast(cast.__dict__)
        print(reaction)
        session.merge(Reaction(**reaction))

    session.commit()

def delete_recast_in_db(engine):
    session = sessionmaker(bind=engine)()
    session.query(Cast).filter(Cast.text.like('recast:farcaster://%')).delete()
    session.commit()

def extract_recast(cast: dict):
    prefix = 'recast:farcaster://casts/'

    return {
        'hash': cast['hash'],
        'reaction_type': 'recast',
        'target_hash':  cast['text'][len(prefix):],
        'timestamp': cast['timestamp'],
        'author_fid': cast['author_fid']
    }

# engine = create_engine('sqlite:///new.db')
# delete_recast_in_db(engine)