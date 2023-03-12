from sqlalchemy.orm import sessionmaker
import re

# get all casts that starts with recast:farcaster://


# def get_recast(engine):
#     session = sessionmaker(bind=engine)()
#     casts = session.query(Cast).filter(
#         Cast.text.like('recast:farcaster://%')).all()

#     for cast in casts:
#         reaction = extract_recast(cast.__dict__)
#         print(reaction)
#         session.merge(Reaction(**reaction))

#     session.commit()


# def delete_recast_in_db(engine):
#     session = sessionmaker(bind=engine)()
#     session.query(Cast).filter(Cast.text.like('recast:farcaster://%')).delete()
#     session.commit()


# def extract_recast(cast: dict):
#     prefix = 'recast:farcaster://casts/'

#     return {
#         'hash': cast['hash'],
#         'reaction_type': 'recast',
#         'target_hash':  cast['text'][len(prefix):],
#         'timestamp': cast['timestamp'],
#         'author_fid': cast['author_fid']
#     }


def set_more_info_from_bio(engine):
    session = sessionmaker(bind=engine)()
    all_user = session.query(User).all()

    for user in all_user:
        if user.bio_text:
            bio_text = user.bio_text.lower()

            if 'twitter' in bio_text and user.twitter is None:
                twitter_username = re.search(r'(\S+)\.twitter', bio_text)
                if twitter_username:
                    twitter_username = twitter_username.group(1)
                    twitter_username = re.sub(
                        r'[^\w\s]|_', '', twitter_username)
                    # print(twitter_username)
                    user.twitter = twitter_username

            if 'telegram' in bio_text and user.telegram is None:
                telegram_username = re.search(r'(\S+)\.telegram', bio_text)
                if telegram_username:
                    telegram_username = telegram_username.group(1)
                    # print(telegram_username)
                    user.telegram = telegram_username

            session.merge(user)
    session.commit()
    session.close()


def not_none(i):
    return i is not None