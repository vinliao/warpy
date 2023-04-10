import os
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from utils.models import Base, User, EthTransaction, ERC1155Metadata, ExternalAddress
from sqlalchemy import func
from datetime import datetime
from sqlalchemy.engine import Engine
from utils.fetcher import AlchemyTransactionFetcher
from utils.utils import save_objects

load_dotenv()


# def insert_transactions_to_db(session, all_transactions):
#     txs = []
#     metadata_objs = []

#     existing_hashes = get_existing_hashes(session)

#     users_from_addresses = get_users_from_addresses(
#         session, [tx['from'] for tx in all_transactions] + [tx['to'] for tx in all_transactions])
#     users_from_addresses_dict = {
#         user.external_address: user for user in users_from_addresses}

#     for transaction in all_transactions:
#         user_from = users_from_addresses_dict.get(transaction['from'])
#         user_to = users_from_addresses_dict.get(transaction['to'])

#         if user_from or user_to:
#             if user_from:
#                 eth_transaction_dict, erc1155_metadata_dicts = make_transaction_models(
#                     transaction, user_from.fid, user_from.external_address)
#             elif user_to:
#                 eth_transaction_dict, erc1155_metadata_dicts = make_transaction_models(
#                     transaction, user_to.fid, user_to.external_address)

#             if eth_transaction_dict['hash'] not in existing_hashes:
#                 txs.append(EthTransaction(**eth_transaction_dict))
#                 existing_hashes.add(eth_transaction_dict['hash'])

#             if erc1155_metadata_dicts:
#                 for erc1155_metadata_dict in erc1155_metadata_dicts:
#                     if not metadata_exists(metadata_objs, erc1155_metadata_dict['token_id'], erc1155_metadata_dict['eth_transaction_hash']):
#                         metadata_objs.append(
#                             ERC1155Metadata(**erc1155_metadata_dict))

#     print(f"inserting {len(txs)} txs and {len(metadata_objs)} metadata")

#     session.add_all(txs)
#     session.add_all(metadata_objs)
#     session.commit()


# def get_users_from_addresses(session, addresses):
#     return session.query(User).filter(User.external_address.in_(addresses)).all()


# def get_existing_hashes(session):
#     return set(tx.hash for tx in session.query(EthTransaction.hash).all())


# def metadata_exists(metadata_objs, token_id, eth_transaction_hash):
#     for metadata in metadata_objs:
#         if metadata.token_id == token_id and metadata.eth_transaction_hash == eth_transaction_hash:
#             return True
#     return False


async def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        # get three random external addresses
        addresses = session.query(ExternalAddress).order_by(
            func.random()).limit(3).all()

        addresses_string = [address.address for address in addresses]

        fetcher = AlchemyTransactionFetcher(key=os.getenv('ALCHEMY_API_KEY'))
        data = await fetcher.fetch_data(addresses_string)
        transactions = fetcher.get_models(data)
        save_objects(session, transactions)
