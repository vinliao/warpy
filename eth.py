import json
import os
from dotenv import load_dotenv
import requests
from sqlalchemy.orm import sessionmaker
from models import Base, User, EthTransaction, ERC1155Metadata
from sqlalchemy import create_engine
from datetime import datetime
import random

load_dotenv()


def get_transactions_from_alchemy(address, page_key=None):
    url = f"https://eth-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY')}"
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "fromBlock": "0x0",
                "toBlock": "latest",
                "toAddress": address,
                "category": ["erc721", "erc1155", "erc20", "specialnft", "external"],
                "withMetadata": True,
                "excludeZeroValue": True,
                "maxCount": "0x3e8",
            }
        ]
    }

    print(address, page_key)

    if page_key:
        payload['params'][0]['pageKey'] = page_key

    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    json_data = response.json()

    return {"transactions": json_data['result']['transfers'],
            "page_key": json_data.get('result', {}).get('pageKey') if json_data.get('result') else None}


def get_all_transactions(address):
    transactions = []
    page_key = None
    while True:
        data = get_transactions_from_alchemy(address, page_key)
        transactions += data['transactions']
        page_key = data['page_key']
        if page_key is None:
            break
    return transactions

# print(get_all_transactions('0xd7029bdea1c17493893aafe29aad69ef892b8ff2'))


def make_transaction_models(transfer, address_fid, address_external):
    eth_transaction_dict = {
        "hash": transfer['hash'],
        "address_fid": address_fid,
        "address_external": address_external,
        "timestamp": int(datetime.strptime(transfer['metadata']['blockTimestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()) * 1000,
        "block_num": int(transfer['blockNum'], 16),
        "from_address": transfer['from'],
        "to_address": transfer['to'],
        "value": transfer['value'],
        "erc721_token_id": transfer['erc721TokenId'] if 'erc721TokenId' in transfer else None,
        "token_id": transfer['tokenId'] if 'tokenId' in transfer else None,
        "asset": transfer['asset'] if 'asset' in transfer else None,
        "category": transfer['category'] if 'category' in transfer else None
    }

    erc1155_metadata_list = transfer.get('erc1155Metadata')
    erc1155_metadata_dicts = []

    if erc1155_metadata_list:
        for metadata in erc1155_metadata_list:
            erc1155_metadata_dict = {
                'eth_transaction_hash': transfer['hash'],
                'token_id': metadata['tokenId'],
                'value': metadata['value']
            }
            erc1155_metadata_dicts.append(erc1155_metadata_dict)

    return eth_transaction_dict, erc1155_metadata_dicts


# def insert_transactions_to_db(session, transactions):
#     txs = []
#     metadata_objs = []

#     for transaction in transactions:
#         user_from = session.query(User).filter(
#             User.external_address == transaction['from']).first()
#         user_to = session.query(User).filter(
#             User.external_address == transaction['to']).first()

#         if user_from or user_to:
#             if user_from:
#                 eth_transaction_dict, erc1155_metadata_dicts = make_transaction_models(
#                     transaction, user_from.fid, user_from.external_address)
#             elif user_to:
#                 eth_transaction_dict, erc1155_metadata_dicts = make_transaction_models(
#                     transaction, user_to.fid, user_to.external_address)

#             eth_transaction = EthTransaction(**eth_transaction_dict)

#             # check if transaction already exists
#             existing_tx = session.query(EthTransaction).filter_by(
#                 hash=eth_transaction_dict['hash']).first()
#             if not existing_tx:
#                 txs.append(eth_transaction)

#             if erc1155_metadata_dicts:
#                 for erc1155_metadata_dict in erc1155_metadata_dicts:
#                     metadata_obj = ERC1155Metadata(**erc1155_metadata_dict)
#                     metadata_objs.append(metadata_obj)

#     print(f"inserting {len(txs)} txs and {len(metadata_objs)} metadata")
#     session.bulk_save_objects(txs)
#     session.bulk_save_objects(metadata_objs)
#     session.commit()


# engine = create_engine(os.getenv('PLANETSCALE_URL'))
# with sessionmaker(bind=engine)() as session:
#     # get all users at once
#     users = session.query(User).all()

#     for user in users:
#         if user.external_address:
#             transactions = get_all_transactions(user.external_address)
#             insert_transactions_to_db(session, transactions)
#             print(
#                 f"Done with {user.fid}, {user.username}, {user.external_address}")

def insert_transactions_to_db(session, transactions):
    txs = []
    metadata_objs = []

    existing_hashes = set(
        tx.hash for tx in session.query(EthTransaction).all())

    for transaction in transactions:
        user_from = session.query(User).filter(
            User.external_address == transaction['from']).first()
        user_to = session.query(User).filter(
            User.external_address == transaction['to']).first()

        if user_from or user_to:
            if user_from:
                eth_transaction_dict, erc1155_metadata_dicts = make_transaction_models(
                    transaction, user_from.fid, user_from.external_address)
            elif user_to:
                eth_transaction_dict, erc1155_metadata_dicts = make_transaction_models(
                    transaction, user_to.fid, user_to.external_address)

            if eth_transaction_dict['hash'] not in existing_hashes:
                eth_transaction = EthTransaction(**eth_transaction_dict)
                txs.append(eth_transaction)
                existing_hashes.add(eth_transaction_dict['hash'])

            if erc1155_metadata_dicts:
                for erc1155_metadata_dict in erc1155_metadata_dicts:
                    metadata_obj = ERC1155Metadata(**erc1155_metadata_dict)
                    metadata_objs.append(metadata_obj)

    print(f"inserting {len(txs)} txs and {len(metadata_objs)} metadata")
    session.bulk_save_objects(txs)
    session.bulk_save_objects(metadata_objs)
    session.commit()


engine = create_engine(os.getenv('PLANETSCALE_URL'))
with sessionmaker(bind=engine)() as session:
    # get all users at once
    users = session.query(User).all()
    # shuffle users
    users = random.sample(users, len(users))

    for user in users:
        if user.external_address:
            transactions = get_all_transactions(user.external_address)
            insert_transactions_to_db(session, transactions)
            print(
                f"Done with {user.fid}, {user.username}, {user.external_address}")

    # user_external_addresses = [
    #     user.external_address for user in users if user.external_address]

    # # get all transactions at once
    # all_transactions = []
    # for external_address in user_external_addresses:
    #     all_transactions += get_all_transactions(external_address)

    # insert_transactions_to_db(session, all_transactions)

    # for user in users:
    #     if user.external_address:
    #         print(f"Done with {user.fid}, {user.username}, {user.external_address}")
