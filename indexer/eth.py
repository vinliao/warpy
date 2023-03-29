import os
from dotenv import load_dotenv
import requests
from sqlalchemy.orm import sessionmaker
from models import Base, User, EthTransaction, ERC1155Metadata
from sqlalchemy import create_engine, text
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


def get_users_from_addresses(session, addresses):
    return session.query(User).filter(User.external_address.in_(addresses)).all()


def get_existing_hashes(session):
    return set(tx.hash for tx in session.query(EthTransaction.hash).all())


def insert_transactions_to_db(session, all_transactions):
    txs = []
    metadata_objs = []

    existing_hashes = get_existing_hashes(session)

    users_from_addresses = get_users_from_addresses(
        session, [tx['from'] for tx in all_transactions] + [tx['to'] for tx in all_transactions])
    users_from_addresses_dict = {
        user.external_address: user for user in users_from_addresses}

    for transaction in all_transactions:
        user_from = users_from_addresses_dict.get(transaction['from'])
        user_to = users_from_addresses_dict.get(transaction['to'])

        if user_from or user_to:
            if user_from:
                eth_transaction_dict, erc1155_metadata_dicts = make_transaction_models(
                    transaction, user_from.fid, user_from.external_address)
            elif user_to:
                eth_transaction_dict, erc1155_metadata_dicts = make_transaction_models(
                    transaction, user_to.fid, user_to.external_address)

            if eth_transaction_dict['hash'] not in existing_hashes:
                txs.append(EthTransaction(**eth_transaction_dict))
                existing_hashes.add(eth_transaction_dict['hash'])

            if erc1155_metadata_dicts:
                for erc1155_metadata_dict in erc1155_metadata_dicts:
                    metadata_objs.append(
                        ERC1155Metadata(**erc1155_metadata_dict))

    print(f"inserting {len(txs)} txs and {len(metadata_objs)} metadata")

    session.add_all(txs)
    session.add_all(metadata_objs)
    session.commit()


def process_user_batch(session, users_batch):
    user_addresses = {user.external_address: user.fid for user in users_batch}

    all_transactions = []
    for address in user_addresses.keys():
        transactions = get_all_transactions(address)
        all_transactions.extend(transactions)

    insert_transactions_to_db(session, all_transactions)


def batch_users(users, batch_size):
    for i in range(0, len(users), batch_size):
        yield users[i:i + batch_size]


def process_users_in_batches(session, users, batch_size=10):
    for users_batch in batch_users(users, batch_size):
        process_user_batch(session, users_batch)
        print(f"Processed batch of {len(users_batch)} users")


def main():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    db_path = os.path.join(parent_dir, 'datasets', 'datasets.db')
    engine = create_engine('sqlite:///' + db_path)

    with sessionmaker(bind=engine)() as session:
        # get all users with external address
        users = session.query(User).filter(User.external_address != None).all()

        process_users_in_batches(session, users, batch_size=3)
        print(f"Done inserting transactions")


if __name__ == "__main__":
    main()
