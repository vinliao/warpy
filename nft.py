import json
import os
from dotenv import load_dotenv
import requests
from sqlalchemy.orm import sessionmaker
from models import Base, User, Location, EthTransaction
from sqlalchemy import create_engine
from datetime import datetime

load_dotenv()


def get_nft_data_from_alchemy(address, page_key=None):
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

# write function to get all transactions for a user (loop until page key is none)


def get_all_transactions(address):
    transactions = []
    page_key = None
    while True:
        data = get_nft_data_from_alchemy(address, page_key)
        transactions += data['transactions']
        page_key = data['page_key']
        if page_key is None:
            break
    return transactions


def transfer_to_eth_transaction(transfer, address_fid, address_external):
    return {
        "hash": transfer['hash'],
        "address_fid": address_fid,
        "address_external": address_external,
        "timestamp": int(datetime.strptime(transfer['metadata']['blockTimestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()) * 1000,
        "block_num": int(transfer['blockNum'], 16),
        "from_address": transfer['from'],
        "to_address": transfer['to'],
        "value": transfer['value'],
        "erc721_token_id": transfer['erc721TokenId'] if 'erc721TokenId' in transfer else None,
        "erc1155_metadata": json.dumps(transfer['erc1155Metadata']) if 'erc1155Metadata' in transfer and transfer['erc1155Metadata'] is not None else None,
        "token_id": transfer['tokenId'] if 'tokenId' in transfer else None,
        "asset": transfer['asset'] if 'asset' in transfer else None,
        "category": transfer['category'] if 'category' in transfer else None
    }


def insert_transactions_to_db(engine, transactions):
    with sessionmaker(bind=engine)() as session:
        txs = []

        for transaction in transactions:
            user_from = session.query(User).filter(
                User.external_address == transaction['from']).first()
            user_to = session.query(User).filter(
                User.external_address == transaction['to']).first()

            if user_from or user_to:
                if user_from:
                    eth_transaction_dict = transfer_to_eth_transaction(
                        transaction, user_from.fid, user_from.external_address)
                elif user_to:
                    eth_transaction_dict = transfer_to_eth_transaction(
                        transaction, user_to.fid, user_to.external_address)

                eth_transaction = EthTransaction(**eth_transaction_dict)
                txs.append(eth_transaction)

        # filter out txs where hash already exists
        txs = list(filter(lambda tx: session.query(EthTransaction).filter(
            EthTransaction.hash == tx.hash).first() is None, txs))
        
        # filter out duplciate hashes
        txs = list({tx.hash: tx for tx in txs}.values())

        print(f"inserting {len(txs)} txs")
        session.add_all(txs)
        session.commit()


engine = create_engine('sqlite:///data.db')
session = sessionmaker(bind=engine)()

fid = session.query(User).order_by(User.fid.desc()).first().fid
print(fid)

# note: fid 166 isn't indexed because there's absurd amounts of transactions
for i in range(7326, fid + 1):
    user = session.query(User).filter(User.fid == i).first()
    if user is not None and user.external_address:
        transactions = get_all_transactions(user.external_address)
        insert_transactions_to_db(engine, transactions)
        print(
            f"Done with {user.fid}, {user.username}, {user.external_address}")
