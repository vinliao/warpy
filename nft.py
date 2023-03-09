import json
import os
from dotenv import load_dotenv
import requests
from sqlalchemy.orm import sessionmaker
from models import Base, User, Location, EthTransaction
from sqlalchemy import create_engine
from datetime import datetime

load_dotenv()

engine = create_engine('sqlite:///data.db')
Session = sessionmaker(bind=engine)
session = Session()

# # from db, get all users where external_address is not null and sort by follower count (get only 1)
# user = session.query(User).filter(User.external_address !=
#                                   None).order_by(User.follower_count.desc()).first()
user = session.query(User).filter(User.fid == 3).first()

address = user.external_address

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
            "category": ["erc721", "erc1155", "erc20", "specialnft"],
            "withMetadata": True,
            "excludeZeroValue": True,
            "maxCount": "0x10"
        }
    ]
}
headers = {
    "accept": "application/json",
    "content-type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)

json_data = response.json()

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
        "erc1155_metadata": transfer['erc1155Metadata'] if 'erc1155Metadata' in transfer else None,
        "token_id": transfer['tokenId'] if 'tokenId' in transfer else None,
        "asset": transfer['asset'] if 'asset' in transfer else None,
        "category": transfer['category'] if 'category' in transfer else None
    }

eth_transactions = []
for transfer in json_data['result']['transfers']:
    user_from = session.query(User).filter(
        User.external_address == transfer['from']).first()
    user_to = session.query(User).filter(
        User.external_address == transfer['to']).first()
    if user_from:
        eth_transaction_dict = transfer_to_eth_transaction(
            transfer, user_from.fid, user_from.external_address)
        eth_transaction = EthTransaction(**eth_transaction_dict)
        eth_transactions.append(eth_transaction)
    if user_to:
        eth_transaction_dict = transfer_to_eth_transaction(
            transfer, user_to.fid, user_to.external_address)
        eth_transaction = EthTransaction(**eth_transaction_dict)
        eth_transactions.append(eth_transaction)

for eth_transaction in eth_transactions:
    session.merge(eth_transaction)
    session.commit()