import json
import os
from dotenv import load_dotenv
import requests
from sqlalchemy.orm import sessionmaker
from models import Base, User, Location
from sqlalchemy import create_engine
from datetime import datetime

load_dotenv()

engine = create_engine('sqlite:///data.db')
Session = sessionmaker(bind=engine)
session = Session()

# from db, get all users where external_address is not null and sort by follower count (get only 1)
user = session.query(User).filter(User.external_address !=
                                  None).order_by(User.follower_count.desc()).first()
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
            "maxCount": "0x05"
        }
    ]
}
headers = {
    "accept": "application/json",
    "content-type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)

json_data = response.json()


def transfer_to_eth_transaction(transfer):
    eth_transaction = {
        "hash": transfer['hash'],
        "timestamp": int(datetime.strptime(transfer['metadata']['blockTimestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()) * 1000,
        "block_num": int(transfer['blockNum'], 16),
        "from_address": transfer['from'],
        "to_address": transfer['to'],
        "value": transfer['value'],
        "erc721_token_id": transfer['erc721TokenId'],
        "erc1155_metadata": transfer['erc1155Metadata'],
        "token_id": transfer['tokenId'],
        "asset": transfer['asset'],
        "category": transfer['category']
    }
    return eth_transaction


eth_transactions = list(map(transfer_to_eth_transaction,
                        json_data['result']['transfers']))
# save as eth_transactions.json


with open('eth_transactions.json', 'w') as f:
    json.dump(eth_transactions, f)
