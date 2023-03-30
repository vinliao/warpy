import os
import asyncio
import aiohttp
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from models import Base, User, EthTransaction, ERC1155Metadata
from sqlalchemy import create_engine, func
from datetime import datetime

load_dotenv()


async def get_transactions_from_alchemy(address, page_key=None):
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

    if page_key:
        payload['params'][0]['pageKey'] = page_key

    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    print(f"Fetching transactions for {address}...")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.post(url, json=payload, headers=headers, timeout=timeout) as response:
                    json_data = await response.json()
                    response.raise_for_status()
                    return {"transactions": json_data['result']['transfers'],
                            "page_key": json_data.get('result', {}).get('pageKey') if json_data.get('result') else None}
            except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                print(f"Error occurred for {address}: {e}. Retrying...")
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying
            except Exception as e:
                print(
                    f"Unknown error occurred for {address}: {e}. Retrying...")
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying


async def get_all_transactions(address):
    transactions = []
    page_key = None
    while True:
        data = await get_transactions_from_alchemy(address, page_key)
        transactions += data['transactions']
        page_key = data['page_key']
        if page_key is None:
            break
    return transactions


async def process_user_batch(session, users_batch):
    user_addresses = {user.external_address: user.fid for user in users_batch}

    tasks = [get_all_transactions(address)
             for address in user_addresses.keys()]
    all_transactions = await asyncio.gather(*tasks)
    all_transactions = [
        transaction for sublist in all_transactions for transaction in sublist]

    insert_transactions_to_db(session, all_transactions)


async def process_users_in_batches(session, users, batch_size=10):
    for users_batch in batch_users(users, batch_size):
        await process_user_batch(session, users_batch)
        print(f"Processed batch of {len(users_batch)} users")


def batch_users(users, batch_size):
    for i in range(0, len(users), batch_size):
        yield users[i:i + batch_size]


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
                    if not metadata_exists(metadata_objs, erc1155_metadata_dict['token_id'], erc1155_metadata_dict['eth_transaction_hash']):
                        metadata_objs.append(
                            ERC1155Metadata(**erc1155_metadata_dict))

    print(f"inserting {len(txs)} txs and {len(metadata_objs)} metadata")

    session.add_all(txs)
    session.add_all(metadata_objs)
    session.commit()


def get_users_from_addresses(session, addresses):
    return session.query(User).filter(User.external_address.in_(addresses)).all()


def get_existing_hashes(session):
    return set(tx.hash for tx in session.query(EthTransaction.hash).all())


def metadata_exists(metadata_objs, token_id, eth_transaction_hash):
    for metadata in metadata_objs:
        if metadata.token_id == token_id and metadata.eth_transaction_hash == eth_transaction_hash:
            return True
    return False


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


def main():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    db_path = os.path.join(parent_dir, 'datasets', 'datasets.db')
    engine = create_engine('sqlite:///' + db_path)

    with sessionmaker(bind=engine)() as session:
        users = session.query(User).filter(
            User.fid.notin_(session.query(EthTransaction.address_fid))
        ).filter(User.external_address.isnot(None)).order_by(func.random()).all()
        print(f"Found {len(users)} users with external addresses to process")

        if len(users) == 0:
            pass

        # not include @4156 because they have too much transactions...
        users = [user for user in users if user.fid != 166]

        asyncio.run(process_users_in_batches(session, users, batch_size=5))
        print(f"Done inserting transactions")


if __name__ == "__main__":
    asyncio.run(main())
