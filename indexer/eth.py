import asyncio
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from utils.models import (
    ENSData,
    ERC1155Metadata,
    EthTransaction,
    User,
    user_eth_transactions_association,
)
from utils.new_fetcher import AsyncFetcher
from utils.utils import save_objects

load_dotenv()


class AlchemyTransactionFetcher(AsyncFetcher):
    def __init__(self, key: str, addresses_blocknum: List[Tuple[str, int]]):
        self.base_url = f"https://eth-mainnet.g.alchemy.com/v2/{key}"
        self.transactions: List[Dict[str, Any]] = []
        self.addresses_blocknum = addresses_blocknum

    def _get_addresses(self) -> List[str]:
        return [address for address, _ in self.addresses_blocknum]

    async def _fetch_data(self):
        # Fetch data concurrently using asyncio.gather
        tasks = [
            self._fetch_data_for_address(address, latest_block_of_user)
            for address, latest_block_of_user in self.addresses_blocknum
        ]
        all_transactions = await asyncio.gather(*tasks)

        # Flatten the list of transactions and update self.transactions
        self.transactions = [
            transaction for sublist in all_transactions for transaction in sublist
        ]

    async def _fetch_data_for_address(self, address: str, latest_block_of_user: int):
        transactions = []
        page_key = None
        while True:
            headers = {"Content-Type": "application/json"}

            to_payload = self._build_payload(
                address=address,
                latest_block_of_user=latest_block_of_user,
                addr_type="toAddress",
                page_key=page_key,
            )
            from_payload = self._build_payload(
                address=address,
                latest_block_of_user=latest_block_of_user,
                addr_type="fromAddress",
                page_key=page_key,
            )

            from_response = await self._make_async_request_with_retry(
                self.base_url, headers=headers, data=from_payload, method="POST"
            )
            to_response = await self._make_async_request_with_retry(
                self.base_url, headers=headers, data=to_payload, method="POST"
            )

            if not from_response and not to_response:
                break

            from_data = from_response.get("result", {})
            to_data = to_response.get("result", {})

            transactions += from_data.get("transfers", []) + to_data.get(
                "transfers", []
            )

            from_page_key = from_data.get("pageKey")
            to_page_key = to_data.get("pageKey")

            if from_page_key is None and to_page_key is None:
                break
            elif from_page_key is not None:
                page_key = from_page_key
            else:
                page_key = to_page_key

        return transactions

    def _get_models(self) -> List[Union[EthTransaction, ERC1155Metadata]]:
        models = []

        for transaction in self.transactions:
            to_address = transaction.get("to")
            from_address = transaction.get("from")

            # Use the original address that was passed to the API
            address = (
                to_address if to_address in self._get_addresses() else from_address
            )

            if address is None:
                continue

            eth_transaction, erc1155_metadata_objs = self._extract_data(
                transaction, address
            )
            models.append(eth_transaction)
            models.extend(erc1155_metadata_objs)

        # txs = [tx for tx in models if isinstance(tx, EthTransaction)]
        # addresses_with_transactions = set([tx.address_external for tx in txs])

        # addresses_without_transactions = [
        #     address
        #     for address in self._get_addresses()
        #     if address not in addresses_with_transactions
        # ]

        # current_block_height = self._get_current_block_height()

        # for address in addresses_without_transactions:
        #     models.append(self._make_empty_transaction(address, current_block_height))

        return models

    def _make_empty_transaction(
        self, address: str, current_block_height: int
    ) -> EthTransaction:
        dead_address = "0x0000000000000000000000000000000000000000"
        return EthTransaction(
            unique_id=f"empty-{address}",
            hash=dead_address,
            timestamp=int(round(time.time() * 1000)),
            block_num=current_block_height,  # current block number (when fetching)
            from_address=dead_address,
            to_address=dead_address,
            value=-1,
            erc721_token_id=None,
            token_id=None,
            asset=None,
            category="empty",
        )

    def _get_current_block_height(self) -> int:
        # data = requests.post(self.base_url, data={"method": "getBlockHeight"}).json()
        payload = {"jsonrpc": "2.0", "method": "eth_blockNumber", "id": 0}
        headers = {"Content-Type": "application/json"}
        response = requests.post(self.base_url, json=payload, headers=headers)
        result_hex = response.json().get("result", "")
        return int(result_hex, 16)

    def _extract_data(
        self, transaction: Dict[str, Any], address: str
    ) -> Tuple[EthTransaction, List[ERC1155Metadata]]:
        eth_transaction = EthTransaction(
            unique_id=transaction["uniqueId"],
            hash=transaction["hash"],
            timestamp=int(
                datetime.strptime(
                    transaction["metadata"]["blockTimestamp"], "%Y-%m-%dT%H:%M:%S.%fZ"
                ).timestamp()
            )
            * 1000,
            block_num=int(transaction["blockNum"], 16),
            from_address=transaction["from"],
            to_address=transaction["to"],
            value=transaction["value"],
            erc721_token_id=transaction.get("erc721TokenId"),
            token_id=transaction.get("tokenId"),
            asset=transaction.get("asset"),
            category=transaction.get("category", "unknown"),
        )

        erc1155_metadata_list = transaction.get("erc1155Metadata") or []
        erc1155_metadata_objs = []

        for metadata in erc1155_metadata_list:
            erc1155_metadata = ERC1155Metadata(
                eth_transaction_hash=transaction["hash"],
                token_id=metadata["tokenId"],
                value=metadata["value"],
            )
            erc1155_metadata_objs.append(erc1155_metadata)

        return eth_transaction, erc1155_metadata_objs

    def _build_payload(
        self,
        address: str,
        latest_block_of_user: int,
        addr_type: str,
        page_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": [
                {
                    "fromBlock": f"0x{latest_block_of_user:x}",
                    "toBlock": "latest",
                    addr_type: address,
                    "category": [
                        "erc721",
                        "erc1155",
                        "erc20",
                        "specialnft",
                        "external",
                    ],
                    "withMetadata": True,
                    "excludeZeroValue": True,
                    "maxCount": "0x3e8",
                }
            ],
        }
        if page_key:
            payload["params"][0]["pageKey"] = page_key
        return payload

    async def fetch(self):
        await self._fetch_data()
        return self._get_models()


def make_user_transaction_association(
    session: Session, user_fid: int, eth_transaction_unique_id: str
):
    print(
        f"Creating association for user {user_fid} and transaction {eth_transaction_unique_id}"
    )
    # Check if the association already exists
    existing_association = (
        session.query(user_eth_transactions_association)
        .filter_by(
            user_fid=user_fid, eth_transaction_unique_id=eth_transaction_unique_id
        )
        .first()
    )

    if not existing_association:
        # If the association doesn't exist, create it
        new_association = user_eth_transactions_association.insert().values(
            user_fid=user_fid, eth_transaction_unique_id=eth_transaction_unique_id
        )
        session.execute(new_association)
        session.commit()
        print("Association created!")
    else:
        print("Association already exists!")


async def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        addresses = session.query(ENSData).all()
        addresses_string = [address.address for address in addresses]
        addresses_string = addresses_string[:10]

        # addresses_blocknum = []
        addresses_blocknum = [(address, 0) for address in addresses_string]
        alchemy_api_key = os.getenv("ALCHEMY_API_KEY")
        if not alchemy_api_key:
            raise ValueError("Missing ALCHEMY_API_KEY")

        batch_size = 5
        for i in range(0, len(addresses_blocknum), batch_size):
            batch = addresses_blocknum[i : i + batch_size]  # noqa: E203
            fetcher = AlchemyTransactionFetcher(
                key=alchemy_api_key, addresses_blocknum=batch
            )
            txs = await fetcher.fetch()
            save_objects(session, txs)

        # make new associations
        users = session.query(User).all()
        for user in users:
            for tx in [x for x in txs if isinstance(x, EthTransaction)]:
                if tx.to_address == user.address:
                    make_user_transaction_association(session, user.fid, tx.unique_id)
                elif tx.from_address == user.address:
                    make_user_transaction_association(session, user.fid, tx.unique_id)
