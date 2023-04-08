from typing import Dict, Any, Optional
import asyncio
import aiohttp
from abc import ABC, abstractmethod
from typing import Any, Dict
import os


class BatchFetcher(ABC):
    def __init__(self, base_url: str, batch_size: int = 10):
        self.base_url = base_url
        self.batch_size = batch_size

    async def fetch(self, items: list) -> Dict[str, Any]:
        tasks = [self._fetch_item(item) for item in items]
        results = await asyncio.gather(*tasks)
        return {item: result for item, result in zip(items, results)}

    async def _fetch_item(self, item: str) -> Any:
        async with aiohttp.ClientSession() as session:
            try:
                url, headers, timeout, method, payload = self._prepare_request(
                    item)
                async with await self._send_request(session, url, headers, timeout, method, payload) as response:
                    await asyncio.sleep(1)
                    response.raise_for_status()
                    data = await response.json()
                    result, _ = self._process_response(data)
                    return result
            except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                print(f"Error occurred for {item}: {e}. Retrying...")
                await asyncio.sleep(5)

    async def _fetch_item_with_pagination(self, item: str, cursor: Optional[str]) -> Any:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    url, headers, timeout, method, payload = self._prepare_request(
                        item, cursor)
                    async with self._send_request(session, url, headers, timeout, method, payload) as response:
                        await asyncio.sleep(1)
                        response.raise_for_status()
                        data = await response.json()
                        result, next_cursor = self._process_response(data)

                        if next_cursor is None:
                            return result

                        cursor = next_cursor
                except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                    print(f"Error occurred for {item}: {e}. Retrying...")
                    await asyncio.sleep(5)

    @abstractmethod
    def _prepare_request(self, item: str, cursor: Optional[str] = None) -> tuple:
        pass

    @abstractmethod
    def _process_response(self, data: Dict[str, Any]) -> tuple:
        pass

    async def _send_request(self, session, url, headers, timeout, method, payload):
        print(f"Sending {method} request to {url}")
        if method == "GET":
            return await session.get(url, headers=headers, timeout=timeout)
        elif method == "POST":
            return await session.post(url, headers=headers, json=payload, timeout=timeout)


class SearchcasterFetcher(BatchFetcher):
    def _prepare_request(self, username: str, cursor: Optional[str] = None) -> tuple:
        url = f'{self.base_url}?username={username}'
        headers = None
        timeout = aiohttp.ClientTimeout(total=10)
        method = "GET"
        payload = None
        return url, headers, timeout, method, payload

    def _process_response(self, data: Dict[str, Any]) -> tuple:
        if data:
            return data[0], None
        else:
            raise ValueError(f"No results found.")


class WarpcastReactionFetcher(BatchFetcher):
    def __init__(self, base_url: str, bearer_token: str, batch_size: int = 10):
        super().__init__(base_url, batch_size)
        self.bearer_token = bearer_token

    def _prepare_request(self, cast_hash: str, cursor: Optional[str]) -> tuple:
        url = f'{self.base_url}?castHash={cast_hash}&limit=100'
        if cursor:
            url += f'&cursor={cursor}'
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        timeout = aiohttp.ClientTimeout(total=10)
        return url, headers, timeout

    def _process_response(self, data: Dict[str, Any]) -> tuple:
        reactions = data['result']['reactions']
        next_cursor = data.get('next', {}).get('cursor')
        return reactions, next_cursor


class AlchemyFetcher(BatchFetcher):
    def __init__(self, base_url: str, api_key: str, batch_size: int = 10):
        super().__init__(base_url, batch_size)
        self.api_key = api_key

    def _prepare_request(self, item: str, cursor: Optional[str]) -> tuple:
        url = f"{self.base_url}{self.api_key}"
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        timeout = aiohttp.ClientTimeout(total=10)

        from_block = cursor if cursor else "0x0"
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": [
                {
                    "fromBlock": from_block,
                    "toBlock": "latest",
                    "toAddress": item,
                    "category": ["erc721", "erc1155", "erc20", "specialnft", "external"],
                    "withMetadata": True,
                    "excludeZeroValue": True,
                    "maxCount": "0x3e8"
                }
            ]
        }

        return url, headers, timeout, payload

    async def _fetch_item_with_pagination(self, item: str, cursor: Optional[str]) -> Any:
        transactions = []
        while True:
            url, headers, timeout, payload = self._prepare_request(
                item, cursor)

            async with aiohttp.ClientSession() as session:
                while True:
                    try:
                        async with session.post(url, json=payload, headers=headers, timeout=timeout) as response:
                            await asyncio.sleep(1)
                            response.raise_for_status()
                            data = await response.json()
                            result, next_cursor = self._process_response(data)

                            transactions.extend(result["transactions"])

                            if next_cursor is None:
                                return transactions

                            cursor = next_cursor
                    except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                        print(f"Error occurred for {item}: {e}. Retrying...")
                        await asyncio.sleep(5)

    def _process_response(self, data: Dict[str, Any]) -> tuple:
        transfers = data["result"]["transfers"]
        page_key = data.get("result", {}).get(
            "pageKey") if data.get("result") else None
        return {"transactions": transfers, "page_key": page_key}, page_key
