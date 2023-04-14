import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import aiohttp
import requests


class Fetcher(ABC):
    @abstractmethod
    def fetch(self):
        """The interface to fetch data from the respective sources."""

    def _make_request_headers(
        self, headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        if headers is None:
            return {}
        return headers

    @abstractmethod
    def _extract_data(self, data):
        """Turn JSON data from API response to SQLAlchemy models."""

    @abstractmethod
    def _fetch_data(self):
        """Fetch data from the respective sources."""

    @abstractmethod
    def _get_models(self):
        """Return clean data models ready for insertion into the database."""

    def _make_request(self, url, headers=None, timeout=10) -> Any:
        """Makes a synchronous GET request to the specified URL."""
        print(f"Fetching from {url}")
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()


class SyncFetcher(Fetcher):
    def _make_request(
        self, url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 10
    ) -> Any:
        """
        Makes a synchronous GET request to the specified URL, returns JSON.
        """
        headers = self._make_request_headers(headers)
        print(f"Fetching from {url}")
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()


class AsyncFetcher(Fetcher):
    async def _make_async_request(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        data: Any = None,
        method: str = "GET",
        timeout: int = 10,
    ) -> Any:
        """
        Makes an asynchronous request to the specified URL, returns JSON.
        """
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as session:
            print(f"Fetching from {url}")
            if method == "GET":
                async with session.get(url, headers=headers) as response:
                    response.raise_for_status()
                    return await response.json()
            elif method == "POST":
                async with session.post(url, headers=headers, json=data) as response:
                    response.raise_for_status()
                    return await response.json()
            else:
                raise ValueError(f"Invalid method: {method}")

    async def _make_async_request_with_retry(
        self,
        url: str,
        max_retries: int = 3,
        delay: int = 5,
        headers: Optional[Dict[str, str]] = None,
        method: str = "GET",
        data: Any = None,
        timeout: int = 10,
    ) -> Optional[Any]:
        """
        Makes an asynchronous request to the specified URL with retries.
        """
        retries = 0
        while retries < max_retries:
            try:
                response = await self._make_async_request(
                    url, headers=headers, data=data, method=method, timeout=timeout
                )

                response_data = response
                if response_data:
                    return response_data
                else:
                    print(f"No results found for the URL: {url}. Retrying...")

            except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                if e is not None:
                    print(f"Error occurred for URL {url}. Error: {e}. Retrying...")
                else:
                    print(f"Error occurred for URL {url}. Retrying...")

            await asyncio.sleep(delay)
            retries += 1

        print(f"Failed to fetch data from URL {url} after {max_retries} attempts.")
        return None
