from typing import Callable, Any, List
import asyncio
import aiohttp


async def batch_fetcher(batch_elements: List, build_url: Callable[[Any, Any], str], process_response: Callable[[Any], Any], error_handler: Callable[[Any], None], get_next_page_token: Callable[[Any], Any], batch_size: int = 10, max_retries: int = 3, timeout_seconds: int = 10):
    async def fetch_single_element(element, session, retries=max_retries):
        page_token = None
        results = []
        while True:
            try:
                url = build_url(element, page_token)
                print(f"Fetching {url}")
                timeout = aiohttp.ClientTimeout(total=timeout_seconds)
                async with session.get(url, timeout=timeout) as response:
                    await asyncio.sleep(1)
                    response.raise_for_status()
                    json_data = await response.json()

                    if json_data:
                        results.append(process_response(json_data))
                        page_token = get_next_page_token(json_data)
                        if page_token is None:
                            break
                    else:
                        raise ValueError(
                            f"No results found for element {element}")
            except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                error_handler(element, e)
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying
                retries -= 1
                if retries <= 0:
                    break
        return results

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(0, len(batch_elements), batch_size):
            tasks += [asyncio.create_task(fetch_single_element(element, session))
                      for element in batch_elements[i:i + batch_size]]
        results = await asyncio.gather(*tasks)

    return list(filter(None, results))
