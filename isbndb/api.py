from typing import Any

import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from isbndb.exceptions import RateLimitExceeded


@retry(
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def fetch_books(client: httpx.AsyncClient, isbns: list[str], api_key: str) -> dict[str, Any]:
    url = "https://api2.isbndb.com/books"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:143.0) Gecko/20100101 Firefox/143.0",
        "Authorization": api_key,
    }
    payload = {"isbns": isbns}
    response = await client.post(url, headers=headers, json=payload)

    if response.status_code == 429:
        raise RateLimitExceeded("Rate limit exceeded. Please try again later.")

    response.raise_for_status()
    return response.json()
