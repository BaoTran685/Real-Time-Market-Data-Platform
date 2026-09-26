import logging
import time
from dataclasses import dataclass
from typing import Any, Iterator

import requests

from market_data.config import Settings

logger = logging.getLogger(__name__)

class APIClientError(RuntimeError):
    pass

@dataclass
class TradePage:
    trades: list[dict[str, Any]]
    next_cursor: str | None

class CoinbaseClient:
    # 200 OK
    # 400 Bad Request
    # 404 Not Found
    # 429 Too Many Requests
    # 500 Internal Server Error
    # 502 Bad Gateway
    # 503 Service Unavilable
    # 504 Gateway Timeout
    RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]

    def __init__(self, settings: Settings):
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "real-time-market-data-platform"
        })

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()

    # _get_retry_delay(response, attempt) returns the time (in seconds) that we should delay before retry calling the API.
    #  If the response does mention such field "retry_after", we would use it.
    #  Otherwise we use exponential delay. The delay time grows as we retry more and more, and is capped
    #   at 8 seconds.
    def _get_retry_delay(self, response: requests.Response | None, attempt: int) -> float:
        if (response is not None):
            retry_after = response.headers.get("Retry-After")
            if (retry_after is not None):
                try:
                    return float(retry_after)
                except ValueError:
                    pass
        
        return min(2 ** attempt, 8)

    # fetch_trade_page(after) calls the API endpoint and apply the after parameter (if applicable).
    # The apply parameter is used to retrieve only the data after some point. For example, since a response may only contain up
    #   a certain number of records, in the next API call we want to retrieve the records after the last record of the previous
    #   API call.
    # This function also implements retry so that if an API fails, for some certain status code of the response, we want to retry
    #   it call it for some n number of times.
    # The for-loop in this function is used for the retry only.
    def fetch_trade_page(self, after: str | None = None) -> TradePage:
        url = (
            f"{self.settings.base_url}"
            f"/products/{self.settings.product_id}/trades"
        )

        params: dict[str, str | int] = {
            "limit": self.settings.page_size
        }

        if (after is not None):
            params["after"] = after

        last_error: Exception | None = None

        for attempt in range(self.settings.max_retries + 1):
            response = None

            try:
                response = self.session.get(url=url, params=params, timeout=self.settings.request_timeout_seconds)

                # check for retry
                if (response.status_code in self.RETRYABLE_STATUS_CODES):
                    if (attempt == self.settings.max_retries):
                        response.raise_for_status()

                    delay = self._get_retry_delay(response, attempt)
                    logger.warning(
                        "Retryable HTTP status %s."
                        "Retrying in %.1f seconds",
                        response.status_code,
                        delay
                    )

                    time.sleep(delay)
                    continue

                # raise if server responded with an HTTP error status
                response.raise_for_status()

                # parse response's json
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise APIClientError("API returned invalid JSON.") from exc
                
                if (not isinstance(payload, list)):
                    raise APIClientError("Expect API response to be a JSON array.")

                next_cursor = response.headers.get("CB-AFTER")
                return TradePage(trades=payload, next_cursor=next_cursor)
            
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = exc

                if (attempt == self.settings.max_retries):
                    break

                delay = self._get_retry_delay(response, attempt)

                logger.warning(
                    "Network error: %s."
                    "Retrying in %.1f seconds",
                    exc,
                    delay
                )

                time.sleep(delay)

            except requests.HTTPError:
                raise

        raise APIClientError("Request faield after maximum retries.") from last_error

    # iter_trade_pages() iteratively calls an API endpoint to pull all of the data.
    # The for-loop here is used to pull data in all pages. As a response may contain up to a certain number of records,
    #   we need to make multiple API calls until we have retrieved all data.
    def iter_trade_pages(self) -> Iterator[tuple[int, list[dict[str, Any]]]]:
        cursor: str | None = None

        for page_number in range(1, self.settings.max_pages + 1):
            page = self.fetch_trade_page(after=cursor)

            # iteratively return data
            yield page_number, page.trades

            if (not page.trades):
                break

            if (page.next_cursor is None):
                break

            if (page.next_cursor == cursor):
                logger.wanring(
                    "Pagination cursor did not change. "
                    "Stopping to avoid infinite loop."
                )
                break

            cursor = page.next_cursor
            time.sleep(self.settings.page_delay_seconds)
