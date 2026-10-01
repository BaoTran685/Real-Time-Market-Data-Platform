# REST API Client: We use this class to get historical data.
# get_trade_page(): makes one request to the API and then give the returned body
#   to Trade to parse.
# iter_trades(): has a for-loop that performs pagination on the API results.
#   In particular, each iteration invokes get_trade_page() to pull one page of data
#   and the next iteration pulls the next page and so on.

import logging
import requests
from market_data.models import Trade

logger = logging.getLogger(__name__)

class CoinbaseRestClient:

    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def __del__(self) -> None:
        self.session.close()

    def get_trade_page(self, product_id: str, limit: int = 1000, after: str | None = None) -> tuple[list[Trade], str | None]:
        url = (
            f"{self.base_url}"
            f"/products/{product_id}/trades"
        )

        params = {
            "limit": limit
        }
        if (after is not None):
            params["after"] = after

        try:
            response = self.session.get(url, params = params, timeout = self.timeout)
            response.raise_for_status()
        except requests.Timeout as error:
            logger.error("Coinbase request timed out")
            raise
        except requests.HTTPError as error:
            logger.error("Coinbase returned HTTP error: %s", error)
            raise
        except requests.RequestException as error:
            logger.error("Coinbase request failed: %s", error)
            raise

        body = response.json()
        trades = []
        for raw_trade in body:
            trades.append(Trade.from_coinbase(raw = raw_trade, product_id = product_id))

        next_cursor = response.headers.get("CB-AFTER")

        return trades, next_cursor

    def iter_trades(self, product_id: str, max_pages: int = 5):
        after = None
        seen: set[tuple[str, int]] = set()

        for _ in range(max_pages):
            trades, after = self.get_trade_page(product_id = product_id, after = after)

            if (not trades):
                return

            for trade in trades:
                if (trade.key in seen):
                    continue
                seen.add(trade.key)
                yield trade

            if (after is None):
                return
        