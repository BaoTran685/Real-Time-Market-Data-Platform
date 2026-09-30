import json
import logging
import time
from collections.abc import Callable
import websocket

from market_data.models import Trade

logger = logging.getLogger(__name__)

class CoinbaseWebSocketClient:

    def __init__(self, url: str, product_ids: tuple[str, ...], on_trade: Callable[[Trade], None]) -> None:
        self.url = url
        self.product_ids = product_ids
        self.on_trade = on_trade

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        subcription = {
            "type": "subscribe",
            "channels": [
                {"name": "matches", "product_ids": list(self.product_ids)},
                {"name": "heartbeat", "product_ids": list(self.product_ids)}
            ]
        }
        ws.send(json.dumps(subcription))

        logger.info(f"Subcribed to {self.product_ids}")

    def _on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        try:
            payload = json.loads(message)

            if (payload.get("type") == "match"):
                trade = Trade.from_coinbase(payload)
                self.on_trade(trade)
            else:
                logger.info(f"Different type of message: {payload}")
        except (KeyError, ValueError) as error:
            logger.warning(f"Skipping invalid message: {error}")

    def _on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        logger.error(f"WebSocket error: {error}")

    def _on_close(self, ws: websocket.WebSocketApp, status_code: str, message: str) -> None:
        logger.warning(f"WebSocket closed: {status_code} {message}")

    def run_forever(self) -> None:
        backoff_seconds = 1

        while True:
            ws = websocket.WebSocketApp(
                self.url,
                on_open = self._on_open,
                on_message = self._on_message,
                on_error = self._on_error,
                on_close = self._on_close
            )

            try:
                ws.run_forever(ping_interval = 20, ping_timeout = 10)
            except KeyboardInterrupt:
                logger.info("Stopping WebSocket client")
                return

            logger.info(f"Reconnecting in {backoff_seconds} seconds")
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 60)
    