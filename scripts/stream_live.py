import logging
from market_data.client_ws import CoinbaseWebSocketClient
from market_data.config import Settings
from market_data.models import Trade

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s %(levelname)s %(name)s - %(message)s"
)

def handle_trade(trade: Trade) -> None:
    logging.info("product=%s trade=%s price=%s size=%s",
                trade.product_id, trade.trade_id, trade.price, trade.size)

def main() -> None:
    settings = Settings.from_env()
    client = CoinbaseWebSocketClient(
        url = settings.websocket_url,
        product_ids = settings.product_ids,
        on_trade = handle_trade
    )
    client.run_forever()

if (__name__ == "__main__"):
    main()
