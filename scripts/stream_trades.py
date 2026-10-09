import logging
from market_data.kafka_producer import TradeProducer
from market_data.client_ws import CoinbaseWebSocketClient
from market_data.config import Settings
from market_data.models import Trade

logging.basicConfig(
    level = logging.DEBUG,
    format = "%(asctime)s %(levelname)s %(name)s - %(message)s"
)

def main() -> None:
    settings = Settings.from_env()

    producer = TradeProducer(
        bootstrap_servers = settings.kafka_bootstrap_servers,
        topic = settings.kafka_topic
    )

    client = CoinbaseWebSocketClient(
        url = settings.websocket_url,
        product_ids = settings.product_ids,
        on_trade = producer.publish
    )

    try:
        client.run_forever()
    finally:
        producer.flush()

if (__name__ == "__main__"):
    main()
