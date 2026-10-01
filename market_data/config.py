from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen = True, slots=True)
class Settings:
    rest_base_url: str
    websocket_url: str
    
    product_ids: tuple[str, ...]

    kafka_bootstrap_servers: str
    kafka_topic: str

    @classmethod
    def from_env(cls) -> "Settings":
        product_ids = tuple(
            product.strip()
            for product in os.getenv("PRODUCT_IDS", "BTC-USD").split(",")
        )

        return cls(
            rest_base_url = os.getenv("COINBASE_REST_BASE_URL", "https://api.exchange.coinbase.com"),
            websocket_url = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com"),
            product_ids = product_ids,
            kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic = os.getenv("KAFKA_TOPIC", "raw_trades")
        )
