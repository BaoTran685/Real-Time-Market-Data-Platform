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
    kafka_topic_prod: str
    kafka_topic_test: str
    kafka_topic_benchmark: str

    clickhouse_host: str
    clickhouse_port: str
    clickhouse_database_prod: str
    clickhouse_database_test: str
    clickhouse_database_benchmark: str
    clickhouse_user: str
    clickhouse_password: str

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
            kafka_topic_prod = os.getenv("KAFKA_TOPIC_PROD", "prod_trades"),
            kafka_topic_test = os.getenv("KAFKA_TOPIC_TEST", "test_trades"),
            kafka_topic_benchmark = os.getenv("KAFKA_TOPIC_BENCHMARK", "benchmark_trades"),

            clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8123"),
            clickhouse_database_prod = os.getenv("CLICKHOUSE_DATABASE_PROD", "prod"),
            clickhouse_database_test = os.getenv("CLICKHOUSE_DATABASE_TEST", "test"),
            clickhouse_database_benchmark = os.getenv("CLICKHOUSE_DATABASE_BENCHMARK", "benchmark"),
            clickhouse_user = os.getenv("CLICKHOUSE_USER", "admin"),
            clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "password")
        )
