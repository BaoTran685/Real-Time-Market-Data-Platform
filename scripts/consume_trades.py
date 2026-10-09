import logging

from market_data.clickhouse_store import ClickHouseTradeStore
from market_data.config import Settings
from market_data.kafka_consumer import TradeConsumer

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s %(levelname)s %(name)s - %(message)s"
)

def main() -> None:
    settings = Settings.from_env()

    store = ClickHouseTradeStore(
        host = settings.clickhouse_host,
        port = settings.clickhouse_port,
        database = settings.clickhouse_database_prod,
        user = settings.clickhouse_user,
        password = settings.clickhouse_password
    )

    consumer = TradeConsumer(
        bootstrap_server = settings.kafka_bootstrap_servers,
        topic = settings.kafka_topic,
        store = store,
        group_id = "prod-clickhouse-write",
        batch_size = 500,
        flush_interval = 10
    )

    consumer.run_forever()


if (__name__ == "__main__"):
    main()
