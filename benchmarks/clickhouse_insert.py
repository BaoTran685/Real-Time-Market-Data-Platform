import time

from market_data.models import Trade, KafkaTradeRecord
from market_data.clickhouse_store import ClickHouseTradeStore
from market_data.config import Settings
from tests.test_kafka import get_random_trade

def benchmark_clickhouse_insert() -> None:
    number_of_trades = 1_000_000
    settings = Settings.from_env()

    store = ClickHouseTradeStore(
        host = settings.clickhouse_host,
        port = settings.clickhouse_port,
        database = settings.clickhouse_database_benchmark,
        user = settings.clickhouse_user,
        password = settings.clickhouse_password
    )
    store.truncate()

    kafka_trade_records: list[KafkaTradeRecord] = []
    for i in range(number_of_trades):
        trade = get_random_trade(i)
        record = KafkaTradeRecord(
            trade = trade,
            kafka_topic = "benchmark_trades",
            kafka_offset = 0,
            kafka_partition = 0
        )
        kafka_trade_records.append(record)

    start = time.perf_counter()

    store.insert(kafka_trade_records)

    end = time.perf_counter()
    elapsed = end - start
    throughput = number_of_trades / elapsed

    print(f"Random events pool -> Producer -> Kafka")
    print(f"Events: {number_of_trades:,.0f}")
    print(f"Elapsed: {elapsed:.2f}s")
    print(f"Throughput: {throughput:,.2f} events/sec")


if (__name__ == "__main__"):
    benchmark_clickhouse_insert()

# After testing this, I realize that writing to clickhouse database is very quick: ~420K events/sec.
    