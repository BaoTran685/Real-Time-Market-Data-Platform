import time

from market_data.config import Settings
from market_data.models import Trade
from tests.test_kafka import get_random_trade
from market_data.kafka_producer import TradeProducer
from market_data.kafka_consumer import TradeConsumer
from market_data.clickhouse_store import ClickHouseTradeStore


def benchmark_producer() -> None:
    number_of_trades = 10_000
    settings = Settings.from_env()

    producer = TradeProducer(
        bootstrap_servers = settings.kafka_bootstrap_servers,
        topic = settings.kafka_topic_benchmark
    )
    
    trades: list[Trade] = [
        get_random_trade(i)
        for i in range(number_of_trades)
    ]

    start = time.perf_counter()

    # produce
    for i in range(number_of_trades):
        producer.publish(trades[i])
    producer.flush()

    end = time.perf_counter()
    elapsed = end - start
    throughput = number_of_trades / elapsed

    print(f"Random events pool -> Producer -> Kafka")
    print(f"Events: {number_of_trades:,.0f}")
    print(f"Elapsed: {elapsed:.2f}s")
    print(f"Throughput: {throughput:,.2f} events/sec")


def benchmark_consumer() -> None:
    number_of_trades = 10_000
    settings = Settings.from_env()

    store = ClickHouseTradeStore(
            host = settings.clickhouse_host,
            port = settings.clickhouse_port,
            database = settings.clickhouse_database_benchmark,
            user = settings.clickhouse_user,
            password = settings.clickhouse_password
        )
    store.truncate()
    consumer = TradeConsumer(
        bootstrap_server = settings.kafka_bootstrap_servers,
        topic = settings.kafka_topic_benchmark,
        store = store,
        group_id = "benchmark-clickhouse-write",
        batch_size = 10000,
        flush_interval = 10.0
    )

    start = time.perf_counter()

    # consume
    consumer.run_forever(number_of_trades)

    end = time.perf_counter()
    elapsed = end - start
    throughput = number_of_trades / elapsed

    print(f"Kafka -> Consumer -> ClickHouse")
    print(f"Events: {number_of_trades:,.0f}")
    print(f"Elapsed: {elapsed:.2f}s")
    print(f"Throughput: {throughput:,.2f} events/sec")


def benchmark() -> None:
    number_of_iterations = 5

    for i in range(number_of_iterations):
        print(f"Iteration: {i}")
        benchmark_producer()
        benchmark_consumer()
        print()

if (__name__ == "__main__"):

    benchmark()
