import time
import threading

from market_data.config import Settings
from market_data.models import Trade
from tests.test_kafka import get_random_trade
from market_data.kafka_producer import TradeProducer
from market_data.kafka_consumer import TradeConsumer
from market_data.clickhouse_store import ClickHouseTradeStore


def benchmark() -> None:
    number_of_trades = 10_000

    settings = Settings.from_env()

    producer = TradeProducer(
        bootstrap_servers = settings.kafka_bootstrap_servers,
        topic = settings.kafka_topic_benchmark
    )
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
    # start consumer program using a new thread so it can pull messages from kafka
    #   while the producer publishes data
    consumer_thread = threading.Thread(
        target = consumer.run_forever,
        args = [number_of_trades],
        name = "benchmark-consumer-thread"
    )
    consumer_thread.start()

    trades: list[Trade] = [
        get_random_trade(i)
        for i in range(number_of_trades)
    ]

    start = time.perf_counter()

    # produce
    for i in range(number_of_trades):
        producer.publish(trades[i])
    producer.flush()

    # consume
    # wait until consumer thread finishes processing all messages from kafka
    consumer_thread.join()

    end = time.perf_counter()
    elapsed = end - start
    throughput = number_of_trades / elapsed

    print(f"Events: {number_of_trades:,.0f}")
    print(f"Elapsed: {elapsed:.2f}s")
    print(f"Throughput: {throughput:,.2f} events/sec")


if (__name__ == "__main__"):
    benchmark()
