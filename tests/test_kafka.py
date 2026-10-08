import random
from datetime import datetime, timedelta, timezone

from market_data.models import Trade
from market_data.kafka_producer import TradeProducer
from market_data.kafka_consumer import TradeConsumer
from market_data.clickhouse_store import ClickHouseTradeStore

def get_random_timestamp(start: datetime, end: datetime) -> str:
    total_seconds = int((end - start).total_seconds())
    random_offset = random.randint(0, total_seconds)
    random_timestamp = start + timedelta(seconds = random_offset)

    return random_timestamp.isoformat().replace("+00:00", "Z")

def get_random_trade(trade_id: int) -> Trade:
    raw = {
        "product_id": random.choice(["BTC-USD", "ETH-USD"]),
        "trade_id": trade_id,
        "price": f"{random.uniform(80_000, 90_000):2f}",
        "size": f"{random.uniform(0.0001, 5):.8f}",
        "time": get_random_timestamp(
            start = datetime(2026, 9, 1, tzinfo = timezone.utc),
            end = datetime(2026, 10, 1, tzinfo = timezone.utc)
        ),
        "side": random.choice(["buy", "sell"])
    }

    return Trade.from_coinbase(raw)

def test_kafka_produce_consume():
    number_of_trades = 10_000

    bootstrap_servers = "localhost:9092"
    topic = "test_kafka_produce_consume"

    clickhouse_host = "localhost"
    clickhouse_port = "8123"
    clickhouse_database = "test"
    clickhouse_user = "admin"
    clickhouse_password = "password"

    # produce
    producer = TradeProducer(
        bootstrap_servers = bootstrap_servers,
        topic = topic
    )
    
    producer_trades: list[Trade] = []
    for i in range(number_of_trades):
        trade = get_random_trade(i)
        producer_trades.append(trade)

        producer.publish(trade)
    
    producer.flush()

    # consume
    store = ClickHouseTradeStore(
        host = clickhouse_host,
        port = clickhouse_port,
        database = clickhouse_database,
        user = clickhouse_user,
        password = clickhouse_password
    )
    store.truncate()

    consumer = TradeConsumer(
        bootstrap_server = bootstrap_servers,
        topic = topic,
        store = store,
        group_id = "test_kafka",
        batch_size = 500,
        flush_interval = 10.0
    )

    consumer.run_forever(number_of_trades)

    consumer_trades: list[Trade] = store.query()

    assert(len(producer_trades) == len(consumer_trades))

    producer_trades.sort(key = lambda trade : trade.trade_id)
    consumer_trades.sort(key = lambda trade : trade.trade_id)

    for i in range(number_of_trades):
        assert(producer_trades[i] == consumer_trades[i])


if (__name__ == "__main__"):
    test_kafka_produce_consume()




    

    
