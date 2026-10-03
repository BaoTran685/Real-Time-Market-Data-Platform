import random
from datetime import datetime, timedelta, timezone
from confluent_kafka import Consumer

from market_data.models import Trade
from market_data.kafka_producer import TradeProducer
from market_data.kafka_consumer import TradeConsumer

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

# test_kafka_produce() generates 1000 random trades and put them into kafk (producer).
#   Then it consumes each trade and check whether they are correct (consumer).
# It uses the TradeProducer class to produce data to kafka but it doesn't use TradeConsumer class
#   to consume data. Actually here, we pull data from kafka ourselves.
def test_kafka_produce():
    number_of_trades = 1000
    bootstrap_servers = "localhost:9092"
    topic = "test_kafka_produce_consume"

    # produce
    producer = TradeProducer(
        bootstrap_servers = bootstrap_servers,
        topic = topic
    )
    
    producer_trades = []
    for i in range(number_of_trades):
        trade = get_random_trade(i)
        producer_trades.append(trade)

        producer.publish(trade)
    
    producer.flush()

    # consume
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "tester",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])

    i = 0
    while (i < number_of_trades):
        message = consumer.poll(timeout = 1.0)
        if (message is None or message.error()):
            continue

        consumer_trade = Trade.from_json(message.value().decode("utf-8"))

        assert producer_trades[i] == consumer_trade

        i += 1
    
    consumer.close()






    

    
