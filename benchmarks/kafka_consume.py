import time
import uuid
from confluent_kafka import Consumer


from tests.test_kafka import get_random_trade
from market_data.config import Settings
from market_data.models import KafkaTradeRecord, Trade


def benchmark_kafka_consume() -> None:
    number_of_trades = 1_000_000
    batch_size = 1000
    settings = Settings.from_env()

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": f"benchmark-kafka-consumer-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    total_trades = 0
    consume_time = 0.0
    consume_count = 0
    parse_time = 0.0
    commit_time = 0.0
    commit_count = 0

    batch: list[KafkaTradeRecord] = []

    try:
        consumer.subscribe([settings.kafka_topic_benchmark])
        
        # polls kafka until it finish assigning us a partition
        while (not consumer.assignment()):
            consumer.poll(0.1)
        
        start = time.perf_counter()
        while (total_trades < number_of_trades):

            # 1. Consume from kafka
            t0 = time.perf_counter()

            messages = consumer.consume(num_messages = batch_size, timeout = 1.0)

            consume_time += time.perf_counter() - t0;
            consume_count += 1

            if (not messages):
                continue

            # 2. Parse records
            t0 = time.perf_counter()

            for message in messages:
                trade = Trade.from_json(message.value().decode("utf-8"))
                record = KafkaTradeRecord(
                    trade = trade,
                    kafka_topic = message.topic(),
                    kafka_partition = message.partition(),
                    kafka_offset = message.offset()
                )
                batch.append(record)

            parse_time += time.perf_counter() - t0
            total_trades += len(batch)
            batch.clear()

            # 3. Commit kafka offsets
            t0  = time.perf_counter()

            consumer.commit(asynchronous = False)

            commit_time += time.perf_counter() - t0
            commit_count += 1

        end = time.perf_counter()
        elapsed = end - start
        throughput = number_of_trades / elapsed

        print(f"Kafka -> Consumer")
        print(f"Events: {number_of_trades:,.0f}")
        print(f"Elapsed: {elapsed:.2f}s")
        print(f"Throughput: {throughput:,.2f} events/sec")
        print(f"Consume time: {consume_time}")
        print(f"Consume count: {consume_count}")
        print(f"Parse time: {parse_time}")
        print(f"Commit time: {commit_time}")
        print(f"Commit count: {commit_count}")

    finally:
        consumer.close()


if (__name__ == "__main__"):
    benchmark_kafka_consume()


# We have identified the source of bottle neck. It is the consumer that is slow here.
# For some reasons, consuming data from kafka is quite slow and the througput is 3K events/sec.

# We have identified the bottleneck. It is coming from two lines of code:
# while not consumer.assignment():
#     consumer.poll(0.1)

# Actually the pipeline is not that slow. It is slow because of not having those two lines of code.
# Technically, we need them to count the timer correctly. When we subscribe to kafka, it doesn't
#   assign us a partition immediately so we need to wait until the partition assignment is finished
#   before starting the timer.

# That's why when we change the number_of_trades to 10^6, the wait time is negligible.
# Actual throughput: 236K

# As input grows, the parsing time grows significantly. I guess it is because for-loop in Python
#   is quite slow.