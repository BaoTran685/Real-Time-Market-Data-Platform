import logging
import time
from confluent_kafka import Consumer

from market_data.clickhouse_store import ClickHouseTradeStore
from market_data.models import Trade

logger = logging.getLogger(__name__)

class TradeConsumer:

    def __init__(self, bootstrap_server: str, topic: str, store: ClickHouseTradeStore, group_id: str,
                 batch_size: int, flush_interval: float) -> None:
        self.topic = topic
        self.store = store
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_server,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        })
    
    def _flush_batch(self, batch: list[Trade]) -> None:
        if (len(batch) == 0):
            return

        self.store.insert(batch)
        self.consumer.commit(asynchronous = False)
        logger.info("Inserted and commited %d trades", len(batch))

        batch.clear()

    def run_forever(self) -> None:
        self.consumer.subscribe([self.topic])
        batch: list[Trade] = []
        last_flush = time.monotonic()

        try:
            while True:
                message = self.consumer.poll(timeout = 1.0)
                if (message is not None):
                    if (message.error()):
                        logger.error("Kafka error: %s", message.error())
                        continue

                    trade = Trade.from_json(message.value().decode("utf-8"))
                    batch.append(trade)

                now = time.monotonic()
                is_batch_full = (len(batch) >= self.batch_size)
                is_flush_due = (len(batch) > 0 and now - last_flush >= self.flush_interval)

                if (is_batch_full or is_flush_due):
                    self._flush_batch(batch)
                    last_flush = now

        except KeyboardInterrupt:
            logger.info("Stopping consumer")

        finally:
            self._flush_batch(batch)
            self.consumer.close()
        