import logging
import json
from confluent_kafka import Producer
from market_data.models import Trade

logger = logging.getLogger(__name__)

class TradeProducer:

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
        })

    def _delivery_callback(self, error, message) -> None:
        if (error is not None):
            logger.error("Kafka delivery failed: %s", error)
        else:
            payload = json.loads(message.value().decode("utf-8"))
            logger.info("Delivered trade_id=%s, product_id=%s, topic=%s partition=%s offset=%s",
                         payload.get("trade_id"), payload.get("product_id"),
                         message.topic(), message.partition(), message.offset())

    def publish(self, trade: Trade) -> None:
        self.producer.produce(
            topic = self.topic,
            key = trade.product_id.encode("utf-8"),
            value = trade.to_json().encode("utf-8"),
            callback = self._delivery_callback
        ) # runs asynchronously
        self.producer.poll(0) # for callback to run immediately

    def flush(self) -> None:
        remaining = self.producer.flush() # wait for queued messages to finish delivery
        if (remaining > 0):
            logger.warning("%d message not delivered", remaining)
    




