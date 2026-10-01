import logging
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
            logger.debug("Delivered topic=%s partition=%s offset=%s",
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
    




