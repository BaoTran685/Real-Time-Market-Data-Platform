import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

@dataclass(frozen=True, slots=True)
class Trade:
    product_id: str
    trade_id: str
    event_time: datetime
    price: Decimal
    size: Decimal
    maker_side: str
    quote_notional: Decimal

    def __post_init__(self) -> None:
        if (self.product_id is None):
            raise ValueError("product_id cannot be empty")

        if (self.trade_id < 0):
            raise ValueError("trade_id must be non-negative")

        if (self.price <= 0):
            raise ValueError("price must be positive")

        if (self.size <= 0):
            raise ValueError("size must be positive")

        if (self.maker_side not in {"buy", "sell"}):
            raise ValueError(f"invalid maker_side: {self.maker_side}")

        if self.event_time.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")

    @property
    def key(self) -> tuple[str, int]:
        return (self.product_id, self.trade_id)

    @classmethod
    def from_coinbase(cls, raw: dict, product_id: str | None = None) -> "Trade":
        # when pulling from REST API, product_id should be populated (not None)
        # when pulling from WebSocket, product_id will be None and we need to get it
        #   from the payload
        if (product_id is None):
            product_id = raw["product_id"]

        price = Decimal(raw["price"])
        size = Decimal(raw["size"])

        event_time = datetime.fromisoformat(raw["time"])

        return cls(
            product_id = product_id,
            trade_id = int(raw["trade_id"]),
            event_time = event_time,
            price = price,
            size = size,
            maker_side = raw["side"],
            quote_notional = price * size
        )

    def to_dict(self) -> dict[str, str | int]:
        return {
            "product_id": self.product_id,
            "trade_id": self.trade_id,
            "event_time": self.event_time.isoformat().replace("+00:00", "Z"),
            "price": format(self.price, "f"),
            "size": format(self.size, "f"),
            "maker_side": self.maker_side,
            "quote_notional": format(self.quote_notional, "f")
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, value: str) -> "Trade":
        raw = json.loads(value)

        return cls(
            product_id = raw["product_id"],
            trade_id = int(raw["trade_id"]),
            event_time = datetime.fromisoformat(raw["event_time"]),
            price = Decimal(raw["price"]),
            size = Decimal(raw["size"]),
            maker_side = raw["maker_side"],
            quote_notional = Decimal(raw["quote_notional"])
        )

@dataclass(frozen=True, slots=True)
class KafkaTradeRecord:
    trade: Trade

    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
