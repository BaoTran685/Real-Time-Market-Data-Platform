from decimal import Decimal
from datetime import datetime, timezone
import pytest

from market_data.models import Trade, KafkaTradeRecord


# Positive Test
def test_trade_from_coinbase():
    raw = {
        "trade_id": 123,
        "price": "100.50",
        "size": "2.00",
        "time": "2026-09-29T18:30:00Z",
        "side": "sell"
    }

    trade = Trade.from_coinbase(raw = raw, product_id = "BTC-USD")

    assert trade.product_id == "BTC-USD"
    assert trade.trade_id == 123
    assert trade.price == Decimal("100.50")
    assert trade.size == Decimal("2.00")
    assert trade.event_time == datetime(2026, 9, 29, 18, 30, 0, tzinfo = timezone.utc)
    assert trade.maker_side == "sell"
    assert trade.quote_notional == Decimal("201.00")

# Negative Tests
def test_empty_product_id_rejected():
    raw = {
        "trade_id": 123,
        "price": "100",
        "size": "2.00",
        "time": "2026-09-29T18:30:00Z",
        "side": "sell"
    }

    with pytest.raises(ValueError):
        Trade.from_coinbase(raw = raw)

def test_negative_trade_id_rejected():
    raw = {
        "trade_id": -123,
        "price": "100",
        "size": "2.00",
        "time": "2026-09-29T18:30:00Z",
        "side": "sell"
    }

    with pytest.raises(ValueError):
        Trade.from_coinbase(raw = raw, product_id = "BTC-USD")

def test_negative_price_rejected():
    raw = {
        "trade_id": 123,
        "price": "-100",
        "size": "2.00",
        "time": "2026-09-29T18:30:00Z",
        "side": "sell"
    }

    with pytest.raises(ValueError):
        Trade.from_coinbase(raw = raw, product_id = "BTC-USD")

def test_negative_size_rejected():
    raw = {
        "trade_id": 123,
        "price": "100",
        "size": "-2.00",
        "time": "2026-09-29T18:30:00Z",
        "side": "sell"
    }

    with pytest.raises(ValueError):
        Trade.from_coinbase(raw = raw, product_id = "BTC-USD")

def test_invalid_side_rejected():
    raw = {
        "trade_id": 123,
        "price": "100",
        "size": "2.00",
        "time": "2026-09-29T18:30:00Z",
        "side": "none"
    }

    with pytest.raises(ValueError):
        Trade.from_coinbase(raw = raw, product_id = "BTC-USD")

def test_missing_timezone_rejected():
    raw = {
        "trade_id": 123,
        "price": "100",
        "size": "2.00",
        "time": "2026-09-29T18:30:00",
        "side": "sell"
    }

    with pytest.raises(ValueError):
        Trade.from_coinbase(raw = raw, product_id = "BTC-USD")

# Parsing Test
def test_json_round_trip():
    raw = {
        "trade_id": 123,
        "price": "100.50",
        "size": "2.00",
        "time": "2026-09-29T18:30:00Z",
        "side": "buy"
    }
    original_trade = Trade.from_coinbase(raw = raw, product_id = "BTC-USD")
    restored_trade = Trade.from_json(original_trade.to_json())

    assert original_trade == restored_trade

# Positive Test
def test_kafka_trade_record():
    raw = {
        "trade_id": 123,
        "price": "100.50",
        "size": "2.00",
        "time": "2026-09-29T18:30:00Z",
        "side": "buy"
    }
    trade = Trade.from_coinbase(raw = raw, product_id = "BTC-USD")
    record = KafkaTradeRecord(
        trade = trade,
        kafka_topic = "my_topic",
        kafka_partition = 12,
        kafka_offset = 5119
    )

    assert record.trade == trade # same trade in memory
    assert record.kafka_topic == "my_topic"
    assert record.kafka_partition == 12
    assert record.kafka_offset == 5119


if (__name__ == "__main__"):
    test_trade_from_coinbase()
    test_negative_price_rejected()
    test_json_round_trip()
    test_kafka_trade_record()
