CREATE TABLE IF NOT EXISTS trades (
    product_id LowCardinality(String),
    trade_id UInt64,
    event_time DateTime64(6, 'UTC'),
    price Decimal(38, 18),
    size Decimal(38, 18),
    maker_side LowCardinality(String),
    quote_notional Decimal(38, 18),
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (product_id, trade_id)
;

