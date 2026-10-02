CREATE TABLE IF NOT EXISTS raw_trades (
    product_id LowCardinality(String),
    trade_id UInt64,
    event_time DateTime(6, 'UTC'),
    price Decimal(38, 18),
    size Decimal(38, 18),
    maker_side LowCardinality(String),
    quote_notional Decimal(38, 18),

    kafka_topic LowCardinality(String),
    kafka_partition Int64,
    kafka_offset Int64,

    ingested_at DateTime(6, 'UTC') DEFAULT now64(6)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (product_id, event_time, kafka_partition, kafka_offset)
;

CREATE TABLE IF NOT EXISTS clean_trades (
    product_id LowCardinality(String),
    trade_id UInt64,
    event_time DateTime64(6, 'UTC'),
    price Decimal(38, 18),
    size Decimal(38, 18),
    maker_side LowCardinality(String),
    quote_notional Decimal(38, 18),

    ingested_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at) -- Uses ingested_at to keep the newest version
PARTITION BY toYYYYMM(event_time)
ORDER BY (product_id, trade_id)
;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw_to_clean
TO clean_trades
AS (
    SELECT
        product_id
        ,trade_id
        ,event_time
        ,price
        ,size
        ,maker_side
        ,quote_notional
        ,ingested_at
    FROM raw_trades
)
;
