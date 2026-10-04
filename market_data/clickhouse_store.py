from collections.abc import Iterable
import clickhouse_connect

from market_data.models import KafkaTradeRecord


class ClickHouseTradeStore:

    def __init__(self, host: str, port: str, database: str, user: str, password: str) -> None:
        self.database = database
        self.client = clickhouse_connect.get_client(
            host = host,
            port = port,
            username = user,
            password = password
        )
        self.column_names = [
            "product_id", "trade_id", "event_time", "price", "size", "maker_side", "quote_notional",
            "kafka_topic", "kafka_partition", "kafka_offset"
        ]

    def insert(self, kafka_trade_records: Iterable[KafkaTradeRecord]) -> None:
        rows = []
        for record in kafka_trade_records:
            rows.append([
                record.trade.product_id,
                record.trade.trade_id,
                record.trade.event_time,
                record.trade.price,
                record.trade.size,
                record.trade.maker_side,
                record.trade.quote_notional,
                record.kafka_topic,
                record.kafka_partition,
                record.kafka_offset
            ])

        if (len(rows) == 0):
            return

        table = "raw_trades"
        self.client.insert(
            table = f"{self.database}.{table}",
            data = rows,
            column_names = self.column_names
        )
    