import json
from collections.abc import Iterable
import clickhouse_connect

from market_data.models import KafkaTradeRecord, Trade


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

    def query(self) -> list[Trade]:
        table = "clean_trades"
        result = self.client.query(
            f"""
            SELECT
                product_id
                ,trade_id
                ,event_time
                ,price
                ,size
                ,maker_side
                ,quote_notional
            FROM {self.database}.{table}
            """,
            tz_mode = "aware"
        )

        trades = []
        for row in result.result_rows:
            trade = Trade(
                product_id = row[0],
                trade_id = row[1],
                event_time = row[2],
                price = row[3],
                size = row[4],
                maker_side = row[5],
                quote_notional = row[6]
            )
            trades.append(trade)

        return trades

    def truncate(self):
        self.client.command(
            f"TRUNCATE TABLE {self.database}.raw_trades;"
        )
        self.client.command(
            f"TRUNCATE TABLE {self.database}.clean_trades;"
        )
    