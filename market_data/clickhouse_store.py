from collections.abc import Iterable
import clickhouse_connect

from market_data.models import Trade


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
            "product_id", "trade_id", "event_time", "price", "size", "maker_side", "quote_notional"
        ]

    def insert(self, trades: Iterable[Trade]) -> None:
        rows = []
        for trade in trades:
            rows.append([
                trade.product_id,
                trade.trade_id,
                trade.event_time,
                trade.price,
                trade.size,
                trade.maker_side,
                trade.quote_notional
            ])

        if (len(rows) == 0):
            return

        self.client.insert(
            table = f"{self.database}.trades",
            data = rows,
            column_names = self.column_names
        )
    