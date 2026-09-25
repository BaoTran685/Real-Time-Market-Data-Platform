from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen = True)
class Settings:
    base_url: str = "https://api.exchange.coinbase.com"

    product_id: str = "BTC-USD"

    page_size: int = 100
    max_pages: int = 10

    request_timeout_seconds: float = 10.0
    max_retries: int = 4

    page_delay_seconds: float = 0.25

    data_dir: Path = Path("data")
