import logging
from pathlib import Path

from market_data.client import CoinbaseRestClient
from market_data.config import Settings

logging.basicConfig(
    level = logging.INFO,
    format = (
        "%(asctime)s "
        "%(levelname)s "
        "%(name)s - "
        "%(message)s"
    )
)

OUTPUT_FILE_PATH = "data/backfill_trades.jsonl"

def main() -> None:
    settings = Settings.from_env()
    client = CoinbaseRestClient(settings.rest_base_url)

    output_path = Path(OUTPUT_FILE_PATH)
    output_path.parent.mkdir(parents = True, exist_ok = True)

    cnt = 0
    with output_path.open("w") as file:
        for product_id in settings.product_ids:
            for trade in client.iter_trades(product_id = product_id, max_pages = 3):
                file.write(trade.to_json() + "\n")
                cnt += 1

    logging.info(f"Backfill complete: {cnt} trades")

if (__name__ == "__main__"):
    main()
