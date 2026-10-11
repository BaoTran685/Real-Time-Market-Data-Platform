import requests
from decimal import Decimal

BASE_URL = "https://api.exchange.coinbase.com"


def get_usd_products() -> list[str]:
    response = requests.get(
        url = f"{BASE_URL}/products",
        timeout = 10
    )
    response.raise_for_status()

    products = response.json()

    return [
        product["id"]
        for product in products
        if (
            product["quote_currency"] == "USD"
            and product["status"] == "online"
            and not product.get("trading_disabled", False)
        )
    ]


def get_volume(product_id: str) -> dict:
    response = requests.get(
        url = f"{BASE_URL}/products/{product_id}/stats",
        timeout = 10
    )
    response.raise_for_status()

    stats = response.json()

    volume = Decimal(stats["volume"])
    price = Decimal(stats["last"])

    return {
        "product_id": product_id,
        "volume_24h": volume,
        "price": price,
        "usd_volume_24h": volume * price
    }


def get_ranked_products() -> list[dict]:
    products = get_usd_products()
    results = []
    
    for product in products:
        try:
            result = get_volume(product)
            results.append(result)
        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Failed {futures[future]}: {e}")

    return sorted(
        results,
        key = lambda x: x["usd_volume_24h"],
        reverse = True
    )


if (__name__ == "__main__"):
    ranked_products = get_ranked_products()

    for rank, product in enumerate(ranked_products, start=1):
        print(
            f"{rank:3}. "
            f"{product["product_id"]:12} "
            f"${product["usd_volume_24h"]:,.2f}"
        )