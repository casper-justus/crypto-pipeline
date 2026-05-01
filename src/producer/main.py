"""Kafka producer for streaming live cryptocurrency prices from CoinGecko API."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

API_URL = os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3")
CRYPTO_IDS = os.getenv(
    "CRYPTO_IDS",
    "bitcoin,ethereum,solana,cardano,polkadot,avalanche-2,chainlink,dogecoin",
)
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL_SEC", "15"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_prices")


def normalize_coin_data(coin: dict[str, Any]) -> dict[str, Any]:
    """Transform raw CoinGecko API response into standardized message format."""
    return {
        "coin_id": coin["id"],
        "symbol": coin["symbol"],
        "name": coin["name"],
        "current_price": coin["current_price"],
        "market_cap": coin.get("market_cap"),
        "total_volume": coin.get("total_volume"),
        "price_change_24h": coin.get("price_change_24h"),
        "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
        "high_24h": coin.get("high_24h"),
        "low_24h": coin.get("low_24h"),
        "circulating_supply": coin.get("circulating_supply"),
    }


def serialize_message(message: dict[str, Any]) -> bytes:
    """Serialize a message dict to Kafka-compatible bytes."""
    return json.dumps(message).encode("utf-8")


def create_producer(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
) -> KafkaProducer:
    """Create and return a configured Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=serialize_message,
        retries=3,
        acks="all",
    )


def fetch_prices(
    api_url: str = API_URL,
    crypto_ids: str = CRYPTO_IDS,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch current market data for tracked cryptocurrencies."""
    url = f"{api_url}/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": crypto_ids,
        "order": "market_cap_desc",
        "sparkline": "false",
        "price_change_percentage": "24h",
    }
    if session is None:
        response = requests.get(url, params=params, timeout=30)
    else:
        response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def main() -> None:
    """Main producer loop — fetch prices and publish to Kafka."""
    logger.info("Starting producer - Topic: %s", KAFKA_TOPIC)
    logger.info("Tracking coins: %s", CRYPTO_IDS)
    logger.info("Fetch interval: %ds", FETCH_INTERVAL)

    producer = create_producer()

    try:
        while True:
            try:
                data = fetch_prices()
                for coin in data:
                    message = normalize_coin_data(coin)
                    producer.send(
                        KAFKA_TOPIC,
                        value=message,
                        key=message["coin_id"].encode(),
                    )
                    logger.info(
                        "Published: %s @ $%s",
                        coin["name"],
                        f"{coin['current_price']:,.2f}",
                    )
                producer.flush()
            except requests.exceptions.RequestException as exc:
                logger.error("API request failed: %s", exc)
            except KafkaError as exc:
                logger.error("Kafka error: %s", exc)
            except Exception as exc:
                logger.error("Unexpected error: %s", exc)

            time.sleep(FETCH_INTERVAL)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
