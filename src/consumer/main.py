"""Kafka consumer for persisting cryptocurrency prices to PostgreSQL."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("CONSUMER_BATCH_SIZE", "50"))
FLUSH_INTERVAL = int(os.getenv("CONSUMER_FLUSH_INTERVAL_SEC", "10"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "consumer_group_1")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_prices")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_pipeline")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_pass")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline_user")

COLUMNS = (
    "coin_id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "total_volume",
    "price_change_24h",
    "price_change_percentage_24h",
    "high_24h",
    "low_24h",
    "circulating_supply",
)

INSERT_QUERY = """
    INSERT INTO crypto_prices ({columns})
    VALUES %s
""".format(columns=", ".join(COLUMNS))


def deserialize_message(raw: bytes) -> dict[str, Any]:
    """Deserialize a raw Kafka message to a Python dict."""
    return __import__("json").loads(raw.decode("utf-8"))


def message_to_row(message: dict[str, Any]) -> tuple:
    """Convert a message dict to a database row tuple."""
    return (
        message["coin_id"],
        message["symbol"],
        message["name"],
        message["current_price"],
        message.get("market_cap"),
        message.get("total_volume"),
        message.get("price_change_24h"),
        message.get("price_change_percentage_24h"),
        message.get("high_24h"),
        message.get("low_24h"),
        message.get("circulating_supply"),
    )


def create_consumer(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
    group_id: str = KAFKA_GROUP_ID,
) -> KafkaConsumer:
    """Create and return a configured Kafka consumer."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=deserialize_message,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
    )


def create_db_connection() -> psycopg2.extensions.connection:
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def insert_batch(
    batch: list[dict[str, Any]],
    conn: psycopg2.extensions.connection,
) -> int:
    """Insert a batch of messages into PostgreSQL.

    Args:
        batch: List of message dicts to insert.
        conn: Active PostgreSQL connection.

    Returns:
        Number of records inserted.

    Raises:
        psycopg2.Error: If the database insert fails.
    """
    if not batch:
        return 0

    values = [message_to_row(m) for m in batch]
    cursor = conn.cursor()
    try:
        execute_values(cursor, INSERT_QUERY, values, page_size=BATCH_SIZE)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
    return len(batch)


def main() -> None:
    """Main consumer loop — read from Kafka and batch-insert to PostgreSQL."""
    logger.info("Starting consumer - Topic: %s, Group: %s", KAFKA_TOPIC, KAFKA_GROUP_ID)
    logger.info("PostgreSQL: %s:%s/%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)

    consumer = create_consumer()
    conn = create_db_connection()
    batch: list[dict[str, Any]] = []
    last_flush = time.time()

    try:
        for message in consumer:
            batch.append(message.value)
            now = time.time()

            if len(batch) >= BATCH_SIZE or (now - last_flush) >= FLUSH_INTERVAL:
                inserted = insert_batch(batch, conn)
                coins = set(m["coin_id"] for m in batch)
                logger.info(
                    "Inserted %d records for %d coins",
                    inserted,
                    len(coins),
                )
                batch = []
                last_flush = now
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
