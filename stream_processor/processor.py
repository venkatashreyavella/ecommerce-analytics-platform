"""Consume clickstream events and upsert rolling five-minute aggregates."""

from __future__ import annotations

import json
import logging
import os
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from psycopg2.extensions import connection as PgConnection

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
LOGGER = logging.getLogger("ecommerce-stream-processor")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_clicks")
WINDOW_MINUTES = int(os.getenv("WINDOW_MINUTES", "5"))


def connect_postgres() -> PgConnection:
    last_error: Exception | None = None
    for attempt in range(1, 13):
        try:
            connection = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                dbname=os.getenv("POSTGRES_DB", "warehouse"),
                user=os.getenv("POSTGRES_USER", "warehouse"),
                password=os.getenv("POSTGRES_PASSWORD", "warehouse"),
                connect_timeout=10,
            )
            connection.autocommit = False
            LOGGER.info("Connected to PostgreSQL on attempt %s", attempt)
            return connection
        except psycopg2.Error as exc:
            last_error = exc
            LOGGER.warning("PostgreSQL connection attempt %s failed: %s", attempt, exc)
            time.sleep(min(attempt * 2, 15))
    raise RuntimeError("Unable to connect to PostgreSQL") from last_error


def build_consumer() -> KafkaConsumer:
    last_error: Exception | None = None
    for attempt in range(1, 13):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVERS,
                group_id="ecommerce-realtime-aggregator",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            LOGGER.info("Connected to Kafka on attempt %s", attempt)
            return consumer
        except KafkaError as exc:
            last_error = exc
            LOGGER.warning("Kafka connection attempt %s failed: %s", attempt, exc)
            time.sleep(min(attempt * 2, 15))
    raise RuntimeError("Unable to connect to Kafka") from last_error


def floor_to_minute(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def persist_aggregates(connection: PgConnection, events: list[dict[str, Any]]) -> None:
    if not events:
        return
    now = datetime.now(UTC)
    window_end = floor_to_minute(now)
    window_start = window_end - timedelta(minutes=WINDOW_MINUTES)
    counts = Counter(event.get("event_type", "unknown") for event in events)
    with connection.cursor() as cursor:
        for event in events:
            cursor.execute(
                """
                INSERT INTO raw.raw_clicks
                    (event_id, user_id, session_id, event_type, product_id, event_timestamp, device_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event["event_id"],
                    event["user_id"],
                    event["session_id"],
                    event["event_type"],
                    event["product_id"],
                    event["timestamp"],
                    event["device_type"],
                ),
            )
        for event_type, count in counts.items():
            cursor.execute(
                """
                INSERT INTO analytics.realtime_click_aggregates
                    (window_start, window_end, event_type, click_count, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (window_start, window_end, event_type)
                DO UPDATE SET click_count = EXCLUDED.click_count,
                              updated_at = EXCLUDED.updated_at
                """,
                (window_start, window_end, event_type, count),
            )
    connection.commit()
    LOGGER.info("Persisted %s event types for window %s - %s", len(counts), window_start, window_end)


def main() -> None:
    consumer = build_consumer()
    connection = connect_postgres()
    batch: list[dict[str, Any]] = []
    try:
        while True:
            try:
                records = consumer.poll(timeout_ms=1000, max_records=500)
                for messages in records.values():
                    batch.extend(message.value for message in messages)
                if batch:
                    persist_aggregates(connection, batch)
                    batch.clear()
            except (KafkaError, psycopg2.Error, ValueError):
                LOGGER.exception("Stream processing cycle failed")
                connection.rollback()
                connection.close()
                connection = connect_postgres()
    finally:
        consumer.close()
        connection.close()
        LOGGER.info("Stream processor stopped")


if __name__ == "__main__":
    main()
