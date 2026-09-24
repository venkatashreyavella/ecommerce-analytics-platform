"""Generate Kafka clickstream events and newline-delimited batch landing files."""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
LOGGER = logging.getLogger("ecommerce-producer")

RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "/raw_data"))
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_clicks")
EVENT_INTERVAL_SECONDS = float(os.getenv("EVENT_INTERVAL_SECONDS", "1"))
BATCH_INTERVAL_SECONDS = int(os.getenv("BATCH_INTERVAL_SECONDS", "60"))

fake = Faker()
RUNNING = True


def stop_handler(_signum: int, _frame: Any) -> None:
    global RUNNING
    RUNNING = False
    LOGGER.info("Shutdown requested")


def build_producer() -> KafkaProducer:
    last_error: Exception | None = None
    for attempt in range(1, 13):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                retries=10,
                acks="all",
                request_timeout_ms=10000,
                max_block_ms=10000,
            )
            LOGGER.info("Connected to Kafka on attempt %s", attempt)
            return producer
        except Exception as exc:  # Kafka clients raise several connection-specific errors.
            last_error = exc
            LOGGER.warning("Kafka connection attempt %s failed: %s", attempt, exc)
            time.sleep(min(attempt * 2, 15))
    raise RuntimeError("Unable to connect to Kafka") from last_error


def utc_now() -> datetime:
    return datetime.now(UTC)


def write_jsonl(filename: str, records: list[dict[str, Any]]) -> None:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    destination = RAW_DATA_DIR / filename
    with destination.open("a", encoding="utf-8") as output:
        for record in records:
            output.write(json.dumps(record) + "\n")
    LOGGER.info("Appended %s records to %s", len(records), destination)


def generate_batch_data() -> None:
    generated_at = utc_now()
    users: list[dict[str, Any]] = []
    products: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    user_ids = [f"user_{uuid.uuid4().hex[:12]}" for _ in range(10)]
    product_ids = [f"product_{uuid.uuid4().hex[:12]}" for _ in range(20)]

    for user_id in user_ids:
        users.append(
            {
                "user_id": user_id,
                "email": fake.unique.email(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "country": fake.country(),
                "signup_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
            }
        )

    for product_id in product_ids:
        products.append(
            {
                "product_id": product_id,
                "product_name": fake.catch_phrase(),
                "category": random.choice(["electronics", "home", "beauty", "sports", "books"]),
                "price": round(random.uniform(9.99, 499.99), 2),
                "updated_at": generated_at.isoformat(),
            }
        )

    for _ in range(15):
        product = random.choice(products)
        user_id = random.choice(user_ids)
        orders.append(
            {
                "order_id": str(uuid.uuid4()),
                "user_id": user_id,
                "product_id": product["product_id"],
                "session_id": f"session_{uuid.uuid4().hex[:12]}",
                "quantity": random.randint(1, 3),
                "unit_price": product["price"],
                "order_timestamp": (generated_at - timedelta(minutes=random.randint(0, 30))).isoformat(),
            }
        )

    batch_id = generated_at.strftime("%Y%m%dT%H%M%SZ")
    write_jsonl(f"users_{batch_id}.jsonl", users)
    write_jsonl(f"products_{batch_id}.jsonl", products)
    write_jsonl(f"orders_{batch_id}.jsonl", orders)


def generate_click_event() -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{uuid.uuid4().hex[:12]}",
        "session_id": f"session_{uuid.uuid4().hex[:12]}",
        "event_type": random.choices(["view", "add_to_cart", "checkout"], weights=[70, 20, 10])[0],
        "product_id": f"product_{random.randint(1, 100):012d}",
        "timestamp": utc_now().isoformat(),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
    }


def main() -> None:
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    producer = build_producer()
    last_batch_at = 0.0
    try:
        while RUNNING:
            now = time.monotonic()
            if now - last_batch_at >= BATCH_INTERVAL_SECONDS:
                generate_batch_data()
                last_batch_at = now

            event = generate_click_event()
            try:
                future = producer.send(KAFKA_TOPIC, event)
                metadata = future.get(timeout=15)
                LOGGER.info("Published %s to %s[%s]@%s", event["event_id"], metadata.topic, metadata.partition, metadata.offset)
            except KafkaError:
                LOGGER.exception("Failed to publish click event")
            time.sleep(EVENT_INTERVAL_SECONDS)
    finally:
        producer.flush(timeout=15)
        producer.close()
        LOGGER.info("Producer stopped")


if __name__ == "__main__":
    main()
