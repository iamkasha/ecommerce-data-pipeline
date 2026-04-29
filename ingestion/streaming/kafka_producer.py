"""
Kafka Producer: Simulates real-time user behaviour events.

Events published:
  - page_view      : user viewed a product page
  - add_to_cart    : user added product to cart
  - remove_from_cart
  - checkout_start : user began checkout
  - order_placed   : user completed an order

Topic: ecom.user_events

Usage:
    python -m ingestion.streaming.kafka_producer
    python -m ingestion.streaming.kafka_producer --rate 10   # 10 events/second
"""
import os
import json
import time
import uuid
import random
import argparse
from datetime import datetime

from confluent_kafka import Producer
from faker import Faker
from dotenv import load_dotenv
from loguru import logger

load_dotenv()
fake = Faker()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = os.getenv("KAFKA_TOPIC_EVENTS", "ecom.user_events")

EVENT_TYPES = [
    ("page_view",           0.50),
    ("add_to_cart",         0.20),
    ("remove_from_cart",    0.05),
    ("checkout_start",      0.10),
    ("order_placed",        0.10),
    ("search",              0.05),
]
WEIGHTS     = [w for _, w in EVENT_TYPES]
NAMES       = [n for n, _ in EVENT_TYPES]

DEVICES     = ["desktop", "mobile", "tablet"]
SOURCES     = ["organic", "paid_search", "email", "social", "direct"]


def make_event(session_pool: list[str], product_pool: list[str]) -> dict:
    event_type = random.choices(NAMES, weights=WEIGHTS, k=1)[0]
    event = {
        "event_id":    str(uuid.uuid4()),
        "event_type":  event_type,
        "session_id":  random.choice(session_pool),
        "user_id":     str(uuid.uuid4()) if random.random() > 0.3 else None,  # ~30% anon
        "timestamp":   datetime.utcnow().isoformat() + "Z",
        "device":      random.choice(DEVICES),
        "source":      random.choice(SOURCES),
        "page":        f"/products/{fake.slug()}",
        "properties":  {},
    }
    if event_type in ("add_to_cart", "remove_from_cart", "page_view", "order_placed"):
        event["properties"]["product_id"] = random.choice(product_pool)
        event["properties"]["price"]      = round(random.uniform(5, 999), 2)
    if event_type == "order_placed":
        event["properties"]["order_id"]   = str(uuid.uuid4())
        event["properties"]["revenue"]    = round(random.uniform(20, 500), 2)
    if event_type == "search":
        event["properties"]["query"]      = fake.word()
    return event


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")


def run(events_per_second: float = 5.0, max_events: int | None = None):
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks":              "1",
        "linger.ms":         10,
        "batch.size":        16384,
    }
    producer  = Producer(conf)
    session_pool  = [str(uuid.uuid4()) for _ in range(200)]
    product_pool  = [str(uuid.uuid4()) for _ in range(100)]
    interval  = 1.0 / events_per_second
    count     = 0

    logger.info(f"Producing events to {TOPIC} @ {events_per_second} evt/s. Ctrl+C to stop.")

    try:
        while max_events is None or count < max_events:
            event = make_event(session_pool, product_pool)
            producer.produce(
                topic=TOPIC,
                key=event["session_id"],
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            count += 1
            if count % 100 == 0:
                logger.info(f"Produced {count} events")
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.flush()
        logger.info(f"Total events produced: {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate",   type=float, default=5.0, help="Events per second")
    parser.add_argument("--max",    type=int,   default=None, help="Max events to produce")
    args = parser.parse_args()
    run(args.rate, args.max)
