"""
Kafka Consumer: ecom.user_events → S3 Bronze (micro-batch)

Reads user events from Kafka and writes them to S3 in micro-batches
partitioned by event_type and hour.

  s3://bucket/bronze/events/event_type=page_view/year=YYYY/month=MM/day=DD/hour=HH/batch.json

Usage:
    python -m ingestion.streaming.kafka_consumer
"""
import os
import json
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = os.getenv("KAFKA_TOPIC_EVENTS", "ecom.user_events")
GROUP_ID          = "s3-sink-consumer"

S3_BUCKET     = os.getenv("MINIO_BUCKET", "ecommerce-lake")
FLUSH_INTERVAL = int(os.getenv("CONSUMER_FLUSH_INTERVAL_SEC", 30))
FLUSH_SIZE     = int(os.getenv("CONSUMER_FLUSH_SIZE", 500))


def get_s3_client():
    env = os.getenv("ENVIRONMENT", "local")
    if env == "local":
        return boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )
    return boto3.client("s3")


def flush_to_s3(s3, buffer: dict[str, list]):
    """Write buffered events to S3 partitioned by event_type and hour."""
    now = datetime.now(timezone.utc)
    prefix = (
        f"bronze/events/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}"
    )
    for event_type, events in buffer.items():
        if not events:
            continue
        key  = f"{prefix}/event_type={event_type}/{int(now.timestamp())}.json"
        body = "\n".join(json.dumps(e) for e in events).encode()
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body)
        logger.info(f"  Flushed {len(events)} {event_type} events → {key}")


def run():
    s3 = get_s3_client()
    conf = {
        "bootstrap.servers":  BOOTSTRAP_SERVERS,
        "group.id":           GROUP_ID,
        "auto.offset.reset":  "latest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    buffer:     dict[str, list] = defaultdict(list)
    total_count = 0
    last_flush  = time.time()

    logger.info(f"Consuming from {TOPIC}. Flushing every {FLUSH_INTERVAL}s or {FLUSH_SIZE} msgs.")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
            else:
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    buffer[event.get("event_type", "unknown")].append(event)
                    total_count += 1
                except json.JSONDecodeError as e:
                    logger.warning(f"Bad message: {e}")

            # Flush when size or time threshold hit
            total_buffered = sum(len(v) for v in buffer.values())
            elapsed = time.time() - last_flush
            if total_buffered >= FLUSH_SIZE or elapsed >= FLUSH_INTERVAL:
                if total_buffered > 0:
                    flush_to_s3(s3, buffer)
                    consumer.commit()
                    buffer = defaultdict(list)
                last_flush = time.time()

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        if any(buffer.values()):
            flush_to_s3(s3, buffer)
            consumer.commit()
    finally:
        consumer.close()
        logger.info(f"Total messages consumed: {total_count}")


if __name__ == "__main__":
    run()
