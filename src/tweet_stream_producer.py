#!/usr/bin/env python3
"""
Kafka tweet-stream producer for Amazon Reviews dataset.

The program reads rows from a CSV file and sends each row as a separate JSON
message to Kafka topic "tweets". It simulates a tweet stream by:
1. converting each review row into a tweet-like event;
2. replacing the original review_date with the current UTC timestamp;
3. sending messages sequentially at 10-15 messages per second.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, Any

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


CRITICAL_COLUMNS = [
    "review_id",
    "customer_id",
    "product_id",
    "star_rating",
    "review_date",
    "review_body",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream Amazon Reviews as tweet-like Kafka messages.")
    parser.add_argument(
        "--input-file",
        default=os.getenv("INPUT_FILE", "/app/data/amazon_reviews.csv"),
        help="Path to the source CSV file.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "tweets"),
        help="Kafka topic name.",
    )
    parser.add_argument(
        "--min-rate",
        type=float,
        default=float(os.getenv("MIN_RATE", "10")),
        help="Minimum messages per second.",
    )
    parser.add_argument(
        "--max-rate",
        type=float,
        default=float(os.getenv("MAX_RATE", "15")),
        help="Maximum messages per second.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=int(os.getenv("MAX_MESSAGES", "0")),
        help="Optional message limit. 0 means unlimited until file ends.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        default=os.getenv("LOOP", "false").lower() == "true",
        help="Loop over the CSV file when the end is reached.",
    )
    return parser.parse_args()


def wait_for_kafka(bootstrap_servers: str, retries: int = 30, delay: int = 2) -> KafkaProducer:
    """Wait until Kafka is available and return a configured producer."""
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda key: str(key).encode("utf-8") if key is not None else None,
                acks="all",
                retries=5,
                linger_ms=10,
            )
            print(f"Connected to Kafka at {bootstrap_servers}", flush=True)
            return producer
        except NoBrokersAvailable as exc:
            last_error = exc
            print(f"Kafka is not available yet ({attempt}/{retries}). Retrying in {delay}s...", flush=True)
            time.sleep(delay)

    raise RuntimeError(f"Could not connect to Kafka at {bootstrap_servers}") from last_error


def is_valid_row(row: Dict[str, str]) -> bool:
    return all(row.get(column) not in (None, "") for column in CRITICAL_COLUMNS)


def row_to_tweet_event(row: Dict[str, str]) -> Dict[str, Any]:
    """Map an Amazon review row to a tweet-like JSON message."""
    now = datetime.now(timezone.utc).isoformat()

    return {
        "tweet_id": row["review_id"],
        "created_at": now,
        "original_review_date": row.get("review_date"),
        "user_id": row["customer_id"],
        "product_id": row["product_id"],
        "product_title": row.get("product_title"),
        "rating": int(row["star_rating"]),
        "verified_purchase": str(row.get("verified_purchase", "")).strip() in {"1", "Y", "y", "true", "True"},
        "text": row.get("review_body") or row.get("review_headline") or "",
        "source": "amazon_reviews_csv",
    }


def read_rows(path: str) -> Iterable[Dict[str, str]]:
    with open(path, mode="r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if is_valid_row(row):
                yield row


def stream_file(args: argparse.Namespace, producer: KafkaProducer) -> None:
    if args.min_rate <= 0 or args.max_rate < args.min_rate:
        raise ValueError("Rate must satisfy 0 < min-rate <= max-rate")

    sent = 0
    print(
        f"Starting stream: file={args.input_file}, topic={args.topic}, "
        f"rate={args.min_rate}-{args.max_rate} msg/s, max_messages={args.max_messages or 'unlimited'}",
        flush=True,
    )

    while True:
        rows_sent_this_pass = 0

        for row in read_rows(args.input_file):
            event = row_to_tweet_event(row)

            producer.send(args.topic, key=event["product_id"], value=event)
            sent += 1
            rows_sent_this_pass += 1

            if sent % 100 == 0:
                producer.flush()
                print(f"Sent {sent} messages. Last tweet_id={event['tweet_id']}", flush=True)

            if args.max_messages and sent >= args.max_messages:
                producer.flush()
                print(f"Finished. Total messages sent: {sent}", flush=True)
                return

            current_rate = random.uniform(args.min_rate, args.max_rate)
            time.sleep(1.0 / current_rate)

        if not args.loop:
            break

        if rows_sent_this_pass == 0:
            print("No valid rows found in the input file. Stopping.", flush=True)
            break

    producer.flush()
    print(f"Finished. Total messages sent: {sent}", flush=True)


def main() -> int:
    args = parse_args()

    if not os.path.exists(args.input_file):
        print(f"Input file not found: {args.input_file}", file=sys.stderr)
        return 1

    producer = wait_for_kafka(args.bootstrap_servers)

    try:
        stream_file(args, producer)
    finally:
        producer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
