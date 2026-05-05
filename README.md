# Amazon Reviews Kafka Tweet Stream

This project demonstrates how to write data to Apache Kafka using Python code.  
The program reads the Amazon Reviews CSV dataset, converts each review into a tweet-like JSON event, replaces the original timestamp with the current UTC time, and sends every event as an individual Kafka message to the `tweets` topic.

## Repository name

Recommended GitHub repository name:

```text
amazon-reviews-kafka-tweet-stream
```

## About description

```text
Python Kafka producer that streams Amazon Reviews as tweet-like events into Apache Kafka using Docker Compose.
```

## Topics

```text
kafka
python
docker
data-engineering
streaming
apache-kafka
amazon-reviews
message-broker
```

## Architecture

```text
Amazon Reviews CSV
        |
        v
Python Producer Container
        |
        v
Kafka topic: tweets
        |
        v
Kafka console consumer
```

## What the producer does

For every valid CSV row:

1. reads one Amazon review;
2. creates a tweet-like JSON message;
3. replaces the timestamp with the current UTC time;
4. sends the message to Kafka topic `tweets`;
5. waits a random interval to keep the speed around 10-15 messages per second.

Example message:

```json
{
  "tweet_id": "RQ58W7SMO911M",
  "created_at": "2026-05-05T12:00:00.000000+00:00",
  "original_review_date": "2005-10-14",
  "user_id": "12076615",
  "product_id": "0385730586",
  "product_title": "Sisterhood of the Traveling Pants (Book 1)",
  "rating": 4,
  "verified_purchase": false,
  "text": "this book was a great learning novel...",
  "source": "amazon_reviews_csv"
}
```

## Project structure

```text
.
├── src/
│   └── tweet_stream_producer.py
├── scripts/
│   ├── build_producer.sh
│   ├── create_topic.sh
│   ├── run_kafka.sh
│   ├── run_producer.sh
│   └── consume_topic.sh
├── data/
│   └── .gitkeep
├── screenshots/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## How to run

### 1. Put the dataset into the project

Copy the provided CSV file to:

```bash
data/amazon_reviews.csv
```

The CSV file is not committed to GitHub because datasets can be large.

### 2. Start Kafka

```bash
docker compose up -d zookeeper kafka
```

or:

```bash
./scripts/run_kafka.sh
```

### 3. Create the topic

```bash
./scripts/create_topic.sh
```

### 4. Build the producer image

```bash
./scripts/build_producer.sh
```

### 5. Run the producer

```bash
./scripts/run_producer.sh
```

Let it run for approximately 5 minutes.  
At 10-15 messages per second, it should send about 3000-4500 messages.

### 6. Read messages from the Kafka topic

```bash
./scripts/consume_topic.sh
```

Alternative command:

```bash
docker exec -it hw8-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tweets \
  --from-beginning \
  --max-messages 10
```

## Deliverables checklist

- Docker-compose with Kafka and Zookeeper.
- Python producer reading the source file and sending messages to Kafka.
- Dockerfile for building the producer container.
- Build script for the producer container.
- Run script for Kafka.
- Run script for the producer container in the same Docker network.
- Screenshots demonstrating:
  - running Kafka installation;
  - running producer container;
  - consumed messages from topic `tweets`.

## Notes

The producer uses `kafka-python`.  
The topic is named `tweets`.  
The timestamp field is replaced by the current UTC timestamp in `created_at`.
