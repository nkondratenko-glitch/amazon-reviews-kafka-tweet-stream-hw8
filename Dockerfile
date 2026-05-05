FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV INPUT_FILE=/app/data/amazon_reviews.csv
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=tweets
ENV MIN_RATE=10
ENV MAX_RATE=15

CMD ["python", "src/tweet_stream_producer.py"]
