#!/usr/bin/env bash
set -euo pipefail

if [ ! -f "data/amazon_reviews.csv" ]; then
  echo "ERROR: data/amazon_reviews.csv not found."
  echo "Copy the provided Amazon Reviews CSV file to data/amazon_reviews.csv first."
  exit 1
fi

docker compose --profile producer up producer
