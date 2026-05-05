#!/usr/bin/env bash
set -euo pipefail

docker exec -it hw8-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic tweets \
  --partitions 3 \
  --replication-factor 1
