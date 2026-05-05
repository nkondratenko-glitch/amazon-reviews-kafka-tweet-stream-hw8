#!/usr/bin/env bash
set -euo pipefail

docker exec -it hw8-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tweets \
  --from-beginning \
  --max-messages 10
