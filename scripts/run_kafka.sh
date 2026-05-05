#!/usr/bin/env bash
set -euo pipefail

docker compose up -d zookeeper kafka
docker compose ps
