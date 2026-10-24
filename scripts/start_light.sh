#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/setup_local.py

base_services=(
  zookeeper
  kafka
  kafka-init
  postgres
  backend
  frontend
  telemetry-producer
)

intelligence_services=(
  ml-inference
  alert-engine
  threat-intelligence-engine
)

echo "Starting laptop-friendly platform mode..."
docker compose up --build -d "${base_services[@]}"
docker compose up --build -d --no-deps "${intelligence_services[@]}"
python3 scripts/health_check.py
