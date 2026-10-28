#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "Docker Compose is required (docker compose or docker-compose)." >&2
  exit 1
fi

TIMEOUT_SECONDS=${LINEAGE_TIMEOUT_SECONDS:-300}
POLL_SECONDS=${LINEAGE_POLL_SECONDS:-0.25}
RUN_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
EVENT_ID="lineage-${RUN_ID}"
ENTITY_ID="2001:db8:${RUN_ID:0:4}:${RUN_ID:4:4}::1"
START_EPOCH=$(date +%s)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/lineage-check.XXXXXX")
RESULTS_FILE="$WORK_DIR/results.tsv"
CONSUMER_PIDS=()
PAUSED_CONTAINERS=()

cleanup() {
  for container in "${PAUSED_CONTAINERS[@]:-}"; do
    docker unpause "$container" >/dev/null 2>&1 || true
  done
  for container in "${PAUSED_CONTAINERS[@]:-}"; do
    for _ in {1..30}; do
      [[ $(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container" 2>/dev/null) == "healthy" ]] && break
      sleep 1
    done
  done
  for pid in "${CONSUMER_PIDS[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  done
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

elapsed() {
  echo $(( $(date +%s) - START_EPOCH ))
}

record() {
  printf '%s\t%s\t%s\n' "$1" "$2" "$(elapsed)" >>"$RESULTS_FILE"
}

print_results() {
  printf '\n%-38s %-6s %s\n' "HOP" "STATUS" "SECONDS"
  printf '%-38s %-6s %s\n' "--------------------------------------" "------" "-------"
  while IFS=$'\t' read -r hop outcome seconds; do
    printf '%-38s %-6s %s\n' "$hop" "$outcome" "$seconds"
  done <"$RESULTS_FILE"
}

fail() {
  local hop=$1
  local reason=$2
  record "$hop" "FAIL"
  echo "FAIL: $reason" >&2
  print_results >&2
  case "$hop" in
    *enriched*|*feature*) docker logs --tail 80 streaming-analytics-spark-streaming >&2 || true ;;
    *prediction*) docker logs --tail 80 streaming-analytics-ml-inference >&2 || true ;;
    *PostgreSQL*) docker logs --tail 80 streaming-analytics-alert-engine >&2 || true ;;
    *API*|*WebSocket*) docker logs --tail 80 streaming-analytics-backend >&2 || true ;;
  esac
  exit 1
}

require_healthy_stack() {
  local unhealthy
  unhealthy=$("${COMPOSE[@]}" ps --format json | python3 -c '
import json, sys
payload = sys.stdin.read().strip()
try:
    decoded = json.loads(payload)
    rows = decoded if isinstance(decoded, list) else [decoded]
except json.JSONDecodeError:
    rows = [json.loads(line) for line in payload.splitlines()]
bad = [row["Service"] for row in rows if row.get("State") != "running" or row.get("Health") not in ("", "healthy")]
print(",".join(bad))
')
  if [[ -n "$unhealthy" ]]; then
    echo "Stack is not healthy: $unhealthy" >&2
    exit 1
  fi
}

start_capture() {
  local topic=$1
  local output=$2
  docker exec streaming-analytics-kafka kafka-console-consumer \
    --bootstrap-server streaming-analytics-kafka:29092 \
    --topic "$topic" \
    --group "lineage-${RUN_ID}-${topic//./-}" \
    --consumer-property auto.offset.reset=latest \
    --property print.value=true >"$output" 2>"$output.err" &
  CONSUMER_PIDS+=("$!")
}

wait_for_file() {
  local hop=$1
  local file=$2
  local pattern=$3
  local deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while (( $(date +%s) < deadline )); do
    if grep -Fq "$pattern" "$file" 2>/dev/null; then
      record "$hop" "PASS"
      return 0
    fi
    sleep "$POLL_SECONDS"
  done
  fail "$hop" "Timed out after ${TIMEOUT_SECONDS}s waiting for $pattern in $file"
}

wait_for_alert_row() {
  local deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while (( $(date +%s) < deadline )); do
    ALERT_ID=$(docker exec streaming-analytics-postgres psql -U streaming_user -d streaming_analytics -Atc \
      "SELECT alert_id FROM alerts WHERE source_topic = 'ml.anomaly.predictions' AND raw_payload::text LIKE '%${EVENT_ID}%' ORDER BY created_at DESC LIMIT 1" 2>/dev/null || true)
    if [[ -n "$ALERT_ID" ]]; then
      record "alert row in PostgreSQL" "PASS"
      docker pause streaming-analytics-alert-engine >/dev/null
      PAUSED_CONTAINERS+=("streaming-analytics-alert-engine")
      return 0
    fi
    sleep "$POLL_SECONDS"
  done
  fail "alert row in PostgreSQL" "No ML-derived alert row contained event $EVENT_ID"
}

require_healthy_stack

start_capture "telemetry.login.events" "$WORK_DIR/source.log"
start_capture "analytics.enriched.events" "$WORK_DIR/enriched.log"
start_capture "analytics.feature.engineering" "$WORK_DIR/features.log"
start_capture "ml.anomaly.predictions" "$WORK_DIR/predictions.log"
sleep 5

EVENT_JSON=$(EVENT_ID="$EVENT_ID" ENTITY_ID="$ENTITY_ID" python3 - <<'PY'
import json, os
from datetime import UTC, datetime, timedelta
print(json.dumps({
    "event_id": os.environ["EVENT_ID"],
    # Stay ahead of time-shifted demo fixtures so Spark does not discard this
    # probe as older than the active event-time watermark.
    "timestamp": (datetime.now(UTC) + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
    "risk_score": 99.0,
    "event_type": "login",
    "user_id": os.environ["EVENT_ID"],
    "username": "lineage-check",
    "source_ip": os.environ["ENTITY_ID"],
    "country": "ZZ",
    "device_type": "lineage-test",
    "login_success": False,
    "failure_reason": "lineage-check",
}, separators=(",", ":")))
PY
)

printf '%s|%s\n' "$EVENT_ID" "$EVENT_JSON" | docker exec -i streaming-analytics-kafka \
  kafka-console-producer --bootstrap-server streaming-analytics-kafka:29092 \
  --topic telemetry.login.events --property parse.key=true --property 'key.separator=|'

# Stateful Spark windows only close after event-time advances beyond the
# configured watermark. Use a distinct documentation-only entity so this
# check remains deterministic when the background synthetic producer is off.
MARKER_ID="lineage-watermark-${RUN_ID}"
MARKER_JSON=$(MARKER_ID="$MARKER_ID" python3 - <<'PY'
import json, os
from datetime import UTC, datetime, timedelta
print(json.dumps({
    "event_id": os.environ["MARKER_ID"],
    "timestamp": (datetime.now(UTC) + timedelta(minutes=20)).isoformat().replace("+00:00", "Z"),
    "risk_score": 0.0,
    "event_type": "login",
    "user_id": os.environ["MARKER_ID"],
    "username": "lineage-watermark",
    "source_ip": "192.0.2.250",
    "country": "ZZ",
    "device_type": "lineage-test",
    "login_success": True,
}, separators=(",", ":")))
PY
)
printf '%s|%s\n' "$MARKER_ID" "$MARKER_JSON" | docker exec -i streaming-analytics-kafka \
  kafka-console-producer --bootstrap-server streaming-analytics-kafka:29092 \
  --topic telemetry.login.events --property parse.key=true --property 'key.separator=|'

wait_for_file "Kafka telemetry.login.events" "$WORK_DIR/source.log" "$EVENT_ID"
wait_for_file "Spark analytics.enriched.events" "$WORK_DIR/enriched.log" "$EVENT_ID"
wait_for_file "Spark analytics.feature.engineering" "$WORK_DIR/features.log" "$EVENT_ID"
wait_for_file "ML ml.anomaly.predictions" "$WORK_DIR/predictions.log" "$EVENT_ID"
docker pause streaming-analytics-producer >/dev/null
PAUSED_CONTAINERS+=("streaming-analytics-producer")
wait_for_alert_row

LINEAGE_USERNAME="lineage-${RUN_ID:0:12}"
LINEAGE_PASSWORD="Lineage-${RUN_ID}-Aa1!"
docker exec \
  -e LINEAGE_USERNAME="$LINEAGE_USERNAME" \
  -e LINEAGE_PASSWORD="$LINEAGE_PASSWORD" \
  streaming-analytics-backend python -c '
import os
from app.auth.service import create_user
from app.db.session import SessionLocal
with SessionLocal() as db, db.begin():
    create_user(db, os.environ["LINEAGE_USERNAME"], os.environ["LINEAGE_PASSWORD"], "viewer")
' >/dev/null

LOGIN_BODY=$(LINEAGE_USERNAME="$LINEAGE_USERNAME" LINEAGE_PASSWORD="$LINEAGE_PASSWORD" python3 -c \
  'import json,os; print(json.dumps({"username":os.environ["LINEAGE_USERNAME"],"password":os.environ["LINEAGE_PASSWORD"]}))')
HTTP_STATUS=$(printf '%s' "$LOGIN_BODY" | curl -sS \
  -o "$WORK_DIR/login.json" -c "$WORK_DIR/cookies.txt" -w '%{http_code}' \
  -H 'Content-Type: application/json' -H 'Origin: http://localhost:5173' -H 'X-Requested-With: SOC' \
  --data-binary @- http://localhost:8000/api/v1/auth/login)
if [[ "$HTTP_STATUS" != "200" ]]; then
  fail "authenticated API response" "Login returned HTTP $HTTP_STATUS: $(cat "$WORK_DIR/login.json")"
fi

curl -sS -b "$WORK_DIR/cookies.txt" -H 'Origin: http://localhost:5173' \
  'http://localhost:8000/api/v1/alerts/recent?limit=200' >"$WORK_DIR/alerts.json"
if ! ALERT_ID="$ALERT_ID" python3 - "$WORK_DIR/alerts.json" <<'PY'
import json, os, sys
rows = json.load(open(sys.argv[1], encoding="utf-8"))
raise SystemExit(0 if any(row.get("alert_id") == os.environ["ALERT_ID"] for row in rows) else 1)
PY
then
  fail "authenticated API response" "Authenticated /alerts/recent did not return alert $ALERT_ID"
fi
record "authenticated API response" "PASS"

SESSION_COOKIE=$(awk '$6 == "soc_session_local" {print $7}' "$WORK_DIR/cookies.txt")
if [[ -z "$SESSION_COOKIE" ]]; then
  fail "dashboard alerts WebSocket" "Login response did not set soc_session_local"
fi
if ! docker exec -e LINEAGE_ALERT_ID="$ALERT_ID" -e LINEAGE_SESSION_COOKIE="$SESSION_COOKIE" \
  streaming-analytics-backend python -c '
import asyncio, json, os
import websockets

async def check():
    async with websockets.connect(
        "ws://127.0.0.1:8000/api/v1/ws/alerts",
        origin="http://localhost:5173",
        additional_headers={"Cookie": "soc_session_local=" + os.environ["LINEAGE_SESSION_COOKIE"]},
    ) as socket:
        for _ in range(3):
            payload = json.loads(await asyncio.wait_for(socket.recv(), timeout=5))
            rows = payload.get("recent_alerts", []) + payload.get("critical_alerts", [])
            if any(row.get("alert_id") == os.environ["LINEAGE_ALERT_ID"] for row in rows):
                return
    raise RuntimeError("alert missing from dashboard WebSocket payload")

asyncio.run(check())
'; then
  fail "dashboard alerts WebSocket" "Authenticated /ws/alerts did not return alert $ALERT_ID"
fi
record "dashboard alerts WebSocket" "PASS"

print_results
echo "Lineage event: $EVENT_ID"
