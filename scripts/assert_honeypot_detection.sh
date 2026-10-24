#!/usr/bin/env bash
set -euo pipefail

SOURCE_IP=${HONEYPOT_TEST_SOURCE_IP:-203.0.113.42}
TIMEOUT_SECONDS=${HONEYPOT_ASSERT_TIMEOUT_SECONDS:-180}
deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))

query() {
  docker exec streaming-analytics-postgres psql -U streaming_user -d streaming_analytics -Atc "$1"
}

while (( $(date +%s) < deadline )); do
  alert_count=$(query "SELECT count(*) FROM alerts WHERE entity_id = '${SOURCE_IP}' AND raw_payload->'rule_hits' @> '[{\"rule_id\":\"brute_force_detection\"}]'::jsonb;")
  incident_count=$(query "SELECT count(*) FROM incidents WHERE raw_payload->>'entity_id' = '${SOURCE_IP}';")
  if (( alert_count >= 1 && incident_count == 1 )); then
    break
  fi
  sleep 2
done

if (( alert_count < 1 || incident_count != 1 )); then
  echo "Expected a brute-force alert and exactly one incident for ${SOURCE_IP}; alerts=${alert_count}, incidents=${incident_count}." >&2
  exit 1
fi

alert_id=$(query "SELECT alert_id FROM alerts WHERE entity_id = '${SOURCE_IP}' AND raw_payload->'rule_hits' @> '[{\"rule_id\":\"brute_force_detection\"}]'::jsonb ORDER BY created_at DESC LIMIT 1;")
incident_id=$(query "SELECT incident_id FROM incidents WHERE raw_payload->>'entity_id' = '${SOURCE_IP}' ORDER BY updated_at DESC LIMIT 1;")
incident_events=$(query "SELECT event_count FROM incidents WHERE incident_id = '${incident_id}';")

username="ci-honeypot-$(date +%s)"
password="Ci-honeypot-$(date +%s)-Aa1!"
docker exec -e CI_USERNAME="$username" -e CI_PASSWORD="$password" streaming-analytics-backend python -c '
import os
from app.auth.service import create_user
from app.db.session import SessionLocal
with SessionLocal() as db, db.begin():
    create_user(db, os.environ["CI_USERNAME"], os.environ["CI_PASSWORD"], "viewer")
' >/dev/null

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/honeypot-assert.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT
CI_USERNAME="$username" CI_PASSWORD="$password" python3 - <<'PY' >"$work_dir/login.json"
import json
import os
print(json.dumps({"username": os.environ["CI_USERNAME"], "password": os.environ["CI_PASSWORD"]}))
PY

status=$(curl -sS -o "$work_dir/session.json" -c "$work_dir/cookies.txt" -w '%{http_code}' \
  -H 'Content-Type: application/json' -H 'Origin: http://localhost:5173' -H 'X-Requested-With: SOC' \
  --data-binary @"$work_dir/login.json" http://localhost:8000/api/v1/auth/login)
if [[ "$status" != 200 ]]; then
  echo "Dashboard API login failed with HTTP ${status}." >&2
  exit 1
fi

curl -fsS -b "$work_dir/cookies.txt" -H 'Origin: http://localhost:5173' \
  'http://localhost:8000/api/v1/alerts/recent?limit=200' >"$work_dir/alerts.json"
curl -fsS -b "$work_dir/cookies.txt" -H 'Origin: http://localhost:5173' \
  'http://localhost:8000/api/v1/incidents/recent?limit=200' >"$work_dir/incidents.json"

ALERT_ID="$alert_id" INCIDENT_ID="$incident_id" python3 - "$work_dir/alerts.json" "$work_dir/incidents.json" <<'PY'
import json
import os
import sys

alerts = json.load(open(sys.argv[1], encoding="utf-8"))
incidents = json.load(open(sys.argv[2], encoding="utf-8"))
if not any(row.get("alert_id") == os.environ["ALERT_ID"] for row in alerts):
    raise SystemExit("Brute-force alert is absent from the authenticated dashboard API")
if not any(row.get("incident_id") == os.environ["INCIDENT_ID"] for row in incidents):
    raise SystemExit("Brute-force incident is absent from the authenticated dashboard API")
PY

echo "Honeypot detection passed: source_ip=${SOURCE_IP} alerts=${alert_count} incidents=${incident_count} incident_events=${incident_events}."
