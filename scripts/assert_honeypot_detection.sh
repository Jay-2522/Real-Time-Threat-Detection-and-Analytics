#!/usr/bin/env bash
set -euo pipefail

DOCUMENTATION_SOURCE_IP=${HONEYPOT_DOCUMENTATION_SOURCE_IP:-203.0.113.42}
GROUPING_SOURCE_IP=${HONEYPOT_GROUPING_SOURCE_IP:-10.20.0.15}
TIMEOUT_SECONDS=${HONEYPOT_ASSERT_TIMEOUT_SECONDS:-180}
deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))

query() {
  docker exec streaming-analytics-postgres psql -U streaming_user -d streaming_analytics -Atc "$1"
}

while (( $(date +%s) < deadline )); do
  documentation_brute_alerts=$(query "SELECT count(*) FROM alerts WHERE entity_id = '${DOCUMENTATION_SOURCE_IP}' AND (raw_payload->'rule_hits')::jsonb @> '[{\"rule_id\":\"brute_force_detection\"}]'::jsonb;")
  grouping_brute_alerts=$(query "SELECT count(*) FROM alerts WHERE entity_id = '${GROUPING_SOURCE_IP}' AND (raw_payload->'rule_hits')::jsonb @> '[{\"rule_id\":\"brute_force_detection\"}]'::jsonb;")
  grouping_lateral_alerts=$(query "SELECT count(*) FROM alerts WHERE entity_id = '${GROUPING_SOURCE_IP}' AND (raw_payload->'rule_hits')::jsonb @> '[{\"rule_id\":\"lateral_movement\"}]'::jsonb;")
  documentation_incidents=$(query "SELECT count(*) FROM incidents WHERE raw_payload->>'entity_id' = '${DOCUMENTATION_SOURCE_IP}';")
  grouping_incidents=$(query "SELECT count(*) FROM incidents WHERE raw_payload->>'entity_id' = '${GROUPING_SOURCE_IP}';")
  if (( documentation_brute_alerts == 1 && grouping_brute_alerts == 1 && grouping_lateral_alerts == 1 && documentation_incidents == 1 && grouping_incidents == 1 )); then
    break
  fi
  sleep 2
done

if (( documentation_brute_alerts != 1 || grouping_brute_alerts != 1 || grouping_lateral_alerts != 1 || documentation_incidents != 1 || grouping_incidents != 1 )); then
  echo "Expected one documentation-source brute-force alert and two RFC1918 grouping alerts across two incidents; documentation_brute_alerts=${documentation_brute_alerts}, grouping_brute_alerts=${grouping_brute_alerts}, grouping_lateral_alerts=${grouping_lateral_alerts}, documentation_incidents=${documentation_incidents}, grouping_incidents=${grouping_incidents}." >&2
  exit 1
fi

documentation_alert_id=$(query "SELECT alert_id FROM alerts WHERE entity_id = '${DOCUMENTATION_SOURCE_IP}' AND (raw_payload->'rule_hits')::jsonb @> '[{\"rule_id\":\"brute_force_detection\"}]'::jsonb ORDER BY created_at DESC LIMIT 1;")
grouping_brute_alert_id=$(query "SELECT alert_id FROM alerts WHERE entity_id = '${GROUPING_SOURCE_IP}' AND (raw_payload->'rule_hits')::jsonb @> '[{\"rule_id\":\"brute_force_detection\"}]'::jsonb ORDER BY created_at DESC LIMIT 1;")
grouping_lateral_alert_id=$(query "SELECT alert_id FROM alerts WHERE entity_id = '${GROUPING_SOURCE_IP}' AND (raw_payload->'rule_hits')::jsonb @> '[{\"rule_id\":\"lateral_movement\"}]'::jsonb ORDER BY created_at DESC LIMIT 1;")
documentation_incident_id=$(query "SELECT incident_id FROM incidents WHERE raw_payload->>'entity_id' = '${DOCUMENTATION_SOURCE_IP}' ORDER BY updated_at DESC LIMIT 1;")
grouping_incident_id=$(query "SELECT incident_id FROM incidents WHERE raw_payload->>'entity_id' = '${GROUPING_SOURCE_IP}' ORDER BY updated_at DESC LIMIT 1;")
documentation_incident_events=$(query "SELECT event_count FROM incidents WHERE incident_id = '${documentation_incident_id}';")
grouping_incident_events=$(query "SELECT event_count FROM incidents WHERE incident_id = '${grouping_incident_id}';")
grouping_linked_alerts=$(query "SELECT count(*) FROM alerts WHERE entity_id = '${GROUPING_SOURCE_IP}' AND incident_id = '${grouping_incident_id}';")
grouping_rule_names=$(query "SELECT string_agg(DISTINCT hit->>'rule_id', ',' ORDER BY hit->>'rule_id') FROM alerts CROSS JOIN LATERAL jsonb_array_elements(COALESCE((raw_payload->'rule_hits')::jsonb, '[]'::jsonb)) AS hit WHERE entity_id = '${GROUPING_SOURCE_IP}';")
total_fixture_alerts=$(( documentation_brute_alerts + grouping_brute_alerts + grouping_lateral_alerts ))
total_fixture_incidents=$(( documentation_incidents + grouping_incidents ))

if (( documentation_incident_events != 1 || grouping_incident_events != 2 || grouping_linked_alerts != 2 )); then
  echo "Expected one single-alert documentation incident and one two-alert RFC1918 incident; documentation_incident_events=${documentation_incident_events}, grouping_incident_events=${grouping_incident_events}, grouping_linked_alerts=${grouping_linked_alerts}." >&2
  exit 1
fi

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

DOCUMENTATION_ALERT_ID="$documentation_alert_id" \
GROUPING_BRUTE_ALERT_ID="$grouping_brute_alert_id" \
GROUPING_LATERAL_ALERT_ID="$grouping_lateral_alert_id" \
DOCUMENTATION_INCIDENT_ID="$documentation_incident_id" \
GROUPING_INCIDENT_ID="$grouping_incident_id" \
python3 - "$work_dir/alerts.json" "$work_dir/incidents.json" <<'PY'
import json
import os
import sys

alerts = json.load(open(sys.argv[1], encoding="utf-8"))
incidents = json.load(open(sys.argv[2], encoding="utf-8"))
expected_alert_ids = {
    os.environ["DOCUMENTATION_ALERT_ID"],
    os.environ["GROUPING_BRUTE_ALERT_ID"],
    os.environ["GROUPING_LATERAL_ALERT_ID"],
}
visible_alert_ids = {row.get("alert_id") for row in alerts}
if not expected_alert_ids.issubset(visible_alert_ids):
    raise SystemExit(f"Fixture alerts are absent from the authenticated dashboard API: {expected_alert_ids - visible_alert_ids}")

documentation_incident = next(
    (row for row in incidents if row.get("incident_id") == os.environ["DOCUMENTATION_INCIDENT_ID"]), None
)
grouping_incident = next(
    (row for row in incidents if row.get("incident_id") == os.environ["GROUPING_INCIDENT_ID"]), None
)
if not documentation_incident or documentation_incident.get("event_count") != 1:
    raise SystemExit("Authenticated dashboard API does not show the single-alert documentation incident")
if not grouping_incident or grouping_incident.get("event_count") != 2:
    raise SystemExit("Authenticated dashboard API does not show the two-alert RFC1918 grouping incident")
linked = [row for row in alerts if row.get("incident_id") == os.environ["GROUPING_INCIDENT_ID"]]
if len(linked) != 2:
    raise SystemExit(f"Authenticated dashboard API shows {len(linked)} alerts for the RFC1918 incident, expected 2")
PY

echo "Synthetic correlation demo passed: documentation_source=${DOCUMENTATION_SOURCE_IP} documentation_alerts=${documentation_brute_alerts} documentation_rules=brute_force_detection documentation_incidents=${documentation_incidents} documentation_incident_events=${documentation_incident_events}; grouping_source=${GROUPING_SOURCE_IP} grouping_alerts=$(( grouping_brute_alerts + grouping_lateral_alerts )) grouping_rules=${grouping_rule_names} grouping_incidents=${grouping_incidents} grouping_incident_events=${grouping_incident_events} grouping_linked_alerts=${grouping_linked_alerts}; total_fixture_alerts=${total_fixture_alerts} total_fixture_incidents=${total_fixture_incidents}. Deliberately constructed fixture records validate correlation logic, not real attacker behavior."
