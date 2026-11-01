#!/usr/bin/env bash
# Confirm the search indices were created with their intended mappings (not implicitly, with a dynamic mapping
# where entity ids are text and source IPs are strings). Run against a stack that has already processed events.
set -euo pipefail

SEARCH_URL=${ELASTICSEARCH_URL:-http://localhost:9200}
TIMEOUT_SECONDS=${SEARCH_MAPPING_TIMEOUT_SECONDS:-120}

python3 - "$SEARCH_URL" "$TIMEOUT_SECONDS" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

base, timeout = sys.argv[1].rstrip("/"), float(sys.argv[2])
EXPECTED = {
    "telemetry-events": {"entity_id": "keyword", "source_ip": "ip", "timestamp": "date", "risk_score": "double"},
    "analytics-events": {"entity_id": "keyword", "source_ip": "ip", "timestamp": "date"},
    "ml-predictions": {"entity_id": "keyword", "severity": "keyword", "timestamp": "date", "ml_risk_score": "double"},
    "deadletter-events": {"entity_id": "keyword", "timestamp": "date"},
    "alerts": {"entity_id": "keyword", "severity": "keyword", "status": "keyword", "timestamp": "date"},
    "incidents": {"entity_id": "keyword", "severity": "keyword", "status": "keyword"},
    "threat-intel-events": {"entity_id": "keyword", "severity": "keyword", "threat_score": "double", "timestamp": "date"},
    "ioc-matches": {"entity_id": "keyword", "threat_score": "double"},
    "detection-rule-hits": {"entity_id": "keyword", "mitre_id": "keyword"},
    "entity-profiles": {"entity_id": "keyword", "threat_score": "double"},
    "attack-timelines": {"entity_id": "keyword", "tactic": "keyword"},
}


def problems():
    found = []
    for index, fields in EXPECTED.items():
        try:
            with urllib.request.urlopen(f"{base}/{index}/_mapping", timeout=10) as response:
                properties = json.load(response)[index]["mappings"].get("properties", {})
        except urllib.error.HTTPError as error:
            found.append(f"{index}: HTTP {error.code} (index missing)")
            continue
        except (urllib.error.URLError, OSError) as error:
            found.append(f"{index}: {error}")
            continue
        for field, expected in fields.items():
            actual = properties.get(field, {}).get("type")
            if actual != expected:
                found.append(f"{index}.{field}: expected {expected}, found {actual}")
    return found


deadline = time.monotonic() + timeout
while True:
    found = problems()
    if not found or time.monotonic() > deadline:
        break
    time.sleep(3)
if found:
    sys.exit("Search indices lack their intended mappings:\n  " + "\n  ".join(found))
print(f"Search mappings verified: {len(EXPECTED)} indices, {sum(map(len, EXPECTED.values()))} field types.")
PY
