# Phase 9 - Threat Intelligence And Detection Engineering

Phase 9 adds a local, free XDR-style threat intelligence and detection engineering layer to the AI SOC platform.

## Architecture

```text
Kafka telemetry, analytics, and ML prediction topics
    -> threat-intelligence-engine
    -> IOC enrichment and local intelligence feeds
    -> Sigma-like YAML detection rules
    -> MITRE ATT&CK mapping
    -> composite threat scoring
    -> entity profiles and attack timelines
    -> PostgreSQL + Elasticsearch
    -> FastAPI threat APIs + WebSocket
    -> React XDR investigation pages
```

## Service

Container:

```text
streaming-analytics-threat-intelligence-engine
```

Metrics:

```text
http://localhost:9106/metrics
```

Prometheus job:

```text
threat-intelligence-engine
```

## Local Intelligence Feeds

The engine uses local/free intelligence only:

- suspicious IP ranges
- TOR exit node examples
- privileged account watchlist
- high-risk country watchlist
- malware hash signatures
- suspicious domain TLD heuristics

No cloud feeds, paid APIs, or external SaaS are used.

## Detection Rules

Rules live in:

```text
detections/rules/
```

Included rules:

- `brute_force_detection.yaml`
- `lateral_movement.yaml`
- `impossible_travel.yaml`
- `suspicious_privilege_escalation.yaml`
- `high_risk_geo_login.yaml`
- `beaconing_activity.yaml`
- `suspicious_process_chain.yaml`

Rules support:

- YAML loading
- enabled/disabled state
- severity
- MITRE tactic/technique metadata
- keyword conditions
- risk thresholds
- country conditions
- execution and hit metrics

## MITRE ATT&CK Mapping

Backend mapper:

```text
backend/app/services/mitre_mapper.py
```

Supported tactic coverage includes:

- Initial Access
- Execution
- Persistence
- Privilege Escalation
- Defense Evasion
- Credential Access
- Discovery
- Lateral Movement
- Collection
- Command and Control
- Exfiltration
- Impact

## Persistence

PostgreSQL tables:

- `threat_intel_events`
- `ioc_matches`
- `entity_profiles`
- `detection_rule_hits`
- `attack_timelines`

Elasticsearch indexes:

- `threat-intel-events`
- `ioc-matches`
- `entity-profiles`
- `detection-rule-hits`
- `attack-timelines`

## Backend APIs

Threat intelligence:

- `GET /api/v1/threat-intel/overview`
- `GET /api/v1/threat-intel/iocs`
- `GET /api/v1/threat-intel/entities`
- `GET /api/v1/threat-intel/mitre`
- `GET /api/v1/threat-intel/attack-timeline`
- `GET /api/v1/threat-intel/risk-heatmap`
- `GET /api/v1/threat-intel/threat-graph`
- `GET /api/v1/threat-intel/search?q=...`
- `WS /api/v1/ws/threat-intel`

Detection engineering:

- `GET /api/v1/detections/rules`
- `GET /api/v1/detections/rules/{rule_id}`
- `GET /api/v1/detections/rule-stats`

Entity intelligence:

- `GET /api/v1/entities/{entity_id}`
- `GET /api/v1/entities/high-risk`
- `GET /api/v1/entities/search?q=...`

MITRE:

- `GET /api/v1/mitre/tactics`
- `GET /api/v1/mitre/techniques`

## Frontend Routes

- `/threat-intelligence`
- `/detection-engineering`
- `/threat-hunting`
- `/mitre-attack`
- `/entities`
- `/attack-timeline`

The frontend uses Recharts, D3, React Flow, and Framer Motion for charts, matrix-style panels, relationship graphs, and smooth SOC dashboard interactions.

## Metrics

Threat intelligence metrics:

- `threat_events_processed_total`
- `ioc_enrichments_total`
- `detection_rule_executions_total`
- `detection_rule_hits_total`
- `threat_engine_errors_total`
- `threat_enrichment_latency_seconds`

Grafana dashboard:

```text
Threat Intelligence
```

## Verification

```bash
docker compose config --quiet
docker compose up --build -d
docker logs -f streaming-analytics-threat-intelligence-engine
```

Verify APIs:

```bash
curl http://localhost:8000/api/v1/threat-intel/overview
curl http://localhost:8000/api/v1/detections/rules
curl http://localhost:8000/api/v1/entities/high-risk
curl http://localhost:8000/api/v1/mitre/tactics
```

Verify monitoring:

```bash
curl http://localhost:9106/metrics
open http://localhost:9090/targets
open http://localhost:3000
```

Verify UI:

```text
http://localhost:5173/threat-intelligence
http://localhost:5173/detection-engineering
http://localhost:5173/threat-hunting
http://localhost:5173/mitre-attack
http://localhost:5173/entities
http://localhost:5173/attack-timeline
```

## Troubleshooting

If rule hit counts are empty, allow Kafka, Spark, ML inference, and the alert/threat services to run long enough for analytics and prediction topics to emit events.

If Elasticsearch indexing is unavailable, PostgreSQL persistence continues and APIs backed by PostgreSQL remain usable.
