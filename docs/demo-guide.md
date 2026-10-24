# Demo Guide

## Start The Platform

Laptop-friendly demo:

```bash
./scripts/start_light.sh
```

Windows:

```powershell
.\scripts\start_light.ps1
```

Full observability demo:

```bash
./scripts/start_full.sh
```

## Open These Pages

- Dashboard: `http://localhost:5173`
- Alerts: `http://localhost:5173/alerts`
- Incidents: `http://localhost:5173/incidents`
- Threat Intelligence: `http://localhost:5173/threat-intelligence`
- Detection Engineering: `http://localhost:5173/detection-engineering`
- Threat Hunting: `http://localhost:5173/threat-hunting`
- MITRE ATT&CK: `http://localhost:5173/mitre-attack`
- Observability: `http://localhost:5173/observability`
- API docs: `http://localhost:8000/docs`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:<GRAFANA_PUBLISHED_PORT>` (user `admin`; read the
  generated password privately from `.env`). Run `python3 scripts/setup_local.py`
  before startup if local settings do not exist.

## Recruiter Demo Script

1. Start on the Overview page and explain the system as a local AI SOC/XDR platform.
2. Open Live Stream to show events, predictions, and WebSocket behavior.
3. Open Alerts and Incidents to show SOC-style alert correlation.
4. Open Threat Intelligence to show IOC enrichment, MITRE mapping, and threat graphs.
5. Open Detection Engineering to show YAML rule hits and rule management analytics.
6. Open MITRE ATT&CK to show tactic and technique coverage.
7. Open Observability to show Prometheus/Grafana-backed health and metrics.
8. Open FastAPI `/docs` to show backend API maturity.

## What To Explain

- Kafka handles event streaming.
- Spark produces enriched analytics and feature streams.
- ML inference scores anomalies with Isolation Forest.
- The alert engine deduplicates and correlates alerts into incidents.
- The threat intelligence engine enriches events with local IOC feeds and MITRE context.
- PostgreSQL and Elasticsearch support structured queries and search.
- Prometheus and Grafana provide local observability.

## Showing Alerts And Incidents

Run the stack for several minutes so producer, Spark, ML, alert, and threat services can process messages. Then open:

```text
http://localhost:5173/alerts
http://localhost:5173/incidents
```

## Stop Services

```bash
./scripts/stop_all.sh
```

Windows:

```powershell
.\scripts\stop_all.ps1
```
