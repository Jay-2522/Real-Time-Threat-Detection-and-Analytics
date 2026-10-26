<div align="center">

# Real-Time Streaming Analytics Platform

[![Fast Checks](https://github.com/Jay-2522/Real-Time-Threat-Detection-and-Analytics/actions/workflows/fast-checks.yml/badge.svg?branch=ci%2Fgithub-actions)](https://github.com/Jay-2522/Real-Time-Threat-Detection-and-Analytics/actions/workflows/fast-checks.yml?query=branch%3Aci%2Fgithub-actions)

### AI-powered real-time SOC/XDR analytics platform for streaming telemetry, anomaly detection, threat intelligence, and incident response.

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-Backend-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-TypeScript-61DAFB?style=for-the-badge&logo=react&logoColor=0B1220)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-Analytics-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Storage-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Dashboards-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

</div>

---

## Project Overview

**Real-Time Streaming Analytics Platform** is a fully local, open-source, production-style AI SOC/XDR system that processes cybersecurity telemetry in real time.

The platform simulates enterprise telemetry, streams it through Kafka, enriches it with Spark Structured Streaming, applies ML-based anomaly detection, correlates incidents, maps detections to MITRE ATT&CK, enriches entities with local threat intelligence, and presents the results through a polished React dashboard and FastAPI APIs.

It is designed to demonstrate practical experience across:

- real-time telemetry processing
- distributed streaming analytics
- AI/ML anomaly detection
- cybersecurity analytics and SOC workflows
- threat intelligence correlation
- observability and platform health monitoring
- Dockerized microservices architecture

Everything runs locally with free/open-source tools. No paid APIs, managed cloud services, or external SaaS dependencies are required.

Measured single-node throughput, latency, resource use, feature-engine comparisons,
and watermark trade-offs are published in [the latest benchmark tables](benchmarks/results/latest.md).
Regenerate a one-minute smoke profile with `python -m benchmarks.run --quick`,
or the complete five-minute sweep with `python -m benchmarks.run --full`.

---

## Key Features

| Capability | Description |
| --- | --- |
| **Real-Time Telemetry Ingestion** | Generates synthetic cybersecurity, login, API, network, and anomaly telemetry. |
| **Kafka Streaming Pipeline** | Uses Kafka topics for telemetry, analytics, ML predictions, alerts, and dead-letter handling. |
| **Spark Streaming Analytics** | Performs structured streaming transformations, aggregations, and feature engineering. |
| **ML Anomaly Detection** | Uses Isolation Forest inference over engineered telemetry features. |
| **Threat Intelligence Engine** | Performs local IOC enrichment, entity profiling, threat scoring, and attack timeline construction. |
| **MITRE ATT&CK Mapping** | Maps detections to tactics, techniques, MITRE IDs, and kill-chain stages. |
| **Detection Engineering** | Supports Sigma-like YAML detection rules and rule hit analytics. |
| **Incident Correlation** | Groups related alerts into incidents with severity escalation and timelines. |
| **Real-Time Alerting** | Streams live SOC alerts and incident updates through FastAPI WebSockets. |
| **Search + Persistence** | Stores structured records in PostgreSQL and indexes searchable data in Elasticsearch. |
| **Observability** | Exposes Prometheus metrics and Grafana dashboards for service and pipeline monitoring. |
| **Enterprise Dashboard** | React + TypeScript dashboard for SOC analytics, threat hunting, entities, alerts, incidents, and system health. |
| **Dockerized Microservices** | Runs as a local Docker Compose platform with full and laptop-friendly startup modes. |

---

## Architecture

```mermaid
flowchart TD
    A["Telemetry Producer"] --> B["Kafka"]
    B --> C["Spark Streaming"]
    C --> D["ML Inference"]
    D --> E["Threat Intelligence Engine"]
    E --> F["Alert Engine"]
    F --> G["PostgreSQL"]
    E --> G
    C --> G
    G --> H["FastAPI Backend"]
    H --> I["React Dashboard"]
    H --> J["Grafana + Prometheus"]
```

<details>
<summary><strong>Detailed Data Flow</strong></summary>

1. Synthetic producers emit telemetry into Kafka topics.
2. Spark Structured Streaming consumes telemetry and produces enriched analytics and feature streams.
3. ML inference consumes analytics topics and publishes anomaly predictions.
4. The alert engine evaluates prediction and analytics streams to create SOC alerts and incidents.
5. The threat intelligence engine enriches activity with local IOC feeds, MITRE mappings, rule hits, and entity risk profiles.
6. PostgreSQL and Elasticsearch persist structured and searchable records.
7. FastAPI exposes REST and WebSocket APIs.
8. React visualizes dashboards, alerts, incidents, threat intelligence, MITRE coverage, entities, timelines, and observability.
9. Prometheus and Grafana provide platform health and metrics visibility.

</details>

For deeper diagrams, see [docs/architecture.md](docs/architecture.md).

---

## Tech Stack

### Frontend

| Technology | Purpose |
| --- | --- |
| React | Enterprise dashboard UI |
| TypeScript | Typed frontend development |
| Vite | Frontend build tooling |
| Tailwind CSS | Dark SOC/XDR styling |
| Recharts | Analytics charts |
| D3.js | Graph and data visualization support |
| React Flow | Attack path and relationship graph visualization |
| Framer Motion | UI animation |
| Axios | API client |

### Backend

| Technology | Purpose |
| --- | --- |
| FastAPI | REST APIs, WebSockets, OpenAPI docs |
| Pydantic v2 | API and event schema validation |
| SQLAlchemy | Database ORM |
| Uvicorn | ASGI runtime |
| prometheus-client | Metrics instrumentation |

### Streaming

| Technology | Purpose |
| --- | --- |
| Apache Kafka | Event streaming backbone |
| Zookeeper | Kafka coordination |
| Python Kafka clients | Producers, consumers, sink workers |

### AI/ML

| Technology | Purpose |
| --- | --- |
| scikit-learn | Isolation Forest anomaly detection |
| pandas / NumPy | Synthetic training data and feature processing |
| joblib | Model artifact persistence |
| Pydantic | Prediction event contracts |

### Databases

| Technology | Purpose |
| --- | --- |
| PostgreSQL | Structured telemetry, predictions, alerts, incidents, threat intelligence |
| Elasticsearch | Searchable telemetry, alerts, IOC matches, entity profiles |

### Observability

| Technology | Purpose |
| --- | --- |
| Prometheus | Metrics scraping |
| Grafana | Dashboards |
| Health checker scripts | Local platform validation |

### DevOps

| Technology | Purpose |
| --- | --- |
| Docker Compose | Local microservices orchestration |
| PowerShell / Bash scripts | Startup, shutdown, verification |
| pytest | Backend, ML, alert, and threat-intel test scaffolding |

---

## Platform Screenshots

### Overview Dashboard

Real-time AI telemetry analytics overview with event volume, anomaly counts, risk trends, severity distribution, and live WebSocket status.

![Overview Dashboard](./screenshots/dashboard-overview.png)

### Real-Time Alerts Dashboard

SOC alert triage view with live alert feed, severity badges, anomaly explanations, risk scores, and open incident counters.

![Real-Time Alerts Dashboard](./screenshots/alerts-dashboard.png)

### Incident Correlation Dashboard

Incident intelligence workspace showing correlated alerts, affected entities, escalation state, and live SOC activity.

![Incident Correlation Dashboard](./screenshots/incidents-dashboard.png)

### Threat Intelligence Dashboard

XDR-style threat intelligence view with enriched events, rule hits, threat relationship graph, score distribution, and IOC activity.

![Threat Intelligence Dashboard](./screenshots/threat-intelligence.png)

### MITRE ATT&CK Dashboard

ATT&CK matrix coverage view for active tactics, mapped techniques, MITRE IDs, and technique frequency signals.

![MITRE ATTACK Matrix](./screenshots/mitre-attack-matrix.png)

### Observability Dashboard

Platform health and monitoring page with service status, pipeline counters, metric summaries, and Prometheus/Grafana links.

![Observability Dashboard](./screenshots/observability-dashboard.png)

### Grafana Dashboard

Optional local Grafana instance for operational dashboards and visual monitoring of the streaming analytics platform.

![Grafana Dashboard](./screenshots/grafana-dashboard.png)

### Prometheus Dashboard

Prometheus query interface for inspecting scraped metrics, service targets, and time-series telemetry.

![Prometheus Dashboard](./screenshots/prometheus-dashboard.png)

---

## API

The backend is powered by **FastAPI** and exposes production-style REST and WebSocket APIs.

| API Surface | URL |
| --- | --- |
| Backend API | `http://localhost:8000` |
| Swagger Docs | `http://localhost:8000/docs` |
| OpenAPI JSON | `http://localhost:8000/api/v1/openapi.json` |
| Health Check | `http://localhost:8000/api/v1/health` |
| Prometheus Metrics | `http://localhost:8000/metrics` |

Example endpoint groups:

- `/api/v1/dashboard/*`
- `/api/v1/events/*`
- `/api/v1/predictions/*`
- `/api/v1/alerts/*`
- `/api/v1/incidents/*`
- `/api/v1/threat-intel/*`
- `/api/v1/detections/*`
- `/api/v1/entities/*`
- `/api/v1/mitre/*`
- `/api/v1/monitoring/*`
- `/api/v1/ws/dashboard`
- `/api/v1/ws/alerts`
- `/api/v1/ws/threat-intel`

---

## Local Setup

### Prerequisites

- Docker Desktop
- Python 3.11+
- Node.js 20+
- Git

### 1. Clone The Repository

```bash
git clone <your-repository-url>
cd streaming-analytics-system
```

### 2. Environment Setup

The project includes safe example environment files:

```text
.env.example
backend/.env.example
frontend/.env.example
ml-models/.env.example
alert-engine/.env.example
storage-sink/.env.example
threat-intelligence-engine/.env.example
```

Generate private local settings before starting Docker Compose:

```bash
python3 scripts/setup_local.py
```

Docker Compose reads `.env`; keep it out of Git. The setup command preserves an
existing `.env`, generates separate PostgreSQL and Grafana passwords, and chooses
alternate host ports when 5432 or 3000 is occupied. Published services bind to
127.0.0.1. These are development settings; database and Grafana credentials only
initialize new volumes. Never delete existing volumes to change credentials.
See [the baseline audit](docs/PHASE_0_BASELINE_AUDIT.md) for verified results and blockers.

### 3. Start Laptop-Friendly Mode

Windows:

```powershell
.\scripts\start_light.ps1
```

Unix/macOS/Linux:

```bash
./scripts/start_light.sh
```

### 4. Start Full Platform

Windows:

```powershell
.\scripts\start_full.ps1
```

Unix/macOS/Linux:

```bash
./scripts/start_full.sh
```

Grafana is off by default. Include the provisioned demo dashboards when needed:

```bash
./scripts/start_full.sh --dashboards
```

### 5. Verify Health

```bash
python scripts/health_check.py
python scripts/verify_phase_10.py
```

### 6. Stop Services

Windows:

```powershell
.\scripts\stop_all.ps1
```

Unix/macOS/Linux:

```bash
./scripts/stop_all.sh
```

---

## Observability

The platform includes a local monitoring stack:

- Prometheus scrapes backend, ML inference, storage sink, alert engine, telemetry producer, and threat intelligence metrics.
- Optional Grafana dashboards for system health, streaming pipeline, ML inference, SOC alerting, and threat intelligence run with the `dashboards` Compose profile.
- FastAPI exposes monitoring APIs for dashboard integration.
- `scripts/health_check.py` validates core service availability.

Example metrics endpoints:

| Component | Metrics Endpoint |
| --- | --- |
| Backend | `http://localhost:8000/metrics` |
| ML Inference | `http://localhost:9101/metrics` |
| Storage Sink | `http://localhost:9102/metrics` |
| Alert Engine | `http://localhost:9103/metrics` |
| Telemetry Producer | `http://localhost:9104/metrics` |
| Threat Intelligence | `http://localhost:9106/metrics` |

---

## Production Readiness

This project is intentionally built with production-style architecture patterns:

- Dockerized microservices
- modular service boundaries
- typed schemas and event contracts
- Kafka-based streaming pipeline
- Spark-based analytics layer
- independent ML inference service
- independent alert and threat intelligence engines
- PostgreSQL persistence
- Elasticsearch search indexes
- PostgreSQL-backed alert cooldown deduplication
- Prometheus metrics
- Grafana dashboards
- FastAPI OpenAPI docs
- WebSocket live updates
- security headers middleware
- health validation scripts
- light and full startup modes

See [docs/production-readiness-checklist.md](docs/production-readiness-checklist.md).

---

---

## License

This project is licensed under the **MIT License**. See [LICENSE](LICENSE).
