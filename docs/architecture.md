# Architecture

## High-Level Platform

```mermaid
flowchart TD
    A["Synthetic Telemetry Producers"] --> B["Kafka Topics"]
    B --> C["Spark Structured Streaming"]
    C --> D["Analytics Topics"]
    D --> E["ML Inference"]
    E --> F["SOC Alert Engine"]
    D --> G["Storage Sink"]
    E --> G
    F --> H["Threat Intelligence Engine"]
    D --> H
    H --> I["PostgreSQL / Elasticsearch"]
    G --> I
    F --> I
    I --> J["FastAPI Query + WebSocket APIs"]
    J --> K["React SOC/XDR Dashboard"]
    J --> L["Prometheus + Grafana Observability"]
```

## Service Architecture

```mermaid
flowchart LR
    subgraph Streaming
        Z["Zookeeper"]
        K["Kafka"]
        P["Telemetry Producer"]
    end
    subgraph Processing
        SM["Spark Master"]
        SW["Spark Worker"]
        SJ["Spark Streaming Job"]
    end
    subgraph Intelligence
        ML["ML Inference"]
        AE["Alert Engine"]
        TI["Threat Intelligence Engine"]
    end
    subgraph Storage
        PG["PostgreSQL"]
        ES["Elasticsearch"]
        SS["Storage Sink"]
    end
    subgraph Experience
        API["FastAPI Backend"]
        UI["React Dashboard"]
        PR["Prometheus"]
        GF["Grafana (optional profile)"]
    end
    P --> K
    K --> SJ
    SJ --> K
    K --> ML
    ML --> K
    K --> AE
    K --> TI
    K --> SS
    SS --> PG
    SS --> ES
    AE --> PG
    TI --> PG
    TI --> ES
    API --> PG
    API --> ES
    UI --> API
    PR --> API
    PR --> ML
    PR --> AE
    PR --> TI
    GF --> PR
```

## Kafka Topics

| Topic | Purpose |
| --- | --- |
| `telemetry.login.events` | Synthetic login telemetry |
| `telemetry.api.events` | API activity telemetry |
| `telemetry.network.events` | Network traffic telemetry |
| `telemetry.anomaly.events` | Synthetic anomaly telemetry |
| `telemetry.deadletter.events` | Malformed/unprocessable records |
| `analytics.enriched.events` | Spark-normalized enriched events |
| `analytics.window.metrics` | Windowed stream metrics |
| `analytics.risk.metrics` | Aggregated risk metrics |
| `analytics.feature.engineering` | ML-ready feature events |
| `ml.anomaly.predictions` | Real-time ML prediction events |

## ML Inference Flow

```mermaid
sequenceDiagram
    participant K as Kafka Analytics Topics
    participant F as Feature Mapper
    participant M as Isolation Forest
    participant P as Prediction Builder
    participant O as Kafka Prediction Topic
    K->>F: analytics event
    F->>M: normalized feature vector
    M->>P: anomaly score
    P->>O: ml.anomaly.predictions
```

## Alert Engine Flow

```mermaid
flowchart TD
    A["ML predictions + analytics"] --> B["Rule Evaluation"]
    B --> C["PostgreSQL Cooldown Deduplication"]
    C --> D["Incident Correlation"]
    D --> E["PostgreSQL Alerts/Incidents"]
    D --> F["Elasticsearch Alerts/Incidents"]
    E --> G["FastAPI Alert APIs"]
    G --> H["Alerts + Incidents UI"]
```

## Threat Intelligence Flow

```mermaid
flowchart TD
    A["Kafka telemetry, analytics, predictions"] --> B["IOC Extraction"]
    B --> C["Local IOC Feeds"]
    A --> D["YAML Detection Rules"]
    A --> E["MITRE Mapper"]
    C --> F["Threat Scoring"]
    D --> F
    E --> F
    F --> G["Entity Profiles"]
    F --> H["Attack Timelines"]
    G --> I["PostgreSQL + Elasticsearch"]
    H --> I
    I --> J["Threat Intelligence APIs"]
    J --> K["XDR Dashboard Pages"]
```

## Observability Flow

```mermaid
flowchart LR
    A["/metrics endpoints"] --> B["Prometheus"]
    B -. optional .-> C["Grafana Dashboards"]
    B --> D["FastAPI Monitoring APIs"]
    D --> E["React Observability Page"]
```

## Storage Flow

```mermaid
flowchart TD
    A["Kafka Topics"] --> B["Storage Sink"]
    B --> C["PostgreSQL structured tables"]
    B --> D["Elasticsearch search indexes"]
    C --> F["FastAPI APIs"]
    D --> F
```
