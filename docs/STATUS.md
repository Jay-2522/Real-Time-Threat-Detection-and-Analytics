# Project status audit

Audited 2026-10-23 at `be38bddf308f8e849bfb778a64ae400596e27dac` on `dev`, using fresh volumes in an isolated Colima profile. **The full pipeline does not work from a clean state.**

## 1. How to run, and what actually happened

The repository entry point preserves `.env`, then requires the Docker Compose plugin:

```text
$ python3 scripts/setup_local.py
Existing .env preserved. Check its credentials and published ports before starting.
$ /usr/bin/time -p ./scripts/start_full.sh
Starting full Streaming Analytics AI SOC platform...
unknown flag: --build
real 0.48
```

There is **no verified working clean-start command on this machine**: only standalone `docker-compose 5.6.0` is installed. A diagnostic start with cached images (no build/pull) did create fresh containers:

```text
$ /usr/bin/time -p docker-compose --context colima-phase4a-recovery --env-file .env -f docker-compose.yml up -d --no-build
... Container streaming-analytics-grafana Started
real 37.61
$ DOCKER_CONTEXT=colima-phase4a-recovery python3 scripts/health_check.py
... [FAIL] Backend API - Remote end closed connection without response
Summary: degraded (11 OK, 1 failed)
```

`docker-compose ... ps -a` reported **zookeeper, kafka, postgres, and elasticsearch** healthy; `kafka-init` exited 0; `spark-streaming` was `Restarting (1)`. The other long-running containers lacked health checks: **UNVERIFIED**. Probes passed for the frontend and infrastructure plus ML/storage/alert/threat metrics, but processing did not: backend lacked `auth_control`, storage/alert waited for `pipeline_receipts`, and Spark could not download its Kafka connector:

```text
sqlalchemy.exc.ProgrammingError: relation "auth_control" does not exist
PostgreSQL connection and schema initialization failed ... relation "pipeline_receipts" does not exist
unresolved dependency: org.apache.spark#spark-sql-kafka-0-10_2.12;3.5.3: not found
```

## 2. One-event data-flow check

| Hop | Result | Proof |
|---|---|---|
| event -> Kafka | **PASS** | Exact key and JSON read back. |
| Kafka -> Spark | **FAIL** | No ID in `analytics.enriched.events` or `analytics.feature.engineering`; Spark was restarting with the unresolved dependency above. |
| Spark -> ML prediction | **UNVERIFIED** | No feature reached ML and no ID appeared in `ml.anomaly.predictions`. |
| prediction -> alert | **UNVERIFIED** | There was no prediction to evaluate; alert-engine was also blocked on `pipeline_receipts`. |
| alert -> PostgreSQL | **FAIL** | The clean database lacked the pipeline tables, including `telemetry_events`. |
| PostgreSQL -> API | **FAIL** | Backend application startup failed; curl received an empty reply. |
| API -> dashboard | **FAIL** | Vite served HTML, but `/auth/me` failed; `SessionGate.tsx` blocks pages without a session. |

```text
$ AUDIT_ID='7a3d63e8-3f26-4cc1-9e47-202610230001'
$ AUDIT_JSON='{"event_id":"7a3d63e8-3f26-4cc1-9e47-202610230001","timestamp":"2026-10-23T08:05:00Z","risk_score":99.0,"event_type":"anomaly","anomaly_type":"credential_stuffing","severity":"critical","source_ip":"203.0.113.77","user_id":"audit-user","description":"Audit trace event"}'
$ printf '%s|%s\n' "$AUDIT_ID" "$AUDIT_JSON" | docker --context colima-phase4a-recovery exec -i streaming-analytics-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic telemetry.anomaly.events --property parse.key=true --property key.separator='|'
$ docker --context colima-phase4a-recovery exec streaming-analytics-kafka bash -lc "timeout 10 kafka-console-consumer --bootstrap-server localhost:9092 --topic telemetry.anomaly.events --from-beginning --timeout-ms 8000 --property print.key=true --property key.separator='|' | grep '$AUDIT_ID' | head -1"
7a3d...0001|{"event_id":"7a3d...0001",...,"risk_score":99.0,"event_type":"anomaly",...}
$ for t in analytics.enriched.events analytics.feature.engineering ml.anomaly.predictions; do docker --context colima-phase4a-recovery exec streaming-analytics-kafka bash -lc "timeout 8 kafka-console-consumer --bootstrap-server localhost:9092 --topic '$t' --from-beginning --timeout-ms 6000 2>/dev/null | grep '$AUDIT_ID' | head -1"; done
analytics.enriched.events =>
analytics.feature.engineering =>
ml.anomaly.predictions =>
$ docker --context colima-phase4a-recovery exec streaming-analytics-postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT source_topic,event_id,risk_score FROM telemetry_events WHERE event_id='"'"'7a3d63e8-3f26-4cc1-9e47-202610230001'"'"'"'
ERROR: relation "telemetry_events" does not exist
$ curl -i --max-time 5 http://localhost:8000/api/v1/health
curl: (52) Empty reply from server
$ curl -i --max-time 5 http://localhost:5173/
HTTP/1.1 200 OK
```

Threat intelligence independently consumed raw synthetic telemetry and created five tables. This is outside the requested Spark/ML/alert chain.

## 3. Dashboard pages

Classes: **(a)** real pipeline, **(b)** mock/hardcoded, **(c)** empty/broken. No page was verified (a) because authentication never started.

| Page | Class | File and intended endpoint/evidence |
|---|---:|---|
| Overview | **b** | `dashboardService.ts`: `/dashboard/*`; substitutes fake trends, counts, and entities. |
| Live Stream | **c** | `LiveStreamPage.tsx`: `/events/recent`, `/predictions/recent`, `/ws/dashboard`; empty fallbacks. |
| ML Predictions | **c** | `MLPredictionsPage.tsx`: `/predictions/recent`, `/risks/high`; empty fallbacks. |
| Risk Analytics | **b** | `dashboardService.ts`: the same hardcoded charts/entities as Overview. |
| Alerts | **c** | `AlertsPage.tsx`: `/alerts/recent`, `/alerts/stats`, `/ws/alerts`; empty/zero fallback. |
| Incidents | **c** | `IncidentsPage.tsx`: `/incidents/recent`, `/incidents/open`; empty fallback. |
| Threat Intelligence | **c** | `ThreatIntelligencePage.tsx`: `/threat-intel/*`; empty fallback. |
| Threat Hunting | **c** | `ThreatHuntingPage.tsx`: `/threat-intel/search` and `/iocs`; empty fallback. |
| Entities | **c** | `EntitiesPage.tsx`: `/threat-intel/entities`; empty fallback. |
| Attack Timeline | **c** | `AttackTimelinePage.tsx`: `/threat-intel/attack-timeline`; empty fallback. |
| MITRE ATT&CK | **c** | `MitreAttackPage.tsx`: `/mitre/tactics`, `/threat-intel/mitre`; empty fallback. |
| Detection Engineering | **b** | `DetectionEngineeringPage.tsx`: always says `Pipeline Health = Active`. |
| Monitoring | **c** | `ObservabilityPage.tsx`: `/monitoring/*`; zero/empty/degraded fallbacks. |

## 4. Compose services

Verified inventory: `docker-compose --env-file .env -f docker-compose.yml config --services` printed these 20 services.

| Service | Purpose; removal impact |
|---|---|
| zookeeper | Kafka metadata; Kafka stops. |
| kafka | Event bus; every streaming hop stops. |
| kafka-init | Creates topics; fresh Kafka has no required topics. |
| kafka-ui | Broker viewer; only the Kafka UI disappears. |
| postgres | Durable records/auth; API, sinks, alerts, and threat persistence fail. |
| elasticsearch | Search indexes; searches and secondary indexing fail. |
| backend | FastAPI/auth; dashboard cannot sign in or query data. |
| frontend | React UI; browser dashboard disappears. |
| telemetry-producer | Generates synthetic events; automatic input stops. |
| telemetry-consumer | Validates/logs raw events and emits its DLQ; that validation path stops. |
| spark-master | Spark coordinator; worker/driver cluster mode fails. |
| spark-worker | Executes Spark work; distributed processing capacity disappears. |
| spark-streaming | Enrichment/windows/features; ML receives no feature windows. |
| ml-inference | Isolation Forest predictions; ML topic and ML-driven alerts stop. |
| storage-sink | Kafka-to-Postgres/Elasticsearch persistence; main API datasets stay empty. |
| alert-engine | Rules, dedup, incidents; alerts/incidents stop. |
| threat-intelligence-engine | Local IOC/rule/MITRE enrichment; threat pages stay empty. |
| prometheus | Metric scraping; monitoring metrics and Grafana source disappear. |
| grafana | Metric dashboards; core processing continues without Grafana. |

## 5. Detection and model

Seven enabled YAML rules exist in `detections/rules/`: `beaconing_activity` (minimum risk 58, score 78), `brute_force_detection` (55, 82), `high_risk_geo_login` (45 plus North Korea/Russia/Iran, 68), `impossible_travel` (50, 72), `lateral_movement` (60, 86), `suspicious_privilege_escalation` (70, 92), and `suspicious_process_chain` (60, 80). Proof command: `for f in detections/rules/*.yaml; do cat "$f"; done` printed those values.

`alert-engine/app/rules/rule_engine.py` adds five code rules: `high_anomaly` at anomaly/risk **0.88/90 critical, 0.72/75 high, 0.55/60 medium**; `credential_attack` on failed login or failed-login rate >= **0.5**; `api_abuse` on error rate >= **0.4**, requests/minute >= **80**, or HTTP >= **500**; `network_spike` on spike >= **6** or average bytes >= **150,000**; `correlated_risk` on suspicious-IP frequency >= **10** or high-risk count >= **5**. Dedup cooldown is 120 seconds and incident window 900 seconds (`alert-engine/app/config/settings.py`).

Compose expects `/app/artifacts/contract-v2/isolation_forest.joblib`, mapped from `${DEV_BIND_ROOT}/ml-models/artifacts/contract-v2/`; it is generated/ignored. The tracked top-level model is legacy 0.1.0 and is not used. Active metadata says Isolation Forest 0.2.0, 18 features, 60-second source-IP windows, 12,000 synthetic samples, contamination 0.06, seed 42, and no independent evaluation. Retrain with:

```text
docker compose run --rm ml-inference python -m app.training.train_model
```

`train_model.py` generates the data, fits `StandardScaler` plus a 250-tree `IsolationForest`, and overwrites four artifacts.

## 6. Tests

Collected: alert-engine 3, backend 21, ML 29, threat-intelligence 3, hardening 31, reliability 30, Spark-recovery 25. Spark-jobs has 5 more, excluded by `pytest.ini`; host collection lacks `pyspark`.

```text
$ .venv/bin/python -m pytest -ra
1 failed, 101 passed, 40 skipped ...
FAILED test_hardened_compose_requires_secret_directory (docker compose plugin absent)
$ DOCKER_CONFIG="$PWD/.codex-logs/phase4c/docker-config" .venv/bin/python -m pytest -ra
102 passed, 40 skipped, 99 warnings in 15.54s
$ docker --context colima-phase4a-recovery run --rm --entrypoint /opt/spark/bin/spark-submit -v "$PWD/spark-jobs:/app" -w /app streaming_analytics_system-spark-streaming:latest tests/test_feature_contract.py
Ran 5 tests in 7.105s
OK
$ cd frontend && npm run lint && npm run build
lint: 0 errors, 2 hook warnings; build: 3080 modules transformed, built in 1.83s (996.90 kB JS chunk warning)
```

The 40 skips include full-stack, recovery, browser-auth, migration, and live-Spark tests; passes do not validate deployment.

## 7. Data ingestion

Only `TelemetryProducer`/`SyntheticTelemetryGenerator` writes events: 5/second, 5% anomaly (`docker-compose.yml`, `telemetry_producer.py`). `rg 'syslog|filebeat|fluent|webhook'` found no real connector. Schema is Pydantic in `kafka-producers/app/schemas/events.py`, duplicated as Spark `StructType` in `spark-jobs/app/schemas/telemetry.py`; there is no schema registry.

## 8. Recommended cut list

Hide Overview/Risk until fake charts are removed; Detection Engineering until it reports real health and loaded rules; and Hunting, Entities, Timeline, MITRE, and Monitoring until API/persistence works. Rename Threat Intelligence “Local demo enrichment” because its feeds, ASN labels, countries, and mappings are hardcoded (`ioc_enricher.py`, `mitre_mapper.py`).

## 9. Top five interview risks

1. **Clean start is broken:** base Compose has no schema/bootstrap service, so required auth, receipt, outbox, and pipeline tables are absent.
2. **Spark downloads a mandatory connector at startup:** an offline/DNS-limited demo loops forever and the pipeline stops at Kafka.
3. **The UI overclaims:** plausible fallback numbers and a hardcoded “Active” state appear independently of real pipeline health.
4. **The ML story is synthetic:** training and evaluation use the same generated distribution, producing a misleading perfect legacy report with no independent validation.
5. **Tests create false confidence:** 40 infrastructure tests skip by default, Spark tests are outside root `pytest`, and the normal test command depends on an unavailable Compose plugin.

**Honest summary:** Kafka, the synthetic producer, local threat enrichment, several metrics endpoints, frontend compilation, and the unit/contract tests genuinely work. A clean full stack does not: migrations are absent, Spark cannot load its connector, the sampled event never reaches ML or alerts, PostgreSQL lacks core tables, the API cannot start, and the authenticated dashboard cannot render. Fix clean database bootstrap first, bake the Spark connector into the image second, add one non-skipped end-to-end lineage test third, remove UI fallbacks/false health states fourth, and only then add a real ingestion source and independently evaluated model.
