# Phase 0 baseline engineering audit

Audit date: 17 October 2026 (Asia/Kolkata). This report distinguishes observed runtime behavior, confirmed source defects, unverified operational risks, and optional improvements. PASS means only the particular check described passed. File existence, a listening port, an HTTP 200, and a metrics endpoint do not establish pipeline correctness.

## Verified follow-up — 17 October 2026

The [Phase 1 Spark–ML contract report](PHASE_1_SPARK_ML_CONTRACT.md) records the
versioned feature-contract correction and directly related test isolation/dependency fixes
on `dev`: 37 Python tests and 5 actual Spark tests passed, with no failures or skips.
Controlled live events produced four semantically verified windows/predictions, persisted
successfully and were visible through the API. This supersedes the specific contract and
test-failure status below; all original audit evidence is preserved, and unrelated production
readiness/security/reliability findings remain open.

## A. Repository overview

Root inspected: `/Users/jay2522/Downloads/Real-Time-Analytics-System-main`.

Before permitted source edits, 338 files were inventoried (excluding dependency environments and build directories; this inventory included two TypeScript build-info files created by the frontend check). All 188 Python source modules were parsed; 160 non-Spark application modules were subsequently imported in isolated processes. Source review covered service entry points, Kafka topic contracts, transformations, feature mapping, inference/training/artifact loading, detection rules, storage writers, API queries/WebSockets, frontend routes/services/hooks, Dockerfiles, Compose, monitoring configuration, scripts, READMEs, phase documents, and the production checklist. Screenshots are illustrative historical assets, not verification evidence. Empty `infrastructure/` and `datasets/` directories contain only `.gitkeep`.

**Original folder has no Git metadata:** initial `git status --short --branch` and `git remote -v` returned `fatal: not a git repository (or any of the parent directories): .git`. The user subsequently supplied `https://github.com/Jay-2522/Real-Time-Analytics-System.git`. `git ls-remote --symref ... HEAD` advertised existing branch `main` at `86bdcbcb2aff9a627fa377467c414c57fbeb4e77`. That branch was cloned to `/Users/jay2522/.local/share/streaming-analytics/repository-phase0`, with remote `origin`. SHA-256 comparison found **zero differences** between the downloaded original source files and that checkout (excluding generated build-info/bytecode). Only the intentional setup/report changes were copied into the checkout. No new working branch was requested or created; no existing history was replaced.

| Component | Actual responsibility and entry point | Dependencies |
| --- | --- | --- |
| Backend | `backend/app/main.py`, `uvicorn app.main:app`; REST queries, incident/alert status mutations, three polling WebSocket streams, `/metrics`; `create_all` on startup | FastAPI, Pydantic 2.10, SQLAlchemy, psycopg, PostgreSQL; Elasticsearch and Redis for search/health; Prometheus query API |
| Frontend | `frontend/src/main.tsx` → `App.tsx`; 15 React routes for overview/live/risk/predictions/search/health/alerts/incidents/observability/threats/rules/hunting/MITRE/entities/timeline | React 18, TS 5.6, Vite 6, Axios, Recharts, D3, React Flow, Framer Motion |
| Telemetry producer | `kafka-producers/app/main.py producer`; synthetic Pydantic login/API/network/anomaly events, controlled rate, delivery callbacks | confluent-kafka 2.6.1, Kafka |
| Spark | `spark-jobs/Dockerfile` invokes Spark 3.5.3 `spark-submit`, Kafka connector `_2.12:3.5.3`, `app/main.py`; four independent streaming Kafka output queries | Spark master/worker/driver, container Python/Java, Maven connector download, checkpoint volume |
| ML | `ml-models/app/main.py inference` or `train`; mapper → scaler → Isolation Forest; publishes predictions; optional automatic synthetic model bootstrap | scikit-learn 1.5.1, numpy 1.26.4, pandas 2.2.2, joblib 1.4.2, Pydantic 2.8, Kafka, mounted model artifacts |
| Storage | `storage-sink/app/main.py`; raw telemetry, analytics, features, risks, predictions → PostgreSQL, then Elasticsearch and Redis | Kafka, PostgreSQL JSONB/upserts, Elasticsearch, Redis |
| Alert engine | `alert-engine/app/main.py`; rules, Redis cooldown, incident correlation, PostgreSQL and Elasticsearch writes | prediction/enriched/risk Kafka topics, PostgreSQL, Redis, Elasticsearch |
| Threat intelligence | `threat-intelligence-engine/app/main.py`; static IOC lists, seven YAML keyword/risk/country rules, heuristic MITRE mapping, entity/timeline persistence | five Kafka topics, PostgreSQL, Redis, Elasticsearch, PyYAML |
| Infrastructure | Compose services include one-shot topic initialization, ZooKeeper, Kafka, three Spark services, data stores, applications, Prometheus and optional Grafana | Local container engine; named persistent volumes |

### Actual data flow

1. Producer publishes four `telemetry.*.events` topics. Storage-sink persists valid records and publishes malformed records to the DLQ.
2. Spark independently reads all four topics, normalizes and filters them, and publishes `analytics.enriched.events`, `analytics.window.metrics`, `analytics.risk.metrics`, and `analytics.feature.engineering`. Window outputs use append mode and event-time watermarks (default two minutes; five-minute metric windows), so these can legitimately lag enriched events.
3. ML consumes **all four** analytics topics, maps each incoming record separately to 15 default-filled features, and publishes `ml.anomaly.predictions`. It does not assemble a full entity/window feature vector across records.
4. Alert engine consumes predictions, enriched events, and risk metrics; it is not a Kafka alert-output producer. Alerts/incidents go directly to databases/cache.
5. Threat engine consumes predictions, enriched events, risk metrics, raw login and raw network events. It does **not** consume alert-engine output, raw API events, or raw anomaly events directly. The architecture diagram's alert→threat dependency is a startup ordering edge, not a verified message channel.
6. Storage consumes nine raw/analytics/prediction topics. Its topic tuple omits the DLQ despite having a DLQ mapper/model.
7. Backend reads PostgreSQL and Elasticsearch; REST/WebSockets deliver snapshots to the frontend. WebSockets perform database queries every three seconds, rather than forwarding Kafka messages.
8. Prometheus scrapes eight configured jobs (itself plus seven application endpoints). Grafana provisions five dashboards. `architecture/system-context.md` still describes only a Phase 1 shell, which is stale relative to the service source; the production checklist correctly names several missing controls. No Kafka/Spark/JVM exporters or Prometheus alert rules/Alertmanager are supplied.

## B. Environment setup

### Host observations

macOS 26.7.1, Darwin 25.6.0, arm64; 10 logical CPUs; 16 GiB physical RAM; initially 274 GiB available on the data volume. Existing host PostgreSQL listens on localhost:5432; an unrelated Node process listens on port 3000. Neither was stopped or reconfigured. Node 22.23.1/npm 10.9.8 and Git 2.55.0 were already available. Default Python is 3.14.7/pip 26.2.1; that interpreter was not used to install the project's old binary package pins. No host Java runtime was found; the selected Spark execution method supplies Java inside its image.

### Installed tools and isolation

| Item | Version | Purpose/location |
| --- | --- | --- |
| Docker CLI | 29.9.1 | Homebrew; client for dedicated development VM |
| Docker Engine | 29.5.2 | Inside Colima VM, not a host database/service |
| Docker Compose | 5.6.0 | Homebrew CLI plugin |
| Docker Buildx | 0.38.0 | Homebrew image builds/manifest inspection |
| Colima | 0.10.3 | Dedicated `phase0-analytics` VM; 4 CPUs, 8 GiB, 60 GiB sparse disk, Apple virtualization, Docker runtime |
| Lima | 2.2.1 | Colima virtualization dependency |
| Python / venv pip | 3.11.17 / 26.2.1 | Homebrew; service-specific virtual environments matching container Python major/minor |
| Existing bundled Python / venv pip | 3.12.14 / 25.0.1 | `.venv` for backend checks/host API; no modification to bundled interpreter |
| pip-audit | 2.10.1 | Separate `.codex-logs/audit-venv`; dependency audit tooling |
| Frontend packages | 362 installed packages; 363 audited | `npm ci` using original lockfile; no audit-fix/lockfile update |

Homebrew command: `HOMEBREW_NO_AUTO_UPDATE=1 brew install colima docker docker-compose docker-buildx python@3.11`. Homebrew also installed/selected formula dependencies OpenSSL 3.5.9 (previously linked 3.6.3), readline 8.3.6 (previously 8.3.3), xz 5.8.4 (previously 5.8.3), and SQLite 3.54.0 (previously 3.53.4). This dependency operation is disclosed because it changes Homebrew-managed dependencies; no `sudo`, shell profile edits, system Java, duplicate host database, or `brew services` changes were used.

Python environments are deliberately separate: backend pins Pydantic 2.10.4/settings 2.7.1, whereas ML pins 2.8.2/2.4.0. Installing all service requirements sequentially into one environment, as suggested in troubleshooting documentation, cannot preserve all exact pins.

Actual commands used:

```bash
# Backend: bundled Python 3.12.14 creates the isolated environment.
/Users/jay2522/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m venv .venv
.venv/bin/python -m pip install -r backend/requirements.txt httpx==0.28.1 PyYAML==6.0.2
/opt/homebrew/bin/python3.11 -m venv ml-models/.venv
ml-models/.venv/bin/python -m pip install -r ml-models/requirements.txt
/opt/homebrew/bin/python3.11 -m venv alert-engine/.venv
alert-engine/.venv/bin/python -m pip install -r alert-engine/requirements.txt
/opt/homebrew/bin/python3.11 -m venv threat-intelligence-engine/.venv
threat-intelligence-engine/.venv/bin/python -m pip install -r threat-intelligence-engine/requirements.txt
(cd frontend && npm ci)
```

`backend/requirements-dev.txt` now declares the missing TestClient dependency `httpx==0.28.1` and includes runtime requirements. PyYAML was additionally installed in the backend inspection environment for configuration/rule inspection; it is not a backend runtime requirement. Full installed Python inventories appear in Appendix 1; npm's unchanged lockfile records exact frontend resolutions. Installation/audit command logs remain local under ignored `.codex-logs/`.

### Local configuration and startup

`python3 scripts/setup_local.py` creates ignored `.env` with mode 0600, independent randomly generated PostgreSQL/Grafana passwords, and conflict-aware published ports. Existing `.env` content is preserved. On this machine PostgreSQL publishes at **127.0.0.1:15432**, Grafana at **127.0.0.1:13000**. All 20 published mappings bind to 127.0.0.1. Internal PostgreSQL/Grafana ports remain 5432/3000. PostgreSQL user/database remain `streaming_user`/`streaming_analytics`. Secrets are not recorded here.

Compose previously ignored `.env` for application env files and explicitly hardcoded credentials. It now consumes `.env`, requires nonempty generated passwords, and interpolates PostgreSQL credentials consistently. Root `.env.example` is a **container-network template**, not host Python configuration. Host processes need localhost overrides. Internal backend Grafana/Prometheus URLs remain container addresses; browser-facing monitoring links still require correction (finding G08).

The dedicated Docker connection avoids changing an existing Docker context or global plugin configuration:

```bash
# Already installed and started during this audit:
colima start --profile phase0-analytics --cpu 4 --memory 8 --disk 60 --vm-type vz --runtime docker --activate=false
# This directory's ignored .docker/config.json only declares Homebrew plugin paths.
export DOCKER_CONFIG="$PWD/.docker"
export DOCKER_HOST="unix://$HOME/.colima/phase0-analytics/docker.sock"
python3 scripts/setup_local.py
docker compose config --quiet
docker compose pull --ignore-buildable
docker compose build
docker compose up -d
python3 scripts/health_check.py --json
```

Startup Bash/PowerShell scripts now invoke the setup helper and resolve the repository root. Bash startup files have owner execute permission. The frontend image uses `npm ci`, and `.dockerignore` files exclude host environments, credentials, caches, and host frontend dependencies from build contexts. This prevents macOS `node_modules` from replacing Linux dependencies during `COPY . .`.

macOS privacy controls prevent Colima from mounting this checkout under Downloads. A real mount probe failed with `stat .../monitoring: operation not permitted`. `python3 scripts/stage_local_mounts.py` copied only backend source, frontend source (excluding dependencies, build output and secrets), supplied ML artifacts and monitoring files to `/Users/jay2522/.local/share/streaming-analytics/dev-a6oozywt`; **113 files were SHA-256 verified**. A second Docker bind probe successfully read the staged Prometheus file. Compose supports this through `DEV_BIND_ROOT` in private `.env`. No macOS privacy setting was changed. Application edits require restaging and container recreation, or using the recovered Git checkout in its VM-accessible directory. Private `.env` and the local Docker plugin-path configuration were copied to the recovered checkout, kept ignored; credentials were not committed. Old snapshots are preserved; this helper never deletes them.

No database, volume, model artifact, or user data was removed. Generated credentials only initialize **new** PostgreSQL/Grafana volumes; an existing volume's credentials must be reconciled administratively, never reset by deleting its volume.

Values still requiring user input: existing-volume credentials if reusing preexisting data; production identity provider, allowed origins/domain/TLS, secret store, routing destinations, retention/SLOs and backup policy before any pilot. No paid feed/API credentials are needed for the current static IOC implementation.

## C. Service verification

Runtime results are recorded below after container startup attempts. The host-only API uses an isolated SQLite database under `.codex-logs/`; its success does not validate PostgreSQL writers.

Final full startup used `docker compose pull --ignore-buildable`, `docker compose build`, and `docker compose up -d` with the dedicated engine and staged bind roots. All nine application build targets succeeded; all required upstream images were pulled. Earlier `compose up --build -d` failed while the VM socket was not yet available; the retry after engine setup succeeded. No failed attempt is treated as a successful run.

| Compose service | Status | Functional evidence / limits |
| --- | --- | --- |
| zookeeper | PASS | Container healthcheck healthy; broker connects and serves metadata |
| kafka | PASS | Healthy; all 10 expected topics listed; controlled publish acknowledged at partition 0/offset 3 |
| kafka-init | PASS | Completed with exit 0, all ten topics present; intentionally exited one-shot service |
| postgres | PASS | Healthy; application writes and direct SQL queries returned controlled telemetry, prediction, alerts and incident links; published host port 15432 |
| redis | PASS | `redis-cli ping` → PONG; application cooldown/cache initialized; application metrics/persistence active |
| elasticsearch | PASS (local single-node) | Healthy container; writes and exact known-entity API/browser search succeed. Cluster is yellow with unassigned replicas on one node; no HA claim |
| backend | PASS | Container API returns persisted controlled prediction/search/entity data, dashboard totals and WebSocket snapshots; known health/security defects remain |
| frontend | PASS | Container-served React app renders; browser search displayed three controlled records including actual prediction risk 61.1336 |
| telemetry-producer | PASS | Sustained publish/delivery logs; telemetry SQL counts increase; controlled schema generation separately verified |
| spark-master | PASS | `/json` HTTP 200, ALIVE, registered worker/application |
| spark-worker | PASS | Worker registered with master; executor RUNNING, 2 cores/1 GiB executor allocation; `/json` HTTP 200 |
| spark-streaming | PASS for live processing | All four query names report progressing input batches; exact controlled enriched record read from Kafka. All four output paths emitted and were persisted after watermark closure (section D) |
| ml-inference | PASS | Supplied artifact loaded, source-linked prediction read from Kafka and PostgreSQL, ML metrics scraped |
| storage-sink | PASS | Controlled raw/enriched/prediction writes queried in PG and search; local successful path only, recovery defects remain |
| alert-engine | PASS | Controlled event produced persisted alerts, two incident IDs and two incident links; dedup/correlation defects remain |
| threat-intelligence-engine | PASS | Controlled entity has two enrichment records and persisted YAML hits; static heuristic accuracy limitations remain |
| prometheus | PASS | `/-/ready` 200; all 8 configured scrape jobs UP: itself, backend, producer, consumer, ML, storage, alert, threat |
| grafana | PASS | `/api/health` 200, internal DB ok; authenticated API verified five provisioned dashboards (plus one folder); host port 13000 |

`python scripts/health_check.py --json` finally returned **12 OK, 0 failed**, versus the earlier host-only 2/10 result. This smoke checker still does not establish delivery guarantees, temporal rule accuracy, or recovery. Full platform status is separately supported by logs, database queries and actual requests.

Spark supplies OpenJDK **11.0.24 / Temurin 11.0.24+8**; no host JVM was installed. Colima logged denied Downloads mounts during provisioning; the application bind problem was reproduced and then resolved by the verified source-staging helper, not by weakening macOS permissions. Host-only API/frontend processes started for diagnosis were stopped before the container equivalents claimed their ports. The unrelated PostgreSQL and Node processes were left running. The controlled prediction was also retrieved from Redis key `latest:prediction:phase0-6a327fcf` with the same prediction ID and risk 61.1336.


## D. Functional verification

### Observed component evidence

The repository's `SyntheticTelemetryGenerator(anomaly_rate=1)` with random seed 42 generated and schema-validated event `17c8bb8d-5545-4868-a73c-d9da34d11672`: `event_type=anomaly`, `anomaly_type=impossible_travel`, `risk_score=91.73`, `user_id=u-1001`. Saved locally as `.codex-logs/controlled-event.json`; no generated dataset is intended for Git.

The supplied model/scaler/metadata loaded without retraining. Direct mapper/inference produced prediction `5af3970b-d072-4ac1-8690-f2fab12f2bfc` linked to that source event: anomaly score **0.611336**, `is_anomaly=true`, ML risk **61.1336**, severity **medium**, confidence **0.222672**. These are actual single-event outputs, not performance estimates. Source and output are in local `ml-inference-probe.log` and `controlled-prediction.json`.

Direct alert rule evaluation of that prediction produced one medium alert for `u-1001`; direct evaluation of the raw event produced one critical candidate using source-IP identity and the source risk of 91.73. The latter topic is not an alert-consumer input, so this raw-event call is **only a rule-function check**. Neither call proves a persisted alert/incident. The identity difference demonstrates inconsistent correlation keys across source types.

The threat rule engine loaded all seven repository rules and matched `impossible_travel`/T1078 on the controlled event. IOC enrichment returned no IOC for this event, country `None`, and `AS-LOCAL Synthetic Telemetry`. This direct call does not establish that the threat consumer received raw anomaly telemetry (it does not subscribe to that topic).

Mapper defect probe: `FeatureMapper().to_feature_vector({'feature_name':'user_failed_login_rate','feature_value':1.0,'event_count':10})['failed_login_rate']` returned **0.0**, confirming the feature-name mismatch.

### Host API and UI evidence

The host API was started with `PYTHONPATH=backend`, `DATABASE_URL=sqlite+pysqlite:///<root>/.codex-logs/host-baseline.db`, localhost Redis/Elasticsearch/Prometheus overrides, and `uvicorn app.main:app --host 127.0.0.1 --port 8000`. Frontend: `npm run dev -- --host 127.0.0.1 --port 5173 --strictPort`.

Actual host API observations (`.codex-logs/host-api-evidence.json`):

- `GET /api/v1/health` → 200, `status=healthy` while the streaming stack was unavailable.
- `GET /api/v1/dashboard/overview` → 200, all counts zero, `system_status=waiting_for_streams`.
- `/events/recent`, `/predictions/recent`, `/alerts/recent` → 200, `[]`.
- `/search/events?q=phase0` → 200, `[]` with Elasticsearch unavailable; this is a masked dependency failure, **not** successful historical search.
- `/threat-intel/overview` → 200 with zero enriched events/rule hits/IOCs.
- `/monitoring/services` falsely reports Grafana healthy without a request, and labels the successful SQLite connection PostgreSQL healthy. Kafka/Spark are merely `observing`.

Browser verification at `http://localhost:5173` rendered the overview and showed `Live channel: connected`, zero total events/predictions and the no-records message. The same page displayed populated fallback trend axes/curves despite an empty database. The localhost hostname matches configured CORS; 127.0.0.1 is not an allowed browser origin by default. No frontend detection result was inferred from these fallback charts.

A real WebSocket client connected to `/api/v1/ws/dashboard` with `Origin: https://untrusted.invalid` and received a snapshot (`total_events=0`). The server accepted it without authentication or Origin validation. Evidence is in ignored `websocket-probe.json`.

### Live end-to-end proof

A fresh event was generated with the repository generator and schema-validated, with a unique phase0 user marker. Source ID **6a327fcf-c6b6-4b86-bb64-829bfadf21a3**, entity **phase0-6a327fcf**, source risk **91.73**, type **impossible_travel**. This is separate from the earlier direct component probe. Event creation time was 17 October 2026, 04:03:20 IST (2026-10-16T22:33:20.306121Z). Test records remain in this dedicated development database; none are source-controlled.

| Step | Status | Actual evidence |
| --- | --- | --- |
| 1. Generate controlled event | PASS | Repository `SyntheticTelemetryGenerator(1)` with seeded random values, unique user marker, then Pydantic `AnomalyEvent`/`parse_telemetry_event` validation |
| 2. Publish to Kafka | PASS | Producer `acks=all`, idempotence enabled; delivery callback `error=null`, `telemetry.anomaly.events`, partition **0**, offset **3**, flush remaining **0** |
| 3. Stream with Spark | PASS | Independent Kafka consumer read the same ID from `analytics.enriched.events`, partition **1**, offset **87**, retaining source partition 0/offset 3; processed at 22:33:36.930Z |
| 4. Calculate metrics/features | PASS for per-event path | Spark normalized risk **0.9173**, risk level **critical**; actual prediction contains the computed 15-feature vector. Window aggregates are independently checked below |
| 5. Infer/evaluate rules | PASS execution; accuracy not certified | Kafka `ml.anomaly.predictions`, partition **0**, offset **78**: prediction **f3a51fec-36a3-45ec-baa2-7f84e18e2ad0**, same source ID, anomaly=true, score **0.611336**, ML risk **61.1336**, severity **medium** |
| 6. Produce alerts/incidents | PASS execution; correlation defect confirmed | Critical alert **19a8168b-47dd-4e71-bb28-2ae777870604** for source IP; medium alert **f92d6e6c-6702-488f-b907-acb081e3153b** for phase0 entity. Incident IDs are `inc-<alert_id>`, i.e. two incidents for the same original event |
| 7. Persist | PASS | SQL returned raw=**1**, enriched=**1**, the source-linked prediction above, **2** incident links, **2** threat enrichment records for the entity and persisted rule hits |
| 8. API exposure | PASS | `GET /api/v1/predictions/phase0-6a327fcf` → 200 with risk 61.1336; `/entities/phase0-6a327fcf` → 200; dashboard overview returned persisted totals |
| 9. Frontend display | PASS | Browser Search page submitted `phase0-6a327fcf` and visibly rendered **3 results**, including the real prediction ID/source ID/risk 61.1336/medium severity plus original and enriched records |
| 10. Historical search | PASS for known records | `/api/v1/search/events?q=phase0-6a327fcf` → 200/**3** results; `/alerts/search?q=phase0-6a327fcf` → 200/**2** results; browser showed the same event/prediction chain |

Representative direct PostgreSQL queries used:

```sql
SELECT count(*) FROM telemetry_events WHERE event_id='6a327fcf-c6b6-4b86-bb64-829bfadf21a3';
SELECT count(*) FROM analytics_metrics WHERE event_id='6a327fcf-c6b6-4b86-bb64-829bfadf21a3';
SELECT prediction_id,entity_id,ml_risk_score,severity FROM anomaly_predictions
 WHERE raw_payload->>'source_event_id'='6a327fcf-c6b6-4b86-bb64-829bfadf21a3';
SELECT alert_id,entity_id,severity,incident_id FROM alerts
 WHERE source_event_id='6a327fcf-c6b6-4b86-bb64-829bfadf21a3';
SELECT conname,contype FROM pg_constraint WHERE conrelid='incident_alert_links'::regclass;
```

The last query returned only `incident_alert_links_pkey|p` and `uq_incident_alert_link|u`, confirming absent foreign keys in the actual PostgreSQL schema. Persisted threat-rule hits included `impossible_travel`, `suspicious_process_chain` on the enriched original and `brute_force_detection` on the prediction, exposing field-name substring false positives. The test proves traversal and persistence, not that the model/rules are valid security detections.

Local reproducible evidence files: `e2e-publish.log`, `e2e-kafka-results.json`, `e2e-postgres.log` (including exact SQL), `e2e-api-evidence.json`, `platform-probes.json`, `health-docker.json`, and Compose build/start/service/query logs under `.codex-logs/`. Kafka's host client briefly tried IPv6 localhost and logged connection refused; IPv4 retry succeeded and both output records were actually received. The broker is intentionally published only at 127.0.0.1.

### Window and streaming-query verification

All four output paths were observed. After allowing default event-time windows and the two-minute watermark to close, direct SQL returned **4 window metric rows**, **1,171 risk metric rows**, and **925 feature snapshots**. The first observed window result was for anomaly events over `22:30:00Z–22:35:00Z`: event_count **23**, high-risk count **23**, anomaly_frequency **23**, mean risk **85.8704**, calculated_at **22:37:23.142Z**. This demonstrates actual aggregation emission and persistence, not just running queries. Initial zero aggregate counts before watermark closure were expected timing, not passed checks.

Spark logs independently reported progressing batches for `analytics_enriched_events`, `analytics_window_metrics`, `analytics_risk_metrics`, and `analytics_feature_engineering`. Occasional 15-second trigger overruns were observed (up to approximately 30 seconds in sampled logs), so these checks do not establish steady-state latency or capacity. No checkpoint deletion, fabricated result insertion, threshold alteration, retraining or replacement processing path was used to make the pipeline pass.


## E. Testing results

Commands run from the root unless otherwise stated. Exact stdout/stderr is retained under `.codex-logs/`; quiet pytest is configured twice (`pytest.ini` and command flags), so counts below are derived from actual PASSED/FAILED test names, not inferred from dot counts alone.

| Command/check | Actual result | Scope/limitation |
| --- | --- | --- |
| `python3 -m compileall -q backend/app ml-models/app alert-engine/app storage-sink/app kafka-producers/app spark-jobs/app threat-intelligence-engine/app scripts` | PASS | All Python source compiles; no runtime dependency proof |
| Isolated module imports (`.codex-logs/import_probe.py <service>`) | PASS: backend 40, ML 25, alert 29, threat 25, producer 17, storage 24; 160 total | Backend uses its venv; ML/alert/threat use theirs; producer uses threat venv with matching pins; storage uses alert venv with matching pins; Spark runtime imports require container |
| `.venv/bin/python -m pytest -q -rA` | **7 passed, 4 failed** | Baseline root environment lacks sklearn for the artifact test; other failures are metric registration collisions; not a supported all-pins unified environment |
| `.venv/bin/python -m pytest backend/tests -q -rA` | **1 passed, 1 failed** | Second test fails during app reimport: duplicate `http_requests*` CollectorRegistry timeseries |
| `ml-models/.venv/bin/python -m pytest ml-models/tests -q -rA` | **3 passed, 0 failed** | Feature mapper, prediction serialization, missing-artifact contract only |
| `alert-engine/.venv/bin/python -m pytest alert-engine/tests -q -rA` | **2 passed, 1 failed** | Escalation test fails on duplicate `alerts_generated*` metrics during module reload |
| `threat-intelligence-engine/.venv/bin/python -m pytest threat-intelligence-engine/tests -q -rA` | **2 passed, 1 failed** | YAML execution test fails on duplicate `ioc_enrichments*` metrics |
| Each of the three registration-collision tests in a fresh individual process | PASS (one test in each process) | Diagnostic isolation only; original suite failures remain unresolved |
| `python -m pip check` in backend/ML/alert/threat venvs | PASS, no broken requirements | Does not establish vulnerability-free dependencies |
| `(cd frontend && ./node_modules/.bin/tsc -b --pretty false)` | PASS | TypeScript project build check |
| `(cd frontend && npm run lint)` | PASS | Existing ESLint config |
| `(cd frontend && npm run build)` | PASS | Vite 6.4.2, 3,074 transformed modules; JS chunk 982.87 kB / gzip 302.35 kB; large-chunk warning, no performance claim |
| `docker compose config --quiet` with dedicated Docker config | PASS | Resolves all 20 services; generated credentials not printed |
| `bash -n scripts/*.sh` | PASS | Syntax only; PowerShell unavailable on this Mac, scripts not executed |
| Setup helper probe in a temporary directory | PASS | Distinct generated credentials, 0600 file mode, second invocation preserves existing bytes; occupied host ports resolved to 15432/13000 |
| `DOCKER_CONFIG="$PWD/.docker" .venv/bin/python scripts/verify_phase_10.py` in host-only mode | Exit 0: 22 OK, 0 warnings | Mostly file/config checks plus three host HTTP endpoints; it does **not** verify full platform operation |
| `scripts/health_check.py --json --timeout 0.5` in host-only mode | **FAIL: 2 OK, 10 failed** | Backend/frontend only; correctly avoids unrelated host PostgreSQL after port fix |
| `DOCKER_CONFIG="$PWD/.docker" DOCKER_HOST="unix://$HOME/.colima/phase0-analytics/docker.sock" ./scripts/verify_all.sh` | PASS, exit 0 | Existing wrapper compiled source, built frontend, validated Compose and returned 12 healthy probes; does not run pytest |
| Integration/load/recovery/end-to-end automated test suites | **UNVERIFIED: none supplied** | No Spark/storage/producer test suites; the actual controlled live end-to-end probe in section D passed, but is not an existing automated regression suite |

No test implementation or application algorithms were altered to hide existing failures. Existing deprecation warnings include FastAPI `on_event`, Pydantic `json_encoders`, and the Starlette/AnyIO portal alias.

### Dependency security audits

`npm audit --json` reported **20 affected packages**: **14 high, 5 moderate, 1 low, 0 critical**. Direct dependencies implicated include Axios, Vite, React Router DOM, PostCSS, and Tailwind (some severity is inherited through transitive paths). Counts describe the audit database response; browser/Node/build-time reachability differs by advisory and is not established as an exploit against this app. No `npm audit fix --force` or major upgrades were performed.

`pip-audit 2.10.1 --path <venv-site-packages> --format json --output <local-file>` results:

| Environment | Raw advisory rows reported | Unique package/advisory pairs | Affected installed packages |
| --- | ---: | ---: | --- |
| Backend | 30 | 15 | pip 25.0.1 (6 unique IDs), pytest 8.3.4 (1), python-dotenv 1.0.1 (1), Starlette 0.41.3 (7) |
| ML | 2 | 1 | pytest 8.3.4 |
| Alert | 4 | 2 | pytest 8.3.4, python-dotenv 1.0.1 |
| Threat | 2 | 1 | pytest 8.3.4 |

The provider returned repeated advisory IDs, so raw rows are explicitly **not** counted as distinct vulnerabilities. Backend Starlette IDs returned include PYSEC-2026-1941/1942/161/2280/2281/248/249. These results warrant a controlled compatibility/security upgrade, then repeat tests and an independent advisory check. They are not a verified exploit and not an OS/container SBOM scan. Image operating-system/library vulnerabilities, Kafka/Spark/Elasticsearch/Grafana images and JVM/Maven dependencies remain **UNVERIFIED** until an image scanner/SBOM workflow is added. Fresh transitive resolutions and audit databases are time-dependent; recorded versions and lockfiles define this baseline.

## F. Security findings

Severity describes potential production impact; this local synthetic-data audit is not evidence of a compromised host. Priorities: P0 before any real-data/network-access pilot; P1 before a reliable demo/pilot; P2 before production; P3 optional improvement. References are repository paths with verified source line locations.

| ID / severity / priority / evidence class | Affected location | Evidence, impact, recommended solution |
| --- | --- | --- |
| S01 **Critical / P0 / confirmed missing control** | `backend/app/api/v1/endpoints/alerts.py:61,69`; `backend/app/main.py:44` | Read/search and incident/alert status mutation routes have only DB dependencies, no identity or authorization policy. Unauthenticated callers could read telemetry/change investigations if exposed. Add authenticated identities, role/object authorization and actor-attributed audit trails before real data. Security headers/CORS do not provide authentication. |
| S02 **High / P0 / confirmed runtime defect** | `backend/app/api/v1/endpoints/dashboard.py:55`, `alerts.py:79`, `threat_intel.py:104` | All sockets accept before any identity/Origin check; an actual client with untrusted Origin received data. HTTP middleware/CORS does not secure these WebSockets. Validate Origin and authenticate/authorize upgrades; bound connections, messages, timeouts and polling cost. |
| S03 **High / P0 / confirmed original configuration; local exposure partially remediated** | `docker-compose.yml:15,32,44,114,132,455` (ports, Kafka listener, Redis, Elasticsearch, Grafana); original snapshot in local logs | Original ports bound all interfaces, PostgreSQL used `streaming_password`, Grafana `admin/admin`, ES security disabled, Redis unauthenticated, Kafka PLAINTEXT. This phase restricts published ports to loopback and generates PG/Grafana passwords. Plaintext/unsecured internal protocols and service settings remain unsuitable for production. Use TLS, service credentials/ACLs and explicit network policies/secret injection; do not publish administrative services. |
| S04 **High / P1 / confirmed audit findings; exploitability unverified** | `frontend/package-lock.json`; `backend/requirements.txt:1`; resolved Starlette/python-dotenv; service pytest pins | Audits returned the package/advisory counts above. Runtime web libraries, development/build dependencies and test tooling need separate reachability review. Upgrade to compatible fixed versions in a dedicated change with lockfile review and regression checks; add CI audits. |
| S05 **Medium / P2 / confirmed design gap** | `ml-models/app/models/artifact_store.py:38`; `ml-models/app/main.py:24`; model bind mount in Compose | `joblib.load` deserializes supplied writable artifacts without signatures/checksums/registry provenance; automatic bootstrap can silently create a new model on missing files. Untrusted replacement artifacts could execute Python code. Restrict write access, verify approved artifacts, version runtime/feature contract, mount immutable production models; disable implicit production training. No malicious artifact was observed. |
| S06 **Medium / P2 / confirmed source risk** | `backend/app/middleware/security.py:23,30`; `backend/app/core/metrics.py:22` | Rate-limit client keys are retained indefinitely; fallback metric labels include arbitrary unmatched URL paths. Unbounded key/time-series cardinality and unauthenticated expensive API/socket polling can consume memory/DB capacity. Expire inactive clients, use bounded/shared rate limiting, normalized route labels, query/connection budgets and load tests. No denial-of-service load test was run. |
| S07 **Medium / P2 / confirmed build/deployment gap** | All application Dockerfiles; `spark-jobs/Dockerfile:3`; Compose env files | Python images generally run as root, Spark explicitly selects root, frontend runs a Vite development server, image tags are mutable, and common `.env` injects unrelated PG/Grafana secrets into application containers. Added ignore files and `npm ci` improve local builds only. Use least-privilege users, minimal per-service secret access, immutable image digests, production frontend serving, dependency/SBOM scans and hardened runtime settings. |

## G. Architecture and reliability findings

| ID / severity / priority / evidence class | Affected location | Evidence, impact, recommended solution |
| --- | --- | --- |
| G01 **High / P1 / confirmed feature-contract defect** | `spark-jobs/app/transformations/normalization.py:41`; `feature_engineering/features.py:31`; `ml-models/app/features/feature_mapper.py:35,55` | Enriched projection omits `login_success`; ML defaults it to true. Spark emits `user_failed_login_rate`, absent from ML `FEATURE_COLUMNS`, so the measured failed-login rate maps to 0.0 (direct probe). Rename/map features consistently, preserve required raw fields and add a real Spark→ML contract test. |
| G02 **High / P1 / confirmed feature-semantics defect** | `spark-jobs/app/feature_engineering/features.py:75`; `ml-models/app/features/feature_mapper.py:40,42`; `ml-models/app/consumers/inference_consumer.py:97` | `anomaly_rate` is mean 0–100 risk, not a rate; failed-auth count uses all events, not failed events; endpoint errors >=400 are copied into `api_5xx_rate`. Each one-feature/window/event record is scored as a separate mostly-default vector, unlike the joint training vectors. Scores can be materially misleading. Define feature units/entity/time windows and assemble consistent vectors; retrain/evaluate after contract corrections. |
| G03 **High / P1 / confirmed delivery-safety gap; loss scenarios not fault-injected** | `ml-models/app/consumers/inference_consumer.py:66,151`; `alert-engine/app/consumers/alert_consumer.py:51,68`; `threat-intelligence-engine/app/consumers/threat_consumer.py:73,103`; `storage-sink/app/consumers/storage_consumer.py:63` | Consumers enable auto commit without coordinating acknowledgments with durable outputs. Alert/threat handlers log and swallow failures; ML delivery callback only logs failure. A later poll/commit can advance failed records; crashes/retries can also duplicate outputs. Commit per partition only after durable idempotent effects, make output acknowledgments explicit, and add retry/DLQ/replay/fault tests. |
| G04 **High / P1 / confirmed replay/idempotency gap** | `storage-sink/app/repositories/postgres_repository.py:30`; `storage-sink/app/db/models.py:39,53,67`; `threat-intelligence-engine/app/repositories/threat_repository.py:13`; `app/schemas/threat.py` | Analytics/risk/feature records have no logical source/window unique key, so `ON CONFLICT DO NOTHING` on generated PKs does not deduplicate replay. Enrichment/alert identifiers are newly generated and `merge` is by PK, not a stable source identity. Kafka/Spark redelivery can inflate events/alerts/IOCs/timelines. Introduce stable identities, source offset/window uniqueness and idempotent writes; verify replay counts. |
| G05 **High / P1 / confirmed startup-order-dependent schema** | `backend/app/db/session.py:12`; backend and writer `app/db/models.py`; `alert-engine/app/db/models.py:73` | Four services independently call `create_all` with divergent metadata. Backend uses JSON and no incident-link FKs; writers use JSONB and FKs. Metadata probe confirms backend FK count 0, alert writer count 2. The live PostgreSQL table also had only primary/unique constraints, confirming the missing foreign keys in this run. Whichever creates a table first controls integrity; `create_all` does not migrate an existing table. Add one schema owner/versioned migrations and migration-before-service startup; test both fresh and upgraded PostgreSQL schemas. No destructive schema changes were made. |
| G06 **High / P1 / confirmed secondary-store recovery defect** | `storage-sink/app/elastic/indexer.py:19,31`; `redis_cache/cache.py`; `threat-intelligence-engine/app/elastic/indexer.py:15`; Compose ES `service_started` gates | A failed one-time availability check leaves indexing/cache disabled until restart. Writes after primary DB commit can fail and be discarded, with no outbox/backfill; storage ES writes lack stable IDs. Search/cache can remain stale permanently even after recovery. Add reconnect/retry and an idempotent durable outbox/backfill workflow; gate startup with readiness where appropriate. |
| G07 **High / P1 / confirmed misleading UI behavior** | `frontend/src/services/dashboardService.ts:14,34,40,56,77`; `frontend/src/pages/DetectionEngineeringPage.tsx:22`; alert/monitoring/threat frontend services | Empty/error API results become fixed nonzero graphs/entities or empty arrays; rule pipeline health is hardcoded Active. Browser observed populated trends with zero rows. Operators can mistake invented activity or masked failure for evidence. Remove automatic demo fallbacks from operational views; show explicit empty/error/stale state and measured readiness. Existing fake data was documented, not used to certify success. |
| G08 **High / P1 / confirmed misleading observability** | `backend/app/services/health_service.py:11`; `monitoring_service.py:71,74,145`; `analytics_query_service.py:59`; `scripts/verify_phase_10.py:126` | `/health` always healthy; Grafana/frontend healthy values are constants; SQLite SELECT 1 is labeled PostgreSQL; historical rows imply active services, and Phase 10 success mostly checks files. Runtime probes confirmed these false positives. Separate liveness/readiness/freshness, interrogate actual dependencies, require consumer lag/recent progress, and test degraded scenarios. Monitoring URLs also mix container DNS names with browser links and retain hardcoded host port 3000. |
| G09 **High / P1 / confirmed search failure masking** | `backend/app/services/analytics_query_service.py:31,45`; `alert_service.py:54`; `threat_intel_service.py:114`; frontend service catches | Search catches all errors and returns `[]` with 200. Actual search returned empty while ES was absent. Outage, invalid query and no matches are indistinguishable. Return a typed unavailable/query-error response, expose UI error state and bound query complexity; verify a known event via a healthy search index. |
| G10 **High / P1 / confirmed test isolation defect** | `backend/tests/test_api_contracts.py:11`; alert/threat test loader helpers; backend/alert/threat module-level metric declarations | Test helpers delete `app.*` modules, then recreate global Prometheus collectors in the same registry. Three service suites fail; fresh-process diagnostics pass. Fix registry/module isolation without suppressing assertions, then run the whole suite and add missing cross-service tests. Root execution also needs an explicit strategy for differing service requirement pins. |
| G11 **Medium / P1 / confirmed DLQ gap** | `storage-sink/app/config/topics.py:12`; `spark-jobs/app/transformations/normalization.py:17,35`; Kafka telemetry/ML consumers | Storage mapper supports DLQ but subscription tuple excludes it. Spark filters malformed/null/invalid rows rather than emitting DLQ evidence. Poison messages may disappear from searchable history. Subscribe/persist DLQ intentionally, retain source offset/reason and add safe replay tooling. |
| G12 **Medium / P2 / confirmed dedup/transaction risk** | `alert-engine/app/dedup/redis_deduplicator.py:31`; `alert-engine/app/services/alert_service.py:36`; `alert-engine/app/incidents/correlator.py:22` | Redis INCR then EXPIRE is non-atomic; cooldown/cache/index side effects occur before PostgreSQL commit. Failed commit can leave a cooldown suppressing a retry, or lost TTL suppress future alerts; Redis failure emits everything. Correlation updates are read/modify/write without concurrency control. Use atomic TTL operations and durable idempotency/outbox boundaries; define degraded behavior and test concurrent incident updates. |
| G13 **High / P2 / confirmed ML evaluation limitation** | `ml-models/app/training/train_model.py:27,38,40`; generated `ml-models/artifacts/contract-v2/training_metrics.json`; `app/inference/predictor.py:40,55` | Scaler/model are fit and scored on the same synthetic dataset. Supplied metrics claim 1.0 accuracy/F1 for 3,000 training records, not unseen detection quality. Confidence is distance from transformed score midpoint; explanation ranks raw feature magnitudes, not model attribution. Use time-separated/held-out realistic labeled evaluation, calibrated thresholds, drift/version/rollback controls and meaningful explanations. No real-world precision/recall or throughput is established. |
| G14 **High / P2 / confirmed rule/intelligence limitation** | `threat-intelligence-engine/app/detections/rule_engine.py:35`; `app/enrichment/ioc_enricher.py:18`; `app/consumers/threat_consumer.py:148`; `detections/rules/impossible_travel.yaml` | Keyword substring plus risk/country matching cannot establish repeated failures, travel speed, process parent chains, periodic beaconing or lateral movement. IOC lists/ASN are static examples; generator country codes differ from full-name watchlists; arbitrary payload fields/feature names can trigger rules. The controlled impossible-travel event additionally hit `suspicious_process_chain` through the `processed_at` field name, and its prediction hit `brute_force_detection` through feature names; persisted hits confirm cross-field false positives. No provenance/expiry/feed refresh or measured detection accuracy. Add actual temporal/event-type predicates, normalized entities/geography, maintained intelligence and positive/negative labeled rule tests; label heuristic mappings honestly. |
| G15 **Medium / P2 / confirmed partial-start behavior** | `scripts/start_light.sh:22`; `scripts/start_light.ps1`; full/verify scripts; `docs/troubleshooting.md` | Light mode omits Spark, storage sink, ES and monitoring, starts ML with `--no-deps`, yet advertises a usable intelligence pipeline. Default host settings cannot reach container DNS; startup health checks are immediate; PowerShell native-command failures are incompletely checked. Document partial capabilities or include a consistent minimal pipeline, add bounded readiness waits, checked native exit codes, and environment-specific commands. |
| G16 **High / P2 / confirmed single-node design; recovery/scaling unverified** | `docker-compose.yml:46,244,475` (Kafka replication, Spark worker, volumes); `spark-jobs/app/streaming/kafka_reader.py:13`; API WebSocket loops | One broker/ZooKeeper/worker/DB, RF=1, local checkpoint volume, `failOnDataLoss=false`, no replay retention/partition-key capacity strategy, and repeated per-client DB snapshots. Runtime Spark logs also showed batches taking 17.1–30.0 seconds against a 15-second trigger; this is an observed overrun, not a capacity benchmark. The default five-events/second producer was used; a short successful run does not establish sustained throughput or bounded lag. These are single points of failure and can hide lost Kafka offsets. Define durability/RPO/RTO, fail-on-loss policy, replicated deployment and measured capacity/backpressure/recovery tests before production. |
| G17 **High / P2 / confirmed operational artifacts missing** | `infrastructure/.gitkeep`; production checklist; absence of `.github/`, migrations, backup/restore/runbooks, collector/exporter/alert-routing configs | No CI/CD, deployment policy, secret/image scan gates, production manifests, backup scheduling/restore drills, alert routing/SLOs or centralized trace/log retention configuration exists. Container stdout/metrics and persistent volumes do not replace these controls. Establish automated gates, documented ownership/deploy/rollback and tested backup/restore before production. |
| G18 **Medium / P2 / confirmed integration mismatch** | `alert-engine/app/rules/rule_engine.py:13`; `ml-models/app/features/feature_mapper.py:19`; `backend/app/services/threat_intel_service.py` detection-rule queries | Alert prefers source IP over user while ML/threat often prefer user. Same source event yields different incident keys (component probe: IP vs u-1001; live pipeline: two persisted incidents for one source event, keyed by IP vs phase0 user). Detection UI's management/list API summarizes observed hits, rather than loading/editing all enabled YAML rules. Normalize entity/source identity and distinguish rule catalog/configuration from hit analytics. |
| G19 **Low / P3 / optional improvement** | Frontend build output, `frontend/src/App.tsx` | All routes are eagerly imported and the main JS bundle is about 983 kB minified. Route splitting is a reasonable later improvement; no measured rendering/network bottleneck is claimed and no frontend redesign was performed. |

## H. Production-readiness gaps

Authentication/authorization, socket security, TLS/ACLs/service secrets, controlled dependency updates and image scans are gates before any real-data/network-access deployment. Reliable Kafka acknowledgment/idempotency, consistent feature semantics, migration ownership, search/cache recovery, honest UI/health behavior and an automated real end-to-end test are gates before treating this as a reliable demo/pilot. Model validation, accurate temporal rules, observability/exporters/routing, load/recovery tests, backup/restore, CI/CD and deployment/runbooks remain required before production.

These gaps cover all requested categories: authentication; API/socket security; passwords/secrets; TLS/network; integrity/migrations; Kafka reliability; Spark recovery; ML lifecycle; detection accuracy; observability; performance/scaling; backup/DR; delivery security; test coverage; frontend integration; operational documentation. A static review and local probes cannot demonstrate absence of additional vulnerabilities. Realistic workloads, multiple failures/restarts, adversarial payloads, production networking and recovery have not been validated.

## I. Recommended implementation order

1. Use the recovered `main` checkout/history; keep generated `.env`, virtual environments, logs, datasets, databases and build artifacts excluded. Retain this baseline with its failing checks.
2. Establish a repeatable full-stack baseline and a controlled source-event → Kafka → Spark → prediction → durable alert/incident → API/search → browser check; make suite/metric isolation reliable and remove misleading UI/health success states. Preserve the event identifier at every hop.
3. Correct feature names, units, projections, identity rules and time-window assembly; add Spark→ML positive/negative contracts and retrain on the corrected feature definition.
4. Introduce one migration-managed schema owner, stable logical keys, explicit durable Kafka acknowledgment, delivery failure handling, idempotent replay and atomic dedup/incident transactions.
5. Add secondary-store reconnect/outbox/reindex recovery; verify DLQ visibility, replay and full search results after restart/failure.
6. Before a real-data pilot, enforce identity/RBAC/actor audit, socket Origin/authorization/budgets, TLS/service ACLs/secret isolation, least-privilege images and tested compatible dependency fixes. This security gate is mandatory even if earlier demo work remains incomplete.
7. Validate ML on held-out realistic data; implement defensible rule semantics, maintained IOC provenance, measured precision/recall, model/rule versioning and rollback.
8. Add CI/CD with all tests/scans, consumer-lag/Spark/JVM monitoring, incident routing/SLOs, measured capacity/backpressure, replicated deployment and documented/tested backup/restore and disaster recovery.
9. Only after correctness and controls, address optional frontend bundle splitting and deployment platform choices. No architecture replacement is required to begin correcting these defects.

## J. Final assessment and Git delivery

**Ready for local development and a limited synthetic-data demonstration. Not ready for a restricted real-data pilot or production deployment.** The full local stack starts after the documented setup/mount corrections, and a controlled event completed the ten verification steps. That success does not validate detection accuracy, durable/recoverable delivery, secure access or production capacity. Three service-suite failures and the recorded security/architecture defects remain open.

All 19 long-running containers are left running in the dedicated `phase0-analytics` VM; `kafka-init` completed with exit 0. Access the dashboard at `http://localhost:5173`, API docs at `http://localhost:8000/docs`, Prometheus at `http://localhost:9090`, and Grafana at `http://localhost:13000`. Grafana's admin password is only in private `.env`. The stopped host-only API/frontend processes were diagnostic; the live endpoints are the containers. Use the matching private `.env` and Docker environment when stopping/restarting this stack. `docker compose down` preserves named data volumes; do not add `--volumes`. `colima stop --profile phase0-analytics` stops this dedicated VM without deleting its data.

Git delivery uses the recovered checkout `/Users/jay2522/.local/share/streaming-analytics/repository-phase0`, existing branch **main**, remote **origin** = `https://github.com/Jay-2522/Real-Time-Analytics-System.git`, initial revision **86bdcbcb2aff9a627fa377467c414c57fbeb4e77**. Fetch comparison before finalization was 0 local/0 upstream commits different; an authenticated dry-run push returned Everything up-to-date. The full intentional source diff was reviewed; lockfile hashes stayed identical. Git ignore checks exclude `.env`, `.docker/`, `.codex-logs/`, virtual environments and node_modules. Generated credential values were scanned against all 25 intended files and were absent. Only those files are staged for the descriptive baseline commit. No model binaries, generated events/databases, runtime logs, dependencies or local secrets are added. The commit/push is executed after this report is finalized; its resulting hash and actual push receipt are reported in the delivery response, avoiding a self-referential commit hash in this document. A divergent remote or rejected push must be reported, never force-pushed/rebased over.

**Highest-priority next engineering task:** correct the Spark→ML feature contract (preserved login fields, feature names, units and entity/window semantics) and prove it with an automated pipeline regression test. Authentication/RBAC/socket protection and the other P0 controls are mandatory before any real-data or network-access pilot.

## Evidence and source references

Local evidence is intentionally under ignored `.codex-logs/` (installation logs, original file manifest/Compose snapshot, test/audit outputs, API/socket probes and generated events), not committed runtime data. This report embeds the material outcomes so it remains useful without those local files. No passwords or complete resolved secret-bearing Compose configuration are included.

Tool installation approach was checked against [Colima's installation documentation](https://colima.run/docs/installation/) and [Homebrew's Colima formula](https://formulae.brew.sh/formula/colima). Package vulnerability output comes from npm audit and [PyPA pip-audit](https://github.com/pypa/pip-audit); advisory counts/reachability caveats are described in section E. Image manifest probes confirmed both `apache/spark:3.5.3` and `confluentinc/cp-kafka:7.6.1` advertise linux/arm64, so CPU architecture incompatibility was not presumed.

## Appendix 1. Installed Python dependency inventory

Exact resolved versions follow. Runtime purposes are given by component in section A; packages not directly declared are transitive support libraries. pytest/httpx are testing tools; pip-audit and its environment are inspection-only. Python standard-library venv/ensurepip tooling is supplied by the listed interpreters. Frontend's unchanged `package-lock.json` is its exact dependency inventory.

### Backend / host checks

```text
annotated-types==0.8.0
anyio==4.15.1
certifi==2026.7.22
click==8.5.0
elastic-transport==8.19.0
elasticsearch==8.17.0
fastapi==0.115.6
h11==0.16.0
httpcore==1.0.9
httptools==0.9.0
httpx==0.28.1
idna==3.20
iniconfig==2.3.1
packaging==26.3
pluggy==1.7.0
prometheus_client==0.21.1
psycopg==3.2.3
psycopg-binary==3.2.3
pydantic==2.10.4
pydantic-settings==2.7.1
pydantic_core==2.27.2
pytest==8.3.4
python-dotenv==1.0.1
PyYAML==6.0.2
redis==5.2.1
SQLAlchemy==2.0.36
starlette==0.41.3
typing_extensions==4.16.0
urllib3==2.8.0
uvicorn==0.34.0
uvloop==0.23.0
watchfiles==1.3.0
websockets==17.2
```

### ML

```text
annotated-types==0.8.0
confluent-kafka==2.5.3
iniconfig==2.3.1
joblib==1.4.2
numpy==1.26.4
packaging==26.3
pandas==2.2.2
pluggy==1.7.0
prometheus_client==0.21.1
pydantic==2.8.2
pydantic-settings==2.4.0
pydantic_core==2.20.1
pytest==8.3.4
python-dateutil==2.9.0.post0
python-dotenv==1.2.4
pytz==2026.5
scikit-learn==1.5.1
scipy==1.17.1
six==1.17.0
threadpoolctl==3.7.0
typing_extensions==4.16.0
tzdata==2026.5
```

### Alert

```text
annotated-types==0.8.0
certifi==2026.7.22
confluent-kafka==2.5.3
elastic-transport==8.19.0
elasticsearch==8.17.0
iniconfig==2.3.1
packaging==26.3
pluggy==1.7.0
prometheus_client==0.21.1
psycopg==3.2.3
psycopg-binary==3.2.3
pydantic==2.10.4
pydantic-settings==2.7.1
pydantic_core==2.27.2
pytest==8.3.4
python-dotenv==1.0.1
redis==5.2.1
SQLAlchemy==2.0.36
typing_extensions==4.16.0
urllib3==2.8.0
```

### Threat intelligence / producer import checks

```text
annotated-types==0.8.0
certifi==2026.7.22
confluent-kafka==2.6.1
elastic-transport==8.19.0
elasticsearch==8.17.0
iniconfig==2.3.1
packaging==26.3
pluggy==1.7.0
prometheus_client==0.21.1
psycopg==3.2.3
psycopg-binary==3.2.3
pydantic==2.10.4
pydantic-settings==2.7.1
pydantic_core==2.27.2
pytest==8.3.4
python-dotenv==1.2.4
PyYAML==6.0.2
redis==5.2.1
SQLAlchemy==2.0.36
typing_extensions==4.16.0
urllib3==2.8.0
```

### Audit tooling only

```text
boolean.py==5.0
CacheControl==0.14.4
certifi==2026.7.22
charset-normalizer==3.5.2
cyclonedx-python-lib==11.12.0
defusedxml==0.7.1
filelock==4.2.0
idna==3.20
license-expression==30.4.4
markdown-it-py==4.2.0
mdurl==0.1.2
msgpack==1.2.3
packageurl-python==0.17.6
packaging==26.3
pip-requirements-parser==32.0.1
pip_api==0.0.35
pip_audit==2.10.1
platformdirs==4.13.3
py-serializable==2.1.0
Pygments==2.21.0
pyparsing==3.3.3
requests==2.34.2
rich==15.0.0
sortedcontainers==2.4.0
tomli==2.5.0
tomli_w==1.2.0
typing_extensions==4.16.0
urllib3==2.8.0
```
