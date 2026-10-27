# Spark–ML contract correction (Phase 1)

## Scope and original evidence

Development branch: `dev`. Historical baseline: `055399a`; policy baseline: `cbe31aa`.
The original audit remains historical evidence. This change addresses feature semantics,
atomicity, artifact compatibility, and directly related test isolation. It does not establish
production readiness or model accuracy.

Confirmed by inspecting the baseline source and executing its mapper:

| Defect | Original evidence | Correction |
| --- | --- | --- |
| Failed-login name mismatch | `features.py` emitted `user_failed_login_rate`; `feature_mapper.py` only copied names in `FEATURE_COLUMNS`. A .5 rate became 0. | Exact `failed_login_rate` in a complete vector. |
| Failed-auth count fabricated | The same mapper assigned `event_count`; four attempts/two failures mapped to four failures. | Count actual `login_success == false`; divide by login count. |
| API error confusion | `endpoint_error_rate=.75` became `api_5xx_rate=.75`, including 4xx. | Separate 400–599 and 500–599 numerators. |
| IP cardinality fabricated | Three network events mapped to `unique_ip_count=3` irrespective of addresses. | Count exact distinct **destination** IPs within source-IP windows; unambiguous new name. Source-IP cardinality within a source-IP group would always be one. |
| Incomplete vectors | Consumer subscribed to four analytics topics, predicting every record separately with 15 defaults. Training independently generated all 15 columns together. | One aggregate vector per source IP/window; subscribe only to feature topic. |
| Identity/window provenance lost | Login grouped by user, API by endpoint, network by source IP, anomaly by category; mapper used fallback IDs; prediction schema omitted window boundaries. | Explicit common source-IP scope and UTC boundaries, complete source event ID list in prediction. No cross-record join in ML. |
| Missing/invalid values disguised | `_safe_float` returned zero for malformed/missing values, passed NaN/Infinity; login outcome missing from enriched selection defaulted to success. | Reject malformed vectors/windows; preserve login outcome; structurally absent rates remain null on wire. |
| Artifact semantics unchecked | Loader compared only 15 column names. Stored metadata has model 0.1.0, 3,000 synthetic rows, no contract version. | Validate version, names/order, scope, duration, preprocessing and fitted dimensions. Preserve and reject legacy artifacts. |
| Other fabricated semantics | `anomaly_rate` was mean risk on 0–100 scale; session score copied risk, spike score divided a single event by 100,000; country cardinality called variance. | Actual anomaly-event fraction; remove unsupported session/spike features; explicit country cardinality name. |

The old training/inference **column order already matched**: order mismatch was a risk,
not a reproduced baseline defect. The confirmed mismatch was meanings and completeness.
Window metrics and risk metrics remain separate analytics products, not model inputs.

## Contract 2.0

Producer: Spark `build_window_features`. Input: normalized telemetry login/API/network/anomaly
streams. Entity: exact `source_ip`; this is an IP-level model, not a user or endpoint model.
NAT/shared IP activity intentionally shares one IP-level window; it does not establish a
single person's identity. Callers should supply canonical IP address strings.

All fields below are emitted together in `features`, ordered as listed for training/scaling.
Window: **60 seconds, sliding every 30 seconds**, UTC, half-open `[window_start, window_end)`.
Counts refer to records observed within the watermark policy, not a guarantee of all real
activity. There is no feature joining across entities, windows, or timestamps.

| Feature (ordered) | Source | Definition / units | Required and missing behavior | Validation |
| --- | --- | --- | --- | --- |
| event_count | All | N = total records; count | Required, >0 | Integer; sum of four type counts |
| login_event_count | Login | L = login records; count | Required, zero if absent | Nonnegative integer |
| api_event_count | API | A = API records; count | Required, zero if absent | Nonnegative integer |
| network_event_count | Network | W = network records; count | Required, zero if absent | Nonnegative integer |
| anomaly_event_count | Anomaly | X = anomaly records; count | Required, zero if absent | Nonnegative integer |
| failed_login_rate | Login | Failed login records / L; fraction | Required key, null iff L=0 | [0,1], consistent with failures/L |
| failed_auth_count | Login | Count false login_success; count | Required, zero if L=0 | Integer, <=L |
| requests_per_minute | API | A / (60/60); requests/min | Required, zero if A=0 | Exactly A |
| avg_api_response_time | API | Mean response_time_ms; milliseconds | Required key, null iff A=0 | Finite, >=0 |
| endpoint_error_rate | API | Count HTTP 400–599 / A; fraction | Required key, null iff A=0 | [0,1]; name retained, scope now source IP across endpoints |
| api_5xx_rate | API | Count HTTP 500–599 / A; fraction | Required key, null iff A=0 | [0,1], <= overall error rate |
| avg_network_bytes | Network | Mean(bytes_sent+bytes_received); bytes/event | Required key, null iff W=0 | Finite, >=0 |
| high_risk_network_event_count | Network | Count network risk_score>=70; count | Required, zero if W=0 | Integer, <=W |
| anomaly_rate | Anomaly/all | X/N; fraction | Required | [0,1], consistent with counts |
| risk_score_moving_average | All | Mean supplied risk_score; score points | Required | [0,100], never normalized to [0,1] |
| high_risk_event_count | All | Count risk_score>=70; count | Required | Integer, <=N |
| distinct_login_country_count | Login | Exact distinct country strings; count | Required, zero if L=0 | Integer, <=L; positive when L>0 |
| unique_destination_ip_count | Network | Exact distinct destination_ip strings; count | Required, zero if W=0 | Integer, <=W; positive when W>0 |

Each Spark-produced field maps to the identically named ML feature. All use the same
source-IP scope and 60-second window above. JSON numeric inputs only: strings, booleans,
NaN, Infinity, negatives, unexpected names and missing keys are rejected. Counts must be
integral. No alias maps event volume to IP cardinality or authentication failures.

The envelope requires `feature_contract_version=2.0`, `event_type=feature_window`,
`entity_type=source_ip`, valid IP `entity_id`, `invalid_event_count=0`, `source_event_ids`,
`window_start`, `window_end`, `first_event_at`, `last_event_at`, and `calculated_at`.
Timestamps are UTC with explicit zone. First/last source times must lie inside the window;
window start aligns to a 30-second boundary. Source IDs preserve all contributing records;
no arbitrary single source event is assigned to a window prediction.

Spark flags a whole window invalid if a contributing record lacks a required login result,
country, API status/latency, network bytes/destination, or valid risk measurement. ML sends
invalid/incompatible windows to the existing DLQ and emits no prediction. Inputs without
an event ID, parseable event timestamp, or nonempty source IP cannot be scoped and are
filtered before aggregation; routing such raw records to a Spark-side DLQ remains future work.

Structural missingness is different: a window with no API records has null API rates and
latency. ML explicitly converts those nulls to zero **only for the model matrix**, while
including all four population counts as model inputs. Training uses exactly the same rule.
This is declared imputation, not an observed zero error rate. `features_used` is the numeric
model input; consult its population counts and the source window for missingness.

## Before and after

Before, one record `{"feature_name":"user_failed_login_rate","feature_value":0.5,"event_count":4}`
produced `failed_login_rate=0, failed_auth_count=4` and thirteen mostly default values.
Another record `endpoint_error_rate=.75` independently produced `api_5xx_rate=.75`.

After, a window with four logins (two failed), API statuses 200/404/500/503, three network
records to two destinations, and one anomaly produces a single payload:

```json
{
  "feature_contract_version": "2.0",
  "event_type": "feature_window",
  "entity_type": "source_ip",
  "entity_id": "192.0.2.1",
  "window_start": "2026-01-01T12:00:00Z",
  "window_end": "2026-01-01T12:01:00Z",
  "first_event_at": "2026-01-01T12:00:10Z",
  "last_event_at": "2026-01-01T12:00:10Z",
  "calculated_at": "2026-01-01T12:04:00Z",
  "invalid_event_count": 0,
  "source_event_ids": ["l1", "l2", "l3", "l4", "a1", "a2", "a3", "a4", "n1", "n2", "n3", "x1"],
  "features": {
    "event_count": 12, "login_event_count": 4, "api_event_count": 4,
    "network_event_count": 3, "anomaly_event_count": 1,
    "failed_login_rate": 0.5, "failed_auth_count": 2, "requests_per_minute": 4,
    "avg_api_response_time": 100, "endpoint_error_rate": 0.75, "api_5xx_rate": 0.5,
    "avg_network_bytes": 150, "high_risk_network_event_count": 3,
    "anomaly_rate": 0.08333333333333333, "risk_score_moving_average": 80,
    "high_risk_event_count": 12, "distinct_login_country_count": 1,
    "unique_destination_ip_count": 2
  }
}
```

## Training and deployment compatibility

The old 15-feature artifact cannot be used: dimensions, semantics and population have
changed. New model version is `0.2.0`; feature contract version is `2.0`. Existing tracked
baseline artifacts are retained, not rewritten. Generated artifacts go into ignored
`ml-models/artifacts/contract-v2/` (the existing parent bind mount is retained).

The loader checks metadata **before** joblib deserialization, then fitted scaler names/order
and both model/scaler dimensions. Only trusted local artifacts should be loaded: metadata
validation does not make arbitrary pickle/joblib files safe. Prediction reports artifact
metadata's model name/version, rather than unrelated environment labels.

Training uses seed 42, coherent event counts, derived failure/error fractions, exact sampled
cardinalities, and consistent risk statistics. Both training and inference use the same
conditional-null conversion followed by the fitted StandardScaler. Isolation Forest still
uses synthetic training data; its in-sample metrics are not independent accuracy evidence.
Scores/confidence remain uncalibrated. Reproduce with:

```bash
docker compose build spark-streaming ml-inference
docker compose run --rm --no-deps ml-inference python -m app.main train
docker compose up -d --no-deps spark-streaming ml-inference
```

Compose defaults select the new artifact directory. Explicitly train before starting when
bootstrap is disabled; bootstrap only trains a missing directory, never silently replaces
an incompatible artifact. Spark uses a **new feature-query checkpoint name** because its
state schema changed. The old checkpoint is preserved; other query checkpoints are unchanged.
The new feature query's default `latest` starting offset intentionally does not backfill
historical raw events. Older feature records encountered by ML are rejected to the DLQ.

## Regression verification

Original results reproduced against `cbe31aa`: per-service **8 passed / 3 failed**, root
backend-only environment **7 passed / 4 failed**. Failures were duplicate Prometheus metric
registration across test reloads and missing scikit-learn in the root environment.

`conftest.py` restores environment/import paths and unregisters only collectors created by
each test. Production metrics still raise on duplicate registration; assertions are retained.
`requirements-test.txt` defines a root test environment, including ML dependencies. Runtime
service pins remain unchanged and are tested separately.

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements-test.txt
mkdir -p .codex-logs
# Supply the normal Docker context/DOCKER_HOST for your development engine.
docker run --rm --user root \
  -v "$PWD/spark-jobs/tests:/tests:ro" -v "$PWD/.codex-logs:/evidence" \
  -e SPARK_CONTRACT_OUTPUT=/evidence/spark-contract.json \
  streaming_analytics_system-spark-streaming \
  /opt/spark/bin/spark-submit --master 'local[2]' /tests/test_feature_contract.py
SPARK_CONTRACT_OUTPUT="$PWD/.codex-logs/spark-contract.json" .venv/bin/python -m pytest
```

Spark tests execute the actual JSON parser, normalization, aggregation and serialization,
then export their real output for the ML mapper regression. This checks repeated destinations,
400-vs-500 errors, normal/suspicious fixtures, invalid inputs, multiple entities/windows,
structural missingness, and provenance. Without the export, the cross-service pytest case
explicitly skips; a skip is not integration evidence. No generated fixture/output is committed.

### Final results — 17 October 2026 (Asia/Kolkata)

Outcome: **PASS for this feature-contract correction**, with the limitations below.

| Verification | Passed | Failed | Skipped | Blocked |
| --- | ---: | ---: | ---: | ---: |
| Full root Python suite, including real Spark-output mapper integration | 37 | 0 | 0 | 0 |
| Same tests in separate service environments: backend 2, ML 29, alert 3, threat 3 | 37 | 0 | 0 | 0 |
| Actual Spark 3.5.3 regression suite (separate JVM/container) | 5 | 0 | 0 | 0 |
| Live producer → Kafka → Spark → ML → prediction verification | 1 | 0 | 0 | 0 |
| Final service health checks | 12 | 0 | 0 | 0 |

The two 37-test rows are alternate environments, not 74 distinct tests. There are 42
permanent automated cases across Python and Spark. Syntax parsing passed for 192 Python
files; `pip check`, Compose configuration, both targeted image builds and Git whitespace
checks passed. Frontend checks are not applicable: no frontend files or consumed field
references changed. No existing additional integration/load suite was present.

Root pytest reported 41 existing Pydantic/FastAPI/Starlette deprecation warnings. Spark
reported a PySpark socket ResourceWarning on process shutdown; all assertions and process
exit codes passed. An initial fixture tried constructing a nonnullable Spark field with
None; the test now exercises the actual Kafka JSON parsing path, which represents malformed
fields as null. An initial timestamp assertion compared `Z` with `+00:00`; it now compares
UTC instants. Neither was hidden or skipped. The original login-value assertions are retained.

### Live evidence

Only Spark and ML were rebuilt/recreated. No volumes/checkpoints or baseline models were
deleted. Both final images ran the verification; the feature query recovered its pending
v2 windows across the final recreation. Normal event-time advancement closed the windows;
no future-time watermark events or manual feature messages were injected.

Run ID: `4901e935-3312-4376-ae8c-96a035066d4e`. Producer input timestamp:
`2026-10-16T23:06:47Z`. Twenty-four events were validated with the production producer
Pydantic schemas, then sent to the four normal telemetry topics using a Kafka producer.
Each of two IPs had 4 logins, 4 API requests, 3 network records to 2 destinations, and 1
anomaly event. All source IDs were matched against the resulting windows and predictions.

| Entity | Scenario | Failed rate/count | Overall / 5xx rate | Distinct destination IPs | Mean input risk |
| --- | --- | --- | --- | --- | --- |
| 192.0.2.171 | Suspicious fixture | .5 / 2 | .75 / .5 | 2 | 80 |
| 192.0.2.172 | Normal fixture | 0 / 0 | 0 / 0 | 2 | 10 |

For each IP, Spark emitted `[23:06:00Z,23:07:00Z)` and `[23:06:30Z,23:07:30Z)`
on 16 October UTC. All 18 feature values matched expected formulas. Both ML inputs
were those atomic Kafka payloads; `features_used` matched them (to the prediction schema's
six-decimal reporting precision), and all 12 contributing IDs per window survived.

| Entity / start (UTC) | Prediction ID | Observed model risk |
| --- | --- | ---: |
| 192.0.2.171 / 23:06:00 | 961bdb9d-6fbb-4ea7-bf16-c94c6a84d0db | 70.8047 |
| 192.0.2.172 / 23:06:00 | 904f9b17-12ad-4088-a132-b2d5c19ee224 | 32.0124 |
| 192.0.2.171 / 23:06:30 | d72319ac-a38f-4c54-986c-3d9ef85b2990 | 70.8047 |
| 192.0.2.172 / 23:06:30 | 059e39ea-735f-4d2d-9697-5ded885eac1c | 32.0124 |

These fixture scores demonstrate execution, not detection accuracy. Predictions identify
model 0.2.0 and contract 2.0. The original fitted scaler/model were separately inspected:
both have 15 dimensions, and the scaler's actual names match the old metadata. The new
loader rejects that metadata; current fitted model/scaler have 18 compatible dimensions.

SQL checks found **24 raw + 24 enriched + 4 feature snapshots + 4 predictions** for the
controlled events. One alert from `ml.anomaly.predictions` persisted (database row 9448),
referencing prediction `961bdb9d-6fbb-4ea7-bf16-c94c6a84d0db` and entity `192.0.2.171`.
GET `/api/v1/predictions/192.0.2.171` and `/api/v1/predictions/192.0.2.172` both returned
HTTP 200 with the latest matching prediction, contract version, exact counts and provenance.
This verifies persistence/API visibility in addition to Kafka delivery.

Raw payloads, output topic partition/offsets, assertions and health results are retained
locally under ignored `.codex-logs/`; generated model binaries/metrics remain outside the
commit. The specific baseline feature-contract defects are resolved for contract 2.0.
Recommended next work: durable consumer offset/side-effect coordination and replay-safe
idempotency, with focused failure/recovery integration tests.

## Remaining limitations

- Sliding windows intentionally overlap: an event can belong to two predictions. This is
  not cross-window mixing; downstream incident deduplication is a separate concern.
- Watermarks (default two minutes) can delay emission and discard late events; an inactive
  input stream may hold back progress. No exactly-once end-to-end guarantee is added.
- Duplicate raw deliveries count as duplicate observations; event-ID deduplication and
  durable consumer offset/side-effect coordination remain unresolved.
- Source event ID lists and exact cardinality sets consume state proportional to events
  per IP/window; production volume limits/load tests are still needed.
- Country/IP string normalization, IP-level NAT ambiguity, source trust, schema validation
  before Spark, security controls, and baseline dependency advisories remain open.
- No claim of improved detection accuracy, calibrated probability, production readiness,
  or complete downstream alert correctness is made.

## Changed files and purpose

- `spark-jobs/app/feature_engineering/features.py`: atomic 18-feature aggregate; exact
  cardinalities and formulas; invalid-window flag; cast byte counters before addition
  to prevent 32-bit sum overflow.
- `spark-jobs/app/transformations/normalization.py`: preserve login outcome and destination;
  retain invalid risk measurements for window rejection instead of inventing zero risk.
- `spark-jobs/app/main.py`: preserve the valid-risk filter for separate analytics outputs;
  version the feature checkpoint.
- `spark-jobs/app/sinks/kafka_sink.py`, `spark-jobs/app/utils/spark.py`: explicit null JSON
  and consistent UTC serialization/session time zone.
- `ml-models/app/features/feature_definitions.py`, `feature_mapper.py`: ordered versioned
  names, strict validation and declared conditional missing-value preprocessing.
- `ml-models/app/config/topics.py`, `settings.py`, `consumers/inference_consumer.py`:
  consume complete windows only; v2 model directory; preserve provenance and route invalid input.
- `ml-models/app/inference/predictor.py`, `schemas/prediction.py`: validate artifact/input
  compatibility and retain window metadata in predictions.
- `ml-models/app/models/artifact_store.py`, `training/train_model.py`,
  `data/synthetic_training_data.py`: reject incompatible artifacts and reproducibly train
  coherent v2 window statistics with matching preprocessing.
- `.env.example`, `docker-compose.yml`, `.gitignore`: select separate generated v2 artifacts
  and keep them out of commits. Existing private `.env` model path/version were synchronized
  locally; credentials were preserved and that file is excluded.
- `spark-jobs/tests/test_feature_contract.py`, `ml-models/tests/test_feature_windows.py`:
  essential permanent formula, provenance, missingness and artifact regressions, including
  real Spark output. `ml-models/tests/test_ml_contracts.py` updates the original login fixture
  to the new envelope while retaining all three original feature-value assertions.
- `conftest.py`, `requirements-test.txt`: test-only service isolation and reproducible root
  dependencies. No production metric registration exceptions are suppressed.
- This document: requested contract, migration, verification and limitations.
- `docs/PHASE_0_BASELINE_AUDIT.md`: a dated follow-up link; original findings remain unchanged.

Local evidence, generated training artifacts, virtual environments, caches, test output,
and the temporary live-verification producer are intentionally excluded from the commit.
