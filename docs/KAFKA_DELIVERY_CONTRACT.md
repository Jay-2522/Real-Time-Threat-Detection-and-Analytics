# Kafka delivery and recovery contract

Scope: ML inference, storage sink, and alert engine, from the Phase 2A cutover.
This is at-least-once consumption with transactional deduplication, not end-to-end
exactly-once delivery. PostgreSQL is authoritative; search converges
through a durable outbox. Feature contract 2.0 and its 18 inputs are unchanged.

## Defects and corrections

| Original path | Failure scenario / root cause | Correction |
| --- | --- | --- |
| Each service's `app/consumers/*_consumer.py` | Auto commits could advance while processing failed; caught exceptions allowed later records to continue | Disable automatic commit/store; one in-flight record; synchronous commit only after success; exit on failure |
| ML consumer | `produce`/`flush` did not prove delivery; random prediction IDs changed on retry | Require successful delivery callback and empty queue; deterministic prediction identity |
| Storage consumer/repository | Crash after SQL commit or replay inserted another row; Elasticsearch errors were swallowed | Receipt, business row and outbox committed in one SQL transaction |
| Alert consumer/service | DB errors were swallowed and cooldown handling was not concurrency-safe | SQL receipt and correlation advisory lock; alert, incident, link and outbox in one transaction |
| Search index adapters | A failed external write was forgotten; unavailable-at-startup flags prevented recovery | Independent persistent retry worker with deterministic Elasticsearch IDs |
| Consumer parsing | Malformed input could disappear without durable disposition | Confirmed ML Kafka DLQ, storage SQL deadletter, alert SQL quarantine |

## Identity and transaction boundaries

- Event identity: SHA-256 of topic plus `event_id` or `prediction_id`.
- Window identity: topic, contract, entity type/scope, feature/metric name and both
  boundaries. Two sliding windows containing the same source events remain distinct.
- Prediction UUID: input identity, model name/version, and SHA-256 of actual model
  and scaler files. The window end supplies a stable prediction timestamp.
- Alert UUID: rules version, input identity, correlation and severity. Separate
  legitimate inputs are retained; correlation groups them into an incident.
- PostgreSQL `(service, key)` receipt primary keys serialize concurrent duplicates.
  Receipt insertion and all related business/outbox writes share one transaction.
  Payload fingerprints detect conflicting reuse of an identity and quarantine it.
  UTC timestamps normalize; processing timestamps and Kafka coordinates do not
  change the semantic fingerprint.
- ML acknowledges only a confirmed prediction or confirmed DLQ publication.
  Kafka producer idempotence handles producer retries, not application replay.
  A crash after publication but before input commit can publish another message;
  downstream SQL receipts prevent duplicate business effects.
- Storage/alerts acknowledge after the SQL transaction, including pending external
  work, commits. No transaction spans Kafka, PostgreSQL, and Elasticsearch.
  Kafka transactions alone would not make the latter systems atomic.

## Retry, shutdown and observability

The common delivery loop commits `message.offset + 1` synchronously and checks
partition errors. Assignment/revocation/loss changes an ownership epoch; changed
ownership refuses acknowledgement. No rebalance callback commits. A processing
or commit failure stops consumption, waits five seconds, and exits for Compose
restart. No later record is acknowledged over the failure. SIGTERM finishes the
current bounded operation; forced termination leaves it replayable.

The outbox worker locks due rows with `FOR UPDATE SKIP LOCKED`. It marks success
only after the effect returns. Failures retain payload/error/attempts with
exponential delay (2–256 seconds). Busy batches continue immediately; idle workers
wait two seconds. A crash after the effect repeats it safely: ES overwrites the
same ID. Elasticsearch versioning prevents older incident retries overwriting newer values.

Monitor Kafka group lag, `pipeline_processing_failures_total`,
`pipeline_commits_total`, `pipeline_rebalances_total`, `pipeline_outbox_pending`,
`pipeline_outbox_failures_total`, and structured `input_unacknowledged`,
`outbox_retry`, and quarantine logs. Inspect pending work with:

```sql
SELECT service, target, count(*), max(attempts)
FROM pipeline_outbox WHERE NOT done GROUP BY service, target;
SELECT service, key, last_error FROM pipeline_outbox WHERE NOT done AND attempts > 0;
```

## Migration and recovery

Use the existing Docker context/environment. Stop all three affected consumers
before upgrading; do not reset groups or delete checkpoint/data volumes.

```sh
docker compose stop ml-inference storage-sink alert-engine
# Supply PGHOST, PGPORT, PGDATABASE, PGUSER and PGPASSWORD securely in the shell.
# Use the host Python environment with SQLAlchemy and psycopg installed.
python -m pipeline_runtime.migrate
docker compose build ml-inference storage-sink alert-engine
docker compose up -d --no-deps ml-inference storage-sink alert-engine
```

The repeatable additive migration creates receipts, outbox and quarantine tables
and seeds receipts from identifiable historical business rows. Review its counts
before starting consumers. It does not rewrite historical rows or create historical
outbox work. Run local Python entrypoints with the repository root on `PYTHONPATH`;
service Docker images include the shared package.

For infrastructure outages, restore the dependency, then restart an exited consumer
with `docker compose up -d --no-deps SERVICE`. Pending outbox work retries independently.
For permanent mapping/data errors, inspect and repair the cause; never mark failed
outbox rows done to hide them. Quarantine preserves exact input bytes (`raw_hex`)
and coordinates. Correct invalid input and publish it with an appropriate new
logical identity; retain the original quarantine for investigation.

## Validation and limits

Permanent tests: `tests/reliability/` exercises real Kafka/PostgreSQL database-session
termination, process exit after SQL commit but before acknowledgement, restart,
duplicate delivery, actual revocation, concurrent alert writes, invalid-input
quarantine, and failed Kafka delivery. Real Elasticsearch connection failures leave
pending outbox rows; restoring the endpoint completes them, and repeated effects
retain one document. These tests use unique local topics/groups
and records, and intentionally do not delete existing data or reset groups.

```sh
PIPELINE_INTEGRATION=1 SPARK_CONTRACT_OUTPUT=/path/to/spark-contract.json python -m pytest
```

Integration tests require the existing local Kafka, PostgreSQL, Elasticsearch and
contract-v2 model artifacts. Without the environment flag, integration skips are
explicit; they do not constitute reliability validation. Phase 1's Spark test
runs separately with Spark 3.5.3 and exports the contract fixture.

Known limits:

- Existing duplicates, missing historical search documents and legacy random-ID
  predictions are not repaired. Old and new ML identities cannot be equated
  retroactively. Backfill prevents re-inserting identifiable historical inputs.
- Receipts must outlive the replay horizon. No automatic pruning
  is introduced. Outbox/quarantine retention and capacity need operational policy.
- Search updates are eventual, not atomic with SQL; PostgreSQL remains correct
  while the Elasticsearch outbox retries.
- Kafka retention bounds recovery. This development broker is single-node; these
  changes do not establish broker high availability or disaster recovery.
- A poison infrastructure/schema failure blocks the affected consumer until fixed;
  it is visible and replayable, never silently skipped. Unknown rule/code errors
  remain retryable rather than being mislabeled as invalid data.
- Other consumers (including threat intelligence) are outside this guarantee.
- During validation, the pre-existing Spark checkpoints contained incomplete offset
  logs (window/enriched batch 182, risk 183, feature-v2 55). Spark contract tests pass,
  but the existing streaming job cannot resume those checkpoints. They were preserved;
  full source-to-Spark live recovery remains blocked pending checkpoint recovery.
  No checkpoint deletion, historical-event deletion or group reset was performed.

## Phase 4A extension (18 October 2026)

The original Phase 2A scope/results above remain historical. The hardened runtime
now extends manual acknowledgement to **telemetry validation and threat
intelligence**, in addition to ML, storage and alerts. This remains at-least-once
processing; no cross-system exactly-once transaction is claimed.

### Corrections

- Telemetry validation disables automatic commit/store and uses `DeliveryLoop`.
  Invalid input is acknowledged only after a confirmed DLQ publication. DLQ IDs
  derive from topic, exact input bytes and source coordinates; retry preserves
  logical identity and Kafka source timestamp (an absent timestamp uses an explicit
  epoch sentinel). This prevents replay from producing conflicting DLQ payloads
  under the same ID. Validation/log metrics are observational, not durable outputs.
- The synthetic producer increments published counts only after a delivery callback
  and empty flush queue confirm success. Transient failure retries the **same**
  generated event rather than generating another one. The generator is not a
  durable upstream event source: process/host loss can lose an unconfirmed in-memory
  event, and an ambiguous acknowledgement can cause replay. Real sources require
  their own durable spool/acknowledgement contract.
- Threat input uses transactional `service='threat'` receipts, stable enrichment
  IDs, SQL quarantine and an Elasticsearch outbox. Receipt, business rows, profile/
  timeline updates and queued search effects commit together. A PostgreSQL advisory
  lock serializes concurrent inputs for the same entity. Duplicate delivery does
  not repeat IOC/rule-hit inserts or increment durable entity/timeline counters.
- Threat search failures propagate to the outbox instead of being swallowed or
  permanently disabled after a startup failure. Partial multi-index completion is
  retried using deterministic document IDs.
- Hardened bootstrap grants the threat identity access only to its own receipt/
  outbox/quarantine rows through row policies. Kafka provisioning adds **literal
  read ACLs for `analytics.enriched.events` and `analytics.risk.metrics`**, matching
  its existing subscriptions; it does not grant general analytics administration.
- The alert Elasticsearch outbox also projects the latest authoritative SQL incident
  snapshot. Snapshot versions use SQL `updated_at`; older external versions cannot
  overwrite a newer incident document. Transport/authentication/indexing failures
  keep work pending. An older-version conflict is already superseded, not a failed
  effect silently discarded. This closes the previously missing incident search
  projection for newly processed/replayed outbox work; it does not backfill retained
  historical databases automatically.

A receipt hit is not permission to prune Kafka history, receipts or pending outbox
rows. Threat detection rules, model artifacts and identity algorithm changes need
an explicit replay/version policy; receipts do not re-run old inputs under a changed
rule set. Existing random-ID threat rows are not retroactively reconciled by the
new deterministic identities.

### Failure evidence and operational checks

The permanent hardened recovery tests use a separate Docker VM/project with mTLS,
SQL/search TLS, row-scoped roles and protected HTTP access. They cover
actual driver/worker process loss, unavailable dependencies, outbox interruption,
confirmed/failed Kafka publication and duplicate delivery. Expected logical counts
are established before input publication; checkpoints, Kafka coordinates, identities
and reconciliation output are retained outside Git. See [Spark recovery](SPARK_RECOVERY.md)
for the exact test scope, final results and the approval boundary for legacy state.

Monitor `pipeline_processing_failures_total`, confirmed commits,
`pipeline_last_commit_timestamp_seconds`, sampled `pipeline_consumer_lag`,
`pipeline_rebalances_total`, `pipeline_outbox_pending`,
`pipeline_outbox_failures_total` and `pipeline_outbox_worker_failures_total`.
Lag observations are sampled on successful processing and may be stale during
idle/failing consumption. Missing/stale observations are not zero lag; compare
broker group offsets with source high watermarks and known incoming traffic.
Retained outbox errors/attempts and repeated process restarts require investigation.

Use the already documented pending-outbox queries. Restore the failed dependency,
then restart exited **isolated/hardened** services without resetting their groups.
A completed effect whose SQL acknowledgement was lost may run again; deterministic
ES IDs and SQL receipt/business uniqueness prevent duplicate business effects.
Search is allowed to be temporarily inconsistent while pending work retries. Pending work
that never converges is a release blocker, not a successful recovery.

Do not run the legacy `PIPELINE_INTEGRATION` tests against preserved development
endpoints to manufacture broader coverage. Keep those exclusions explicit. A
single-node KRaft broker, one Docker host and a small functional test VM do not
establish broker quorum HA, host-loss recovery or production throughput capacity.
