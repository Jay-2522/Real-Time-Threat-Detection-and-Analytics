# Spark 3.5.3 checkpoint recovery

Scope: the four contract-2.0 queries. Original checkpoint recovery remains blocked;
this change hardens future recovery and adds isolated failure regression tests.
Never substitute a new checkpoint merely to make the service healthy.

## Forensic findings (17 October 2026)

The driver uses a Docker named volume, `streaming-analytics-spark-checkpoints`, at
`/opt/spark/checkpoints`. The original standalone worker had **no volume mounts**.
Its HDFSBackedStateStoreProvider used the same `file:` path in the worker's writable
container layer. Driver offset/commit logs and executor aggregation state therefore
lived on different filesystems. Worker recreation could destroy state independently
of the driver logs. This is a confirmed configuration defect, not proof of what
caused the zero-byte files.

| Query / checkpoint suffix | Latest complete offset | Latest complete commit | Empty offsets | Empty commits |
| --- | ---: | ---: | --- | --- |
| enriched / analytics_enriched_events | 180 | 180 | 181,182 | 181,182 |
| window / analytics_window_metrics | 180 | 179 | 181,182 | 180,181,182 |
| risk / analytics_risk_metrics | 181 | 181 | 182,183 | 182,183 |
| features / analytics_feature_engineering_v2 | 53 | 52 | 54,55 | 53,54 |

Query UUIDs respectively: `3191b5aa-f63b-4434-9503-1da829b2e16c`,
`21d988b7-a1c1-45c8-acd6-3334268ee2af`, `fea232ea-f2ed-43e5-9105-b042ac103f6b`,
`712bda49-0a42-4140-ae26-85c4babea3aa`.

The driver archive contains 1,886 files (441,028 data bytes), including 34 empty
files counting checksum sidecars. Worker state contains 15,208 files (5,272,691
bytes), including 24 empty files in window/risk state and checksum sidecars. Empty
file mtimes cluster at 2026-10-16 23:20:24–23:20:53 UTC, before this recovery task.
Feature-v2 has state through version 55; risk through 184; window through 183.
There are six shuffle partitions per current stateful operator. File presence is
not a validation of compressed state contents or a complete state-version chain.
The older feature checkpoint/state is also preserved but incompatible with v2.
The three current state-schema files are byte-for-byte identical to schemas produced
by the isolated current-code run. A state-schema change is therefore not supported
as the cause of these startup failures; damaged progress/state contents remain the blocker.

The immediate startup failure is Spark reading an empty final offset log. Its
version-specific OffsetSeqLog deserializer rejects an empty file. Empty committed
names and multiple affected stores mean this cannot be treated as a single normal
uncommitted batch. Evidence does **not** establish whether VM/storage durability,
interrupted writes, or another historical operation emptied those files. No disk
fault or particular shutdown is claimed as the proven cause.

All four current source topics retain offset zero. Login/API/network each have three
partitions, anomaly two; retention is seven days, unlimited retention bytes, delete
policy. At the preserved inventory snapshot, latest offsets by partition were:
login `[2064,2054,2057]`, API `[2117,2021,2056]`, network `[2024,2014,2007]`,
anomaly `[520,494]`. Every recorded original initial/current source offset is within
these ranges. Availability must be rechecked before any future reconstruction.

Original Kafka output scanning found 14,294 enriched, 36 window, 26,710 risk and
19,522 feature records at the snapshot. Enriched output exists at 23:20:45 UTC;
risk/feature output at 23:20:30 UTC, overlapping the incomplete-log interval.
An empty commit file does not mean no output escaped. Window output cannot be
attributed to an incomplete batch from its filename alone. Raw records are retained,
but omitted historical micro-batch/watermark decisions and incomplete tail metadata
prevent a claim of exact historical state reconstruction.

## Preservation and strategy

The restart loop was stopped before backup. No executor process remained in the
worker and no running container mounted the driver checkpoint volume. The complete
driver volume was archived read-only; worker checkpoints were separately archived.
Both archives were read, counted and SHA-256 manifests generated per file. Archives,
source offset/config inventory, original logs and output evidence remain locally in
ignored `.codex-logs/phase2b/`. No progress files, volumes, databases or Kafka group
offsets were deleted, reset or rewritten. Keep both archives: neither alone contains
the original complete distributed checkpoint layout.

- **A — normal continuation:** blocked by empty offset/commit logs and state defects.
- **B — supported metadata recovery:** Spark 3.5.3 has no identified supported method
  to infer the missing contents while preserving this history. We do not fabricate
  logs, rename an older batch into place or delete the tail.
- **C — reconstruction:** source replay is possible in a separate namespace, beginning
  at each query's saved initial source offsets (feature-v2 starts later than the
  other three). This is the earliest available logical starting boundary, not a
  proven exact state restoration. Replaying in different batches changes watermark
  timing and potentially late-event inclusion. Existing output must be reconciled
  by event/window keys and full semantic payload before publishing replacements.
  Legacy random prediction IDs also predate the Phase 2A identity guarantee.

No reconstruction or original-state promotion was performed. A future recovery
proposal must preserve both archives, replay into isolated output topics, compare
all previously published keys/payloads, quantify unresolved windows and late records,
and obtain explicit approval for any production-topic promotion or alteration of
original state. The original driver remains stopped; it has not been declared healthy.

## Independently safe corrections

- Kafka `failOnDataLoss=true`: unavailable required offsets fail visibly, rather than
  silently advancing. `startingOffsets` applies only to new queries, not recovery.
- Compose gives worker and driver the same named checkpoint volume and matching UID.
  This fixes the single-Docker-host layout only; a multi-host cluster needs shared
  durable storage. The existing worker has not been recreated: doing so would hide
  its original container-local state. Do not apply the new mount as an implicit migration.
- Read-only preflight rejects incomplete logs, retained-sequence gaps, impossible
  offset/commit tails and absent/empty required local state versions. Spark remains
  responsible for decoding state and checking its schema and delta chain.
- A separate application contract records query/output/source identity, broker,
  watermark, output schema, semantic source hashes and shuffle configuration. A
  changed contract fails closed. A legacy populated checkpoint without this manifest
  requires reviewed adoption; there is no silent adoption or automatic fresh path.
- SIGTERM/SIGINT request orderly query shutdown; Compose allows 90 seconds. Partial
  query startup is covered by cleanup. Monitoring reports inactive queries/errors.

Kafka sink output is at-least-once. A batch may publish before Spark commits it;
restart can publish duplicates. Phase 2A event/window/prediction identities and SQL
receipts provide downstream idempotence. Separate sliding windows are not collapsed.
No model was retrained and no feature semantics were changed.

## Validation and operating procedure

Run ordinary tests with `python -m pytest`; set `PIPELINE_INTEGRATION=1` for real
Phase 2A infrastructure tests and `SPARK_RECOVERY_INTEGRATION=1` for the isolated Spark
recovery test. Supply the existing Docker environment and Phase 1 exported contract
via `SPARK_CONTRACT_OUTPUT`. Test infrastructure uses the built Spark image, dedicated
Kafka topics and an ignored per-run checkpoint directory. Do not delete them to make
an assertion pass.

The Spark recovery regression runs the real four transforms against real Kafka in
Spark 3.5.3 `local[2]`. A test-only foreachBatch Kafka sink exits the Python driver
after feature batch 1 publishes and before its commit. Restart uses the same query
UUID and checkpoint, followed by a clean restart with additional source records.
The test then invokes the actual ML/storage/alert handlers with the fitted existing
model, real Kafka and PostgreSQL, asserting durable counts. This is not a restart
of all deployed service processes or a distributed-worker failure test.

Preflight unit tests cover invalid logs, missing state, compatibility, namespace
isolation and insufficient retention. Insufficient retention is simulated by bounds;
no Kafka records were deleted to manufacture that condition.

For a valid established checkpoint: verify all four query statuses and source
retention, send controlled events, stop the driver gracefully, back up progress and
state consistently, restart with unchanged code/configuration, and compare source
IDs, window identities, query UUIDs, SQL counts and commit progression. A container
being up is insufficient evidence. On preflight failure, stop writers and preserve
state before investigating; never bypass the guard or reset offsets to suppress it.

References: [Spark 3.5.3 checkpoint format](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/OffsetSeqLog.scala),
[checkpoint atomic-write implementation](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/CheckpointFileManager.scala),
[version-specific streaming guide](https://github.com/apache/spark/blob/v3.5.3/docs/structured-streaming-programming-guide.md),
and [Kafka integration](https://github.com/apache/spark/blob/v3.5.3/docs/structured-streaming-kafka-integration.md).

## Verified results

Final combined Python run: **65 passed, 0 failed, 0 skipped**, with 59 dependency
warnings; this includes real Phase 2A integration and the isolated Spark recovery
case. Separate Spark feature-contract suite: **5 passed**. The isolated crash /
replay / clean continuation scenario passed twice in separate namespaces. Each run
verified 24 controlled source events, 8 five-minute event-type aggregates, 4 risk
windows, 6 feature messages representing 4 logical windows, 6 prediction messages,
and exactly 4 durable predictions plus 4 alerts. Query UUID and checkpoint identity
survived restart. The backend recent-alert API exposed the controlled alerts.

All 12 existing endpoint/service health checks passed; they do not test Spark query
health. Original driver preflight intentionally failed on empty offset 181, before
query startup, and original-volume SHA-256 verification remained unchanged. Original
query recovery and distributed-worker failure recovery remain **blocked/unverified**,
respectively. Insufficient retention was unit-tested, not induced by deleting data.
No full Phase 2B PASS or original-state restoration is claimed.

Committed scope: Spark main lifecycle, Kafka reader/sink, checkpoint preflight,
query monitor, Compose/pytest configuration, three recovery regression source files,
and this runbook. Archives, checksums, logs, generated records/checkpoints, local
credentials and test outputs are deliberately excluded from version control.

## Phase 4A hardened recovery and legacy disposition (18 October 2026)

The Phase 2B findings above remain historical. The original damaged checkpoints
have not been repaired or replaced. Phase 4A uses Spark 3.5.9 / Scala 2.12 / Java
17 / Python 3.11 with the Phase 3C base filesystem and code-only test derivatives.
The dedicated `phase4a-recovery` Colima VM is ARM64, two CPUs, 4 GiB RAM and 2 GiB
of temporary swap; this is functional recovery evidence, not capacity/soak or HA
qualification. Fresh Compose projects, credentials, Kafka state and volumes are
separate from the development VM and all retained Phase 3 deployments.

### State preservation boundary

The preserved development VM/context is `phase0-analytics` /
`colima-phase0-analytics`. Driver `322fa4d3af1c` remains stopped, and worker
`7d1305599c45` retains its original container-local state. Driver state is the
`streaming-analytics-spark-checkpoints` volume mounted at `/opt/spark/checkpoints`;
worker state was in that same path on a different filesystem. The two read-only
archive inputs are `.codex-logs/phase2b/original-checkpoints.tar` and
`.codex-logs/phase2b/worker-checkpoints.tar`; their before/after hashes are retained
in ignored Phase 4A evidence. No test mounts either archive or original volume.

Original stores use `streaming-analytics-postgres-data`,
`streaming-analytics-elasticsearch-data`,
`streaming-analytics-kafka-data`, ZooKeeper data/log volumes, and retained
Prometheus/Grafana volumes. Original topics include the four `telemetry.*.events`
sources, four `analytics.*` products, `ml.anomaly.predictions` and the DLQ.
Existing application groups include `streaming-analytics-ml-inference`, `streaming-analytics-storage-sink`,
`streaming-analytics-alert-engine` and `streaming-analytics-threat-intelligence`,
plus retained `reliability-*` test groups. Group listing was read-only; none was
reset. The original producer continues its pre-existing operation independently.

Historical data ownership is **unknown**, not verified synthetic. Generator code,
reserved-address fixtures, known test groups and contract-v2 artifacts establish
that synthetic records exist; they do not establish that every retained event,
account, imported IOC or user annotation is disposable. Source retention and data
completeness at a future approval date must be re-inventoried. Historical offset
ranges in Phase 2B are not a present retention guarantee.

### Recovery tests and observability

`tests/hardening/test_full_stack_recovery.py` is an explicit opt-in destructive
**test-environment-only** suite. It requires a `test-only` credential marker and a
`soc-phase4a*` project. Never set the marker on retained or production state.
`HARDENED_PROJECT` and `HARDENED_COMPOSE_OVERRIDE` select the dedicated test stack;
the existing hardened tests retain their default project for backward compatibility.

The deterministic dataset fixes counts before publication: two batches, each with
four logins, four API events, three network events and one anomaly. Each batch has
one source IP, two failed logins, HTTP 200/404/500/503, two destination IPs and risk
80. Eight separately identified watermark-control records are not target events.
Expected target products are 24 enriched events, eight five-minute event-type
windows, four overlapping risk windows, four contract-2.0 feature vectors and four
predictions. Feature lineage and every one of the 18 numeric values are checked;
raw Kafka counts are not substituted for unique logical outputs.

The real four-query service is killed and restarted using the same state volume.
The test compares all query UUIDs, offset/commit progress and subsequent outputs.
The separate-worker regression runs real Spark tasks on the standalone worker,
uses a test-only observable task stall, kills/recreates that worker, and records
executor-host/task-attempt witnesses. Both worker and driver mount `spark_state`
at `/opt/spark/checkpoints`; state survives container replacement. This remains a
single-Docker-host guarantee. Multiple hosts require a supported shared durable
filesystem and a checkpoint preflight supporting its URI; container-local `file:`
paths do not become distributed storage by sharing the same spelling.

The full-stack scenario stops PostgreSQL and Elasticsearch, publishes the
next known input, restores SQL, verifies pending outbox work, kills outbox service
processes, restores search and checks convergence. Separate secured tests
exercise failed SQL transactions, process exit after SQL commit before Kafka
acknowledgement, group reassignment and replay. Another real-broker test pauses
Kafka, requires publication confirmation to fail, resumes it and repeats the same
logical DLQ record without adding another SQL business record.

Spark now exposes query-active, batch-ID, input-row and monitor-timestamp metrics
on internal port 9107. A bearer token is required and comes from the provisioned
monitoring credential; no host port is added. Prometheus scrapes this eighth job.
Authenticated monitoring reports Spark healthy only when all four queries are
active and the observation is fresh. Missing, failed or stale observations degrade
pipeline status. `/health/live` is process liveness and `/health/ready` is database/
authorization-backed API readiness; neither certifies complete analytics health.
Use authenticated monitoring for pipeline readiness.

Consumer metrics expose confirmed commits, last-commit time, observed partition
lag, rebalances and processing failures. Lag is sampled during successful
processing, not a continuous broker-wide group exporter: stale/absent lag is
unknown, not zero. Correlate last-commit age with known incoming input, broker
group offsets and scrape status. Outbox database-observation failures now have a
counter in addition to pending-work/failure metrics and retained SQL error details.
Query status/metrics do not prove correctness without dataset reconciliation.

Run only against a newly provisioned isolated deployment with sufficient resources:

```sh
export HARDENED_PROJECT=soc-phase4a-new
export HARDENED_SECRETS_DIR=/absolute/private/new-test-credentials
export HARDENED_MODEL_DIR=/absolute/path/to/verified-contract-v2-artifacts
# Select the dedicated Docker endpoint explicitly; provision credentials with --test-only.
# Build/import the tested images, initialize stores/topics/ACLs, then start workers.
HARDENED_INTEGRATION=1 HARDENED_FULL_STACK_RECOVERY=1 \
  .venv/bin/python -m pytest tests/hardening/test_full_stack_recovery.py -ra
HARDENED_INTEGRATION=1 HARDENED_WORKERS_INTEGRATION=1 \
  .venv/bin/python -m pytest tests/hardening/test_stack.py -ra
# Reserve the small test worker for the separate one-shot driver tests.
HARDENED_STREAMING_INTEGRATION=1 HARDENED_RECOVERY_INTEGRATION=1 \
  .venv/bin/python -m pytest tests/hardening/test_streaming.py tests/hardening/test_recovery.py -ra
```

The full-stack baseline test requires fresh data. Reruns use a new project/volumes;
never truncate a retained dataset or revise expected counts after observing output.
Stop profiled test services with `--profile '*'`; an unprofiled Compose stop/down
may leave streaming services running. Failed-attempt evidence remains separate.

### Legacy options and proposed approval boundary

| Option | Preservation / possible gaps | Replay and retention risk | Verification, rollback and approval |
| --- | --- | --- | --- |
| A: supported historical continuation | Preserve both original filesystems, archives, offsets and published output. Missing log contents and damaged state remain unexplained. | Empty tail metadata can overlap already published output. No supported reconstruction of those missing contents has been demonstrated. | Work only from copies with matching Spark/Python versions. Require successful decoding and complete state/version chains plus output reconciliation. Currently blocked; do not fabricate/delete progress logs. Original archives remain rollback evidence. |
| B: bounded reconciliation/reconstruction | Preserve original history; create separate checkpoints, topics, SQL identities and search indexes. Produce an explicit gap/conflict ledger. | Requires all source offsets from each query's saved initial boundary to remain available. Different micro-batches/watermarks may change late-record inclusion; old random prediction IDs cannot be equated automatically. | Recheck every partition's retention bounds; replay copies into a shadow namespace; compare event IDs, window boundaries, all feature values and published payloads. No promotion while unexplained discrepancies remain. Roll back by stopping the shadow writers and retaining both datasets. Promotion requires owner approval. |
| C: approved clean cutover | Archive the complete historical environment and begin new state at a declared boundary. This does not recover historical state. | Old state may remain permanently unreadable; explicitly disclose missing history and avoid replay into the new identity namespace without reconciliation. | Requires owner attestation that excluded history is disposable synthetic data, or explicit acceptance of identified loss. Ownership is currently unknown, so this condition is unmet. Preserve old/new state separately and define read-only historical access. No deletion or replacement is authorized here. |

Recommendation: preserve the original environment and investigate **B in a shadow
namespace** if source retention still permits it. Keep A open only if a supported,
verifiable procedure becomes available. Do not select C merely because clean test
volumes pass. No legacy reconstruction, producer rerouting or cutover was executed.

Before a future cutover, obtain the data owner's disposition decision and approve
an exact source-offset/time boundary, gap ledger, identity mapping and rollback
point. Back up Kafka configuration/offset inventory, both Spark state filesystems,
SQL, search and provisioning configuration consistently; verify restore on copies.
Drain/stop old writers only under that approved plan. Initialize a distinct hardened
namespace, test mTLS identities/ACLs and replay policy, reconcile through the
boundary, and switch producers/readers in a controlled maintenance window. Retain
all old artifacts and record which history is visible in each system.

Rollback must preserve writes already accepted by the new deployment. Stop new
writers, reconcile that interval and restore matching tested image/state backups
on copies before rerouting. Do not point an older Spark binary at newer checkpoints,
downgrade a migrated monitoring database in place, or restart the damaged original
driver as an automatic rollback. Public deployment, production approval, destructive
migration and promotion to `main` remain outside this phase.


### Observed deterministic results

The final fresh application baseline used project `soc-phase4a-r4`; previous
attempts retain separate private diagnostics and are not substituted for this run.

| Controlled target outcome | Expected | Observed |
| --- | ---: | ---: |
| Source IDs, excluding watermark controls | 24 | 24 |
| Additional watermark controls | 8 | 8 |
| Enriched source identities | 24 | 24 |
| Five-minute event-type windows | 8 | 8 |
| Overlapping risk windows | 4 | 4 |
| Contract-2.0 windows, with all 18 values and source lineage | 4 | 4 |
| Unique durable predictions | 4 | 4 |
| Unique durable alerts | 44 | 44 |
| Unique target incidents | 4 | 4 |
| Target threat inputs | 46 | 46 |
| Target prediction / alert / incident search documents | 4 / 44 / 4 | 4 / 44 / 4 |
| Target threat search documents | 46 | 46 |
| Pending outbox rows after restored dependencies | 0 | 0 |

All four application query UUIDs survived driver SIGKILL/restart. Their source
next-offset maps reconciled to login 10, API 10, network 8 and anomaly 4 on partition
0 (32 total source/control records). The authenticated HTTPS alert API returned
all 44 controlled alerts and denied anonymous access. Three duplicate enriched
publications left SQL cardinalities at 24 enriched, 44 alerts and 46 threat inputs.

The separate-worker run interrupted a **nonempty** feature batch: stage 12,
partition 3, attempt 0 ran on worker `38f2b0cb3027`; after SIGKILL/recreation,
attempt 1 ran on worker `8b0fa561b095`. A further partition completed on the new
worker. Four checkpoint UUIDs survived a second driver application; sixteen
source IDs matched enriched identities, and six expected feature windows each
contained four events and 18 features after new input. Each stateful query retained
24 state-store delta files across the two applications. An initial probe overwrote
witness files and failed its evidence assertion; the corrected permanent probe
retains individual task attempts and the fresh rerun passed. No multiple-worker,
multi-host, broker-quorum or host-loss qualification is claimed.


## Phase 4C legacy-state reconstruction preflight (18 October 2026)

**Outcome: CONDITIONAL PASS for the authorized metadata-only scope.** The
preservation, retention and isolation preflight is complete, but ownership of the
original business namespaces remains unknown. No historical payload was replayed,
no shadow stack was started and no cutover was performed. Fresh-state recovery
results above are not evidence of legacy reconstruction.

### Preservation and environment inventory

The Git checkout was clean at baseline `74c3ac0596e0a375685775b916c6842c48632cdb`
on `dev`, tracking `origin/dev`; local `main` remained
`055399ab90c1f07315188bdd31d5ffaa118b815f`. Three Colima profiles exist:
`phase0-analytics` is running, `phase3c-security` is stopped, and
`phase4a-recovery` is running with all retained `soc-phase4a*` test projects
stopped. The original `streaming_analytics_system` project is still live on
`phase0-analytics`; its producer and data services continue to advance. Its Spark
driver `streaming-analytics-spark-streaming` remains exited, while the original
worker is running with its container-local historical state. The separate
`soc-hardened` project is stopped. Phase 4A/4B projects, volumes and credentials
remain separate from both original projects.

The driver and worker archives were streamed without extraction and checked
against every manifest row. The driver archive matched **1,886/1,886 files** and
the worker archive matched **15,208/15,208 files**, with zero missing, extra,
size-changed or hash-changed files. Their archive hashes remain:

- driver: `a4c08f4741f42b22a22cca57a62e61b52184c2376c175d6583afbb6493053578`
- worker: `809332d463f44f1b93bd26505f61a1d094e3a6ced29deb7f5d1820c886ab461e`

No original checkpoint, Docker volume, topic, consumer group, database, index,
container lifecycle state or offset was changed by this phase.

### Ownership and historical inventory

The isolated `spark-recovery-*`, `reliability-*` and `soc-phase4a*` fixtures are
confirmed synthetic. Generator code and test artifacts prove that synthetic data
also exists in the original business namespaces, but do not prove that every row
is synthetic. There is no evidence-backed set of confirmed non-synthetic records.
Consequently all records in the original `telemetry.*`, `analytics.*` and
`ml.anomaly.predictions` topics, original PostgreSQL business tables,
Elasticsearch business indexes, alerts/incidents and threat records
are **unknown provenance**. No source payload or potentially sensitive ledger was
exported. Replay requires owner authorization and a declared interval.

At the read-only 2026-10-18 04:01:59 UTC SQL snapshot, exact relevant row counts
were: telemetry 270,536; analytics metrics 14,330; risk metrics 26,710; feature
snapshots 19,521; predictions 47,452; alerts 10,942; incidents 5,500; threat-intel
events 259,779; IOC matches 20; dead letters 16; receipts 389,318; and outbox rows
512,016. The retained search outbox included `elasticsearch` 255,996 done / 4 pending
and legacy `es` 8 done. Counts establish inventory only;
they do not establish identity or semantic equality.

At 04:02:00 UTC Elasticsearch reported: telemetry 270,475; analytics-events
60,561; ML predictions 47,452; alerts 10,942; incidents 0; threat-intel events
259,721; IOC matches 20; attack timelines 259,721; detection-rule hits 27,682;
entity profiles 259,721; and dead letters 16. The original producer and consumers
were active across these separate observations. Differences such as SQL incidents
5,500 versus search incidents 0 are confirmed inventory differences; moving
telemetry/threat totals cannot be labeled loss or duplication without one approved
offset/time boundary. Matching totals likewise do not prove matching records.

### Current Kafka retention and Spark boundaries

Read-only broker metadata at 2026-10-18 04:01:19 UTC showed `cleanup.policy=delete`,
`retention.ms=604800000` (seven days), `retention.bytes=-1` and
`segment.ms=604800000` for every business source/output topic. All 11 source
partitions still retained offset zero. High watermarks, expressed as exclusive
offsets in partition order, were:

| Topic | Partitions | Retained range |
| --- | ---: | --- |
| `telemetry.login.events` | 3 | `[0,28852)`, `[0,28299)`, `[0,28748)` |
| `telemetry.api.events` | 3 | `[0,28379)`, `[0,28662)`, `[0,28592)` |
| `telemetry.network.events` | 3 | `[0,28412)`, `[0,28459)`, `[0,28453)` |
| `telemetry.anomaly.events` | 2 | `[0,6660)`, `[0,6791)` |

Metadata-only reads of the first and then-current last records placed retained
source timestamps between 2026-10-16 22:31:45.863 UTC and at least
2026-10-18 04:00:04.158 UTC. High watermarks continued advancing during the
inspection. A future replay must take a new atomic-enough inventory and freeze an
approved end boundary; these values are evidence for this preflight only.

The three original contract-predecessor query starts, in partition order, are
login `[34,25,32]`, API `[27,36,48]`, network `[28,28,39]`, and anomaly `[2,7]`.
The feature-v2 starts are login `[1075,1073,1093]`, API `[1124,1093,1152]`,
network `[1080,1078,1065]`, and anomaly `[270,264]`. No required starting offset
has expired. From these starts to the 04:01:19 high watermarks, 270,001 source
records are available to each older query and 259,940 are available to feature-v2.
These are replay-available records, not proven reconstructible outputs.

The last provably complete batch and its source next-offsets are:

| Query | Complete batch | Login | API | Network | Anomaly |
| --- | ---: | --- | --- | --- | --- |
| enriched | 180 | `[1520,1507,1527]` | `[1551,1503,1555]` | `[1511,1505,1484]` | `[394,370]` |
| window | 179 | `[1512,1505,1521]` | `[1545,1497,1548]` | `[1498,1491,1478]` | `[390,367]` |
| risk | 181 | `[1520,1507,1527]` | `[1551,1503,1555]` | `[1511,1505,1484]` | `[394,370]` |
| feature-v2 | 52 | `[1512,1505,1521]` | `[1545,1497,1548]` | `[1498,1491,1478]` | `[390,367]` |

The respective query UUIDs remain those recorded above. Empty progress tails are
enriched offsets/commits 181-182; window offsets 181-182 and commits 180-182;
risk offsets/commits 182-183; and feature-v2 offsets 54-55 plus commits 53-54.
Window state has one empty delta in each of six partitions at version 183; risk
has one in each partition at version 184. Feature-v2 has nonempty state through
version 55. The retained state schemas match current isolated schemas, but the
complete historical state-version chain has not been decoded by Spark. Only the
last roughly 100 progress entries remain for the older queries, so earlier
micro-batch and watermark decisions are absent. Exact continuation and exact
watermark-dependent reproduction remain unsupported.

Original output topics remain `[0,4821/4679/4794)` enriched (**14,294 records**),
`[0,16/20)` window (**36**), `[0,13438/13272)` risk (**26,710**),
`[0,9495/10027)` features (**19,522**) and `[0,15779/15855/15787)` predictions
(**47,421**). Preserved evidence shows publication overlapping the incomplete
tail. Which tail records escaped cannot be proved solely from empty commit names.
Legacy random prediction IDs, overlapping window identities, pre-contract-2.0
features and processing/watermark differences prevent count-only comparison.

### Reconstruction outcome and discrepancy boundary

For the unknown-provenance historical scope: **0 records are fully reconciled**,
**0 are proven reconstructible but not replayed**, **0 are proven partially
reconstructible**, and **0 are proven lost by Kafka retention**. The 270,001 and
259,940 source-record intervals above are available but remain **unknown/blocked
by ownership authorization and schema/semantic validation**. Exact original state
for the incomplete Spark tail is unrecoverable with the present progress and
watermark evidence; the number of affected logical outputs is unknown. No vague
completeness percentage is assigned.

The reusable reconciliation helper records identities, expected/observed behavior
and semantic digests without copying payloads. It distinguishes duplicate IDs,
overlapping windows, feature-contract mismatches, legacy prediction-ID drift,
partial retention, SQL/search differences and incomplete
outbox coverage. The ledger for a future approved replay must remain local and
excluded from Git when it contains historical identities.

### Proposed shadow reconstruction, cutover and rollback

An approved replay must use a new Compose project, consumer groups, output topics,
checkpoint root, PostgreSQL database/schema, Elasticsearch index prefix and
credential set. The recovery preflight rejects any overlap. Source
access should use a read-only principal where Kafka policy permits it; no original
consumer identity or sink is reused. The current hardened TLS, ACL, RBAC and
authentication controls remain mandatory.

The unexecuted procedure is:

1. Obtain owner attestation for the exact topics, offset interval and downstream
   records; classify sensitive and disposable synthetic data explicitly.
2. Record a new per-partition retention/configuration/timestamp inventory and an
   exact end boundary without resetting or draining anything outside an approved
   maintenance plan.
3. Back up and hash Kafka configuration, both Spark filesystems, PostgreSQL and
   Elasticsearch, retaining matching image digests.
4. Provision the fully disjoint shadow namespace and prove its ACLs cannot write
   original topics, schemas, indexes or volumes.
5. Replay only the authorized bounded offsets, preserving event IDs, event time,
   source coordinates, provenance and original contract version.
6. Reconcile by stable event/window identities and semantic payload digests;
   preserve overlapping windows and report legacy prediction IDs separately.
7. Compare PostgreSQL receipts/business rows, outbox effects and search documents;
   disposition every ledger entry and accepted gap in writing.
8. Run interruption/restart, duplicate publication, SQL/search convergence,
   authentication, monitoring and rollback acceptance tests on the shadow data.
9. Under a separately approved cutover window, define the producer/consumer
    boundary and reconcile writes accepted on both sides before routing changes.
10. Roll back by stopping shadow/new writers, retaining their state, reconciling
    all writes accepted after the boundary, and restoring matching old image/state
    copies. Never restart the damaged driver.
11. Abandon cutover on expired required offsets, archive mismatch, unresolved
    identity/payload discrepancies, schema semantics that cannot be proved,
    incomplete outbox evidence, isolation failure, failed recovery tests or
    inability to reconcile writes accepted during rollback.

Required future approvals are historical data ownership/replay authorization, any
accepted data-loss or semantic-gap disposition, a cutover maintenance window,
traffic/offset boundary, rollback policy
and production promotion. This phase grants none of them.
