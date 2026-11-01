# Kafka delivery contract

The application contract is at-least-once consumption with idempotent durable
effects. A Kafka offset is committed only after the corresponding PostgreSQL
transaction commits. A crash after the database commit but before the offset commit
therefore replays an input; deterministic identities and receipt claims make that
replay harmless.

## Transaction boundaries

- Storage batches receipt claims, business-table upserts, and Elasticsearch outbox
  entries in one PostgreSQL transaction. It commits the highest processed offset per
  partition only after that transaction succeeds.
- Alert processing holds the receipt claim, cooldown decision, correlation lock,
  alert, incident link, and outbox entry in one transaction.
- Threat-intelligence durable effects use the same receipt/outbox pattern.
- ML publishes with a stable prediction identity. Invalid records go to the shared
  dead-letter topic with a deterministic failure identity.

Kafka producer idempotence covers broker retries; it does not replace application
idempotency. PostgreSQL is the source of truth. Elasticsearch writes are retryable
outbox work with deterministic document IDs, so search may lag without losing the
authoritative row.

When Elasticsearch is unreachable the outbox worker stops writing to it and probes
with a single row after 5, 10, 20, 40, then every 60 seconds, instead of retrying every
pending row; a bad document is still retried on its own. The backlog is capped at
`PIPELINE_OUTBOX_MAX_PENDING` (default 500000 rows per service). Past the cap the oldest
rows are marked done with a `dropped:` note and counted in
`pipeline_outbox_dropped_total`; the search index lacks them until it is rebuilt from
PostgreSQL. `pipeline_outbox_target_unavailable` is 1 while a target is being probed.

Each indexer creates its indices with their intended mappings before its first write,
and retries that until it succeeds, so Elasticsearch may start after the services. An
index that was created implicitly earlier keeps its dynamic mapping; delete it and
rebuild it from PostgreSQL to apply the intended one.

## Failure behavior

- Validation failures are sent to `telemetry.deadletter.events` and acknowledged only
  after the DLQ delivery succeeds.
- A database or publish failure stops offset advancement. On restart, Kafka replays
  the unacknowledged record or batch.
- A lost database connection or database timeout is transient: the consumer retries
  the same record or batch in place, with backoff up to 30 seconds, and commits only
  after it succeeds. The retry window is 240 seconds, shorter than
  `max.poll.interval.ms`, so the consumer keeps its partitions. Past that window, or
  for any other error, the process exits and restarts as described above.
  `pipeline_transient_retries_total` counts these retries.
- Receipt payload conflicts fail closed instead of silently treating different data
  as a duplicate.
- Shutdown allows the bounded in-flight operation to finish; forced termination
  leaves it replayable.

These guarantees are covered by unit tests and opt-in live recovery tests. They do
not provide broker quorum availability, host-loss recovery, or a multi-node capacity
claim. Kafka retention and durable receipt retention still bound the replay horizon.
