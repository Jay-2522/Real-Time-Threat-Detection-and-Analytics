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

## Failure behavior

- Validation failures are sent to `telemetry.deadletter.events` and acknowledged only
  after the DLQ delivery succeeds.
- A database or publish failure stops offset advancement. On restart, Kafka replays
  the unacknowledged record or batch.
- Receipt payload conflicts fail closed instead of silently treating different data
  as a duplicate.
- Shutdown allows the bounded in-flight operation to finish; forced termination
  leaves it replayable.

These guarantees are covered by unit tests and opt-in live recovery tests. They do
not provide broker quorum availability, host-loss recovery, or a multi-node capacity
claim. Kafka retention and durable receipt retention still bound the replay horizon.
