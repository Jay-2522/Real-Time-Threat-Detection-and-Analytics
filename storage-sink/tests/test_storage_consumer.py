import json
import sys
from pathlib import Path
from types import SimpleNamespace


for name in list(sys.modules):
    if name == "app" or name.startswith("app."):
        del sys.modules[name]
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.consumers.storage_consumer as storage_module
from app.consumers.storage_consumer import StorageSinkConsumer
from app.schemas.records import StorageRecord
from pipeline_runtime.durable import BatchClaimResult


class ConfirmedProducer:
    def __init__(self):
        self.messages = []

    def produce(self, **message):
        self.messages.append(message)
        message["callback"](None, object())

    def flush(self, _timeout):
        return 0


class TransactionalSession:
    def __init__(self, transactions):
        self.transactions = transactions

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def begin(self):
        transactions = self.transactions

        class Transaction:
            def __enter__(self):
                transactions.append("begin")

            def __exit__(self, *_args):
                transactions.append("commit")
                return False

        return Transaction()


class RecordingRepository:
    def __init__(self):
        self.batches = []

    def persist_many(self, _session, records):
        self.batches.append(records)


def record(event_id):
    payload = {"event_id": event_id, "event_type": "login", "source_ip": "192.0.2.10"}
    return StorageRecord(
        source_topic="telemetry.login.events",
        raw_payload=payload,
        record_kind="telemetry",
        event_id=event_id,
    )


def batch_consumer(monkeypatch):
    consumer = StorageSinkConsumer.__new__(StorageSinkConsumer)
    transactions = []
    repository = RecordingRepository()
    consumer._session_factory = lambda: TransactionalSession(transactions)
    consumer._repository = repository
    outbox = []
    monkeypatch.setattr(storage_module, "enqueue_many", lambda _session, _service, _target, payloads: outbox.append(payloads))
    return consumer, repository, transactions, outbox


def test_batch_persists_business_rows_and_outbox_in_one_transaction(monkeypatch):
    consumer, repository, transactions, outbox = batch_consumer(monkeypatch)
    monkeypatch.setattr(
        storage_module,
        "claim_many",
        lambda _session, _service, payloads: BatchClaimResult(set(payloads), set(), set()),
    )

    consumer._persist_batch([(object(), record("one"), "one"), (object(), record("two"), "two")])

    assert transactions == ["begin", "commit"]
    assert [[item.event_id for item in batch] for batch in repository.batches] == [["one", "two"]]
    assert all(item.timestamp is not None for item in repository.batches[0])
    assert [set(batch) for batch in outbox] == [{"one", "two"}]


def test_duplicate_batch_is_idempotent(monkeypatch):
    consumer, repository, transactions, outbox = batch_consumer(monkeypatch)
    claimed = set()

    def claims(_session, _service, payloads):
        inserted = set(payloads) - claimed
        duplicates = set(payloads) & claimed
        claimed.update(inserted)
        return BatchClaimResult(inserted, duplicates, set())

    monkeypatch.setattr(storage_module, "claim_many", claims)
    batch = [(object(), record("one"), "one"), (object(), record("two"), "two")]

    consumer._persist_batch(batch)
    consumer._persist_batch(batch)

    assert sum(len(items) for items in repository.batches) == 2
    assert sum(len(items) for items in outbox) == 2
    assert transactions == ["begin", "commit", "begin", "commit"]


def test_malformed_raw_event_is_published_to_deadletter_topic():
    consumer = StorageSinkConsumer.__new__(StorageSinkConsumer)
    consumer._settings = SimpleNamespace(deadletter_topic="telemetry.deadletter.events")
    consumer._deadletter_producer = ConfirmedProducer()
    persisted = []
    consumer._persist = lambda record, key: persisted.append((record, key))

    consumer._handle_message(
        "telemetry.login.events",
        b"not-json",
        {"topic": "telemetry.login.events", "partition": 0, "offset": 7},
    )

    assert len(consumer._deadletter_producer.messages) == 1
    message = consumer._deadletter_producer.messages[0]
    assert message["topic"] == "telemetry.deadletter.events"
    body = json.loads(message["value"])
    assert body["source_topic"] == "telemetry.login.events"
    assert body["raw_hex"] == b"not-json".hex()
    assert len(persisted) == 1


def test_malformed_deadletter_is_not_republished_recursively():
    consumer = StorageSinkConsumer.__new__(StorageSinkConsumer)
    consumer._settings = SimpleNamespace(deadletter_topic="telemetry.deadletter.events")
    consumer._deadletter_producer = ConfirmedProducer()
    consumer._persist = lambda record, key: None

    consumer._handle_message("telemetry.deadletter.events", b"not-json", {"partition": 0, "offset": 8})

    assert consumer._deadletter_producer.messages == []
