"""Indices must exist with their intended mapping before the first document, even when Elasticsearch was down at
startup. Otherwise the first write creates them with a dynamic mapping (text instead of keyword, string instead of ip)."""
import importlib
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline_runtime.search import ensure_indices

ROOT = Path(__file__).resolve().parents[2]


class FakeElasticsearch:
    def __init__(self, reachable):
        self.reachable = reachable
        self.created = {}
        self.calls = []
        self.indices = SimpleNamespace(exists=self._exists, create=self._create)

    def _require_connection(self):
        if not self.reachable:
            raise ConnectionError("connection refused")

    def _exists(self, index):
        self._require_connection()
        return index in self.created

    def _create(self, index, mappings):
        self._require_connection()
        self.calls.append(("create", index))
        self.created[index] = mappings

    def index(self, **kwargs):
        self._require_connection()
        self.calls.append(("index", kwargs["index"]))


def load_indexer(service):
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path.insert(0, str(ROOT / service))
    return importlib.import_module("app.elastic.indexer")


def storage_write(indexer):
    record = SimpleNamespace(record_kind="telemetry", timestamp_or_created=datetime.now(UTC), event_type="login",
                             entity_id="u-1", raw_payload={"source_ip": "10.0.0.1"}, risk_score=1.0,
                             ml_risk_score=None, severity="low", explanation=None, source_topic="telemetry.login.events")
    indexer.index(record, "key-1")


def alert_write(indexer):
    indexer.index_alert(SimpleNamespace(alert_id="alert-1", model_dump=lambda mode: {}))


def threat_write(indexer):
    indexer.index(SimpleNamespace(model_dump=lambda mode: {}, iocs=[], rule_hits=[], entity_id=None, event_id="event-1"))


CASES = {
    "storage-sink": (lambda module: object.__new__(module.ElasticsearchIndexer), storage_write, 4),
    "alert-engine": (lambda module: object.__new__(module.AlertIndexer), alert_write, 2),
    "threat-intelligence-engine": (lambda module: object.__new__(module.ThreatIndexer), threat_write, 5),
}


def test_ensure_indices_creates_only_missing_indices_and_tolerates_a_concurrent_creator():
    client = FakeElasticsearch(reachable=True)
    client.created["existing"] = {}
    ensure_indices(client, {"existing": {"a": 1}, "new": {"b": 2}})
    assert client.calls == [("create", "new")] and client.created["new"] == {"b": 2}

    def lost_race(index, mappings):
        raise RuntimeError("BadRequestError(400, 'resource_already_exists_exception')")

    client.indices.create = lost_race
    ensure_indices(client, {"raced": {}})  # another process created it first: fine

    def rejected(index, mappings):
        raise RuntimeError("mapper_parsing_exception")

    client.indices.create = rejected
    with pytest.raises(RuntimeError, match="mapper_parsing_exception"):
        ensure_indices(client, {"broken": {}})


@pytest.mark.parametrize("service", CASES)
def test_indices_are_created_with_their_mapping_on_the_first_write_after_elasticsearch_returns(service):
    build, write, index_count = CASES[service]
    indexer = build(load_indexer(service))
    indexer._indices_ready = False
    indexer._client = FakeElasticsearch(reachable=False)

    with pytest.raises(ConnectionError):  # started while Elasticsearch was down: nothing may be created implicitly
        write(indexer)
    assert indexer._client.calls == [] and not indexer._indices_ready

    indexer._client.reachable = True
    write(indexer)
    kinds = [kind for kind, _name in indexer._client.calls]
    assert kinds == ["create"] * index_count + ["index"]  # every index exists, with a mapping, before the first document
    assert all("entity_id" in mapping["properties"] or "source_ip" in mapping["properties"]
               for mapping in indexer._client.created.values())
    assert all(mapping["properties"].get("entity_id", {"type": "keyword"})["type"] == "keyword"
               for mapping in indexer._client.created.values())

    indexer._client.calls.clear()
    write(indexer)
    assert [kind for kind, _name in indexer._client.calls] == ["index"]  # creation is not repeated per write
