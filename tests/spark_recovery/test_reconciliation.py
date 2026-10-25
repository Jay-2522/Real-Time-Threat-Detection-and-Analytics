"""Permanent fail-closed regressions for legacy reconstruction preflight."""

import importlib.util
import io
import json
import sys
import tarfile
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location(
    "reconciliation",
    Path(__file__).parents[2] / "spark-jobs/app/utils/reconciliation.py",
)
reconcile = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = reconcile
spec.loader.exec_module(reconcile)


def test_kafka_retention_boundary_detection():
    assert reconcile.assess_offset_coverage({"source": {"0": 4, "1": 10}}, {"source": {"0": [4, 10], "1": [4, 10]}}) == []


def test_missing_historical_offsets_are_precise():
    ledger = reconcile.assess_offset_coverage({"source": {"0": 2}}, {"source": {"0": [4, 10]}})
    assert ledger[0].reason == "expired_offset_range:2-3"


def test_duplicate_event_identities_are_not_hidden_by_counts():
    expected = [{"event_id": "a", "value": 1}]
    observed = [{"event_id": "a", "value": 1}, {"event_id": "a", "value": 1}]
    assert [x.reason for x in reconcile.compare_records(expected, observed, ("event_id",))] == ["duplicate_observed_identity"]


def test_overlapping_spark_windows_remain_distinct():
    rows = [
        {"window_start": 0, "window_end": 5, "entity_id": "a"},
        {"window_start": 1, "window_end": 6, "entity_id": "a"},
    ]
    assert len({reconcile.window_identity(row) for row in rows}) == 2


def test_feature_contract_mismatch_fails_closed():
    ledger = reconcile.contract_mismatches([{"event_id": "a", "feature_contract_version": "1.0"}])
    assert ledger[0].reason == "feature_contract_version_mismatch"


def test_historical_prediction_identity_difference_is_explicit():
    base = {"window_start": 0, "window_end": 5, "entity_id": "a", "model_version": "m1", "score": 0.5}
    ledger = reconcile.compare_predictions([{**base, "prediction_id": "old"}], [{**base, "prediction_id": "new"}])
    assert [x.reason for x in ledger] == ["prediction_identity_difference"]


def test_partial_source_history_reports_only_expired_partition():
    required = {"a": {"0": 0}, "b": {"0": 5}}
    available = {"a": {"0": [3, 9]}, "b": {"0": [0, 8]}}
    assert [x.record_identity for x in reconcile.assess_offset_coverage(required, available)] == ["a/0"]


def test_inconsistent_sql_search_records_are_identified():
    ledger = reconcile.compare_identity_sets({"a", "b"}, {"b", "c"})
    assert {(x.record_identity, x.reason) for x in ledger} == {
        ("a", "sql/search:missing_record"), ("c", "sql/search:unexpected_record")
    }


def test_incomplete_durable_outbox_history_blocks_counter_rebuild():
    ledger = reconcile.durable_outbox_gaps({"a", "b"}, ["a"])
    assert [(x.record_identity, x.observed_behavior) for x in ledger] == [("b", "0 effects")]


def test_shadow_output_isolation_rejects_original_topic():
    original = {key: f"old-{key}" for key in ("compose_project", "consumer_group", "checkpoint", "database", "elasticsearch_index", "credential_set")}
    original["output_topics"] = ["analytics.enriched.events"]
    shadow = {key: f"shadow-{key}" for key in original if key != "output_topics"}
    shadow["output_topics"] = ["analytics.enriched.events"]
    with pytest.raises(reconcile.RecoveryBlocked):
        reconcile.validate_shadow_isolation(original, shadow)


def test_reconciliation_restart_is_deterministic_after_interruption():
    expected = [{"event_id": "a", "value": 1}, {"event_id": "b", "value": 2}]
    observed = [{"event_id": "a", "value": 9}]
    first = reconcile.compare_records(expected, observed, ("event_id",))
    restarted = reconcile.compare_records(list(expected), list(observed), ("event_id",))
    assert [x.as_dict() for x in first] == [x.as_dict() for x in restarted]


def test_archive_verification_never_modifies_original_state(tmp_path):
    archive_path = tmp_path / "state.tar"
    payload = b"preserved"
    with tarfile.open(archive_path, "w") as archive:
        info = tarfile.TarInfo("checkpoint/offsets/0")
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))
    import hashlib
    manifest = [{"path": "./checkpoint/offsets/0", "size": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}]
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    before = archive_path.read_bytes()
    assert reconcile.verify_archive_manifest(archive_path, manifest_path) == {"files": 1, "bytes": len(payload)}
    assert archive_path.read_bytes() == before


def test_archive_verification_rejects_manifest_mismatch(tmp_path):
    archive_path = tmp_path / "state.tar"
    with tarfile.open(archive_path, "w") as archive:
        info = tarfile.TarInfo("state")
        info.size = 1
        archive.addfile(info, io.BytesIO(b"x"))
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps([{"path": "./state", "size": 1, "sha256": "0" * 64}]))
    with pytest.raises(reconcile.RecoveryBlocked):
        reconcile.verify_archive_manifest(archive_path, manifest_path)
