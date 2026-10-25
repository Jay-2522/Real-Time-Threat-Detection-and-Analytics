"""Fail-closed helpers for bounded historical reconciliation.

The helpers deliberately operate on caller-supplied metadata and records.  They do
not connect to Kafka or mutate a source system, and discrepancy output contains
identities and digests rather than business payloads.
"""

from __future__ import annotations

import hashlib
import json
import tarfile
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path


class RecoveryBlocked(RuntimeError):
    """The requested recovery cannot be proved safe with the supplied evidence."""


@dataclass(frozen=True)
class Discrepancy:
    record_identity: str
    expected_behavior: str
    observed_behavior: str
    reason: str

    def as_dict(self):
        return asdict(self)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def semantic_digest(record, ignored_fields=()):
    ignored = set(ignored_fields)
    filtered = {key: value for key, value in record.items() if key not in ignored}
    return hashlib.sha256(_canonical(filtered).encode()).hexdigest()


def stable_identity(record, fields):
    missing = [field for field in fields if field not in record]
    if missing:
        raise RecoveryBlocked(f"record is missing identity fields: {', '.join(missing)}")
    values = [record[field] for field in fields]
    return str(values[0]) if len(values) == 1 else _canonical(values)


def window_identity(record):
    """Keep legitimate overlapping windows distinct."""
    dimensions = ["window_start", "window_end"]
    dimensions += [field for field in ("event_type", "entity_id", "source_ip") if field in record]
    return stable_identity(record, dimensions)


def compare_records(expected, observed, identity_fields, ignored_fields=()):
    """Return a deterministic, payload-free discrepancy ledger."""
    groups = []
    for records in (expected, observed):
        grouped = defaultdict(list)
        for record in records:
            grouped[stable_identity(record, identity_fields)].append(record)
        groups.append(grouped)
    expected_by_id, observed_by_id = groups
    ledger = []
    for identity in sorted(set(expected_by_id) | set(observed_by_id)):
        left = expected_by_id.get(identity, [])
        right = observed_by_id.get(identity, [])
        if len(left) > 1:
            ledger.append(Discrepancy(identity, "one expected identity", f"{len(left)} records", "duplicate_expected_identity"))
        if len(right) > 1:
            ledger.append(Discrepancy(identity, "one observed identity", f"{len(right)} records", "duplicate_observed_identity"))
        if not left:
            ledger.append(Discrepancy(identity, "absent", "present", "unexpected_record"))
        elif not right:
            ledger.append(Discrepancy(identity, "present", "absent", "missing_record"))
        elif len(left) == len(right) == 1:
            expected_hash = semantic_digest(left[0], ignored_fields)
            observed_hash = semantic_digest(right[0], ignored_fields)
            if expected_hash != observed_hash:
                ledger.append(Discrepancy(identity, expected_hash, observed_hash, "semantic_payload_mismatch"))
    return ledger


def compare_predictions(expected, observed):
    """Compare legacy predictions by lineage while reporting random-ID drift."""
    identity_fields = ("window_start", "window_end", "entity_id", "model_version")
    ledger = compare_records(expected, observed, identity_fields, ignored_fields=("prediction_id",))
    left = {stable_identity(row, identity_fields): row for row in expected}
    right = {stable_identity(row, identity_fields): row for row in observed}
    for identity in sorted(left.keys() & right.keys()):
        if left[identity].get("prediction_id") != right[identity].get("prediction_id"):
            ledger.append(Discrepancy(identity, "historical ID retained", "different ID", "prediction_identity_difference"))
    return sorted(ledger, key=lambda item: (item.record_identity, item.reason))


def assess_offset_coverage(required, available):
    """Require earliest <= next offset <= the exclusive high watermark."""
    result = []
    for topic, partitions in sorted(required.items()):
        for partition, required_offset in sorted(partitions.items(), key=lambda item: int(item[0])):
            bounds = available.get(topic, {}).get(str(partition))
            if bounds is None:
                result.append(Discrepancy(f"{topic}/{partition}", str(required_offset), "unknown", "partition_metadata_missing"))
                continue
            earliest, latest = bounds
            if required_offset < earliest:
                result.append(Discrepancy(
                    f"{topic}/{partition}", str(required_offset), f"[{earliest},{latest})",
                    f"expired_offset_range:{required_offset}-{earliest - 1}",
                ))
            elif required_offset > latest:
                result.append(Discrepancy(f"{topic}/{partition}", str(required_offset), f"[{earliest},{latest})", "required_offset_beyond_high_watermark"))
    return result


def contract_mismatches(records, expected_version="2.0"):
    ledger = []
    for record in records:
        identity = str(record.get("event_id") or record.get("window_id") or "unknown")
        actual = record.get("feature_contract_version")
        if actual != expected_version:
            ledger.append(Discrepancy(identity, expected_version, str(actual), "feature_contract_version_mismatch"))
    return ledger


def compare_identity_sets(sql_ids, search_ids, label="sql/search"):
    expected = [{"identity": value} for value in sql_ids]
    observed = [{"identity": value} for value in search_ids]
    ledger = compare_records(expected, observed, ("identity",))
    return [Discrepancy(x.record_identity, x.expected_behavior, x.observed_behavior, f"{label}:{x.reason}") for x in ledger]


def durable_outbox_gaps(business_ids, outbox_ids):
    counts = Counter(outbox_ids)
    ledger = []
    for identity in sorted(set(business_ids)):
        count = counts[identity]
        if count != 1:
            ledger.append(Discrepancy(str(identity), "one durable effect", f"{count} effects", "incomplete_durable_outbox_history"))
    return ledger


def validate_shadow_isolation(original, shadow):
    """Require every state and identity namespace to differ before replay."""
    required = ("compose_project", "consumer_group", "checkpoint", "database", "elasticsearch_index", "credential_set")
    for field in required:
        if not original.get(field) or not shadow.get(field) or original[field] == shadow[field]:
            raise RecoveryBlocked(f"shadow {field} is absent or overlaps original state")
    original_topics = set(original.get("output_topics", ()))
    shadow_topics = set(shadow.get("output_topics", ()))
    if not shadow_topics or original_topics & shadow_topics:
        raise RecoveryBlocked("shadow output topics are absent or overlap original topics")


def verify_archive_manifest(archive_path, manifest_path):
    """Stream a tar archive and verify the recorded file inventory without extraction."""
    expected_rows = json.loads(Path(manifest_path).read_text())
    expected = {row["path"].removeprefix("./"): (row["size"], row["sha256"]) for row in expected_rows}
    actual = {}
    unsafe_members = []
    with tarfile.open(archive_path, "r") as archive:
        for member in archive:
            if not (member.isfile() or member.isdir()):
                unsafe_members.append(member.name)
                continue
            if not member.isfile():
                continue
            name = member.name.removeprefix("./")
            if name in actual:
                raise RecoveryBlocked(f"archive contains duplicate member: {name}")
            stream = archive.extractfile(member)
            digest = hashlib.sha256()
            size = 0
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
            actual[name] = (size, digest.hexdigest())
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    changed = sorted(name for name in expected.keys() & actual.keys() if expected[name] != actual[name])
    if missing or extra or changed or unsafe_members:
        raise RecoveryBlocked(
            f"archive manifest mismatch: missing={missing}, extra={extra}, "
            f"changed={changed}, unsafe={unsafe_members}"
        )
    return {"files": len(actual), "bytes": sum(size for size, _ in actual.values())}
