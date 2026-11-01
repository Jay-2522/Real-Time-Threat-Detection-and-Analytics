from __future__ import annotations

import math
from datetime import datetime
from ipaddress import ip_address
from typing import Any

from app.features.feature_definitions import (
    FEATURE_COLUMNS, FEATURE_CONTRACT_VERSION, NULLABLE_BY_COUNT, COUNT_COLUMNS, RATE_COLUMNS,
)


def validate_features(raw: dict) -> dict[str, float]:
    if not isinstance(raw, dict) or set(raw) != set(FEATURE_COLUMNS):
        raise ValueError("Feature names must exactly match contract 2.0")
    result = {}
    for name in FEATURE_COLUMNS:
        value = raw[name]
        if value is None and name in NULLABLE_BY_COUNT:
            if raw[NULLABLE_BY_COUNT[name]] != 0:
                raise ValueError(f"Missing observed feature: {name}")
            # Explicit structural missingness. Counts are model inputs, not guessed measurements.
            result[name] = 0.0
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"Invalid numerical feature: {name}")
        if name in COUNT_COLUMNS and value != int(value):
            raise ValueError(f"Nonintegral count: {name}")
        if (name in RATE_COLUMNS and value > 1) or (name == "risk_score_moving_average" and value > 100):
            raise ValueError(f"Out-of-range feature: {name}")
        if name in NULLABLE_BY_COUNT and raw[NULLABLE_BY_COUNT[name]] == 0:
            raise ValueError(f"Unobserved feature must be null: {name}")
        result[name] = float(value)
    n, login, api, network, anomaly = (result[k] for k in FEATURE_COLUMNS[:5])
    if n <= 0 or n != login + api + network + anomaly:
        raise ValueError("Event counts are inconsistent")
    for child, parent in [("failed_auth_count", login), ("distinct_login_country_count", login),
                          ("unique_destination_ip_count", network), ("high_risk_network_event_count", network),
                          ("high_risk_event_count", n)]:
        if result[child] > parent:
            raise ValueError(f"Count exceeds scope: {child}")
    # Login country is optional (honeypot sensors do not report it); a network event always has a destination.
    if network > 0 and result["unique_destination_ip_count"] < 1:
        raise ValueError("Missing distinct observations")
    for name, expected in [("failed_login_rate", result["failed_auth_count"] / login if login else 0),
                           ("anomaly_rate", anomaly / n), ("requests_per_minute", api)]:
        if not math.isclose(result[name], expected, abs_tol=1e-9):
            raise ValueError(f"Inconsistent derived feature: {name}")
    if result["api_5xx_rate"] > result["endpoint_error_rate"]:
        raise ValueError("5xx rate exceeds overall error rate")
    return result


class FeatureMapper:
    def to_feature_vector(self, payload: dict[str, Any]) -> dict[str, float]:
        if not isinstance(payload, dict) or payload.get("feature_contract_version") != FEATURE_CONTRACT_VERSION:
            raise ValueError("Unsupported or missing feature contract version")
        if payload.get("entity_type") != "source_ip" or payload.get("event_type") != "feature_window":
            raise ValueError("Invalid feature entity/event scope")
        if not isinstance(payload.get("entity_id"), str):
            raise ValueError("Entity ID must be an IP string")
        ip_address(payload["entity_id"])
        if type(payload.get("invalid_event_count")) is not int or payload["invalid_event_count"] != 0:
            raise ValueError("Window contains invalid source measurements")
        times = {}
        for name in ("window_start", "window_end", "first_event_at", "last_event_at", "calculated_at"):
            if not isinstance(payload.get(name), str):
                raise ValueError(f"Missing or invalid timestamp: {name}")
            value = datetime.fromisoformat(payload[name].replace("Z", "+00:00"))
            if value.utcoffset() is None or value.utcoffset().total_seconds() != 0:
                raise ValueError("Contract timestamps must be UTC")
            times[name] = value
        start, end = times["window_start"], times["window_end"]
        if ((end - start).total_seconds() != 60 or start.timestamp() % 30 != 0
                or not start <= times["first_event_at"] <= times["last_event_at"] < end):
            raise ValueError("Invalid window boundaries or event timestamps")
        result = validate_features(payload["features"])
        ids = payload.get("source_event_ids")
        if not isinstance(ids, list) or len(ids) != result["event_count"] or any(not isinstance(v, str) or not v for v in ids):
            raise ValueError("Incomplete source event provenance")
        return result

    def entity_id(self, payload):
        return payload["entity_id"]

    def source_event_id(self, payload):
        return None  # A window has multiple sources; never pick an arbitrary event.

    def event_type(self, payload, source_topic):
        return payload["event_type"]
