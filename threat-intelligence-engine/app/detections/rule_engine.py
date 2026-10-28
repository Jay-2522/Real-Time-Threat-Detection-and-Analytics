from __future__ import annotations

import ipaddress
import logging
from collections import defaultdict, deque
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from app.schemas.threat import RuleHit
from app.utils.metrics import DETECTION_RULE_EXECUTIONS_TOTAL, DETECTION_RULE_HITS_TOTAL, DETECTION_RULE_LOADED

logger = logging.getLogger(__name__)

RFC1918_NETWORKS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
)


class DetectionRuleEngine:
    def __init__(self, rules_path: str) -> None:
        self._rules_path = Path(rules_path)
        self._rules: list[dict[str, Any]] = []
        self._failed_logins: dict[str, deque[datetime]] = defaultdict(deque)
        self.reload()

    def reload(self) -> None:
        rules: list[dict[str, Any]] = []
        for path in sorted(self._rules_path.glob("*.yaml")):
            with path.open("r", encoding="utf-8") as handle:
                rule = yaml.safe_load(handle) or {}
            if self._valid(rule) and rule["enabled"]:
                rules.append(rule)
            elif not self._valid(rule):
                logger.warning("invalid_detection_rule path=%s", path)
        self._rules = rules
        DETECTION_RULE_LOADED.clear()
        for rule in rules:
            DETECTION_RULE_LOADED.labels(str(rule["id"]), str(rule["name"]), str(rule["severity"])).set(1)
        logger.info("detection_rules_loaded count=%s path=%s", len(rules), self._rules_path)

    def evaluate(self, payload: dict[str, Any], risk_score: float) -> list[RuleHit]:
        hits: list[RuleHit] = []
        for rule in self._rules:
            rule_id = str(rule["id"])
            conditions = rule.get("conditions", {})
            min_risk = float(conditions.get("min_risk_score", 0))
            matched = (
                risk_score >= min_risk
                and self._matches_fields(payload, conditions)
                and self._matches_detector(payload, conditions)
            )
            DETECTION_RULE_EXECUTIONS_TOTAL.labels(rule_id, str(matched).lower()).inc()
            if matched:
                hit = RuleHit(rule_id=rule_id, rule_name=rule["name"], severity=rule["severity"], tactic=rule["tactic"], technique=rule["technique"], mitre_id=rule["mitre_id"], score=float(rule.get("score", min_risk)))
                hits.append(hit)
                DETECTION_RULE_HITS_TOTAL.labels(rule_id, hit.severity).inc()
        return hits

    @classmethod
    def _matches_fields(cls, payload: dict[str, Any], conditions: dict[str, Any]) -> bool:
        for field, expected in conditions.get("field_equals", {}).items():
            values = expected if isinstance(expected, list) else [expected]
            if not any(cls._equal(payload.get(field), value) for value in values):
                return False
        for field, expected in conditions.get("field_contains", {}).items():
            values = expected if isinstance(expected, list) else [expected]
            actual = payload.get(field)
            if actual is None or not any(str(value).lower() in str(actual).lower() for value in values):
                return False
        return True

    def _matches_detector(self, payload: dict[str, Any], conditions: dict[str, Any]) -> bool:
        detector = conditions.get("detector")
        if detector is None:
            return True
        if detector == "failed_login_burst":
            return self._failed_login_burst(payload, conditions)
        if detector == "private_remote_service":
            return self._private_remote_service(payload, conditions)
        logger.warning("unknown_detection_rule_detector detector=%s", detector)
        return False

    def _failed_login_burst(self, payload: dict[str, Any], conditions: dict[str, Any]) -> bool:
        source_ip = payload.get("source_ip")
        if not source_ip:
            return False
        observed_at = self._timestamp(payload.get("timestamp") or payload.get("event_timestamp"))
        window = timedelta(seconds=int(conditions.get("window_seconds", 300)))
        threshold = int(conditions.get("threshold", 5))
        observations = self._failed_logins[str(source_ip)]
        while observations and observed_at - observations[0] > window:
            observations.popleft()
        observations.append(observed_at)
        return len(observations) >= threshold

    @staticmethod
    def _private_remote_service(payload: dict[str, Any], conditions: dict[str, Any]) -> bool:
        try:
            source = ipaddress.ip_address(str(payload.get("source_ip")))
            destination = ipaddress.ip_address(str(payload.get("destination_ip")))
            port = int(payload.get("port"))
        except (TypeError, ValueError):
            return False
        return DetectionRuleEngine._is_rfc1918(source) and DetectionRuleEngine._is_rfc1918(destination) and port in {
            int(value) for value in conditions.get("ports", [])
        }

    @staticmethod
    def _is_rfc1918(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
        return address.version == 4 and any(address in network for network in RFC1918_NETWORKS)

    @staticmethod
    def _timestamp(value: Any) -> datetime:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                parsed = datetime.now(UTC)
        else:
            parsed = datetime.now(UTC)
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)

    @staticmethod
    def _equal(actual: Any, expected: Any) -> bool:
        if isinstance(actual, str) and isinstance(expected, str):
            return actual.casefold() == expected.casefold()
        return actual == expected

    @staticmethod
    def _valid(rule: dict[str, Any]) -> bool:
        required = {"id", "name", "enabled", "severity", "tactic", "technique", "mitre_id", "conditions"}
        return required.issubset(rule.keys()) and isinstance(rule.get("enabled"), bool)
