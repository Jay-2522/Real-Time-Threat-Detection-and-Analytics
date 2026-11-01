"""Shared synthetic enterprise traffic model.

One model drives both the telemetry producer and ML training, so the model is
trained on the same distribution it later scores. It simulates a small company
(stable users, workstations and servers) with realistic benign behaviour and six
labelled attack scenarios, and it mirrors the Spark source-IP/60-second feature
windows so training windows can be built without Spark.

The data is synthetic. Quality measured on it says nothing about real traffic.
"""
from __future__ import annotations

import ipaddress
import math
import random
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Iterator

# Bump when the simulated behaviour changes; stored in model metadata so a model
# trained on an older profile is retrained instead of silently reused.
PROFILE_VERSION = "3"

HOME_COUNTRIES = ["United States", "India", "Germany", "United Kingdom", "Singapore", "Brazil", "Netherlands"]
# Names match detections/rules/high_risk_geo_login.yaml.
HIGH_RISK_COUNTRIES = ["North Korea", "Russia", "Iran"]
SCENARIOS = ("brute_force", "api_abuse", "lateral_movement", "data_exfiltration", "dns_beaconing", "geo_login")
_SCENARIO_WEIGHTS = (25, 15, 15, 15, 15, 15)
_MEAN_SCENARIO_EVENTS = 13.0
_FIRST_NAMES = [
    "alex.chen", "maya.singh", "nora.patel", "sam.roberts", "lina.garcia", "omar.hassan", "li.wei",
    "carla.mendes", "jonas.weber", "priya.nair", "tom.becker", "ana.silva", "kenji.sato", "ella.fischer",
    "raj.mehta", "zoe.clark", "ivan.petrov", "mia.rossi", "noah.king", "sara.lopez",
]
_ENDPOINTS = ["/api/v1/accounts", "/api/v1/payments", "/api/v1/admin/users", "/api/v1/reports"]
_ATTACK_USERNAMES = ["root", "admin", "administrator", "ubuntu", "test", "oracle", "postgres", "guest"]
_DEVICES = ["windows-laptop", "macbook", "linux-workstation", "ios", "android"]
_ANOMALY_TYPE = {
    "brute_force": "credential_stuffing", "data_exfiltration": "data_exfiltration",
    "geo_login": "impossible_travel", "api_abuse": "api_abuse",
}
_ANOMALY_TEXT = {
    "credential_stuffing": "Elevated login failures observed across user identities.",
    "data_exfiltration": "Outbound transfer volume exceeded baseline threshold.",
    "impossible_travel": "User activity observed from geographically distant regions.",
    "api_abuse": "Sustained high-volume API access pattern detected.",
}
_BENIGN_FAILURE_CAP = 3  # a user never fails more than this per source IP per 5 minutes


@dataclass(frozen=True)
class RawEvent:
    """JSON-ready event fields (schema-compatible, without id) plus ground truth."""
    fields: dict[str, Any]
    scenario: str | None = None
    scenario_id: str | None = None


@dataclass(frozen=True)
class User:
    user_id: str
    username: str
    country: str
    office_ip: str
    workstation_ip: str
    device: str


def _public_ip(rng: random.Random) -> str:
    while True:
        address = ipaddress.IPv4Address(rng.randint(int(ipaddress.IPv4Address("23.0.0.1")), int(ipaddress.IPv4Address("223.255.255.254"))))
        if address.is_global and not address.is_multicast and not address.is_reserved:
            return str(address)


def _lognormal(rng: random.Random, median: float, sigma: float, low: float, high: float) -> float:
    return min(high, max(low, rng.lognormvariate(math.log(median), sigma)))


class TrafficModel:
    """One call to :meth:`next_event` returns one event; attack bursts interleave benign traffic."""

    def __init__(self, anomaly_rate: float = 0.05, seed: int | None = None) -> None:
        self._rng = random.Random(seed)
        self._anomaly_rate = anomaly_rate
        pop = random.Random(0)  # fixed population: the same company in every run
        self.users = [
            User(f"u-{1001 + index}", name, pop.choice(HOME_COUNTRIES), _public_ip(pop),
                 f"10.10.{index // 250}.{index % 250 + 10}", pop.choice(_DEVICES))
            for index, name in enumerate(_FIRST_NAMES)
        ]
        self.servers = [f"10.20.0.{20 + index}" for index in range(10)]
        self._destinations = [_public_ip(pop) for _ in range(30)]
        self._active: list[tuple[str, str, Iterator[dict[str, Any]]]] = []
        self._scenario_counter = 0
        self._failures: dict[str, deque[datetime]] = defaultdict(deque)

    # -- public ---------------------------------------------------------------
    def next_event(self, now: datetime) -> RawEvent:
        rng = self._rng
        if rng.random() < self._anomaly_rate / _MEAN_SCENARIO_EVENTS:
            self._start_scenario()
        if self._active and rng.random() < 0.6:
            index = rng.randrange(len(self._active))
            name, scenario_id, steps = self._active[index]
            try:
                return RawEvent({**next(steps), "timestamp": now}, name, scenario_id)
            except StopIteration:
                self._active.pop(index)
        return RawEvent({**self._benign(now), "timestamp": now})

    # -- benign ---------------------------------------------------------------
    def _benign(self, now: datetime) -> dict[str, Any]:
        kind = self._rng.choices(("login", "api", "network"), weights=(25, 40, 35))[0]
        return getattr(self, f"_benign_{kind}")(now)

    def _benign_login(self, now: datetime) -> dict[str, Any]:
        rng = self._rng
        user = rng.choice(self.users)
        travelling = rng.random() < 0.02
        source_ip = _public_ip(rng) if travelling else user.office_ip
        country = rng.choice(HOME_COUNTRIES) if travelling else user.country
        failures = self._failures[source_ip]
        while failures and (now - failures[0]).total_seconds() > 300:
            failures.popleft()
        success = rng.random() > 0.03 or len(failures) >= _BENIGN_FAILURE_CAP
        if not success:
            failures.append(now)
        return {
            "event_type": "login", "user_id": user.user_id, "username": user.username, "source_ip": source_ip,
            "country": country, "device_type": user.device, "login_success": success,
            "failure_reason": None if success else rng.choice(["invalid_password", "mfa_failed"]),
            "risk_score": round(rng.uniform(3, 35) if success else rng.uniform(20, 55), 2),
        }

    def _benign_api(self, now: datetime) -> dict[str, Any]:
        rng = self._rng
        user = rng.choice(self.users)
        status = rng.choices([200, 201, 204, 400, 401, 403, 404, 429, 500, 503],
                             weights=[60, 7, 9, 3, 3, 2, 3, 1, 0.8, 0.4])[0]
        return {
            "event_type": "api", "user_id": user.user_id, "endpoint": rng.choice(_ENDPOINTS),
            "method": rng.choice(["GET", "GET", "GET", "POST", "PUT", "PATCH", "DELETE"]), "status_code": status,
            "response_time_ms": int(_lognormal(rng, 110, 0.7, 5, 4000)), "source_ip": user.office_ip,
            "request_size": int(_lognormal(rng, 4000, 1.0, 100, 250_000)),
            "risk_score": round(rng.uniform(15, 55) if status >= 400 else rng.uniform(3, 35), 2),
        }

    def _benign_network(self, now: datetime) -> dict[str, Any]:
        rng = self._rng
        user = rng.choice(self.users)
        internal = rng.random() < 0.15
        if internal:  # database/search/web traffic to servers; never remote-administration ports
            destination, port = rng.choice(self.servers), rng.choice([443, 5432, 8080, 9200])
        else:
            destination = rng.choice(self._destinations)
            port = rng.choices([443, 80, 53, 123], weights=[70, 15, 12, 3])[0]
        dns = port in (53, 123)
        return {
            "event_type": "network", "source_ip": user.workstation_ip, "destination_ip": destination,
            "protocol": "UDP" if dns else "TCP",
            "bytes_sent": int(_lognormal(rng, 120 if dns else 8000, 0.5 if dns else 1.2, 60, 3_000_000)),
            "bytes_received": int(_lognormal(rng, 300 if dns else 60_000, 0.5 if dns else 1.3, 60, 6_000_000)),
            "port": port, "packet_count": rng.randint(1, 400),
            "risk_score": round(rng.uniform(3, 40) if rng.random() > 0.01 else rng.uniform(40, 57), 2),
        }

    # -- scenarios ------------------------------------------------------------
    def _start_scenario(self) -> None:
        name = self._rng.choices(SCENARIOS, weights=_SCENARIO_WEIGHTS)[0]
        self._scenario_counter += 1
        scenario_id = f"{name}#{self._scenario_counter}"
        self._active.append((name, scenario_id, getattr(self, f"_scenario_{name}")()))

    def _anomaly_signal(self, scenario: str, source_ip: str, user_id: str | None) -> dict[str, Any]:
        kind = _ANOMALY_TYPE[scenario]
        return {
            "event_type": "anomaly", "anomaly_type": kind, "source_ip": source_ip, "user_id": user_id,
            "severity": self._rng.choices(["low", "medium", "high", "critical"], weights=[3, 5, 4, 1])[0],
            "description": _ANOMALY_TEXT[kind], "risk_score": round(self._rng.uniform(70, 99.5), 2),
        }

    def _scenario_brute_force(self) -> Iterator[dict[str, Any]]:
        rng = self._rng
        attacker = _public_ip(rng)
        country = rng.choice(HIGH_RISK_COUNTRIES + HOME_COUNTRIES)
        signal_at = rng.randint(3, 6) if rng.random() < 0.5 else None
        attempts = rng.randint(8, 20)
        for attempt in range(attempts):
            if attempt == signal_at:
                yield self._anomaly_signal("brute_force", attacker, None)
            takeover = attempt == attempts - 1 and rng.random() < 0.2
            username = rng.choice(_ATTACK_USERNAMES + [u.username for u in self.users[:3]])
            yield {
                "event_type": "login", "user_id": None, "username": username, "source_ip": attacker,
                "country": country, "device_type": None, "login_success": takeover,
                "failure_reason": None if takeover else "invalid_password",
                "risk_score": round(rng.uniform(80, 95) if takeover else rng.uniform(55, 90), 2),
            }

    def _scenario_api_abuse(self) -> Iterator[dict[str, Any]]:
        rng = self._rng
        attacker, user = _public_ip(rng), rng.choice(self.users)
        signal_at = rng.randint(4, 10) if rng.random() < 0.5 else None
        for index in range(rng.randint(20, 45)):
            if index == signal_at:
                yield self._anomaly_signal("api_abuse", attacker, user.user_id)
            yield {
                "event_type": "api", "user_id": user.user_id, "endpoint": rng.choice(_ENDPOINTS),
                "method": rng.choice(["GET", "POST", "DELETE"]),
                "status_code": rng.choices([200, 401, 403, 429, 500], weights=[20, 25, 20, 25, 10])[0],
                "response_time_ms": int(_lognormal(rng, 400, 0.6, 20, 4000)), "source_ip": attacker,
                "request_size": rng.randint(200, 20_000), "risk_score": round(rng.uniform(45, 90), 2),
            }

    def _scenario_lateral_movement(self) -> Iterator[dict[str, Any]]:
        rng = self._rng
        host = rng.choice(self.users).workstation_ip
        targets = rng.sample(self.servers, k=min(len(self.servers), rng.randint(5, 9)))
        for target in targets + [rng.choice(self.users).workstation_ip]:
            yield {
                "event_type": "network", "source_ip": host, "destination_ip": target, "protocol": "TCP",
                "bytes_sent": rng.randint(300, 6000), "bytes_received": rng.randint(300, 9000),
                "port": rng.choice([22, 445, 3389, 5985]), "packet_count": rng.randint(4, 60),
                "risk_score": round(rng.uniform(35, 80), 2),
            }

    def _scenario_data_exfiltration(self) -> Iterator[dict[str, Any]]:
        rng = self._rng
        user = rng.choice(self.users)
        drop = _public_ip(rng)
        signal_at = rng.randint(1, 3) if rng.random() < 0.6 else None
        for index in range(rng.randint(3, 7)):
            if index == signal_at:
                yield self._anomaly_signal("data_exfiltration", user.workstation_ip, user.user_id)
            yield {
                "event_type": "network", "source_ip": user.workstation_ip, "destination_ip": drop, "protocol": "TCP",
                "bytes_sent": rng.randint(9_000_000, 60_000_000), "bytes_received": rng.randint(500, 20_000),
                "port": 443, "packet_count": rng.randint(5_000, 40_000), "risk_score": round(rng.uniform(60, 95), 2),
            }

    def _scenario_dns_beaconing(self) -> Iterator[dict[str, Any]]:
        rng = self._rng
        host = rng.choice(self.users).workstation_ip
        command_server = _public_ip(rng)
        for _ in range(rng.randint(8, 16)):
            yield {
                "event_type": "network", "source_ip": host, "destination_ip": command_server, "protocol": "UDP",
                "bytes_sent": rng.randint(100, 500), "bytes_received": rng.randint(100, 500), "port": 53,
                "packet_count": rng.randint(1, 4), "risk_score": round(rng.uniform(56, 88), 2),
            }

    def _scenario_geo_login(self) -> Iterator[dict[str, Any]]:
        rng = self._rng
        user = rng.choice(self.users)
        attacker, country = _public_ip(rng), rng.choice(HIGH_RISK_COUNTRIES)
        signal_at = 1 if rng.random() < 0.5 else None
        for index in range(rng.randint(1, 3)):
            if index == signal_at:
                yield self._anomaly_signal("geo_login", attacker, user.user_id)
            yield {
                "event_type": "login", "user_id": user.user_id, "username": user.username, "source_ip": attacker,
                "country": country, "device_type": rng.choice(_DEVICES), "login_success": True,
                "failure_reason": None, "risk_score": round(rng.uniform(45, 85), 2),
            }
        for _ in range(rng.randint(3, 8)):  # session use after takeover
            yield {
                "event_type": "api", "user_id": user.user_id, "endpoint": rng.choice(_ENDPOINTS), "method": "GET",
                "status_code": 200, "response_time_ms": int(_lognormal(rng, 150, 0.5, 20, 2000)),
                "source_ip": attacker, "request_size": rng.randint(200, 5000), "risk_score": round(rng.uniform(30, 70), 2),
            }


# -- simulation and Spark-equivalent windows -----------------------------------
@dataclass(frozen=True)
class SimEvent:
    timestamp: datetime
    fields: dict[str, Any]
    scenario: str | None
    scenario_id: str | None


@dataclass
class SimWindow:
    entity_id: str
    window_start: datetime
    features: dict[str, float | None]
    scenario_ids: set[str] = field(default_factory=set)

    @property
    def is_attack(self) -> bool:
        return bool(self.scenario_ids)


def simulate(seed: int, seconds: float, anomaly_rate: float = 0.05, events_per_second: float = 5.0,
             start: datetime | None = None) -> list[SimEvent]:
    """Run the traffic model on a simulated clock (no sleeping)."""
    model = TrafficModel(anomaly_rate=anomaly_rate, seed=seed)
    clock = start or datetime(2026, 1, 1, tzinfo=UTC)
    step = timedelta(seconds=1.0 / events_per_second)
    events = []
    for _ in range(int(seconds * events_per_second)):
        raw = model.next_event(clock)
        events.append(SimEvent(clock, {k: v for k, v in raw.fields.items() if k != "timestamp"}, raw.scenario, raw.scenario_id))
        clock += step
    return events


def build_windows(events: list[SimEvent]) -> list[SimWindow]:
    """Mirror of spark-jobs build_window_features: source IP, 60 s window, 30 s slide.

    Only windows that end before the last event are returned, so none is partial.
    """
    if not events:
        return []
    groups: dict[tuple[str, int], list[SimEvent]] = defaultdict(list)
    for event in events:
        ip = event.fields.get("source_ip")
        if not ip:
            continue
        slot = int(event.timestamp.timestamp() // 30) * 30
        groups[(ip, slot)].append(event)
        groups[(ip, slot - 30)].append(event)
    horizon = events[-1].timestamp.timestamp()
    windows = []
    for (ip, slot), members in sorted(groups.items(), key=lambda item: (item[0][1], item[0][0])):
        if slot + 60 > horizon:
            continue
        windows.append(SimWindow(ip, datetime.fromtimestamp(slot, UTC), _window_features(members),
                                 {m.scenario_id for m in members if m.scenario_id}))
    return windows


def _window_features(members: list[SimEvent]) -> dict[str, float | None]:
    by_type = defaultdict(list)
    for member in members:
        by_type[member.fields["event_type"]].append(member.fields)
    logins, apis, networks, anomalies = (by_type[k] for k in ("login", "api", "network", "anomaly"))
    risks = [m.fields["risk_score"] for m in members]
    failed = sum(1 for e in logins if e["login_success"] is False)
    errors = sum(1 for e in apis if 400 <= e["status_code"] <= 599)
    server_errors = sum(1 for e in apis if 500 <= e["status_code"] <= 599)
    byte_totals = [e["bytes_sent"] + e["bytes_received"] for e in networks
                   if e.get("bytes_sent") is not None and e.get("bytes_received") is not None]
    n = len(members)
    return {
        "event_count": n, "login_event_count": len(logins), "api_event_count": len(apis),
        "network_event_count": len(networks), "anomaly_event_count": len(anomalies),
        "failed_login_rate": failed / len(logins) if logins else None, "failed_auth_count": failed,
        "requests_per_minute": float(len(apis)),
        "avg_api_response_time": sum(e["response_time_ms"] for e in apis) / len(apis) if apis else None,
        "endpoint_error_rate": errors / len(apis) if apis else None,
        "api_5xx_rate": server_errors / len(apis) if apis else None,
        # Network events without byte counts (honeypot sensors) contribute no measurement; 0 means "none reported".
        "avg_network_bytes": (sum(byte_totals) / len(byte_totals) if byte_totals else 0.0) if networks else None,
        "high_risk_network_event_count": sum(1 for e in networks if e["risk_score"] >= 70),
        "anomaly_rate": len(anomalies) / n, "risk_score_moving_average": sum(risks) / n,
        "high_risk_event_count": sum(1 for r in risks if r >= 70),
        "distinct_login_country_count": len({e["country"] for e in logins if e.get("country")}),
        "unique_destination_ip_count": len({e["destination_ip"] for e in networks if e.get("destination_ip")}),
    }
