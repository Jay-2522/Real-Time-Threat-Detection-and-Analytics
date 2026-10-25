#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


@dataclass
class CheckResult:
    name: str
    status: str
    target: str
    reason: str


def local_value(name: str, default: str) -> str:
    if name in os.environ:
        return os.environ[name]
    path = Path(__file__).resolve().parents[1] / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            key, separator, value = line.partition("=")
            if separator and key == name:
                return value.strip()
    return default


HTTP_CHECKS = [
    ("Backend API", "http://localhost:8000/api/v1/health"),
    ("Frontend", "http://localhost:5173"),
    ("Elasticsearch", "http://localhost:9200/_cluster/health"),
    ("Prometheus", "http://localhost:9090/-/ready"),
    ("ML Inference", "http://localhost:9101/metrics"),
    ("Storage Sink", "http://localhost:9102/metrics"),
    ("Alert Engine", "http://localhost:9103/metrics"),
    ("Threat Intelligence Engine", "http://localhost:9106/metrics"),
]

GRAFANA_CHECK = ("Grafana", f"http://localhost:{local_value('GRAFANA_PUBLISHED_PORT', '3000')}/api/health")

TCP_CHECKS = [
    ("Kafka", "localhost", 9092),
    ("PostgreSQL", "localhost", int(local_value("POSTGRES_PUBLISHED_PORT", "5432"))),
]


def check_http(name: str, url: str, timeout: float) -> CheckResult:
    try:
        request = Request(url, headers={"User-Agent": "streaming-analytics-health-check/1.0"})
        with urlopen(request, timeout=timeout) as response:
            if 200 <= response.status < 500:
                return CheckResult(name, "ok", url, f"HTTP {response.status}")
            return CheckResult(name, "failed", url, f"HTTP {response.status}")
    except URLError as exc:
        return CheckResult(name, "failed", url, str(exc.reason))
    except Exception as exc:
        return CheckResult(name, "failed", url, str(exc))


def check_tcp(name: str, host: str, port: int, timeout: float) -> CheckResult:
    target = f"{host}:{port}"
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return CheckResult(name, "ok", target, "TCP connection accepted")
    except Exception as exc:
        return CheckResult(name, "failed", target, str(exc))


def run(timeout: float, include_grafana: bool = False) -> list[CheckResult]:
    http_checks = [*HTTP_CHECKS, GRAFANA_CHECK] if include_grafana else HTTP_CHECKS
    results = [check_http(name, url, timeout) for name, url in http_checks]
    results.extend(check_tcp(name, host, port, timeout) for name, host, port in TCP_CHECKS)
    return sorted(results, key=lambda result: result.name)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate local Streaming Analytics platform health.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output.")
    parser.add_argument("--timeout", type=float, default=2.0, help="Per-check timeout in seconds.")
    parser.add_argument("--include-grafana", action="store_true", help="Check the optional Grafana dashboard service.")
    args = parser.parse_args()

    results = run(args.timeout, args.include_grafana)
    ok_count = sum(1 for result in results if result.status == "ok")
    summary = {"status": "ok" if ok_count == len(results) else "degraded", "ok": ok_count, "failed": len(results) - ok_count, "checks": [asdict(result) for result in results]}

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        for result in results:
            marker = "OK" if result.status == "ok" else "FAIL"
            print(f"[{marker}] {result.name} - {result.reason}")
        print(f"\nSummary: {summary['status']} ({summary['ok']} OK, {summary['failed']} failed)")
    return 0 if summary["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
