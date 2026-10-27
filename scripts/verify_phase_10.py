#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


@dataclass
class Check:
    name: str
    status: str
    detail: str
    critical: bool = True


REQUIRED_DOCS = [
    "README.md",
    "docs/demo-guide.md",
    "docs/architecture.md",
    "docs/troubleshooting.md",
    "docs/AUTHENTICATION.md",
    "docs/KAFKA_DELIVERY_CONTRACT.md",
]

REQUIRED_SCRIPTS = [
    "scripts/health_check.py",
    "scripts/start_full.ps1",
    "scripts/start_light.ps1",
    "scripts/stop_all.ps1",
]

REQUIRED_FRONTEND = [
    "frontend/package.json",
    "frontend/package-lock.json",
    "frontend/src/App.tsx",
]

REQUIRED_FOLDERS = [
    "backend/app",
    "ml-models",
    "alert-engine",
    "storage-sink",
    "threat-intelligence-engine",
]

OPTIONAL_HTTP_CHECKS = [
    ("Frontend", "http://localhost:5173"),
    ("Backend health", "http://localhost:8000/api/v1/health"),
    ("Backend docs", "http://localhost:8000/docs"),
]


def check_path(path: str, label: str, critical: bool = True) -> Check:
    target = ROOT / path
    if target.exists():
        return Check(label, "OK", path, critical)
    return Check(label, "FAIL" if critical else "WARN", f"Missing: {path}", critical)


def check_compose_config() -> Check:
    command = ["docker", "compose"] if subprocess.run(
        ["docker", "compose", "version"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0 else [shutil.which("docker-compose") or "docker-compose"]
    try:
        result = subprocess.run(
            [*command, "config", "--quiet"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=45,
            check=False,
        )
    except FileNotFoundError:
        return Check("Docker Compose config", "WARN", "Docker CLI not found; skipping compose validation.", critical=False)
    except subprocess.TimeoutExpired:
        return Check("Docker Compose config", "WARN", "Docker Compose config check timed out.", critical=False)

    if result.returncode == 0:
        return Check("Docker Compose config", "OK", f"{' '.join(command)} config --quiet passed")
    detail = (result.stderr or result.stdout or "docker compose config failed").strip()
    return Check("Docker Compose config", "FAIL", detail)


def check_http(name: str, url: str) -> Check:
    try:
        request = Request(url, headers={"User-Agent": "phase-10-verifier/1.0"})
        with urlopen(request, timeout=2.0) as response:
            if 200 <= response.status < 500:
                return Check(name, "OK", f"{url} returned HTTP {response.status}", critical=False)
            return Check(name, "WARN", f"{url} returned HTTP {response.status}", critical=False)
    except URLError as exc:
        return Check(name, "WARN", f"{url} unavailable: {exc.reason}", critical=False)
    except Exception as exc:
        return Check(name, "WARN", f"{url} unavailable: {exc}", critical=False)


def run() -> list[Check]:
    checks: list[Check] = [check_compose_config()]
    checks.extend(check_path(path, f"Documentation: {path}") for path in REQUIRED_DOCS)
    checks.extend(check_path(path, f"Script: {path}") for path in REQUIRED_SCRIPTS)
    checks.extend(check_path(path, f"Frontend file: {path}") for path in REQUIRED_FRONTEND)
    checks.extend(check_path(path, f"Service folder: {path}") for path in REQUIRED_FOLDERS)
    checks.extend(check_http(name, url) for name, url in OPTIONAL_HTTP_CHECKS)
    return checks


def main() -> int:
    checks = run()
    print("\nFINAL PLATFORM VERIFICATION SUMMARY\n")
    for check in checks:
        print(f"[{check.status}] {check.name} - {check.detail}")

    critical_failures = [check for check in checks if check.critical and check.status == "FAIL"]
    warn_count = sum(1 for check in checks if check.status == "WARN")
    ok_count = sum(1 for check in checks if check.status == "OK")

    print("\nResult:")
    if critical_failures:
        print(f"[FAIL] Critical verification failed: {len(critical_failures)} failure(s), {warn_count} warning(s), {ok_count} OK.")
        return 1

    print(f"[OK] Core Phase 10 verification passed: {ok_count} OK, {warn_count} warning(s), 0 critical failures.")
    print("[OK] Light mode is considered valid; optional heavy services may be stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
