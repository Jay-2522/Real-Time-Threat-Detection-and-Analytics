"""Benchmark-only Spark/Python feature-engine comparison helpers."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

from benchmarks.harness import ROOT, RunConfig
from benchmarks.loadgen import LoadConfig, make_producer, run_load
from benchmarks.measurements import RuntimeSampler, TopicCapture, parse_datetime, percentile


BASE_COMPOSE = ["docker-compose", "-f", str(ROOT / "docker-compose.yml"), "-f", str(ROOT / "benchmarks/compose.yml")]


def _feature_key(payload: dict[str, Any]) -> tuple[str, float, float]:
    return (
        str(payload["entity_id"]),
        parse_datetime(payload["window_start"]).timestamp(),
        parse_datetime(payload["window_end"]).timestamp(),
    )


def compare_feature_values(
    spark_features: list[dict[str, Any]],
    python_features: list[dict[str, Any]],
    tolerance: float = 1e-6,
) -> dict[str, Any]:
    spark = {_feature_key(row): row for row in spark_features}
    python = {_feature_key(row): row for row in python_features}
    common = sorted(spark.keys() & python.keys())
    mismatches = 0
    compared = 0
    examples = []
    for key in common:
        names = sorted(set(spark[key]["features"]) | set(python[key]["features"]))
        for name in names:
            left = spark[key]["features"].get(name)
            right = python[key]["features"].get(name)
            compared += 1
            equal = left is None and right is None
            if left is not None and right is not None:
                equal = abs(float(left) - float(right)) <= tolerance
            if not equal:
                mismatches += 1
                if len(examples) < 5:
                    examples.append({"window": key, "feature": name, "spark": left, "python": right})
    return {
        "spark_windows": len(spark),
        "python_windows": len(python),
        "common_windows": len(common),
        "spark_only_windows": len(spark.keys() - python.keys()),
        "python_only_windows": len(python.keys() - spark.keys()),
        "compared_values": compared,
        "mismatched_values": mismatches,
        "mismatch_rate": mismatches / compared if compared else None,
        "examples": examples,
    }


@dataclass
class ProcessSampler:
    pid: int
    interval_seconds: float = 1.0
    samples: list[tuple[float, float]] = field(default_factory=list)
    _stop: threading.Event = field(default_factory=threading.Event, init=False)
    _thread: threading.Thread | None = field(default=None, init=False)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._sample, name="benchmark-process-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.interval_seconds + 2)

    def _sample(self) -> None:
        while not self._stop.is_set():
            result = subprocess.run(
                ["ps", "-o", "%cpu=,rss=", "-p", str(self.pid)],
                check=False,
                capture_output=True,
                text=True,
            ).stdout.strip()
            if result:
                cpu, rss_kib = result.split()[:2]
                self.samples.append((float(cpu), float(rss_kib) / 1024))
            self._stop.wait(self.interval_seconds)

    def summary(self) -> dict[str, float | None]:
        cpus = [row[0] for row in self.samples]
        memories = [row[1] for row in self.samples]
        return {
            "cpu_median_percent": percentile(cpus, 0.5),
            "cpu_max_percent": max(cpus) if cpus else None,
            "memory_median_mib": percentile(memories, 0.5),
            "memory_max_mib": max(memories) if memories else None,
        }


def _environment(config: RunConfig) -> dict[str, str]:
    environment = os.environ.copy()
    environment["DEV_BIND_ROOT"] = str(ROOT)
    environment["SYNTHETIC_PRODUCER_ENABLED"] = "false"
    environment["BENCHMARK_WATERMARK"] = f"{config.watermark_seconds} seconds"
    return environment


def _run(command: list[str], environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, env=environment, check=True, capture_output=True, text=True)


def _clean(environment: dict[str, str]) -> None:
    _run(BASE_COMPOSE + ["down", "-v", "--remove-orphans"], environment)


def _wait_container(name: str, *, completed: bool = False, timeout: int = 600) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}", name],
            check=False,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if completed and result.startswith("exited"):
            code = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.ExitCode}}", name],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            if code == "0":
                return
            raise RuntimeError(f"{name} exited with {code}")
        if not completed and result == "running healthy":
            return
        time.sleep(2)
    raise TimeoutError(f"Timed out waiting for {name}")


def _start_plain_consumer(config: RunConfig, group_id: str, output: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "benchmarks.plain_consumer",
            "--group-id",
            group_id,
            "--expected-events",
            str(round(config.rate * config.duration)),
            "--watermark-seconds",
            str(config.watermark_seconds),
            "--timeout-seconds",
            str(config.duration + config.drain_timeout_seconds),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def run_feature_engine(config: RunConfig, mode: str, *, shared_python: bool = False) -> dict[str, Any]:
    if mode not in {"spark-local", "python"}:
        raise ValueError(f"Unknown feature engine: {mode}")
    environment = _environment(config)
    compose = BASE_COMPOSE
    _clean(environment)
    run_id = f"feature-{mode}-{int(config.rate)}-{config.repetition}-{uuid4().hex[:8]}"

    with tempfile.TemporaryDirectory(prefix="streaming-benchmark-") as directory:
        plain_output = Path(directory) / "python-features.json"
        plain_process = None
        process_sampler = None
        capture = None
        runtime_sampler = None

        if mode == "python":
            _run(BASE_COMPOSE + ["up", "-d", "kafka-init"], environment)
            _wait_container("streaming-analytics-kafka-init", completed=True)
        else:
            _run(compose + ["up", "--build", "-d", "spark-streaming"], environment)
            _wait_container("streaming-analytics-spark-streaming")
            capture = TopicCapture("127.0.0.1:9092", f"benchmark-capture-{run_id}")
            capture.start()
            runtime_sampler = RuntimeSampler(compose)
            runtime_sampler.start()

        if mode == "python" or shared_python:
            plain_process = _start_plain_consumer(config, f"benchmark-python-{run_id}", plain_output)
            process_sampler = ProcessSampler(plain_process.pid)
            process_sampler.start()

        time.sleep(2)
        load = run_load(
            LoadConfig(
                rate=config.rate,
                duration=config.duration,
                source_ips=config.source_ips,
                out_of_order_fraction=config.out_of_order_fraction,
                run_id=run_id,
            ),
            make_producer("127.0.0.1:9092"),
        )
        if runtime_sampler:
            runtime_sampler.stop()
        if process_sampler:
            process_sampler.stop()

        drained = True
        if capture:
            deadline = time.monotonic() + config.drain_timeout_seconds
            while len(capture.enriched) < load.delivered and time.monotonic() < deadline:
                time.sleep(2)
            drained = len(capture.enriched) == load.delivered
            time.sleep(5)
            capture.stop()
        if plain_process:
            try:
                plain_process.wait(timeout=config.drain_timeout_seconds)
            except subprocess.TimeoutExpired:
                plain_process.terminate()
                plain_process.wait(timeout=10)
                drained = False
            if plain_process.returncode != 0:
                stderr = plain_process.stderr.read() if plain_process.stderr else ""
                raise RuntimeError(f"Plain consumer failed ({plain_process.returncode}): {stderr}")
        plain_payload = json.loads(plain_output.read_text()) if plain_output.exists() else None
        resources = {}
        lag = {}
        if runtime_sampler:
            resources, lag = runtime_sampler.summary()
        spark_features = [payload for _timestamp, payload in capture.features.values()] if capture else []
        comparison = None
        if capture and plain_payload:
            comparison = compare_feature_values(spark_features, plain_payload["features"])
        return {
            "mode": mode,
            "config": asdict(config),
            "run_id": run_id,
            "load": asdict(load),
            "drained": drained,
            "spark_received": len(capture.enriched) if capture else None,
            "spark_windows": len(spark_features),
            "spark_latency_seconds": capture.kafka_latency_summary()["enriched_to_feature"] if capture else None,
            "python": plain_payload["result"] if plain_payload else None,
            "comparison": comparison,
            "container_resources": resources,
            "python_resources": process_sampler.summary() if process_sampler else None,
            "consumer_lag": lag,
            "features": spark_features if capture else plain_payload["features"],
        }
