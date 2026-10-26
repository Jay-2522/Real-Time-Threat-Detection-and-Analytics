"""Run reproducible quick or full single-node benchmark suites."""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from benchmarks.comparison import run_feature_engine
from benchmarks.harness import ROOT, RunConfig, run_once


RESULT = ROOT / "benchmarks/results/latest.md"
CHECKPOINT = Path("/tmp/streaming-analytics-benchmark-checkpoint.json")
FULL_RATES = [50, 200, 500, 1000, 2000]


def _command(command: list[str]) -> str:
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout.strip()


def hardware() -> dict[str, str]:
    profile = json.loads(_command(["system_profiler", "SPHardwareDataType", "-json"]))["SPHardwareDataType"][0]
    colima = json.loads(_command(["colima", "status", "-j"]))
    docker = _command(["docker", "version", "--format", "client={{.Client.Version}} server={{.Server.Version}}"])
    os_name = _command(["sw_vers", "-productName"])
    os_version = _command(["sw_vers", "-productVersion"])
    os_build = _command(["sw_vers", "-buildVersion"])
    return {
        "machine": str(profile.get("machine_name", "unknown")),
        "chip": str(profile.get("chip_type", "unknown")),
        "ram": str(profile.get("physical_memory", "unknown")),
        "os": f"{os_name} {os_version} ({os_build})",
        "docker": docker,
        "docker_vm": f"Colima {colima['cpu']} CPU, {colima['memory'] / 1024**3:.0f} GiB RAM, {colima['arch']}",
    }


def _save_checkpoint(payload: dict[str, Any]) -> None:
    CHECKPOINT.write_text(json.dumps(payload, default=str), encoding="utf-8")


def _run_recorded(payload: dict[str, Any], section: str, label: str, action: Callable[[], dict[str, Any]]) -> None:
    print(f"START {section} {label}", flush=True)
    try:
        result = action()
        result.pop("features", None)
        payload[section].append(result)
        print(f"PASS {section} {label}", flush=True)
    except Exception as error:
        payload[section].append({"label": label, "error": f"{type(error).__name__}: {error}"})
        print(f"FAIL {section} {label}: {type(error).__name__}: {error}", flush=True)
    _save_checkpoint(payload)


def _fmt(value: float | int | None, digits: int = 2) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def _median_range(values: list[float], digits: int = 2) -> str:
    if not values:
        return "n/a"
    return f"{statistics.median(values):.{digits}f} ({min(values):.{digits}f}–{max(values):.{digits}f})"


def _safe_median(values: Iterable[float | None]) -> float | None:
    available = [value for value in values if value is not None]
    return statistics.median(available) if available else None


def _resource_total(run: dict[str, Any], names: list[str], field: str) -> float | None:
    values = [run["resources"][name][field] for name in names if name in run.get("resources", {})]
    return sum(value for value in values if value is not None) if values else None


def _throughput_rows(runs: list[dict[str, Any]], rates: list[int]) -> list[list[str]]:
    rows = []
    for rate in rates:
        selected = [run for run in runs if "error" not in run and int(run["config"]["rate"]) == rate]
        achieved = [run["load"]["achieved_rate"] for run in selected]
        persisted = [100 * run["database"]["distinct_telemetry_rows"] / run["load"]["delivered"] for run in selected]
        max_lag = [max((group["max"] or 0) for group in run["consumer_lag"].values()) for run in selected]
        valid = [
            run
            for run in selected
            if run["drained"] and run["database"]["lost"] == 0 and run["database"]["duplicates"] == 0
        ]
        bottlenecks = []
        for run in selected:
            if run in valid:
                continue
            if run.get("capture_counts", {}).get("enriched", 0) < run["load"]["delivered"]:
                bottlenecks.append("Spark enriched path")
            else:
                final = {name: value["final"] or 0 for name, value in run["consumer_lag"].items()}
                bottlenecks.append(max(final, key=final.get) if final else "unresolved drain")
        rows.append(
            [
                str(rate),
                str(len(selected)),
                _median_range(achieved),
                _median_range(persisted),
                _median_range([float(value) for value in max_lag], 0),
                f"{len(valid)}/{len(selected)}",
                ", ".join(sorted(set(bottlenecks))) or "none observed",
            ]
        )
    return rows


def _hop_rows(runs: list[dict[str, Any]]) -> list[list[str]]:
    mappings = [
        ("raw topic → enriched topic", "kafka_latency_seconds", "raw_to_enriched"),
        ("enriched topic → feature topic", "kafka_latency_seconds", "enriched_to_feature"),
        ("feature topic → prediction topic", "kafka_latency_seconds", "feature_to_prediction"),
        ("enriched topic → direct alert row", "database", "direct_enriched_to_alert_seconds"),
        ("feature topic → windowed alert row", "database", "window_feature_to_alert_seconds"),
        ("window first event → alert row", "database", "window_total_seconds"),
        ("deliberate window + watermark", "database", "window_deliberate_seconds"),
        ("remaining window processing", "database", "window_processing_seconds"),
    ]
    rows = []
    usable = [run for run in runs if "error" not in run]
    for label, section, key in mappings:
        medians = [run[section][key]["median"] for run in usable if run[section][key]["median"] is not None]
        p95s = [run[section][key]["p95"] for run in usable if run[section][key]["p95"] is not None]
        samples = sum(run[section][key]["samples"] for run in usable)
        rows.append([label, str(samples), _fmt(statistics.median(medians) if medians else None), _fmt(statistics.median(p95s) if p95s else None)])
    return rows


def _comparison_rows(throughput: list[dict[str, Any]], engines: list[dict[str, Any]], rates: list[int]) -> list[list[str]]:
    rows = []
    spark_names = ["streaming-analytics-spark-streaming"]
    for rate in rates:
        cluster = [run for run in throughput if "error" not in run and int(run["config"]["rate"]) == rate]
        if cluster:
            rows.append(
                [
                    "Spark local[2] full stack",
                    str(rate),
                    _median_range([run["load"]["achieved_rate"] for run in cluster]),
                    _fmt(_safe_median(_resource_total(run, spark_names, "cpu_median_percent") for run in cluster)),
                    _fmt(_safe_median(_resource_total(run, spark_names, "memory_median_mib") for run in cluster)),
                    _fmt(_safe_median(run["kafka_latency_seconds"]["enriched_to_feature"]["median"] for run in cluster)),
                    "yes" if all(run["drained"] for run in cluster) else "no",
                ]
            )
        for mode, label in (("spark-local", "Spark local[2]"), ("python", "Plain Python")):
            selected = [run for run in engines if "error" not in run and run["mode"] == mode and int(run["config"]["rate"]) == rate]
            for run in selected:
                if mode == "python":
                    cpu = run["python_resources"]["cpu_median_percent"]
                    memory = run["python_resources"]["memory_median_mib"]
                    latency = run["python"]["latency_seconds"]["median"]
                    achieved = run["python"]["achieved_rate"]
                else:
                    resource = run["container_resources"].get("streaming-analytics-spark-streaming", {})
                    cpu = resource.get("cpu_median_percent")
                    memory = resource.get("memory_median_mib")
                    latency = run["spark_latency_seconds"]["median"]
                    achieved = run["load"]["achieved_rate"]
                rows.append([label, str(rate), _fmt(achieved), _fmt(cpu), _fmt(memory), _fmt(latency), "yes" if run["drained"] else "no"])
    return rows


def _table(headers: list[str], rows: list[list[str]]) -> str:
    return "\n".join(
        [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
            *("| " + " | ".join(row) + " |" for row in rows),
        ]
    )


def render(payload: dict[str, Any]) -> str:
    hw = payload["hardware"]
    rates = payload["rates"]
    mismatch = next(
        (run["comparison"] for run in payload["feature_engines"] if "error" not in run and run.get("comparison")),
        None,
    )
    watermark_rows = []
    for run in payload["watermarks"]:
        if "error" in run:
            watermark_rows.append([run.get("label", "unknown"), "error", "n/a", "n/a", "n/a", "no"])
            continue
        coverage = run["watermark_coverage"]
        watermark_rows.append(
            [
                f"{run['config']['watermark_seconds']} s",
                _fmt(run["database"]["window_total_seconds"]["median"]),
                str(coverage["out_of_order_events"]),
                str(coverage["missing_event_window_memberships"]),
                _fmt(100 * coverage["missing_membership_rate"] if coverage["missing_membership_rate"] is not None else None),
                "yes" if run["drained"] else "no (timeout)",
            ]
        )
    lines = [
        "# Latest single-node benchmark",
        "",
        f"Generated: {payload['generated_at']}",
        "",
        "These are single-node laptop measurements, not production capacity claims.",
        "",
        "## Hardware",
        "",
        _table(["Item", "Value"], [[key.replace("_", " "), value] for key, value in hw.items()]),
        "",
        "## Full-stack throughput",
        "",
        _table(
            ["Requested events/s", "Runs", "Achieved median (range)", "Persisted % median (range)", "Max lag median (range)", "Clean runs", "First observed bottleneck"],
            _throughput_rows(payload["throughput"], rates),
        ),
        "",
        "## Per-hop latency",
        "",
        _table(["Hop", "Samples", "Median seconds", "Median p95 seconds"], _hop_rows(payload["throughput"])),
        "",
        "## Feature engine comparison",
        "",
        _table(["Engine", "Requested events/s", "Achieved events/s", "Median CPU %", "Median memory MiB", "Median window latency s", "Drained"], _comparison_rows(payload["throughput"], payload["feature_engines"], rates)),
        "",
        "## Shared-sample feature equivalence",
        "",
        _table(
            ["Common windows", "Compared values", "Mismatched values", "Mismatch rate", "Spark-only windows", "Python-only windows"],
            [[
                str(mismatch["common_windows"] if mismatch else 0),
                str(mismatch["compared_values"] if mismatch else 0),
                str(mismatch["mismatched_values"] if mismatch else 0),
                _fmt(100 * mismatch["mismatch_rate"] if mismatch and mismatch["mismatch_rate"] is not None else None),
                str(mismatch["spark_only_windows"] if mismatch else 0),
                str(mismatch["python_only_windows"] if mismatch else 0),
            ]],
        ),
        "",
        "## Watermark trade-off at 200 events/s and 10% configured disorder",
        "",
        _table(["Watermark", "Median end-to-end s", "Observed out-of-order events", "Missing closed-window memberships", "Missing %", "Drained"], watermark_rows),
        "",
        "## Scope",
        "",
        f"Profile: `{payload['profile']}`; duration per load: {payload['duration_seconds']} s; throughput repetitions: {payload['repetitions']}.",
        "No multi-node test, multi-hour soak, failure injection, cloud network, or production-server hardware was measured.",
        "Tail windows that cannot close without newer event time are excluded from closed-window loss calculations.",
        "Spark Structured Streaming does not expose a conventional committed Kafka consumer group here; drain also requires one enriched output per delivered input.",
        "",
    ]
    failures = [run for section in ("throughput", "feature_engines", "watermarks") for run in payload[section] if "error" in run]
    if failures:
        lines.extend(["## Failed measurements", "", *[f"- {run.get('label', 'run')}: {run['error']}" for run in failures], ""])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    profile = parser.add_mutually_exclusive_group(required=True)
    profile.add_argument("--quick", action="store_true")
    profile.add_argument("--full", action="store_true")
    parser.add_argument("--repetitions", type=int, default=None)
    args = parser.parse_args()

    full = args.full
    rates = FULL_RATES if full else [50, 200]
    duration = 300 if full else 60
    repetitions = args.repetitions if args.repetitions is not None else (3 if full else 1)
    payload: dict[str, Any] = {
        "profile": "full" if full else "quick",
        "generated_at": datetime.now(UTC).isoformat(),
        "duration_seconds": duration,
        "repetitions": repetitions,
        "rates": rates,
        "hardware": hardware(),
        "throughput": [],
        "feature_engines": [],
        "watermarks": [],
    }
    _save_checkpoint(payload)

    throughput_watermark = 120 if full else 30
    for rate in rates:
        for repetition in range(1, repetitions + 1):
            config = RunConfig(rate=rate, duration=duration, repetition=repetition, watermark_seconds=throughput_watermark)
            _run_recorded(payload, "throughput", f"rate={rate} repetition={repetition}", lambda config=config: run_once(config))

    for mode in ("spark-local", "python"):
        for rate in rates:
            config = RunConfig(rate=rate, duration=duration, repetition=1, watermark_seconds=throughput_watermark)
            _run_recorded(
                payload,
                "feature_engines",
                f"mode={mode} rate={rate}",
                lambda config=config, mode=mode: run_feature_engine(config, mode),
            )

    shared_duration = duration if full else 90
    shared = RunConfig(rate=200, duration=shared_duration, repetition=1, watermark_seconds=throughput_watermark)
    _run_recorded(
        payload,
        "feature_engines",
        "shared Spark/Python feature sample",
        lambda: run_feature_engine(shared, "spark-local", shared_python=True),
    )

    for watermark in (120, 60, 30):
        config = RunConfig(
            rate=200,
            duration=duration,
            repetition=1,
            out_of_order_fraction=0.1,
            watermark_seconds=watermark,
        )
        _run_recorded(payload, "watermarks", f"watermark={watermark}", lambda config=config: run_once(config))

    RESULT.parent.mkdir(parents=True, exist_ok=True)
    RESULT.write_text(render(payload), encoding="utf-8")
    print(f"results={RESULT}")


if __name__ == "__main__":
    main()
