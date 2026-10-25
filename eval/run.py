"""Run the fixed CIC-IDS-2017 day split and regenerate aggregate results."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from eval.data import FileProfile, dataset_root, profile_dataset
from eval.detectors import (
    AllBenignBaseline,
    ThresholdRules,
    production_model_coverage,
    train_isolation_forest,
    train_supervised_reference,
    tune_threshold_rules,
)
from eval.features import FEATURE_COLUMNS, build_day_windows
from eval.metrics import evaluate_attack_types, evaluate_days


RESULT_PATH = Path(__file__).resolve().parent / "results/latest.md"


def _cell(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value).replace("|", "\\|").replace("\n", " ")


def _table(rows: Iterable[dict[str, object]], columns: list[tuple[str, str]]) -> str:
    materialized = list(rows)
    header = "| " + " | ".join(title for _, title in columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    body = ["| " + " | ".join(_cell(row.get(key, "")) for key, _ in columns) + " |" for row in materialized]
    return "\n".join([header, divider, *body])


def _profile_rows(profiles: list[FileProfile]) -> list[dict[str, object]]:
    rows = []
    for profile in profiles:
        attacks = ", ".join(f"{label}={count}" for label, count in profile.labels.items() if label != "BENIGN") or "none"
        sources = "; ".join(
            f"{label}: {', '.join(f'{ip} ({count})' for ip, count in counts.items())}"
            for label, counts in profile.attack_sources.items()
        ) or "none"
        rows.append(
            {
                "file": profile.name,
                "rows": profile.rows,
                "benign": profile.benign,
                "attack": profile.attack,
                "attack_types": attacks,
                "attack_sources": sources,
            }
        )
    return rows


def _cleaning_rows(profiles: list[FileProfile]) -> list[dict[str, object]]:
    return [
        {
            "file": p.name,
            "leading": p.leading_space_columns,
            "duplicate": ", ".join(p.duplicate_columns),
            "blank": p.blank_rows_dropped,
            "inf": p.infinity_values,
            "nan": p.nan_values,
            "resolution": p.timestamp_resolution,
            "raw_range": f"{p.raw_min_timestamp} to {p.raw_max_timestamp}",
            "corrected_range": f"{p.corrected_min_timestamp} to {p.corrected_max_timestamp}",
        }
        for p in profiles
    ]


def generate() -> str:
    root = dataset_root()
    profiles = profile_dataset(root)
    windows = {day: build_day_windows(day, root) for day in ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")}
    train = windows["Monday"]
    validation = windows["Tuesday"]
    test_days = {day: windows[day] for day in ("Wednesday", "Thursday", "Friday")}

    thresholds = tune_threshold_rules(validation)
    detectors = [
        ThresholdRules(thresholds),
        train_isolation_forest(train, validation),
        train_supervised_reference(validation),
        AllBenignBaseline(),
    ]
    day_metrics, predictions, accuracy_context = evaluate_days(detectors, test_days)
    type_metrics, latencies = evaluate_attack_types(detectors, test_days, predictions)
    coverage = production_model_coverage()

    split_rows = [
        {
            "day": day,
            "role": {"Monday": "train: benign only", "Tuesday": "validation/tuning"}.get(day, "test: final only"),
            "windows": len(frame),
            "benign": int((~frame["is_attack"]).sum()),
            "attack": int(frame["is_attack"].sum()),
        }
        for day, frame in windows.items()
    ]
    coverage_rows = [
        {"production_feature": name, "flow_equivalent": equivalent}
        for name, equivalent in coverage["equivalents"].items()
    ] + [{"production_feature": name, "flow_equivalent": "unavailable"} for name in coverage["missing"]]

    sections = [
        "# CIC-IDS-2017 detection evaluation",
        "## Dataset labels and attack sources",
        _table(
            _profile_rows(profiles),
            [("file", "File"), ("rows", "Rows"), ("benign", "Benign"), ("attack", "Attack"), ("attack_types", "Attack labels"), ("attack_sources", "Attack source IPs")],
        ),
        "## Cleaning findings",
        _table(
            _cleaning_rows(profiles),
            [("file", "File"), ("leading", "Leading-space headers"), ("duplicate", "Duplicate header"), ("blank", "Blank rows dropped"), ("inf", "Flow Bytes/s infinities"), ("nan", "Flow Bytes/s NaNs"), ("resolution", "Timestamp resolution"), ("raw_range", "Raw range"), ("corrected_range", "Corrected range")],
        ),
        "## Day split",
        _table(split_rows, [("day", "Day"), ("role", "Role"), ("windows", "Windows"), ("benign", "Benign"), ("attack", "Attack")]),
        "## Window features",
        _table([{"feature": feature} for feature in FEATURE_COLUMNS], [("feature", "Model feature")]),
        "## Tuesday-selected thresholds",
        _table(
            [
                {"setting": "rule.flow_count", "value": thresholds.flow_count},
                {"setting": "rule.top_destination_port_flow_count", "value": thresholds.top_destination_port_flow_count},
                {"setting": "rule.distinct_destination_ports", "value": thresholds.distinct_destination_ports},
                {"setting": "isolation_forest.anomaly_score", "value": detectors[1].threshold},
            ],
            [("setting", "Setting"), ("value", "Value")],
        ),
        "## Final test metrics by day",
        _table(
            day_metrics,
            [("detector", "Detector"), ("scope", "Test day"), ("tn", "TN"), ("fp", "FP"), ("fn", "FN"), ("tp", "TP"), ("precision", "Precision"), ("recall", "Recall"), ("f1", "F1"), ("false_positive_rate", "FPR"), ("alerts_per_benign_hour", "Alerts/benign hour")],
        ),
        "## All-benign accuracy context only",
        _table(accuracy_context, [("day", "Test day"), ("all_benign_accuracy_context", "All-benign accuracy"), ("missed_attack_windows", "Missed attack windows")]),
        "## Final metrics by attack type",
        _table(
            type_metrics,
            [("detector", "Detector"), ("scope", "Test day and attack"), ("tn", "TN"), ("fp", "FP"), ("fn", "FN"), ("tp", "TP"), ("precision", "Precision"), ("recall", "Recall"), ("f1", "F1"), ("false_positive_rate", "FPR")],
        ),
        "## First detection latency",
        _table(latencies, [("detector", "Detector"), ("day", "Day"), ("attack_type", "Attack type"), ("attack_windows", "Attack windows"), ("latency_windows", "Latency windows")]),
        "## Production Isolation Forest feature coverage",
        _table(coverage_rows, [("production_feature", "Production feature"), ("flow_equivalent", "Real flow equivalent")]),
        "## Production model applicability",
        _table(
            [
                {
                    "contract": coverage["contract_feature_count"],
                    "equivalent": coverage["real_equivalent_count"],
                    "artifact": coverage["artifact_feature_count"] or "not available",
                    "evaluated": coverage["can_evaluate"],
                    "reason": coverage["reason"],
                }
            ],
            [("contract", "Contract features"), ("equivalent", "Real equivalents"), ("artifact", "Contract-v2 metadata features"), ("evaluated", "Evaluated"), ("reason", "Reason")],
        ),
    ]
    return "\n\n".join(sections) + "\n"


def main() -> None:
    result = generate()
    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULT_PATH.write_text(result, encoding="utf-8")
    print(result, end="")
    print(f"results={RESULT_PATH}")


if __name__ == "__main__":
    main()
