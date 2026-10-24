"""Dataset-free regression tests for the offline detection evaluation."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from eval.data import header_findings, load_file
from eval.detectors import production_model_coverage
from eval.features import FEATURE_COLUMNS, aggregate_windows, assert_no_identity_leakage


RAW_HEADERS = [
    "Flow ID",
    " Source IP",
    " Source Port",
    " Destination IP",
    " Destination Port",
    " Timestamp",
    " Flow Duration",
    " Total Fwd Packets",
    " Total Backward Packets",
    "Total Length of Fwd Packets",
    " Total Length of Bwd Packets",
    "Flow Bytes/s",
    " SYN Flag Count",
    " RST Flag Count",
    " PSH Flag Count",
    " ACK Flag Count",
    " Average Packet Size",
    " Fwd Header Length",
    " Fwd Header Length",
    " Label",
]


def _write_fake_csv(path: Path) -> None:
    rows = [
        [
            "synthetic-flow-1",
            "203.0.113.10",
            50000,
            "198.51.100.20",
            22,
            "4/7/2017 1:05",
            100,
            3,
            4,
            300,
            400,
            np.inf,
            1,
            0,
            1,
            2,
            70,
            20,
            999,
            "BENIGN",
        ],
        [
            "synthetic-flow-2",
            "203.0.113.10",
            50001,
            "198.51.100.21",
            23,
            "4/7/2017 1:05",
            300,
            5,
            6,
            500,
            600,
            np.nan,
            2,
            1,
            0,
            3,
            90,
            24,
            999,
            "SSH-Patator",
        ],
        [
            "synthetic-flow-3",
            "203.0.113.11",
            50002,
            "198.51.100.20",
            443,
            "03/07/2017 08:55:58",
            50,
            1,
            1,
            100,
            100,
            10,
            0,
            0,
            1,
            1,
            50,
            16,
            999,
            "BENIGN",
        ],
        [None] * len(RAW_HEADERS),
    ]
    pd.DataFrame(rows, columns=RAW_HEADERS).to_csv(path, index=False, encoding="cp1252")


def test_loader_cleans_headers_duplicate_numeric_values_and_timestamps(tmp_path):
    path = tmp_path / "synthetic-flows.csv"
    _write_fake_csv(path)

    frame = load_file(path, chunksize=2)
    leading, duplicates = header_findings(path)

    assert len(frame) == 3
    assert leading == 17
    assert duplicates == ("fwd_header_length",)
    assert frame.loc[0, "timestamp"] == pd.Timestamp("2017-07-04 13:05:00")
    assert frame.loc[2, "timestamp"] == pd.Timestamp("2017-07-03 08:55:58")
    assert pd.isna(frame.loc[0, "flow_bytes_per_s"])
    assert pd.isna(frame.loc[1, "flow_bytes_per_s"])
    assert frame["fwd_header_length"].tolist() == [20, 24, 16]


def test_windowing_aggregates_per_minute_and_labels_any_attack(tmp_path):
    path = tmp_path / "synthetic-flows.csv"
    _write_fake_csv(path)
    windows = aggregate_windows(load_file(path, chunksize=2))
    target = windows[(windows["source_ip"] == "203.0.113.10") & (windows["window_start"] == pd.Timestamp("2017-07-04 13:05:00"))].iloc[0]

    assert target["flow_count"] == 2
    assert target["distinct_destination_ports"] == 2
    assert target["distinct_destination_ips"] == 2
    assert target["total_fwd_packets"] == 8
    assert target["total_bwd_packets"] == 10
    assert target["mean_flow_duration"] == 200
    assert target["is_attack"]
    assert target["attack_types"] == ("SSH-Patator",)


def test_identity_columns_cannot_reach_models():
    assert not ({"source_ip", "source_port", "destination_ip", "flow_id", "timestamp"} & set(FEATURE_COLUMNS))
    with pytest.raises(ValueError, match="cannot reach a model"):
        assert_no_identity_leakage([*FEATURE_COLUMNS, "source_ip"])


def test_production_model_coverage_uses_contract_v2_definition():
    coverage = production_model_coverage()

    assert coverage["contract_feature_count"] == 18
    assert coverage["real_equivalent_count"] == 4
    assert coverage["equivalents"] == {
        "event_count": "flow_count",
        "network_event_count": "flow_count",
        "avg_network_bytes": "(total_fwd_bytes + total_bwd_bytes) / flow_count",
        "unique_destination_ip_count": "distinct_destination_ips",
    }
    assert coverage["artifact_feature_count"] in (None, 18)
