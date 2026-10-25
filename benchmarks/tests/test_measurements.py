from datetime import UTC, datetime, timedelta

import pytest

from benchmarks.measurements import parse_consumer_lag, parse_memory_mib, percentile, window_latency_parts


def test_latency_percentile_interpolates_without_selecting_best_run():
    assert percentile([1, 4, 2, 3], 0.5) == 2.5
    assert percentile([1, 2, 3, 100], 0.95) == pytest.approx(85.45)
    assert percentile([], 0.5) is None


def test_window_latency_separates_deliberate_wait_from_processing():
    first = datetime(2026, 10, 26, 12, 0, 10, tzinfo=UTC)
    window_end = datetime(2026, 10, 26, 12, 1, tzinfo=UTC)
    alert = window_end + timedelta(seconds=125)
    total, deliberate, processing = window_latency_parts(first, window_end, alert, 120)
    assert total == 175
    assert deliberate == 170
    assert processing == 5


def test_docker_memory_and_consumer_lag_parsing():
    assert parse_memory_mib("1.5GiB / 8GiB") == 1536
    assert parse_memory_mib("512MiB / 8GiB") == 512
    output = """GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
storage telemetry.login.events 0 10 15 5 id host client
storage telemetry.login.events 1 20 22 2 id host client
alerts analytics.enriched.events 0 8 8 0 id host client
"""
    assert parse_consumer_lag(output) == {"storage": 7, "alerts": 0}
