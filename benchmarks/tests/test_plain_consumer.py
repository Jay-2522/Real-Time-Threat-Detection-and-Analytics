from datetime import UTC, datetime

from benchmarks.plain_consumer import PlainWindowEngine, window_starts


def event(event_id, timestamp, event_type, **values):
    payload = {
        "event_id": event_id,
        "timestamp": timestamp,
        "event_type": event_type,
        "source_ip": "198.18.0.1",
        "risk_score": values.pop("risk_score", 20),
    }
    payload.update(values)
    return payload


def test_sliding_window_assignment_matches_one_minute_window_and_30_second_slide():
    epoch = datetime(2026, 10, 26, 12, 0, 42, tzinfo=UTC).timestamp()
    starts = window_starts(epoch)
    assert [datetime.fromtimestamp(value, UTC).isoformat() for value in starts] == [
        "2026-10-26T12:00:00+00:00",
        "2026-10-26T12:00:30+00:00",
    ]


def test_feature_values_and_late_drop_follow_documented_watermark():
    engine = PlainWindowEngine(watermark_seconds=30)
    engine.add(
        event(
            "login",
            "2026-10-26T12:00:10+00:00",
            "login",
            login_success=False,
            country="US",
            risk_score=80,
        ),
        1_000,
        emitted_ms=10_000,
    )
    engine.add(
        event(
            "api",
            "2026-10-26T12:00:20+00:00",
            "api",
            status_code=503,
            response_time_ms=100,
        ),
        2_000,
        emitted_ms=10_000,
    )
    engine.add(
        event(
            "network",
            "2026-10-26T12:01:31+00:00",
            "network",
            bytes_sent=100,
            bytes_received=300,
            destination_ip="203.0.113.10",
        ),
        3_000,
        emitted_ms=10_000,
    )
    matching = [row for row in engine.emitted if row["window_start"] == "2026-10-26T12:00:00+00:00"]
    assert len(matching) == 1
    features = matching[0]["features"]
    assert features["event_count"] == 2
    assert features["failed_login_rate"] == 1
    assert features["api_5xx_rate"] == 1
    assert features["risk_score_moving_average"] == 50
    assert features["distinct_login_country_count"] == 1

    engine.add(
        event("late", "2026-10-26T12:00:00+00:00", "login", login_success=True, country="US"),
        4_000,
        emitted_ms=10_000,
    )
    assert engine.late_events == 1


def test_empty_category_averages_match_spark_null_semantics():
    engine = PlainWindowEngine(watermark_seconds=1)
    engine.add(
        event("one", "2026-10-26T12:00:10+00:00", "login", login_success=True, country="US"),
        1_000,
        emitted_ms=10_000,
    )
    engine.add(
        event("marker", "2026-10-26T12:01:02+00:00", "login", login_success=True, country="US"),
        2_000,
        emitted_ms=10_000,
    )
    row = next(row for row in engine.emitted if row["window_start"] == "2026-10-26T12:00:00+00:00")
    assert row["features"]["avg_api_response_time"] is None
    assert row["features"]["avg_network_bytes"] is None
    assert row["features"]["endpoint_error_rate"] is None
