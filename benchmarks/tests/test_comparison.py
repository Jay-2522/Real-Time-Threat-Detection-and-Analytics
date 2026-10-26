from benchmarks.comparison import compare_feature_values


def row(entity, start, value):
    return {
        "entity_id": entity,
        "window_start": start,
        "window_end": "2026-10-26T12:01:00+00:00",
        "features": {"event_count": value, "avg_api_response_time": None},
    }


def test_feature_comparison_reports_values_and_missing_windows_separately():
    spark = [row("one", "2026-10-26T12:00:00Z", 4.0), row("spark-only", "2026-10-26T12:00:00Z", 1)]
    python = [row("one", "2026-10-26T12:00:00+00:00", 4.0000001), row("python-only", "2026-10-26T12:00:00Z", 1)]
    result = compare_feature_values(spark, python)
    assert result["common_windows"] == 1
    assert result["spark_only_windows"] == result["python_only_windows"] == 1
    assert result["compared_values"] == 2
    assert result["mismatched_values"] == 0
    assert result["mismatch_rate"] == 0


def test_feature_comparison_counts_real_value_mismatch():
    result = compare_feature_values(
        [row("one", "2026-10-26T12:00:00Z", 4)],
        [row("one", "2026-10-26T12:00:00Z", 5)],
    )
    assert result["mismatched_values"] == 1
    assert result["mismatch_rate"] == 0.5
