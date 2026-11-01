"""Actual Spark execution. Optional export feeds the ML cross-service regression test.
Run with spark-submit --master local[2] tests/test_feature_contract.py.
"""
import json
import os
import unittest
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from app.config.settings import SparkStreamingSettings
from app.feature_engineering.features import build_feature_pipeline
from app.schemas.telemetry import login_event_schema, api_event_schema, network_event_schema, anomaly_event_schema
from app.transformations.normalization import normalize_event, select_enriched_columns, parse_kafka_json


class SparkFeatureContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("feature-contract-regression")
                     .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        rows = {"login": [], "api": [], "network": [], "anomaly": []}

        def event(kind, **fields):
            row = dict(event_id=f"{kind}-{sum(map(len, rows.values()))}", timestamp="2026-01-01T12:00:10Z",
                       risk_score=80.0, event_type=kind, source_ip="192.0.2.1")
            row.update(fields)
            rows[kind].append(row)

        for success in [False, True, False, True]:
            event("login", login_success=success, country="IN", user_id="u1")
        for status in [200, 404, 500, 503]:
            event("api", status_code=status, response_time_ms=100, endpoint="/a")
        for peer in ["198.51.100.1", "198.51.100.1", "198.51.100.2"]:
            event("network", destination_ip=peer, bytes_sent=100, bytes_received=50)
        event("anomaly", anomaly_type="test")
        event("login", source_ip="192.0.2.2", login_success=True, country="US", risk_score=10.0)
        event("login", timestamp="2026-01-01T12:02:10Z", login_success=True, country="US", risk_score=10.0)
        event("login", source_ip="192.0.2.3", country="IN", login_success=None)
        event("api", source_ip="192.0.2.4", status_code=600, response_time_ms=-1)
        event("network", source_ip="192.0.2.5", destination_ip=None, bytes_sent=None, bytes_received=20)
        event("anomaly", source_ip="192.0.2.6", risk_score=None)
        # Honeypot sensors report neither a login country nor network byte counts: not reported, not invalid.
        event("login", source_ip="192.0.2.8", login_success=False, country=None, risk_score=0.0)
        event("network", source_ip="192.0.2.9", destination_ip="198.51.100.1", bytes_sent=None, bytes_received=None, risk_score=0.0)
        event("network", source_ip="192.0.2.10", destination_ip="198.51.100.1", bytes_sent=5, bytes_received=None)
        event("network", source_ip="192.0.2.7", destination_ip="198.51.100.1",
              bytes_sent=2000000000, bytes_received=2000000000)
        frames = [normalize_event(parse_kafka_json(
            cls.spark.createDataFrame([(json.dumps(row),) for row in rows[kind]], ["value"])
            .withColumn("topic", F.lit(f"telemetry.{kind}.events"))
            .withColumn("partition", F.lit(0)).withColumn("offset", F.lit(0))
            .withColumn("timestamp", F.current_timestamp()).withColumn("key", F.lit("test")), schema))
            for kind, schema in
                  zip(rows, [login_event_schema, api_event_schema, network_event_schema, anomaly_event_schema])]
        cls.login = frames[0]
        output = build_feature_pipeline(*frames, SparkStreamingSettings())
        # Same JSON options as the production Kafka sink, including explicit nulls and UTC.
        cls.output = [json.loads(r.value) for r in output.select(F.to_json(F.struct(*output.columns),
                      {"ignoreNullFields": "false", "timeZone": "UTC"}).alias("value")).collect()]
        export = os.environ.get("SPARK_CONTRACT_OUTPUT")
        if export:
            Path(export).write_text(json.dumps(cls.output, indent=2))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def window(self, ip="192.0.2.1", start="2026-01-01T12:00:00.000Z"):
        return next(p for p in self.output if p["entity_id"] == ip and p["window_start"] == start)

    def test_rates_counts_distinct_and_risk(self):
        f = self.window()["features"]
        self.assertEqual(f["failed_login_rate"], .5)
        self.assertEqual(f["failed_auth_count"], 2)
        self.assertEqual(f["endpoint_error_rate"], .75)
        self.assertEqual(f["api_5xx_rate"], .5)
        self.assertEqual(f["unique_destination_ip_count"], 2)
        self.assertEqual(f["network_event_count"], 3)
        self.assertEqual(f["avg_network_bytes"], 150)
        self.assertEqual(f["risk_score_moving_average"], 80)
        self.assertEqual(f["anomaly_rate"], 1 / 12)

    def test_entity_window_provenance_and_missingness(self):
        mixed = self.window()
        self.assertEqual(len(mixed["source_event_ids"]), 12)
        self.assertEqual(mixed["window_end"], "2026-01-01T12:01:00.000Z")
        for p in [self.window("192.0.2.2"), self.window(start="2026-01-01T12:02:00.000Z")]:
            self.assertEqual(p["features"]["event_count"], 1)
            self.assertEqual(p["features"]["failed_login_rate"], 0)
            self.assertIsNone(p["features"]["api_5xx_rate"])
            self.assertFalse(set(p["source_event_ids"]) & set(mixed["source_event_ids"]))
        self.assertEqual(len([p for p in self.output if p["entity_id"] == "192.0.2.1"]), 4)

    def test_invalid_measurements_flag_whole_window(self):
        for ip in ["192.0.2.3", "192.0.2.4", "192.0.2.5", "192.0.2.6"]:
            self.assertEqual(self.window(ip)["invalid_event_count"], 1)

    def test_unreported_country_and_bytes_are_valid_and_half_reported_bytes_are_not(self):
        login, network, half = self.window("192.0.2.8"), self.window("192.0.2.9"), self.window("192.0.2.10")
        self.assertEqual((login["invalid_event_count"], network["invalid_event_count"]), (0, 0))
        self.assertEqual(login["features"]["distinct_login_country_count"], 0)
        self.assertEqual(login["features"]["failed_login_rate"], 1.0)
        self.assertEqual(network["features"]["avg_network_bytes"], 0.0)  # reported as "none reported", never null
        self.assertEqual(half["invalid_event_count"], 1)

    def test_network_byte_sum_does_not_overflow_int32(self):
        self.assertEqual(self.window("192.0.2.7")["features"]["avg_network_bytes"], 4000000000)

    def test_enriched_login_outcome_is_preserved(self):
        self.assertIn("login_success", select_enriched_columns(self.login).columns)
        self.assertEqual(select_enriched_columns(self.login).filter(F.col("login_success") == False).count(), 3)


if __name__ == "__main__":
    unittest.main()
