"""The ML service trains on Python windows built from the shared traffic model, but scores windows built by
Spark. If the two ever disagree, the model silently scores a distribution it was not trained on.
Run from spark-jobs/ with: python -m unittest tests.test_traffic_model_parity (repo root on PYTHONPATH).
"""
import json
import math
import unittest

from pyspark.sql import SparkSession, functions as F

from app.config.settings import SparkStreamingSettings
from app.feature_engineering.features import build_feature_pipeline
from app.schemas.telemetry import anomaly_event_schema, api_event_schema, login_event_schema, network_event_schema
from app.transformations.normalization import normalize_event, parse_kafka_json
from pipeline_runtime.traffic_model import build_windows, simulate

SCHEMAS = {"login": login_event_schema, "api": api_event_schema, "network": network_event_schema, "anomaly": anomaly_event_schema}


class TrafficModelParityTests(unittest.TestCase):
    def test_python_windows_match_spark_windows_for_every_feature(self):
        spark = (SparkSession.builder.master("local[2]").appName("traffic-parity")
                 .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        self.addCleanup(spark.stop)
        events = simulate(seed=7, seconds=900)
        rows = {kind: [] for kind in SCHEMAS}
        for index, event in enumerate(events):
            rows[event.fields["event_type"]].append(
                {**event.fields, "event_id": f"e{index}", "timestamp": event.timestamp.isoformat()})
        frames = [normalize_event(parse_kafka_json(
            spark.createDataFrame([(json.dumps(row),) for row in rows[kind]], ["value"])
            .withColumn("topic", F.lit(f"telemetry.{kind}.events")).withColumn("partition", F.lit(0))
            .withColumn("offset", F.lit(0)).withColumn("timestamp", F.current_timestamp())
            .withColumn("key", F.lit("test")), schema)) for kind, schema in SCHEMAS.items()]
        output = build_feature_pipeline(*frames, SparkStreamingSettings())
        spark_windows = {
            (r["entity_id"], r["window_start"]): r for r in (json.loads(x.value) for x in output.select(
                F.to_json(F.struct(*output.columns), {"ignoreNullFields": "false", "timeZone": "UTC"}).alias("value")).collect())
        }
        python_windows = build_windows(events)
        self.assertGreater(len(python_windows), 500)
        for window in python_windows:
            key = (window.entity_id, window.window_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
            self.assertIn(key, spark_windows)
            self.assertEqual(spark_windows[key]["invalid_event_count"], 0, key)
            for name, expected in window.features.items():
                actual = spark_windows[key]["features"][name]
                if expected is None:
                    self.assertIsNone(actual, (key, name))
                else:
                    self.assertTrue(math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9), (key, name, actual, expected))


if __name__ == "__main__":
    unittest.main()
