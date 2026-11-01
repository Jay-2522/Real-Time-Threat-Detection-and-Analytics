"""Contract 2.0: atomic, source-IP-scoped, event-time window vectors."""
from pyspark.sql import DataFrame, functions as F
from app.config.settings import SparkStreamingSettings
from app.transformations.normalization import select_enriched_columns

CONTRACT_VERSION = "2.0"


def build_feature_pipeline(login_events, api_events, network_events, anomaly_events,
                           settings: SparkStreamingSettings) -> DataFrame:
    events = select_enriched_columns(login_events)
    for frame in (api_events, network_events, anomaly_events):
        events = events.unionByName(select_enriched_columns(frame))
    return build_window_features(events, settings)


def build_window_features(events: DataFrame, settings: SparkStreamingSettings) -> DataFrame:
    login = F.col("event_type") == "login"
    api = F.col("event_type") == "api"
    network = F.col("event_type") == "network"
    anomaly = F.col("event_type") == "anomaly"

    def count(condition):
        return F.sum(F.when(condition, 1).otherwise(0))

    def present(name):
        return F.col(name).isNotNull() & (F.length(F.trim(F.col(name))) > 0)

    valid = (F.col("risk_score").between(0, 100)
             & (login | api | network | anomaly)
             # Honeypot sensors (Cowrie) report neither a login country nor network byte counts. Missing is
             # "not reported", not "invalid"; a negative or half-reported measurement still is invalid.
             & (~login | F.col("login_success").isNotNull())
             & (~api | (F.col("status_code").between(100, 599) & (F.col("response_time_ms") >= 0)))
             & (~network | (present("destination_ip")
                            & ((F.col("bytes_sent").isNull() & F.col("bytes_received").isNull())
                               | ((F.col("bytes_sent") >= 0) & (F.col("bytes_received") >= 0))))))
    base = (events.filter(present("source_ip"))
        .withWatermark("event_timestamp", settings.watermark_delay)
        .groupBy(F.window("event_timestamp", "1 minute", "30 seconds"), "source_ip")
        .agg(
            F.count("*").alias("event_count"),
            count(login).alias("login_event_count"), count(api).alias("api_event_count"),
            count(network).alias("network_event_count"), count(anomaly).alias("anomaly_event_count"),
            count(~F.coalesce(valid, F.lit(False))).alias("invalid_event_count"),
            count(login & (F.col("login_success") == F.lit(False))).alias("failed_auth_count"),
            count(api & F.col("status_code").between(400, 599)).alias("api_error_count"),
            count(api & F.col("status_code").between(500, 599)).alias("api_5xx_count"),
            F.avg(F.when(api, F.col("response_time_ms"))).alias("avg_api_response_time"),
            F.avg(F.when(network & F.col("bytes_sent").isNotNull() & F.col("bytes_received").isNotNull(),
                         F.col("bytes_sent").cast("double") + F.col("bytes_received").cast("double"))).alias("avg_network_bytes"),
            count(network & (F.col("risk_score") >= 70)).alias("high_risk_network_event_count"),
            F.avg("risk_score").alias("risk_score_moving_average"),
            count(F.col("risk_score") >= 70).alias("high_risk_event_count"),
            F.size(F.collect_set(F.when(login, F.col("country")))).alias("distinct_login_country_count"),
            F.size(F.collect_set(F.when(network, F.col("destination_ip")))).alias("unique_destination_ip_count"),
            F.sort_array(F.collect_list("event_id")).alias("source_event_ids"),
            F.min("event_timestamp").alias("first_event_at"),
            F.max("event_timestamp").alias("last_event_at"),
        ))

    def ratio(numerator, denominator):
        return F.when(F.col(denominator) > 0, F.col(numerator) / F.col(denominator))

    base = (base.withColumn("failed_login_rate", ratio("failed_auth_count", "login_event_count"))
        .withColumn("endpoint_error_rate", ratio("api_error_count", "api_event_count"))
        .withColumn("api_5xx_rate", ratio("api_5xx_count", "api_event_count"))
        .withColumn("anomaly_rate", ratio("anomaly_event_count", "event_count"))
        .withColumn("requests_per_minute", F.col("api_event_count").cast("double"))
        # Network events without byte counts contribute no measurement; 0 means "none reported".
        .withColumn("avg_network_bytes", F.when(F.col("network_event_count") > 0,
                                                F.coalesce(F.col("avg_network_bytes"), F.lit(0.0)))))
    feature_names = [
        "event_count", "login_event_count", "api_event_count", "network_event_count", "anomaly_event_count",
        "failed_login_rate", "failed_auth_count", "requests_per_minute", "avg_api_response_time",
        "endpoint_error_rate", "api_5xx_rate", "avg_network_bytes", "high_risk_network_event_count",
        "anomaly_rate", "risk_score_moving_average", "high_risk_event_count",
        "distinct_login_country_count", "unique_destination_ip_count",
    ]
    return base.select(
        F.lit(CONTRACT_VERSION).alias("feature_contract_version"),
        F.lit("feature_window").alias("event_type"), F.lit("source_ip").alias("entity_type"),
        F.col("source_ip").alias("entity_id"), "source_event_ids", "invalid_event_count",
        F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
        "first_event_at", "last_event_at", F.current_timestamp().alias("calculated_at"),
        F.struct(*[F.col(name).cast("double").alias(name) for name in feature_names]).alias("features"),
    )
