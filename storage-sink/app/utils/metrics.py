from prometheus_client import Counter, Gauge, start_http_server

RECORDS_PERSISTED_TOTAL = Counter("records_persisted_total", "Storage sink records persisted", ["record_kind"])
POSTGRES_WRITE_ERRORS_TOTAL = Counter("postgres_write_errors_total", "PostgreSQL write errors")
ELASTICSEARCH_INDEX_ERRORS_TOTAL = Counter("elasticsearch_index_errors_total", "Elasticsearch index errors")
STORAGE_SINK_LAG_ESTIMATE = Gauge("storage_sink_lag_estimate", "Storage sink lag estimate")


def start_metrics_server(port: int) -> None:
    start_http_server(port)
