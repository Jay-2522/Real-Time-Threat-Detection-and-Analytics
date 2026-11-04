from prometheus_client import Counter, start_http_server

ALERTS_GENERATED_TOTAL = Counter("alerts_generated_total", "Alerts generated", ["severity"])
INCIDENTS_CREATED_TOTAL = Counter("incidents_created_total", "Incidents created")
INCIDENTS_AUTO_RESOLVED_TOTAL = Counter("incidents_auto_resolved_total", "Quiet, untouched incidents resolved automatically")
ALERT_DEDUPLICATIONS_TOTAL = Counter("alert_deduplications_total", "Alerts deduplicated")
CRITICAL_ALERTS_TOTAL = Counter("critical_alerts_total", "Critical alerts generated")
ALERT_ENGINE_ERRORS_TOTAL = Counter("alert_engine_errors_total", "Alert engine errors")


def start_metrics_server(port: int) -> None:
    start_http_server(port)
