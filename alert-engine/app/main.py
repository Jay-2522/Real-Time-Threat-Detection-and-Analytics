import logging

from app.config.settings import get_settings
from app.consumers.alert_consumer import AlertEngineConsumer
from app.db.session import create_session_factory
from app.elastic.indexer import AlertIndexer
from app.incidents.correlator import IncidentCorrelator
from app.repositories.alert_repository import AlertRepository
from app.rules.rule_engine import AlertRulesEngine
from app.services.alert_service import AlertProcessingService
from app.utils.logging import configure_logging
from app.utils.metrics import start_metrics_server
from app.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    start_metrics_server(settings.metrics_port)
    logger.info("starting_alert_engine kafka=%s postgres_host=%s", settings.kafka_bootstrap_servers, settings.postgres_host)

    session_factory = retry_with_backoff(lambda: create_session_factory(settings), logger, "PostgreSQL alert schema initialization")
    indexer = AlertIndexer(settings)
    indexer.initialize()

    service = AlertProcessingService(
        rules=AlertRulesEngine(),
        correlator=IncidentCorrelator(settings),
        repository=AlertRepository(),
        indexer=indexer,
        dedup_cooldown_seconds=settings.dedup_cooldown_seconds,
    )
    AlertEngineConsumer(settings, session_factory, service).run()


if __name__ == "__main__":
    main()
