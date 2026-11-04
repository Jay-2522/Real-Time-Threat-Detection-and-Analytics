import logging

from prometheus_client import start_http_server

from app.config.settings import get_settings
from app.consumers.threat_consumer import ThreatIntelConsumer
from app.db.session import SessionLocal, initialize_database
from app.detections.rule_engine import DetectionRuleEngine
from app.elastic.indexer import ThreatIndexer
from app.enrichment.ioc_enricher import IOCEnricher
from app.repositories.threat_repository import ThreatRepository
from app.scoring.threat_scorer import ThreatScorer
from app.utils.logging import configure_logging
from app.utils.retry import retry_with_backoff


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = logging.getLogger(__name__)
    start_http_server(settings.metrics_port)
    logger.info("threat_metrics_started port=%s", settings.metrics_port)
    retry_with_backoff(initialize_database, logger, "PostgreSQL threat schema initialization")
    rules = DetectionRuleEngine(settings.rules_path)
    repository = ThreatRepository()

    def publish_rule_catalog() -> None:
        with SessionLocal() as session, session.begin():
            repository.sync_rule_catalog(session, rules.catalog())

    retry_with_backoff(publish_rule_catalog, logger, "Detection rule catalog report")
    consumer = ThreatIntelConsumer(
        settings=settings,
        session_factory=SessionLocal,
        enricher=IOCEnricher(),
        rules=rules,
        scorer=ThreatScorer(),
        repository=repository,
        indexer=ThreatIndexer(settings.elasticsearch_host),
    )
    consumer.run()


if __name__ == "__main__":
    main()
