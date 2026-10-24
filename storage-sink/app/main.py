import logging

from app.config.settings import get_settings
from app.consumers.storage_consumer import StorageSinkConsumer
from app.db.session import create_session_factory
from app.elastic.indexer import ElasticsearchIndexer
from app.repositories.postgres_repository import PostgresStorageRepository
from app.utils.logging import configure_logging
from app.utils.metrics import start_metrics_server
from app.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.storage_sink_log_level)
    start_metrics_server(settings.metrics_port)
    logger.info("starting_storage_sink kafka=%s postgres_host=%s", settings.kafka_bootstrap_servers, settings.postgres_host)

    session_factory = retry_with_backoff(lambda: create_session_factory(settings), logger, "PostgreSQL connection and schema initialization")
    logger.info("postgres_connected schema_initialized=true")

    indexer = ElasticsearchIndexer(settings)
    indexer.initialize()

    StorageSinkConsumer(
        settings=settings,
        session_factory=session_factory,
        repository=PostgresStorageRepository(),
        indexer=indexer,
    ).run()


if __name__ == "__main__":
    main()
