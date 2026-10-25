"""Public probes expose no topology; detailed status remains behind RBAC."""
from functools import lru_cache
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from app.core.settings import settings


@lru_cache
def readiness_engine():
    options = {'connect_timeout': 2, 'options': '-c statement_timeout=2000 -c lock_timeout=1000'} if settings.database_url.startswith('postgresql') else {'timeout': 2}
    return create_engine(settings.database_url, poolclass=NullPool, connect_args=options)


def is_ready():
    # PostgreSQL is essential for both authorization and persisted security records.
    # Search/monitoring degradation is reported by authenticated monitoring APIs.
    try:
        with readiness_engine().connect() as connection:
            if connection.execute(text('SELECT id FROM auth_control WHERE id=1')).scalar() != 1:
                return False
            connection.execute(text('SELECT id FROM alerts LIMIT 0'))
        return True
    except Exception:
        return False  # Never return driver exception strings or credentials publicly.
