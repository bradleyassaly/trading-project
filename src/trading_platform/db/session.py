from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from trading_platform.db.settings import DatabaseSettings


def create_engine_from_settings(settings: DatabaseSettings) -> Engine | None:
    if not settings.enabled or not settings.database_url:
        return None
    engine = create_engine(settings.database_url, echo=settings.echo_sql, future=True)
    if settings.database_schema:
        engine = engine.execution_options(schema_translate_map={None: settings.database_schema})
    return engine


def create_session_factory(settings: DatabaseSettings) -> sessionmaker[Session] | None:
    engine = create_engine_from_settings(settings)
    if engine is None:
        return None
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, class_=Session)


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
