from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.db.models import Base

logger = logging.getLogger(__name__)

_settings = get_settings()
_connect_args = {}
_url = make_url(_settings.database_url)
if _url.get_backend_name() == "sqlite" and _url.database:
    Path(_url.database).resolve().parent.mkdir(parents=True, exist_ok=True)
    _connect_args = {"check_same_thread": False}

engine = create_engine(
    _settings.database_url,
    connect_args=_connect_args,
    future=True,
    echo=False,
)

if _settings.database_url.startswith("sqlite"):

    @event.listens_for(Engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):  # type: ignore[no-redef]
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    logger.info("Veritabanı tabloları hazır.")
