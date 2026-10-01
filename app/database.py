import logging
import time

from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

import config

log = logging.getLogger("node.db")

_connect_args = {"check_same_thread": False} if config.DATABASE_URL.startswith("sqlite") else {}

# pool_pre_ping — kontroluje, či pripojenie k DB je stále platné (dôležité pri reštarte kontajnerov)
engine = create_engine(config.DATABASE_URL, pool_pre_ping=True, connect_args=_connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def wait_for_db(retries: int = 30, delay: float = 2.0) -> None:
    """Čakáme, kým BD nebude dostupná (dôležité po reštarte kontajnerov)."""
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as exc:  # noqa: BLE001
            log.warning("DB not ready (attempt %s/%s): %s", attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError("Database is not reachable")


def db_ok() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False
