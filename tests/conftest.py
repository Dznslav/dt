import os
import sys
from pathlib import Path

# Testy fungujú bez Docker/PostgreSQL – na SQLite
os.environ.setdefault("DATABASE_URL", "sqlite:///./test_api_node.db")
os.environ.setdefault("NODE_ID", "A")
os.environ.setdefault("PEERS", "")
os.environ.setdefault("CLUSTER_TOKEN", "test-token")
os.environ.setdefault("AUTO_SYNC", "false")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

import pytest  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402


@pytest.fixture
def make_node():
    """Továreň izolovaných "uzlov" – každý má svoju in-memory databázu."""
    from database import Base
    import models  # noqa: F401 – registrácia tabuliek

    sessions = []

    def _make():
        engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
        Base.metadata.create_all(engine)
        s = sessionmaker(bind=engine)()
        sessions.append(s)
        return s

    yield _make
    for s in sessions:
        s.close()
