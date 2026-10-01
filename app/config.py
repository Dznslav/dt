import os


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


NODE_ID: str = os.getenv("NODE_ID", "A")

# Susedia: "B=http://api-b:8000,C=http://api-c:8000"
# (pre kompatibilitu sa prijíma aj starý formát "api-b:8000,api-c:8000")
def _parse_peers(raw: str) -> dict[str, str]:
    peers: dict[str, str] = {}
    for part in [p.strip() for p in raw.split(",") if p.strip()]:
        if "=" in part:
            name, url = part.split("=", 1)
        else:
            name, url = part.split(":")[0], part
        if not url.startswith("http"):
            url = "http://" + url
        peers[name.strip()] = url.rstrip("/")
    return peers


PEERS: dict[str, str] = _parse_peers(os.getenv("PEERS", ""))

# Databáza: buď plný DATABASE_URL, alebo PostgreSQL z častí
DATABASE_URL: str = os.getenv("DATABASE_URL") or (
    "postgresql+psycopg2://{u}:{p}@{h}:{port}/{db}".format(
        u=os.getenv("POSTGRES_USER", "admin"),
        p=os.getenv("POSTGRES_PASSWORD", "secretpassword"),
        h=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        db=os.getenv("POSTGRES_DB", "node_db"),
    )
)

# Spoločný kľúč pre medziuzlové požiadavky (hlavička X-Cluster-Token)
CLUSTER_TOKEN: str = os.getenv("CLUSTER_TOKEN", "change-me")

# Synchronizácia na pozadí
AUTO_SYNC: bool = _bool("AUTO_SYNC", True)
SYNC_INTERVAL: float = float(os.getenv("SYNC_INTERVAL", "5"))
PEER_TIMEOUT: float = float(os.getenv("PEER_TIMEOUT", "3"))
MAX_BACKOFF: float = float(os.getenv("MAX_BACKOFF", "60"))
PULL_BATCH: int = int(os.getenv("PULL_BATCH", "500"))
