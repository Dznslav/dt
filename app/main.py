import os
from fastapi import FastAPI

# Считываем конфигурацию узла
NODE_ID = os.getenv("NODE_ID", "unknown")
PEERS = [peer for peer in os.getenv("PEERS", "").split(",") if peer]
DB_HOST = os.getenv("DB_HOST", "localhost")

app = FastAPI(title=f"Distributed Node {NODE_ID}")

@app.get("/")
async def root():
    """Главная страница узла для быстрого дебага."""
    return {
        "status": "online",
        "node_id": NODE_ID,
        "database_host": DB_HOST,
        "configured_peers": PEERS
    }

@app.get("/health")
async def health_check():
    """
    Эндпоинт для проверки жизнеспособности (health checks).
    Docker Compose использует его, чтобы узнать, поднялось ли API.
    """
    # В будущем здесь можно добавить проверку подключения к БД
    return {"status": "ok", "node_id": NODE_ID}