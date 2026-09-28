import os
import uuid
import asyncio
import httpx
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel

import models
from database import engine, get_db, SessionLocal

# Автоматически создаем таблицы в базе данных при старте приложения
models.Base.metadata.create_all(bind=engine)

NODE_ID = os.getenv("NODE_ID", "unknown")
PEERS = [peer for peer in os.getenv("PEERS", "").split(",") if peer]

app = FastAPI(title=f"Distributed Node {NODE_ID}")

# --- Схемы данных (Pydantic) ---
class ItemCreate(BaseModel):
    name: str
    content: str

class ItemResponse(BaseModel):
    id: uuid.UUID
    name: str
    content: str

    model_config = {"from_attributes": True}

class ReplicationPayload(BaseModel):
    id: str  # Принимаем как строку, чтобы избежать проблем сериализации
    name: str
    content: str


# --- Эндпоинты ---

@app.get("/")
def dashboard(db: Session = Depends(get_db)):
    """Jednoduchý stavový panel uzlov."""
    items_count = db.query(models.Item).count()
    pending_sync = db.query(models.OutboxEvent).filter(models.OutboxEvent.status == "pending").count()
    
    return {
        "node_id": NODE_ID,
        "status": "online",
        "configured_peers": PEERS,
        "db_stats": {
            "total_items": items_count,
            "pending_sync_tasks": pending_sync
        },
        "links": {
            "api_docs": "/docs",
            "health_check": "/health"
        }
    }

@app.post("/items/", response_model=ItemResponse)
def create_item(item: ItemCreate, db: Session = Depends(get_db)):
    """
    Создание новой записи с локальным сохранением и генерацией событий для соседей.
    """
    # 1. Создаем локальную запись
    db_item = models.Item(name=item.name, content=item.content)
    db.add(db_item)
    
    # 2. Формируем тело сообщения для отправки
    payload = {
        "id": str(db_item.id),
        "name": db_item.name,
        "content": db_item.content
    }
    
    # 3. Записываем события в Outbox для каждого соседа
    # Гарантирует постоянное сохранение неотправленных операций при сбоях сети[cite: 2].
    for peer in PEERS:
        outbox_event = models.OutboxEvent(
            target_peer=peer,
            action="CREATE",
            payload=payload
        )
        db.add(outbox_event)
        
    # 4. Фиксируем транзакцию в БД
    db.commit()
    db.refresh(db_item)
    return db_item

@app.get("/items/", response_model=list[ItemResponse])
def get_items(db: Session = Depends(get_db)):
    """Возвращает все локальные записи узла."""
    return db.query(models.Item).all()

@app.get("/health")
def health_check():
    """Эндпоинт для проверки жизнеспособности (health checks)[cite: 2]."""
    return {"status": "ok", "node_id": NODE_ID}

@app.post("/replicate/")
def replicate_item(payload: ReplicationPayload, db: Session = Depends(get_db)):
    """
    Эндпоинт, на который соседи присылают свои изменения.
    Реализует идемпотентное повторение операций[cite: 2].
    """
    try:
        item_uuid = uuid.UUID(payload.id)
    except ValueError:
        return {"status": "error", "message": "Invalid UUID format"}

    # Проверяем, существует ли уже такая запись (защита от дубликатов)
    existing_item = db.query(models.Item).filter(models.Item.id == item_uuid).first()
    
    if existing_item:
        return {"status": "ignored", "reason": "already exists"}
    
    # Если записи нет, создаем её локально
    new_item = models.Item(
        id=item_uuid,
        name=payload.name,
        content=payload.content
    )
    db.add(new_item)
    db.commit()
    
    return {"status": "replicated"}


# --- Фоновый воркер синхронизации ---

async def sync_outbox():
    """
    Фоновый процесс, который циклично проверяет таблицу outbox_events
    и отправляет неотправленные операции соседям.
    """
    while True:
        await asyncio.sleep(5)  # Проверка очереди каждые 5 секунд
        
        db = SessionLocal()
        try:
            pending_events = db.query(models.OutboxEvent).filter(models.OutboxEvent.status == "pending").all()
            
            if not pending_events:
                continue

            async with httpx.AsyncClient() as client:
                for event in pending_events:
                    target_url = f"http://{event.target_peer}/replicate/"
                    try:
                        response = await client.post(target_url, json=event.payload, timeout=5.0)
                        if response.status_code == 200:
                            event.status = "sent"
                            db.commit()
                    except httpx.RequestError:
                        # Узел недоступен (имитация обрыва сети). 
                        # Задача остается в статусе "pending" до восстановления связи[cite: 2].
                        pass
        finally:
            db.close()

@app.on_event("startup")
async def startup_event():
    """Запускаем воркер синхронизации при старте приложения."""
    asyncio.create_task(sync_outbox())