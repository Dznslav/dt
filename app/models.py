import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, JSON
from sqlalchemy.dialects.postgresql import UUID
from database import Base

class Item(Base):
    """
    Основная таблица для хранения бизнес-данных.
    Используем UUID вместо Integer, чтобы избежать коллизий при синхронизации баз с разных узлов.
    """
    __tablename__ = "items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, index=True)
    content = Column(String)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class OutboxEvent(Base):
    """
    Таблица для реализации паттерна Outbox.
    Хранит все операции, которые нужно отправить соседним узлам.
    """
    __tablename__ = "outbox_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    target_peer = Column(String, index=True) # Адрес соседнего узла (например, api2:8000)
    action = Column(String)                  # Тип операции: CREATE, UPDATE, DELETE
    payload = Column(JSON)                   # Сами данные в формате JSON
    status = Column(String, default="pending") # Статус: pending (ожидает) или sent (отправлено)
    created_at = Column(DateTime, default=datetime.utcnow)