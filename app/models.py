"""Model údajov uzla.

items          — obchodné údaje (aktuálny stav záznamu vrátane tombstonu pri zmazaní)
operations     — záznam všetkých operácií (lokálnych a prijatých) = inbox pre idempotenciu
outbox         — neodoslané operácie pre každého suseda (prežijú reštart / výpadok siete)
conflicts      — záznam zistených konfliktov paralelných zmien
node_state     — Lamportove hodiny a počítadlo vlastných operácií uzla
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, String, Text, UniqueConstraint

from database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_uuid() -> str:
    return str(uuid.uuid4())


class Item(Base):
    __tablename__ = "items"

    # UUID – globálne jedinečné ID; kolízie medzi uzlami nie sú možné
    id = Column(String(36), primary_key=True, default=new_uuid)
    name = Column(String(200), nullable=False, index=True)
    content = Column(Text, nullable=False, default="")
    # Pôvod záznamu
    origin_node = Column(String(20), nullable=False)       # uzol, kde bol záznam vytvorený
    created_at = Column(DateTime, nullable=False, default=utcnow)
    # Verzie
    version = Column(Integer, nullable=False, default=1)   # číslo revízie záznamu
    lamport = Column(Integer, nullable=False, default=0)   # logický čas poslednej zmeny
    updated_by = Column(String(20), nullable=False)        # uzol poslednej zmeny
    updated_at = Column(DateTime, nullable=False, default=utcnow)
    deleted = Column(Boolean, nullable=False, default=False)  # tombstone
    checksum = Column(String(64), nullable=False)          # SHA-256 obsahu záznamu


class Operation(Base):
    __tablename__ = "operations"
    __table_args__ = (UniqueConstraint("origin_node", "seq", name="uq_origin_seq"),)

    op_id = Column(String(36), primary_key=True)           # idempotentný kľúč operácie
    origin_node = Column(String(20), nullable=False, index=True)
    seq = Column(Integer, nullable=False)                  # poradové číslo na zdrojovom uzle
    lamport = Column(Integer, nullable=False)
    action = Column(String(10), nullable=False)            # CREATE | UPDATE | DELETE
    item_id = Column(String(36), nullable=False, index=True)
    base_version = Column(Integer, nullable=True)          # na akej verzii je založená zmena
    base_lamport = Column(Integer, nullable=True)
    base_node = Column(String(20), nullable=True)
    payload = Column(JSON, nullable=False)                 # úplný stav záznamu po operácii
    created_at = Column(DateTime, nullable=False, default=utcnow)
    received_at = Column(DateTime, nullable=False, default=utcnow)
    result = Column(String(20), nullable=False, default="applied")  # applied | superseded
    # Idempotency-Key od klienta: opakovaná rovnaká požiadavka nevytvorí duplicitný záznam
    request_id = Column(String(64), nullable=True, unique=True)


class OutboxEntry(Base):
    __tablename__ = "outbox"
    __table_args__ = (UniqueConstraint("op_id", "target_peer", name="uq_outbox_op_peer"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    op_id = Column(String(36), nullable=False, index=True)
    target_peer = Column(String(20), nullable=False, index=True)
    status = Column(String(10), nullable=False, default="pending", index=True)  # pending | sent
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(String(300), nullable=True)
    created_at = Column(DateTime, nullable=False, default=utcnow)
    sent_at = Column(DateTime, nullable=True)


class Conflict(Base):
    __tablename__ = "conflicts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(String(36), nullable=False, index=True)
    op_id = Column(String(36), nullable=False, unique=True)
    local_version = Column(Integer)
    local_lamport = Column(Integer)
    local_node = Column(String(20))
    remote_version = Column(Integer)
    remote_lamport = Column(Integer)
    remote_node = Column(String(20))
    winner = Column(String(10), nullable=False)            # local | remote
    detected_at = Column(DateTime, nullable=False, default=utcnow)


class NodeState(Base):
    __tablename__ = "node_state"

    node_id = Column(String(20), primary_key=True)
    lamport = Column(Integer, nullable=False, default=0)
    local_seq = Column(Integer, nullable=False, default=0)
