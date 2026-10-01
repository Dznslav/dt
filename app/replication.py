"""
Jadro replikácie.

Princípy:
*   Každá zmena (CREATE/UPDATE/DELETE) je operácia s globálne unikátnym op_id, 
    číslom seq na zdrojovom uzle a Lamportovým logickým časom.
*   Operácia nesie ÚPLNÝ stav záznamu po zmene (state-based replikácia), 
    preto nezáleží na poradí doručenia a opakované doručenie je bezpečné.
*   Idempotencia: op_id sa ukladá v logu operations; 
    opätovne prijatá operácia sa rozpozná a ignoruje („duplicate“).
*   Konflikty: Last-Writer-Wins na základe dvojice (lamport, node_id) — deterministicky,
    všetky uzly konvergujú k rovnakému stavu. Paralelné zmeny (značka base operácie ≠ aktuálnej značke záznamu) 
    sa zapisujú do tabuľky conflicts.

Odstránenie = tombstone (deleted=true), aby sa odstránenie tiež replikovalo a záznam pri synchronizácii „neožil“.
"""
from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime

from sqlalchemy import and_, or_, func
from sqlalchemy.orm import Session

from models import Conflict, Item, NodeState, Operation, OutboxEntry, new_uuid, utcnow

# Jeden proces = jeden uzol; zámok serializuje zmeny/sekvencie hodín
_lock = threading.RLock()


class NotFound(Exception):
    pass


class Gone(Exception):
    pass


class VersionMismatch(Exception):
    def __init__(self, current: int):
        super().__init__(f"current version is {current}")
        self.current = current


class IntegrityFailure(Exception):
    pass


# ---------------------------------------------------------------- helpers

def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat(timespec="microseconds") if dt else None


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


CHECKSUM_FIELDS = ("id", "name", "content", "origin_node", "version", "lamport", "updated_by", "deleted")


def compute_checksum(data: dict) -> str:
    canonical = json.dumps({k: data[k] for k in CHECKSUM_FIELDS}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def item_to_dict(item: Item) -> dict:
    return {
        "id": item.id,
        "name": item.name,
        "content": item.content,
        "origin_node": item.origin_node,
        "created_at": _iso(item.created_at),
        "version": item.version,
        "lamport": item.lamport,
        "updated_by": item.updated_by,
        "updated_at": _iso(item.updated_at),
        "deleted": bool(item.deleted),
        "checksum": item.checksum,
    }


def op_to_dict(op: Operation) -> dict:
    return {
        "op_id": op.op_id,
        "origin_node": op.origin_node,
        "seq": op.seq,
        "lamport": op.lamport,
        "action": op.action,
        "item_id": op.item_id,
        "base_version": op.base_version,
        "base_lamport": op.base_lamport,
        "base_node": op.base_node,
        "payload": op.payload,
        "created_at": _iso(op.created_at),
    }


def _stamp(lamport: int, node: str) -> tuple[int, str]:
    return (int(lamport), str(node))


def get_state(db: Session, node_id: str) -> NodeState:
    state = db.get(NodeState, node_id)
    if state is None:
        state = NodeState(node_id=node_id, lamport=0, local_seq=0)
        db.add(state)
        db.flush()
    return state


def _apply_payload(item: Item, p: dict) -> None:
    item.name = p["name"]
    item.content = p["content"]
    item.origin_node = p["origin_node"]
    item.created_at = _parse_dt(p["created_at"])
    item.version = p["version"]
    item.lamport = p["lamport"]
    item.updated_by = p["updated_by"]
    item.updated_at = _parse_dt(p["updated_at"])
    item.deleted = p["deleted"]
    item.checksum = p["checksum"]


# ---------------------------------------------------------------- local writes

def local_change(
    db: Session,
    node_id: str,
    peers: list[str],
    action: str,
    item_id: str | None = None,
    name: str | None = None,
    content: str | None = None,
    expected_version: int | None = None,
    request_id: str | None = None,
) -> tuple[Item, Operation, bool]:
    """Lokálne zmeny: zápis + operácia do logu + outbox pre každého suseda.
    Všetko v JEDNEJ transakcii — operácia sa nemôže stratiť (transactional outbox).
    Vracia (item, op, replayed): replayed=True, ak bol požiadavok s týmto request_id už vykonaný."""
    with _lock:
        if request_id:
            prev = db.query(Operation).filter(Operation.request_id == request_id).first()
            if prev is not None:
                return db.get(Item, prev.item_id), prev, True
        state = get_state(db, node_id)
        now = utcnow()

        if action == "CREATE":
            item = Item(id=new_uuid(), origin_node=node_id, created_at=now, version=0,
                        lamport=0, updated_by=node_id, deleted=False, name="", content="")
            base = (None, None, None)
            db.add(item)
        else:
            item = db.get(Item, item_id)
            if item is None:
                raise NotFound(item_id)
            if item.deleted:
                raise Gone(item_id)
            if expected_version is not None and expected_version != item.version:
                raise VersionMismatch(item.version)
            base = (item.version, item.lamport, item.updated_by)

        state.lamport = max(state.lamport, item.lamport) + 1
        state.local_seq += 1

        if action in ("CREATE", "UPDATE"):
            if name is not None:
                item.name = name
            if content is not None:
                item.content = content
        elif action == "DELETE":
            item.deleted = True

        item.version = (item.version or 0) + 1
        item.lamport = state.lamport
        item.updated_by = node_id
        item.updated_at = now
        payload = item_to_dict(item)
        payload["checksum"] = compute_checksum(payload)
        item.checksum = payload["checksum"]

        op = Operation(
            op_id=new_uuid(), origin_node=node_id, seq=state.local_seq, lamport=state.lamport,
            action=action, item_id=item.id, base_version=base[0], base_lamport=base[1],
            base_node=base[2], payload=payload, created_at=now, received_at=now, result="applied",
            request_id=request_id,
        )
        db.add(op)
        for peer in peers:
            db.add(OutboxEntry(op_id=op.op_id, target_peer=peer, status="pending"))
        db.commit()
        db.refresh(item)
        return item, op, False


# ---------------------------------------------------------------- remote writes

def apply_remote_op(db: Session, node_id: str, op: dict) -> str:
    """Aplikácia operácie od iného uzla. Vracia applied | superseded | duplicate."""
    with _lock:
        if db.get(Operation, op["op_id"]) is not None:
            return "duplicate"

        payload = op["payload"]
        if compute_checksum(payload) != payload.get("checksum"):
            raise IntegrityFailure(f"checksum mismatch for op {op['op_id']}")
        if payload["id"] != op["item_id"]:
            raise IntegrityFailure("payload id does not match item_id")

        state = get_state(db, node_id)
        state.lamport = max(state.lamport, int(op["lamport"]))

        item = db.get(Item, op["item_id"])
        incoming = _stamp(payload["lamport"], payload["updated_by"])
        result = "applied"

        if item is None:
            item = Item(id=payload["id"])
            _apply_payload(item, payload)
            db.add(item)
        else:
            local = _stamp(item.lamport, item.updated_by)
            local_version = item.version
            concurrent = op["action"] in ("UPDATE", "DELETE") and (
                op.get("base_lamport") is not None
                and _stamp(op["base_lamport"], op["base_node"]) != local
                and local != incoming
            )
            if incoming > local:
                winner = "remote"
                _apply_payload(item, payload)
            else:
                winner = "local"
                result = "superseded"
            if concurrent:
                db.add(Conflict(
                    item_id=item.id, op_id=op["op_id"],
                    local_version=local_version,
                    local_lamport=local[0], local_node=local[1],
                    remote_version=payload["version"], remote_lamport=incoming[0],
                    remote_node=incoming[1], winner=winner,
                ))

        db.add(Operation(
            op_id=op["op_id"], origin_node=op["origin_node"], seq=op["seq"], lamport=op["lamport"],
            action=op["action"], item_id=op["item_id"], base_version=op.get("base_version"),
            base_lamport=op.get("base_lamport"), base_node=op.get("base_node"), payload=payload,
            created_at=_parse_dt(op.get("created_at")) or utcnow(), received_at=utcnow(), result=result,
        ))
        db.commit()
        return result


# ---------------------------------------------------------------- anti-entropy

def version_vector(db: Session) -> dict[str, int]:
    """Pre každý zdrojový uzol – dĺžka nepretržitého prefixu známych operácií (seq 1..N).
    Pull požaduje od suseda všetko, čo je po N; ak sa vyskytne "diera",
    doplní sa a už známe operácie sa zahodia ako duplicate."""
    vector: dict[str, int] = {}
    rows = db.query(Operation.origin_node, Operation.seq).order_by(Operation.origin_node, Operation.seq).all()
    for origin, seq in rows:
        n = vector.get(origin, 0)
        if seq == n + 1:
            vector[origin] = seq
        else:
            vector.setdefault(origin, n)
    return vector


def ops_since(db: Session, vector: dict[str, int], limit: int = 500) -> list[Operation]:
    conds = [and_(Operation.origin_node == o, Operation.seq > int(n)) for o, n in vector.items()]
    if vector:
        conds.append(Operation.origin_node.notin_(list(vector.keys())))
        q = db.query(Operation).filter(or_(*conds))
    else:
        q = db.query(Operation)
    return q.order_by(Operation.origin_node, Operation.seq).limit(limit).all()


def manifest(db: Session, node_id: str, include_items: bool = True) -> dict:
    items = db.query(Item).order_by(Item.id).all()
    lines = [f"{i.id}|{i.version}|{i.lamport}|{i.updated_by}|{int(bool(i.deleted))}|{i.checksum}" for i in items]
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()
    live = [i for i in items if not i.deleted]
    result = {
        "node_id": node_id,
        "items_total": len(items),
        "items_live": len(live),
        "items_deleted": len(items) - len(live),
        "operations": db.query(func.count(Operation.op_id)).scalar(),
        "version_vector": version_vector(db),
        "manifest_checksum": digest,
    }
    if include_items:
        result["items"] = [
            {"id": i.id, "version": i.version, "lamport": i.lamport, "updated_by": i.updated_by,
             "origin_node": i.origin_node, "deleted": bool(i.deleted), "checksum": i.checksum}
            for i in items
        ]
    return result


def verify_integrity(db: Session) -> dict:
    """Kontrola, či uložené kontrolné súčty zodpovedajú obsahu."""
    bad = [i.id for i in db.query(Item).all() if compute_checksum(item_to_dict(i)) != i.checksum]
    return {"ok": not bad, "corrupted_items": bad}
