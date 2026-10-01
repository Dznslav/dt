from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import Session

import config
import logging_setup
import replication
import sync
from database import Base, db_ok, engine, get_db, wait_for_db
from models import Conflict, Item, Operation, OutboxEntry

logging_setup.setup()
log = logging.getLogger("node.api")
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(_: FastAPI):
    await asyncio.to_thread(wait_for_db)
    Base.metadata.create_all(bind=engine)
    log.info("node %s started, peers=%s", config.NODE_ID, config.PEERS)
    task = asyncio.create_task(sync.worker())
    yield
    task.cancel()


app = FastAPI(title=f"Distributed IS – node {config.NODE_ID}", version="2.0", lifespan=lifespan)


@app.middleware("http")
async def access_log(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    path = request.url.path
    noisy = path in ("/health", "/live", "/metrics", "/internal/ping", "/internal/status", "/internal/ops/since") or (
        request.method == "GET" and (path.startswith("/api/") or path == "/"))
    if not noisy:
        log.info("%s %s -> %s", request.method, path, response.status_code,
                 extra={"method": request.method, "path": path, "status": response.status_code,
                        "ms": round((time.perf_counter() - start) * 1000, 1)})
    return response


# ================================================================ schemas

class ItemCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    content: str = Field(default="", max_length=10_000)


class ItemUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    content: str | None = Field(default=None, max_length=10_000)
    expected_version: int | None = Field(default=None, description="optimistic locking")


class OpsBatch(BaseModel):
    ops: list[dict]


class SinceRequest(BaseModel):
    vector: dict[str, int] = {}
    limit: int = Field(default=500, ge=1, le=5000)


class Toggle(BaseModel):
    enabled: bool


# ================================================================ security (medziuzlové požiadavky)

def require_cluster(x_cluster_token: str | None = Header(default=None)):
    if x_cluster_token != config.CLUSTER_TOKEN:
        raise HTTPException(status_code=401, detail="invalid cluster token")
    if sync.runtime.isolated:
        # simulácia prerušenia: uzol "ne počuje" susedov
        raise HTTPException(status_code=503, detail="node isolated")


# ================================================================ health / status

@app.get("/live", include_in_schema=False)
def live():
    return {"status": "alive", "node_id": config.NODE_ID}


@app.get("/health")
def health():
    """Health check pre Docker: proces je živý a databáza je dostupná."""
    ok = db_ok()
    body = {"status": "ok" if ok else "degraded", "node_id": config.NODE_ID, "database": ok}
    return JSONResponse(body, status_code=200 if ok else 503)


def _status(db: Session) -> dict:
    pending = dict(db.query(OutboxEntry.target_peer, func.count(OutboxEntry.id))
                   .filter(OutboxEntry.status == "pending").group_by(OutboxEntry.target_peer).all())
    m = replication.manifest(db, config.NODE_ID, include_items=False)
    rt = sync.runtime
    return {
        "node_id": config.NODE_ID,
        "status": "isolated" if rt.isolated else "online",
        "database": db_ok(),
        "auto_sync": rt.auto_sync,
        "started_at": rt.started_at.isoformat(timespec="seconds"),
        "items_total": m["items_total"],
        "items_live": m["items_live"],
        "items_deleted": m["items_deleted"],
        "operations": m["operations"],
        "version_vector": m["version_vector"],
        "manifest_checksum": m["manifest_checksum"],
        "outbox_pending": sum(pending.values()),
        "outbox_pending_by_peer": {p: pending.get(p, 0) for p in rt.peers},
        "conflicts": db.query(func.count(Conflict.id)).scalar(),
        "peers": {p: {k: v for k, v in i.items() if k != "backoff_until"} for p, i in rt.peers.items()},
        "last_sync": rt.last_sync,
    }


@app.get("/api/status")
def status(db: Session = Depends(get_db)):
    return _status(db)


@app.get("/api/cluster")
async def cluster(db: Session = Depends(get_db)):
    """Súhrnný stav všetkých uzlov (z pohľadu tohto uzla)."""
    nodes = [dict(_status(db), reachable=True)]
    async with httpx.AsyncClient() as client:
        async def fetch(peer: str, url: str):
            try:
                r = await sync._request(client, "GET", f"{url}/internal/status")
                return dict(r.json(), reachable=True)
            except sync.PeerUnreachable as exc:
                return {"node_id": peer, "reachable": False, "error": str(exc)}
        nodes += await asyncio.gather(*(fetch(p, i["url"]) for p, i in sync.runtime.peers.items()))
    sums = {n["manifest_checksum"] for n in nodes if n.get("reachable")}
    return {"viewed_from": config.NODE_ID, "nodes": sorted(nodes, key=lambda n: n["node_id"]),
            "all_reachable": all(n.get("reachable") for n in nodes),
            "consistent": len(sums) == 1 and all(n.get("reachable") for n in nodes)}


@app.get("/metrics", response_class=PlainTextResponse, include_in_schema=False)
def metrics(db: Session = Depends(get_db)):
    s = _status(db)
    lines = [
        f'dt_items_live{{node="{s["node_id"]}"}} {s["items_live"]}',
        f'dt_items_deleted{{node="{s["node_id"]}"}} {s["items_deleted"]}',
        f'dt_operations_total{{node="{s["node_id"]}"}} {s["operations"]}',
        f'dt_conflicts_total{{node="{s["node_id"]}"}} {s["conflicts"]}',
    ]
    for peer, n in s["outbox_pending_by_peer"].items():
        lines.append(f'dt_outbox_pending{{node="{s["node_id"]}",peer="{peer}"}} {n}')
    for peer, i in s["peers"].items():
        lines.append(f'dt_peer_up{{node="{s["node_id"]}",peer="{peer}"}} {1 if i["reachable"] else 0}')
    return "\n".join(lines) + "\n"


# ================================================================ items (CRUD)

@app.get("/api/items")
def list_items(include_deleted: bool = False, db: Session = Depends(get_db)):
    q = db.query(Item)
    if not include_deleted:
        q = q.filter(Item.deleted.is_(False))
    return [replication.item_to_dict(i) for i in q.order_by(Item.created_at).all()]


@app.get("/api/items/{item_id}")
def get_item(item_id: str, db: Session = Depends(get_db)):
    item = db.get(Item, item_id)
    if item is None:
        raise HTTPException(404, "item not found")
    history = db.query(Operation).filter(Operation.item_id == item_id).order_by(Operation.lamport).all()
    return dict(replication.item_to_dict(item),
                history=[{k: v for k, v in replication.op_to_dict(o).items() if k != "payload"}
                         | {"result": o.result} for o in history])


def _peers() -> list[str]:
    return list(sync.runtime.peers.keys())


@app.post("/api/items", status_code=201)
def create_item(body: ItemCreate, db: Session = Depends(get_db),
                idempotency_key: str | None = Header(default=None, max_length=64)):
    item, op, replayed = replication.local_change(
        db, config.NODE_ID, _peers(), "CREATE", name=body.name, content=body.content,
        request_id=idempotency_key)
    data = replication.item_to_dict(item)
    return JSONResponse(dict(data, op_id=op.op_id, replayed=replayed), status_code=200 if replayed else 201)


@app.put("/api/items/{item_id}")
def update_item(item_id: str, body: ItemUpdate, db: Session = Depends(get_db),
                idempotency_key: str | None = Header(default=None, max_length=64)):
    if body.name is None and body.content is None:
        raise HTTPException(422, "nothing to update")
    try:
        item, op, replayed = replication.local_change(
            db, config.NODE_ID, _peers(), "UPDATE", item_id=item_id, name=body.name,
            content=body.content, expected_version=body.expected_version, request_id=idempotency_key)
    except replication.NotFound:
        raise HTTPException(404, "item not found")
    except replication.Gone:
        raise HTTPException(410, "item was deleted")
    except replication.VersionMismatch as exc:
        raise HTTPException(409, f"version conflict: current version is {exc.current}")
    return dict(replication.item_to_dict(item), op_id=op.op_id, replayed=replayed)


@app.delete("/api/items/{item_id}")
def delete_item(item_id: str, db: Session = Depends(get_db),
                idempotency_key: str | None = Header(default=None, max_length=64)):
    try:
        item, op, replayed = replication.local_change(
            db, config.NODE_ID, _peers(), "DELETE", item_id=item_id, request_id=idempotency_key)
    except replication.NotFound:
        raise HTTPException(404, "item not found")
    except replication.Gone:
        # opätovné odstránenie – idempotentné
        return {"id": item_id, "deleted": True, "replayed": True}
    return dict(replication.item_to_dict(item), op_id=op.op_id, replayed=replayed)


# ================================================================ diagnostika / replikácia

@app.get("/api/manifest")
def get_manifest(db: Session = Depends(get_db)):
    return replication.manifest(db, config.NODE_ID)


@app.get("/api/integrity")
def integrity(db: Session = Depends(get_db)):
    return replication.verify_integrity(db)


@app.get("/api/operations")
def operations(limit: int = 50, db: Session = Depends(get_db)):
    ops = db.query(Operation).order_by(Operation.received_at.desc()).limit(min(limit, 1000)).all()
    return [{k: v for k, v in replication.op_to_dict(o).items() if k != "payload"}
            | {"result": o.result, "name": o.payload.get("name")} for o in ops]


@app.get("/api/outbox")
def outbox(status: str = "pending", db: Session = Depends(get_db)):
    rows = db.query(OutboxEntry).filter(OutboxEntry.status == status).order_by(OutboxEntry.id).limit(500).all()
    return [{"id": r.id, "op_id": r.op_id, "target_peer": r.target_peer, "status": r.status,
             "attempts": r.attempts, "last_error": r.last_error,
             "created_at": r.created_at.isoformat(), "sent_at": r.sent_at and r.sent_at.isoformat()}
            for r in rows]


@app.get("/api/conflicts")
def conflicts(db: Session = Depends(get_db)):
    rows = db.query(Conflict).order_by(Conflict.id.desc()).limit(200).all()
    return [{c: getattr(r, c) for c in ("id", "item_id", "op_id", "local_version", "local_lamport",
             "local_node", "remote_version", "remote_lamport", "remote_node", "winner")}
            | {"detected_at": r.detected_at.isoformat()} for r in rows]


@app.post("/api/sync")
async def manual_sync():
    """Manuálna synchronizácia. Ak je aspoň jeden sused nedostupný – HTTP 503 (bezpečné odmietnutie):
    nič sa nestratí, neodoslané operácie zostanú v outboxe."""
    results = await sync.sync_all(manual=True)
    ok = all(r["ok"] for r in results)
    db = next(get_db())
    try:
        pending = db.query(func.count(OutboxEntry.id)).filter(OutboxEntry.status == "pending").scalar()
    finally:
        db.close()
    body = {"node_id": config.NODE_ID, "ok": ok, "results": results, "outbox_pending_after": pending}
    return JSONResponse(body, status_code=200 if ok else 503)


@app.post("/api/admin/isolate")
def isolate(body: Toggle):
    """Programová simulácia odpojenia uzla od siete (alternatíva k docker network disconnect)."""
    sync.runtime.isolated = body.enabled
    log.warning("node isolation set to %s", body.enabled)
    return {"node_id": config.NODE_ID, "isolated": body.enabled}


@app.post("/api/admin/autosync")
def autosync(body: Toggle):
    sync.runtime.auto_sync = body.enabled
    log.info("auto sync set to %s", body.enabled)
    return {"node_id": config.NODE_ID, "auto_sync": body.enabled}


# ================================================================ internal (uzol ↔ uzol)

@app.get("/internal/ping", dependencies=[Depends(require_cluster)])
def ping():
    return {"node_id": config.NODE_ID}


@app.get("/internal/status", dependencies=[Depends(require_cluster)])
def internal_status(db: Session = Depends(get_db)):
    return _status(db)


@app.post("/internal/ops", dependencies=[Depends(require_cluster)])
def receive_ops(batch: OpsBatch, db: Session = Depends(get_db)):
    """Prijatie operácií od suseda (push). Idempotentne podľa op_id."""
    results = []
    for op in batch.ops:
        try:
            results.append(replication.apply_remote_op(db, config.NODE_ID, op))
        except (replication.IntegrityFailure, KeyError) as exc:
            db.rollback()
            log.error("rejected op %s: %s", op.get("op_id"), exc)
            results.append("rejected")
    return {"node_id": config.NODE_ID, "results": results}


@app.post("/internal/ops/since", dependencies=[Depends(require_cluster)])
def ops_since(req: SinceRequest, db: Session = Depends(get_db)):
    """Anti-entropy: vrátiť operácie, ktoré chýbajú požadujúcemu uzlu."""
    ops = replication.ops_since(db, req.vector, req.limit)
    return {"node_id": config.NODE_ID, "ops": [replication.op_to_dict(o) for o in ops]}


# ================================================================ dashboard

@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(STATIC / "index.html")
