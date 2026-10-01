"""
Synchronizácia medzi uzlami (push outbox + pull anti-entropy).

push — posielame susedovi svoje neodoslané operácie z outboxu (v poradí).
pull —  posielame susedovi svoj vektor verzií, on vráti operácie, ktoré nám chýbajú. Takto uzol,
        ktorý sa pripojil po výpadku, dobehne aj tie operácie, ktorých zdroj je momentálne nedostupný (má ich tretí uzol).
Akákoľvek chyba siete ponechá operácie v outboxe => nič sa nestratí, opakovanie je bezpečné.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

import httpx

import config
import replication
from database import SessionLocal
from models import Operation, OutboxEntry, utcnow

log = logging.getLogger("node.sync")


class PeerUnreachable(Exception):
    pass


class Runtime:
    """Stav uzla v pamäti procesu."""

    def __init__(self) -> None:
        self.isolated = False               # programová simulácia výpadku siete
        self.auto_sync = config.AUTO_SYNC
        self.peers: dict[str, dict] = {
            name: {"url": url, "reachable": None, "last_ok": None, "last_error": None,
                   "last_attempt": None, "failures": 0, "backoff_until": 0.0}
            for name, url in config.PEERS.items()
        }
        self.last_sync: dict | None = None
        self.started_at = datetime.now(timezone.utc)
        self.lock = asyncio.Lock()


runtime = Runtime()

HEADERS = {"X-Cluster-Token": config.CLUSTER_TOKEN, "X-Node-Id": config.NODE_ID}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def _request(client: httpx.AsyncClient, method: str, url: str, **kw) -> httpx.Response:
    if runtime.isolated:
        raise PeerUnreachable("node is isolated from the network (simulation)")
    try:
        resp = await client.request(method, url, headers=HEADERS, timeout=config.PEER_TIMEOUT, **kw)
    except httpx.HTTPError as exc:
        raise PeerUnreachable(f"{type(exc).__name__}: {exc}") from exc
    if resp.status_code >= 500:
        raise PeerUnreachable(f"HTTP {resp.status_code}: {resp.text[:120]}")
    if resp.status_code >= 400:
        raise PeerUnreachable(f"HTTP {resp.status_code}: {resp.text[:120]}")
    return resp


# ---------------------------------------------------------------- Pomocné funkcie DB (v vlákne)

def _pending_for(peer: str, limit: int) -> list[tuple[int, dict]]:
    db = SessionLocal()
    try:
        rows = (db.query(OutboxEntry, Operation)
                .join(Operation, Operation.op_id == OutboxEntry.op_id)
                .filter(OutboxEntry.target_peer == peer, OutboxEntry.status == "pending")
                .order_by(OutboxEntry.id).limit(limit).all())
        return [(o.id, replication.op_to_dict(op)) for o, op in rows]
    finally:
        db.close()


def _mark(entry_ids: list[int], results: list[str]) -> None:
    db = SessionLocal()
    try:
        for eid, res in zip(entry_ids, results):
            e = db.get(OutboxEntry, eid)
            e.attempts += 1
            if res == "rejected":
                e.status = "rejected"
                e.last_error = "rejected by peer (integrity)"
            else:
                e.status = "sent"
                e.sent_at = utcnow()
                e.last_error = None
        db.commit()
    finally:
        db.close()


def _mark_failed(peer: str, error: str) -> None:
    db = SessionLocal()
    try:
        (db.query(OutboxEntry)
         .filter(OutboxEntry.target_peer == peer, OutboxEntry.status == "pending")
         .update({OutboxEntry.attempts: OutboxEntry.attempts + 1,
                  OutboxEntry.last_error: error[:300]}, synchronize_session=False))
        db.commit()
    finally:
        db.close()


def _vector() -> dict[str, int]:
    db = SessionLocal()
    try:
        return replication.version_vector(db)
    finally:
        db.close()


def _apply_many(ops: list[dict]) -> dict[str, int]:
    stats = {"applied": 0, "superseded": 0, "duplicate": 0, "rejected": 0}
    db = SessionLocal()
    try:
        for op in ops:
            try:
                stats[replication.apply_remote_op(db, config.NODE_ID, op)] += 1
            except replication.IntegrityFailure as exc:
                db.rollback()
                stats["rejected"] += 1
                log.error("rejected op %s: %s", op.get("op_id"), exc)
        return stats
    finally:
        db.close()


# ---------------------------------------------------------------- sync logic

async def sync_peer(client: httpx.AsyncClient, peer: str) -> dict:
    info = runtime.peers[peer]
    url = info["url"]
    info["last_attempt"] = _now_iso()
    result = {"peer": peer, "ok": False, "pushed": 0, "pulled": 0,
              "applied": 0, "duplicate": 0, "superseded": 0, "error": None}
    try:
        await _request(client, "GET", f"{url}/internal/ping")

        # 1) PUSH: svoje neodoslané operácie, dávkami, striktne v poradí
        while True:
            batch = await asyncio.to_thread(_pending_for, peer, config.PULL_BATCH)
            if not batch:
                break
            resp = await _request(client, "POST", f"{url}/internal/ops",
                                  json={"ops": [op for _, op in batch]})
            results = resp.json()["results"]
            await asyncio.to_thread(_mark, [eid for eid, _ in batch], results)
            result["pushed"] += len(batch)

        # 2) PULL: operácie, ktoré u nás ešte nie sú (vrátane od tretích uzlov)
        while True:
            vector = await asyncio.to_thread(_vector)
            resp = await _request(client, "POST", f"{url}/internal/ops/since",
                                  json={"vector": vector, "limit": config.PULL_BATCH})
            ops = resp.json()["ops"]
            if not ops:
                break
            stats = await asyncio.to_thread(_apply_many, ops)
            result["pulled"] += len(ops)
            for k in ("applied", "duplicate", "superseded"):
                result[k] += stats[k]
            if len(ops) < config.PULL_BATCH or stats["applied"] + stats["superseded"] == 0:
                break

        result["ok"] = True
        info.update(reachable=True, last_ok=_now_iso(), last_error=None, failures=0, backoff_until=0.0)
    except PeerUnreachable as exc:
        result["error"] = str(exc)
        info["failures"] += 1
        info.update(reachable=False, last_error=str(exc),
                    backoff_until=time.monotonic() + min(config.MAX_BACKOFF, 2 ** info["failures"]))
        await asyncio.to_thread(_mark_failed, peer, str(exc))
    if result["ok"] and (result["pushed"] or result["applied"]):
        log.info("sync with %s: %s", peer, result)
    elif not result["ok"]:
        log.warning("sync with %s failed: %s", peer, result["error"])
    return result


async def sync_all(manual: bool = False) -> list[dict]:
    async with runtime.lock:
        async with httpx.AsyncClient() as client:
            peers = [p for p, i in runtime.peers.items()
                     if manual or i["backoff_until"] <= time.monotonic()]
            results = await asyncio.gather(*(sync_peer(client, p) for p in peers))
    runtime.last_sync = {"at": _now_iso(), "manual": manual, "results": list(results)}
    return list(results)


async def probe_peers() -> None:
    """Ľahká kontrola dostupnosti susedov (pre panel), bez synchronizácie."""
    async with httpx.AsyncClient() as client:
        async def one(peer: str):
            try:
                await _request(client, "GET", f"{runtime.peers[peer]['url']}/internal/ping")
                runtime.peers[peer]["reachable"] = True
            except PeerUnreachable as exc:
                runtime.peers[peer].update(reachable=False, last_error=str(exc))
        await asyncio.gather(*(one(p) for p in runtime.peers))


async def worker() -> None:
    """Služba synchronizácie na pozadí: automatická synchronizácia po obnovení spojenia."""
    log.info("sync worker started (interval=%ss, peers=%s)", config.SYNC_INTERVAL, list(runtime.peers))
    while True:
        await asyncio.sleep(config.SYNC_INTERVAL)
        try:
            if runtime.auto_sync:
                await sync_all(manual=False)
            else:
                await probe_peers()
        except Exception:  # noqa: BLE001 – worker nesmie zomrieť
            log.exception("sync worker iteration failed")
