"""Testy REST API (FastAPI TestClient, SQLite)."""
import os
import uuid

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    path = "test_api_node.db"
    if os.path.exists(path):
        os.remove(path)
    import main
    with TestClient(main.app) as c:
        yield c
    main.engine.dispose()
    if os.path.exists(path):
        os.remove(path)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["database"] is True


def test_crud_flow(client):
    r = client.post("/api/items", json={"name": "doc", "content": "a"})
    assert r.status_code == 201
    item = r.json()
    assert item["version"] == 1 and item["origin_node"] == "A" and len(item["checksum"]) == 64

    r = client.put(f"/api/items/{item['id']}", json={"content": "b", "expected_version": 1})
    assert r.status_code == 200 and r.json()["version"] == 2

    r = client.put(f"/api/items/{item['id']}", json={"content": "c", "expected_version": 1})
    assert r.status_code == 409  # optimistická blokácia

    assert client.delete(f"/api/items/{item['id']}").status_code == 200
    assert client.delete(f"/api/items/{item['id']}").json()["replayed"] is True  # opakovaná idempotentná požiadavka
    assert client.put(f"/api/items/{item['id']}", json={"content": "x"}).status_code == 410
    assert all(i["id"] != item["id"] for i in client.get("/api/items").json())
    assert any(i["id"] == item["id"] for i in client.get("/api/items?include_deleted=true").json())


def test_validation(client):
    assert client.post("/api/items", json={"name": ""}).status_code == 422
    assert client.post("/api/items", json={"name": "x" * 201}).status_code == 422
    assert client.put(f"/api/items/{uuid.uuid4()}", json={"content": "x"}).status_code == 404


def test_idempotency_key(client):
    key = str(uuid.uuid4())
    r1 = client.post("/api/items", json={"name": "once"}, headers={"Idempotency-Key": key})
    r2 = client.post("/api/items", json={"name": "once"}, headers={"Idempotency-Key": key})
    assert r1.status_code == 201 and r2.status_code == 200
    assert r1.json()["id"] == r2.json()["id"] and r2.json()["replayed"] is True


def test_internal_endpoints_require_token(client):
    assert client.get("/internal/ping").status_code == 401
    assert client.get("/internal/ping", headers={"X-Cluster-Token": "wrong"}).status_code == 401
    assert client.get("/internal/ping", headers={"X-Cluster-Token": "test-token"}).status_code == 200


def test_isolation_blocks_internal_traffic(client):
    client.post("/api/admin/isolate", json={"enabled": True})
    try:
        assert client.get("/internal/ping", headers={"X-Cluster-Token": "test-token"}).status_code == 503
        assert client.post("/api/items", json={"name": "offline"}).status_code == 201  # lokálna práca pokračuje
    finally:
        client.post("/api/admin/isolate", json={"enabled": False})


def test_manifest_and_status(client):
    m = client.get("/api/manifest").json()
    assert m["items_total"] == len(m["items"]) and len(m["manifest_checksum"]) == 64
    s = client.get("/api/status").json()
    assert s["node_id"] == "A" and s["manifest_checksum"] == m["manifest_checksum"]
    assert client.get("/api/integrity").json()["ok"] is True
    assert "dt_items_live" in client.get("/metrics").text


def test_sync_without_peers_is_ok(client):
    r = client.post("/api/sync")
    assert r.status_code == 200 and r.json()["ok"] is True


def test_dashboard_served(client):
    r = client.get("/")
    assert r.status_code == 200 and "Stavový panel" in r.text
