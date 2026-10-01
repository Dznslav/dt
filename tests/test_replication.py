"""Unit testy jadra replikácie: idempotencia, LWW-konflikty, tombstone, integrita."""
import pytest

import replication as r
from models import Conflict, Item, OutboxEntry


def ship(src, dst, dst_id):
    """Preniesť všetky operácie src na uzol dst (ako to robí pull)."""
    vector = r.version_vector(dst)
    return [r.apply_remote_op(dst, dst_id, r.op_to_dict(op)) for op in r.ops_since(src, vector)]


def test_create_writes_outbox_for_each_peer(make_node):
    a = make_node()
    item, op, replayed = r.local_change(a, "A", ["B", "C"], "CREATE", name="x", content="1")
    assert not replayed and item.origin_node == "A" and item.version == 1
    peers = sorted(e.target_peer for e in a.query(OutboxEntry).filter_by(op_id=op.op_id))
    assert peers == ["B", "C"]


def test_replication_is_idempotent(make_node):
    a, b = make_node(), make_node()
    item, op, _ = r.local_change(a, "A", ["B"], "CREATE", name="x", content="1")
    payload = r.op_to_dict(op)
    assert r.apply_remote_op(b, "B", payload) == "applied"
    assert r.apply_remote_op(b, "B", payload) == "duplicate"
    assert r.apply_remote_op(b, "B", payload) == "duplicate"
    assert b.query(Item).count() == 1
    assert b.get(Item, item.id).checksum == item.checksum


def test_client_idempotency_key(make_node):
    a = make_node()
    i1, _, rep1 = r.local_change(a, "A", [], "CREATE", name="x", content="", request_id="k1")
    i2, _, rep2 = r.local_change(a, "A", [], "CREATE", name="x", content="", request_id="k1")
    assert i1.id == i2.id and not rep1 and rep2
    assert a.query(Item).count() == 1


def test_concurrent_updates_converge_with_lww(make_node):
    a, b = make_node(), make_node()
    item, _, _ = r.local_change(a, "A", ["B"], "CREATE", name="x", content="orig")
    ship(a, b, "B")
    # paralelné zmeny počas výpadku siete
    r.local_change(a, "A", ["B"], "UPDATE", item_id=item.id, content="from A")
    r.local_change(b, "B", ["A"], "UPDATE", item_id=item.id, content="from B")
    ship(a, b, "B")
    ship(b, a, "A")
    ia, ib = a.get(Item, item.id), b.get(Item, item.id)
    assert ia.content == ib.content == "from B"  # rovnaký lamport -> vyhráva väčšie node_id
    assert ia.checksum == ib.checksum
    assert r.manifest(a, "A")["manifest_checksum"] == r.manifest(b, "B")["manifest_checksum"]
    assert a.query(Conflict).count() + b.query(Conflict).count() >= 1


def test_later_write_wins(make_node):
    a, b = make_node(), make_node()
    item, _, _ = r.local_change(a, "A", ["B"], "CREATE", name="x", content="v1")
    ship(a, b, "B")
    r.local_change(b, "B", ["A"], "UPDATE", item_id=item.id, content="B1")
    r.local_change(a, "A", ["B"], "UPDATE", item_id=item.id, content="A1")
    r.local_change(a, "A", ["B"], "UPDATE", item_id=item.id, content="A2")  # vyšší lamport
    ship(a, b, "B")
    ship(b, a, "A")
    assert a.get(Item, item.id).content == b.get(Item, item.id).content == "A2"


def test_delete_is_replicated_as_tombstone(make_node):
    a, b = make_node(), make_node()
    item, _, _ = r.local_change(a, "A", ["B"], "CREATE", name="x", content="")
    ship(a, b, "B")
    r.local_change(b, "B", ["A"], "DELETE", item_id=item.id)
    ship(b, a, "A")
    assert a.get(Item, item.id).deleted is True
    with pytest.raises(r.Gone):
        r.local_change(a, "A", [], "UPDATE", item_id=item.id, content="zombie")


def test_out_of_order_delivery(make_node):
    a, b, c = make_node(), make_node(), make_node()
    item, _, _ = r.local_change(a, "A", [], "CREATE", name="x", content="v1")
    ship(a, b, "B")
    r.local_change(b, "B", [], "UPDATE", item_id=item.id, content="v2 from B")
    # C najprv dostane zmenu od B a potom vytvorenie od A
    for op in r.ops_since(b, {"A": 1}):
        r.apply_remote_op(c, "C", r.op_to_dict(op))
    ship(a, c, "C")
    assert c.get(Item, item.id).content == "v2 from B"
    assert r.manifest(c, "C")["manifest_checksum"] == r.manifest(b, "B")["manifest_checksum"]


def test_tampered_payload_is_rejected(make_node):
    a, b = make_node(), make_node()
    _, op, _ = r.local_change(a, "A", [], "CREATE", name="x", content="ok")
    bad = r.op_to_dict(op)
    bad["payload"] = dict(bad["payload"], content="hacked")
    with pytest.raises(r.IntegrityFailure):
        r.apply_remote_op(b, "B", bad)
    assert b.query(Item).count() == 0


def test_version_vector_counts_contiguous_prefix(make_node):
    a, b = make_node(), make_node()
    for i in range(3):
        r.local_change(a, "A", [], "CREATE", name=f"x{i}", content="")
    ops = r.ops_since(a, {})
    r.apply_remote_op(b, "B", r.op_to_dict(ops[0]))
    r.apply_remote_op(b, "B", r.op_to_dict(ops[2]))  # "diera" na seq 2
    assert r.version_vector(b) == {"A": 1}
    assert [o.seq for o in r.ops_since(a, r.version_vector(b))] == [2, 3]
    ship(a, b, "B")
    assert r.version_vector(b) == {"A": 3}


def test_expected_version_mismatch(make_node):
    a = make_node()
    item, _, _ = r.local_change(a, "A", [], "CREATE", name="x", content="")
    with pytest.raises(r.VersionMismatch):
        r.local_change(a, "A", [], "UPDATE", item_id=item.id, content="y", expected_version=5)


def test_integrity_check_detects_corruption(make_node):
    a = make_node()
    item, _, _ = r.local_change(a, "A", [], "CREATE", name="x", content="")
    assert r.verify_integrity(a)["ok"]
    a.get(Item, item.id).content = "changed directly in DB"
    a.commit()
    assert r.verify_integrity(a)["corrupted_items"] == [item.id]
