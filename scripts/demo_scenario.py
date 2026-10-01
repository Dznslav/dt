#!/usr/bin/env python3
"""Automatizovaný scenár obhajoby (Jednotný test pri obhajobe).

Použitie:
    python scripts/demo_scenario.py                 # Docker Compose, reálne odpojenie siete
    python scripts/demo_scenario.py --mode app      # simulácia odpojenia v aplikácii
    python scripts/demo_scenario.py --pause         # čaká na Enter medzi krokmi (na prezentáciu)

Iba štandardná knižnica Pythonu (3.9+). Návratový kód 0 = všetky kontroly prešli.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

NODES = {"A": "http://localhost:8001", "B": "http://localhost:8002", "C": "http://localhost:8003"}
CONTAINERS = {"A": "dt-api-a", "B": "dt-api-b", "C": "dt-api-c"}
CLUSTER_NET = "dt_cluster"
ROOT = Path(__file__).resolve().parent.parent  # priečinok s docker-compose.yml

args: argparse.Namespace
failures: list[str] = []
isolated: set[str] = set()

# ------------------------------------------------------------------ výpis

G, R, Y, B_, X = "\033[32m", "\033[31m", "\033[33m", "\033[1m", "\033[0m"


def step(n: int, title: str) -> None:
    print(f"\n{B_}=== Krok {n}: {title} ==={X}")
    if args.pause:
        input("   [Enter] pokračovať… ")


def check(cond: bool, msg: str) -> bool:
    print(f"   {G + '✔' if cond else R + '✘'}{X} {msg}")
    if not cond:
        failures.append(msg)
    return cond


def info(msg: str) -> None:
    print(f"   {Y}·{X} {msg}")


# ------------------------------------------------------------------ HTTP

def _http(url: str, method: str, body, headers: dict, timeout: float = 10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", **headers})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, raw.decode(errors="replace")


EXEC_SNIPPET = (
    "import json,sys,urllib.request,urllib.error;a=json.loads(sys.argv[1]);"
    "d=json.dumps(a['body']).encode() if a['body'] is not None else None;"
    "q=urllib.request.Request('http://localhost:8000'+a['path'],data=d,method=a['method'],headers=dict(a['headers'],**{'Content-Type':'application/json'}))\n"
    "try:\n r=urllib.request.urlopen(q,timeout=15);print(json.dumps([r.status,json.loads(r.read() or b'null')]))\n"
    "except urllib.error.HTTPError as e:\n print(json.dumps([e.code,json.loads(e.read() or b'null')]))"
)


def call(node: str, method: str, path: str, body=None, headers: dict | None = None):
    """HTTP volanie uzla. Ak je uzol odpojený od siete (docker režim) a port z hosta
    nie je dostupný, volanie sa vykoná zvnútra kontajnera cez `docker exec`."""
    headers = headers or {}
    try:
        return _http(NODES[node] + path, method, body, headers)
    except (urllib.error.URLError, ConnectionError, TimeoutError, OSError):
        if args.mode != "docker":
            raise
        payload = json.dumps({"path": path, "method": method, "body": body, "headers": headers})
        out = subprocess.run(["docker", "exec", CONTAINERS[node], "python", "-c", EXEC_SNIPPET, payload],
                             capture_output=True, text=True, check=True).stdout
        status, data = json.loads(out)
        return status, data


def docker(*cmd: str, check_rc: bool = True) -> subprocess.CompletedProcess:
    info("$ docker " + " ".join(cmd))
    return subprocess.run(["docker", *cmd], capture_output=True, text=True, check=check_rc, cwd=ROOT)


# ------------------------------------------------------------------ pomocné

saved_ip: dict[str, str] = {}


def disconnect(node: str) -> None:
    if args.mode == "docker":
        fmt = "{{(index .NetworkSettings.Networks \"%s\").IPAddress}}" % CLUSTER_NET
        saved_ip[node] = docker("inspect", "-f", fmt, CONTAINERS[node]).stdout.strip()
        docker("network", "disconnect", CLUSTER_NET, CONTAINERS[node])
    else:
        call(node, "POST", "/api/admin/isolate", {"enabled": True})
    isolated.add(node)


def reconnect(node: str) -> None:
    if args.mode == "docker":
        base = ["network", "connect", "--alias", f"api-{node.lower()}"]
        # pokus o zachovanie pôvodnej IP (ak sieť má pevne definovanú podsieť), inak nová IP
        ip = saved_ip.get(node)
        if not (ip and docker(*base, "--ip", ip, CLUSTER_NET, CONTAINERS[node], check_rc=False).returncode == 0):
            docker(*base, CLUSTER_NET, CONTAINERS[node])
    else:
        call(node, "POST", "/api/admin/isolate", {"enabled": False})
    isolated.discard(node)


def items(node: str, deleted: bool = True) -> dict[str, dict]:
    _, data = call(node, "GET", f"/api/items?include_deleted={'true' if deleted else 'false'}")
    return {i["id"]: i for i in data}


def wait_for(pred, timeout: float = 30, interval: float = 1) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            if pred():
                return True
        except Exception:  # noqa: BLE001
            pass
        time.sleep(interval)
    return False


def create(node: str, name: str, content: str) -> dict:
    status, data = call(node, "POST", "/api/items", {"name": name, "content": content},
                        {"Idempotency-Key": str(uuid.uuid4())})
    check(status == 201, f"na uzle {node} vytvorený záznam '{name}' (id {data['id'][:8]}…, HTTP {status})")
    return data


def sync(node: str) -> tuple[int, dict]:
    status, data = call(node, "POST", "/api/sync")
    for r in data["results"]:
        if r["ok"]:
            info(f"{node} ↔ {r['peer']}: OK, odoslané {r['pushed']}, prijaté {r['pulled']} "
                 f"(aplikované {r['applied']}, duplicitné {r['duplicate']}, prekonané {r['superseded']})")
        else:
            info(f"{node} ↔ {r['peer']}: ZLYHANIE – {r['error'][:90]}")
    return status, data


def manifests() -> dict[str, dict]:
    return {n: call(n, "GET", "/api/manifest")[1] for n in NODES}


def print_manifests(ms: dict[str, dict]) -> None:
    print(f"   {'uzol':<5}{'spolu':>6}{'živé':>6}{'zmaz.':>6}{'operácie':>10}  {'vektor verzií':<24}kontrolný súčet")
    for n, m in ms.items():
        print(f"   {n:<5}{m['items_total']:>6}{m['items_live']:>6}{m['items_deleted']:>6}{m['operations']:>10}  "
              f"{json.dumps(m['version_vector']):<24}{m['manifest_checksum'][:16]}…")


def all_consistent() -> bool:
    ms = manifests()
    return len({m["manifest_checksum"] for m in ms.values()}) == 1


# ------------------------------------------------------------------ scenár

def main() -> int:
    tag = time.strftime("%H%M%S")

    step(1, "Spustenie uzlov A, B, C")
    if args.mode == "docker" and args.up:
        docker("compose", "up", "-d", "--build", "--wait")
    for n in NODES:
        ok = wait_for(lambda n=n: call(n, "GET", "/health")[0] == 200, 90)
        check(ok, f"uzol {n} je zdravý (/health)")
    for n in NODES:  # na obhajobe spúšťame synchronizáciu ručne
        call(n, "POST", "/api/admin/autosync", {"enabled": args.autosync})
    info(f"automatická synchronizácia: {'zapnutá' if args.autosync else 'vypnutá (ručné spúšťanie)'}")
    for n in NODES:
        sync(n)
    before = {n: len(items(n)) for n in NODES}

    step(2, "Vytvorenie údaja na A a preukázanie replikácie")
    x = create("A", f"objednavka-{tag}", "vytvorené na A")
    sync("A")
    for n in ("B", "C"):
        check(wait_for(lambda n=n: x["id"] in items(n), 20), f"záznam {x['id'][:8]}… je na uzle {n}")
    y = create("A", f"na-zmazanie-{tag}", "bude zmazané na B počas výpadku")
    sync("A")

    step(3, "Odpojenie uzla B od siete")
    disconnect("B")
    check(wait_for(lambda: not call("A", "GET", "/api/cluster")[1]["all_reachable"], 15),
          "uzol A vidí B ako nedostupný")

    step(4, "Rozdielne údaje na A aj B počas výpadku (lokálne pokračovanie práce)")
    a1 = create("A", f"A-offline-{tag}", "vytvorené na A počas výpadku B")
    b1 = create("B", f"B-offline-1-{tag}", "vytvorené na B offline")
    b2 = create("B", f"B-offline-2-{tag}", "vytvorené na B offline")
    s1, _ = call("A", "PUT", f"/api/items/{x['id']}", {"content": "upravené na A počas výpadku"})
    s2, _ = call("B", "PUT", f"/api/items/{x['id']}", {"content": "upravené na B počas výpadku"})
    check(s1 == 200 and s2 == 200, "ten istý záznam upravený na A aj na B (budúci konflikt)")
    s3, _ = call("B", "DELETE", f"/api/items/{y['id']}")
    check(s3 == 200, "záznam zmazaný na B počas výpadku")
    _, stB = call("B", "GET", "/api/status")
    check(stB["outbox_pending"] > 0, f"B eviduje {stB['outbox_pending']} neodoslaných operácií v outboxe (trvalo v DB)")
    check(b1["id"] not in items("A"), "údaj z B sa na A zatiaľ nenachádza")

    step(5, "Synchronizácia počas výpadku – musí bezpečne zlyhať")
    status, data = sync("B")
    check(status == 503 and not data["ok"], f"synchronizácia na B zlyhala s HTTP {status}")
    check(data["outbox_pending_after"] == stB["outbox_pending"], "neodoslané operácie na B zostali zachované")
    status, data = sync("A")
    check(status == 503, f"synchronizácia na A hlási nedostupnosť B (HTTP {status}), s C prebehla")

    step(6, "Obnovenie spojenia")
    reconnect("B")
    check(wait_for(lambda: call("B", "GET", "/health")[0] == 200, 30), "B je opäť dostupný")

    step(7, "Doplnenie všetkých chýbajúcich operácií")
    for n in ("B", "A", "C"):
        wait_for(lambda n=n: sync(n)[0] == 200, 30, 2)
    check(wait_for(all_consistent, 30), "manifesty všetkých uzlov sú zhodné")
    for n in NODES:
        its = items(n)
        check(all(i in its for i in (x["id"], y["id"], a1["id"], b1["id"], b2["id"])),
              f"uzol {n} má všetky záznamy vytvorené počas výpadku")
        check(its[y["id"]]["deleted"], f"uzol {n}: zmazanie z B sa prenieslo (tombstone)")
    contents = {items(n)[x["id"]]["content"] for n in NODES}
    check(len(contents) == 1, f"konflikt vyriešený rovnako na všetkých uzloch (LWW): '{contents.pop()}'")
    _, conf = call("A", "GET", "/api/conflicts")
    _, confB = call("B", "GET", "/api/conflicts")
    check(any(c["item_id"] == x["id"] for c in conf + confB), "konflikt je zaznamenaný v evidencii konfliktov")
    for n in NODES:
        _, s = call(n, "GET", "/api/status")
        check(s["outbox_pending"] == 0, f"outbox uzla {n} je prázdny")

    step(8, "Opakovaná synchronizácia – nesmú vzniknúť duplicity")
    snapshot = manifests()
    for _ in range(2):
        for n in NODES:
            sync(n)
    after = manifests()
    check(all(after[n]["manifest_checksum"] == snapshot[n]["manifest_checksum"] for n in NODES),
          "manifesty sa po opakovanej synchronizácii nezmenili")
    check(all(after[n]["operations"] == snapshot[n]["operations"] for n in NODES), "počet operácií sa nezmenil")
    k = str(uuid.uuid4())
    r1 = call("A", "POST", "/api/items", {"name": f"idem-{tag}", "content": "x"}, {"Idempotency-Key": k})
    r2 = call("A", "POST", "/api/items", {"name": f"idem-{tag}", "content": "x"}, {"Idempotency-Key": k})
    check(r1[1]["id"] == r2[1]["id"] and r2[1]["replayed"],
          "opakovaný klientský požiadavok s rovnakým Idempotency-Key nevytvoril duplicitu")
    for n in NODES:
        sync(n)

    step(9, "Porovnanie manifestov, počtov, ID, verzií a kontrolných súčtov")
    wait_for(all_consistent, 20)
    ms = manifests()
    print_manifests(ms)
    ids = {n: {i["id"]: (i["version"], i["checksum"], i["deleted"]) for i in m["items"]} for n, m in ms.items()}
    check(ids["A"] == ids["B"] == ids["C"], "ID, verzie a kontrolné súčty záznamov sú na všetkých uzloch rovnaké")
    check(len({m["manifest_checksum"] for m in ms.values()}) == 1, "kontrolný súčet manifestu je rovnaký")
    for n in NODES:
        check(call(n, "GET", "/api/integrity")[1]["ok"], f"integrita dát na uzle {n} (prepočítané SHA-256)")
    check(all(len(items(n)) == before[n] + 6 for n in NODES), "počty záznamov zodpovedajú (+6 na každom uzle)")

    step(10, "Reštart uzla a zachovanie údajov")
    node = "C"
    snap = ms[node]["manifest_checksum"]
    if args.mode == "docker":
        docker("compose", "restart", "api-c", "db-c")
    elif args.restart_cmd:
        info("$ " + args.restart_cmd)
        subprocess.run(args.restart_cmd, shell=True, check=True)
    else:
        info("režim app: reštart preskočený (použi --restart-cmd)")
    check(wait_for(lambda: call(node, "GET", "/health")[0] == 200, 90), f"uzol {node} po reštarte beží")
    check(call(node, "GET", "/api/manifest")[1]["manifest_checksum"] == snap,
          f"údaje uzla {node} sú po reštarte zachované (rovnaký manifest)")

    for n in NODES:
        call(n, "POST", "/api/admin/autosync", {"enabled": True})

    print()
    if failures:
        print(f"{R}{B_}NEÚSPECH – {len(failures)} kontrol zlyhalo:{X}")
        for f in failures:
            print(f"   - {f}")
        return 1
    print(f"{G}{B_}Všetky kontroly scenára prešli.{X}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["docker", "app"], default="docker",
                    help="docker = docker network disconnect; app = simulácia v aplikácii")
    ap.add_argument("--up", action="store_true", help="najprv spustiť docker compose up --build")
    ap.add_argument("--pause", action="store_true", help="čakať na Enter pred každým krokom")
    ap.add_argument("--autosync", action="store_true", help="nechať zapnutú automatickú synchronizáciu")
    ap.add_argument("--restart-cmd", help="príkaz na reštart uzla C (režim app)")
    args = ap.parse_args()
    if sys.platform == "win32":
        import os
        os.system("")  # zapne ANSI farby vo Windows konzole
        sys.stdout.reconfigure(encoding="utf-8")
    try:
        sys.exit(main())
    finally:
        for n in list(isolated):  # nikdy nenechať uzol odpojený
            try:
                reconnect(n)
            except Exception:  # noqa: BLE001
                pass
