"""Offline tests: SQLite store, heuristic router, and the whole loop against the
echo runner (control.devrunner), served over real HTTP so the peer API is used."""

import asyncio
import socket
import sys
import threading
import time
import types

import httpx
import pytest
import uvicorn

from control import service
from control.app import create_app
from control.providers import LocalProvider
from control.router import HeuristicRouter, clean_plan
from control.store import SqliteStore

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def test_store_roundtrip(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo", "https://example.com/r.git")
    t = await s.create_thread(p["id"], "Fix it", "fix the bug")
    await s.update_thread(t["id"], state="working", files=["a.py"])
    e1 = await s.append_event(p["id"], t["id"], "assistant", {"content": "hi"})
    e2 = await s.append_event(p["id"], None, "route", {"summary": "spawned 1"})
    assert e2["seq"] > e1["seq"]
    assert [e["type"] for e in await s.list_events(p["id"])] == ["assistant", "route"]
    assert [e["type"] for e in await s.list_events(p["id"], t["id"])] == ["assistant"]
    assert await s.list_events(p["id"], after=e2["seq"]) == []
    got = await s.get_thread(t["id"])
    assert got["state"] == "working" and got["files"] == ["a.py"]
    m = await s.add_message(p["id"], "user", t["id"], "hello")
    assert len(await s.list_messages(t["id"], unread_only=True)) == 1
    await s.mark_read([m["id"]])
    assert await s.list_messages(t["id"], unread_only=True) == []
    with pytest.raises(ValueError):
        await s.update_thread(t["id"], state="bogus")
    await s.close()


async def test_heuristic_router():
    r = HeuristicRouter()
    one = await r.plan("add a login page", [])
    assert len(one["spawn"]) == 1 and not one["send"]
    many = await r.plan("Do these:\n1. login page\n2. export bug\n- docs", [])
    assert [s["request"] for s in many["spawn"]] == ["login page", "export bug", "docs"]
    sent = await r.plan("@t_abc also handle the empty case", [{"id": "t_abc"}])
    assert sent["send"] == [{"thread": "t_abc", "message": "also handle the empty case"}]


def test_clean_plan_drops_unknown_threads():
    plan = clean_plan({"spawn": [{"request": "x"}, {"title": "no request"}],
                       "send": [{"thread": "t_nope", "message": "m"}, {"thread": "t_1", "message": "m"}]},
                      [{"id": "t_1"}])
    assert len(plan["spawn"]) == 1 and plan["send"] == [{"thread": "t_1", "message": "m"}]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def server(tmp_path, monkeypatch):
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.05")
    # One running thread per worker, so peer calls have to cross workers.
    monkeypatch.setattr(service, "CAPACITY", 1)
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     LocalProvider(workdir=str(tmp_path / "workers"), use_devrunner=True),
                     HeuristicRouter())
    srv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    th = threading.Thread(target=srv.run, daemon=True)
    th.start()
    deadline = time.time() + 10
    while not srv.started and time.time() < deadline:
        time.sleep(0.05)
    yield url
    srv.should_exit = True
    th.join(10)


async def wait_for(fn, timeout=15.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        v = await fn()
        if v:
            return v
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("timed out")
        await asyncio.sleep(0.1)


async def test_end_to_end_with_echo_runner(server):
    async with httpx.AsyncClient(base_url=server, timeout=20) as c:
        assert (await c.get("/")).status_code in (200, 503)
        p = (await c.post("/api/projects", json={"name": "demo"})).json()
        pid = p["id"]

        r = (await c.post(f"/api/projects/{pid}/requests",
                          json={"text": "- login page\n- export bug"})).json()
        assert r["summary"] == "spawned 2"
        ids = [s["id"] for s in r["spawned"]]

        async def all_idle():
            ts = (await c.get(f"/api/projects/{pid}/threads")).json()
            return len(ts) == 2 and all(t["state"] == "idle" and t["summary"] for t in ts)
        await wait_for(all_idle)

        tr = (await c.get(f"/api/threads/{ids[0]}/transcript")).json()
        assert any(e["type"] == "assistant" and "login page" in e["text"] for e in tr)
        # The request shows once, as "created"; the runner's echo of it is dropped.
        assert [e["type"] for e in tr if "login page" in e["text"]].count("created") == 1
        assert not any(e["type"] == "message_in" for e in tr)

        # A thread spawns a child through the peer API; the child reports back.
        await c.post(f"/api/projects/{pid}/requests",
                     json={"text": "spawn: write tests", "mode": "thread", "target": ids[0]})

        async def child_reported():
            ts = (await c.get(f"/api/projects/{pid}/threads")).json()
            child = [t for t in ts if t["parent_id"] == ids[0]]
            if not child:
                return None
            tr = (await c.get(f"/api/threads/{ids[0]}/transcript")).json()
            return any(e["type"] == "message_in" and e["text"].startswith("Finished:") for e in tr)
        await wait_for(child_reported)

        # Workers never call the control plane, so it serves no peer API.
        assert (await c.get(f"/api/peer/projects/{pid}/threads")).status_code == 404

        # Across workers: list, read and message threads on another worker.
        on = {t["id"]: t["worker_id"] for t in (await c.get(f"/api/projects/{pid}/threads")).json()}
        assert on[ids[0]] and on[ids[1]] and on[ids[0]] != on[ids[1]]
        await c.post(f"/api/projects/{pid}/requests", json={
            "text": f"list: export\nread {ids[0]}\ntell {ids[0]}: hello from the export thread",
            "mode": "thread", "target": ids[1]})

        async def peers_answered():
            tr = (await c.get(f"/api/threads/{ids[1]}/transcript")).json()
            results = {e["label"]: e["text"] for e in tr if e["type"] == "tool_result"}
            got = (await c.get(f"/api/threads/{ids[0]}/transcript")).json()
            told = any(e["type"] == "message_in" and "hello from the export thread" in e["text"] for e in got)
            if "list_threads result" in results and "read_thread result" in results and told:
                return results
        results = await wait_for(peers_answered)
        assert "export bug" in results["list_threads result"] and "login page" not in results["list_threads result"]
        assert ids[0] in results["read_thread result"]

        # The project SSE stream replays from ?after.
        async with c.stream("GET", f"/api/projects/{pid}/events", params={"after": 0}) as s:
            seen = 0
            async for line in s.aiter_lines():
                if line.startswith("data:"):
                    seen += 1
                    if seen >= 5:
                        break
        assert seen >= 5

        await c.post(f"/api/threads/{ids[1]}/stop")
        assert (await c.get(f"/api/threads/{ids[1]}")).json()["state"] == "stopped"


@pytest.fixture
def postgres(tmp_path):
    import shutil
    import subprocess

    import os

    if not shutil.which("initdb"):
        pytest.skip("postgresql not installed")
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("initdb refuses to run as root")
    data, port = tmp_path / "pg", free_port()
    log = tmp_path / "postgres.log"

    def run(*args):
        return subprocess.run(args, check=True, capture_output=True, text=True, timeout=70)

    run("initdb", "-D", str(data), "-U", "postgres", "--auth=trust")
    try:
        # `-l` sends the server's output to a file. Without it the daemon inherits
        # pg_ctl's stdout/stderr pipes and capture_output never sees EOF, so the
        # call blocks until the server exits.
        # No Unix socket: its path is capped at 107 bytes and tmp_path can be longer.
        # The test connects over TCP, so listen_addresses is all it needs.
        run("pg_ctl", "-D", str(data), "-l", str(log), "-w", "-t", "60", "-o",
            f"-p {port} -c listen_addresses=127.0.0.1 -c unix_socket_directories=", "start")
    except subprocess.CalledProcessError as e:
        pytest.fail(f"postgres failed to start:\n{e.stderr}\n{e.stdout}"
                    + (f"\nserver log:\n{log.read_text()}" if log.exists() else ""))
    try:
        yield f"postgresql://postgres@127.0.0.1:{port}/postgres"
    finally:
        subprocess.run(["pg_ctl", "-D", str(data), "-m", "fast", "-w", "-t", "60", "stop"],
                       capture_output=True, timeout=70)


async def test_postgres_store_and_notify(postgres):
    from control.store import PostgresStore

    a, b = PostgresStore(postgres), PostgresStore(postgres)
    await a.open()
    await b.open()
    p = await a.create_project("pg")
    t = await a.create_thread(p["id"], "T", "req")
    assert (await b.get_thread(t["id"]))["title"] == "T"
    # An append on instance a wakes a waiter on instance b (LISTEN/NOTIFY).
    waiter = asyncio.create_task(b.wait_events(p["id"], 0, 5.0))
    await asyncio.sleep(0.2)
    start = asyncio.get_running_loop().time()
    await a.append_event(p["id"], t["id"], "assistant", {"content": "x"})
    await waiter
    assert asyncio.get_running_loop().time() - start < 2.0
    assert [e["payload"] for e in await b.list_events(p["id"])] == [{"content": "x"}]
    await a.update_thread(t["id"], files=["f.py"], state="idle")
    assert (await b.get_thread(t["id"]))["files"] == ["f.py"]
    await a.close()
    await b.close()


def test_secrets_accept_an_item_added_in_a_keyring_gui(monkeypatch):
    """The key someone adds by hand in a keyring GUI (Login keyring, item named
    after the variable) has no service/username attributes; it is found by its
    label, skipping locked collections and ignoring other labels."""
    from control import secrets

    class Item:
        def __init__(self, label, secret):
            self.label, self.secret = label, secret

        def get_label(self):
            return self.label

        def get_secret(self):
            return self.secret

    class Collection:
        def __init__(self, locked, items):
            self.locked, self.items = locked, items

        def is_locked(self):
            return self.locked

        def get_all_items(self):
            return self.items

    collections = [Collection(True, [Item("DEEPSEEK_API_KEY", b"locked")]),
                   Collection(False, [Item("deepseek_api_key", b"wrong-case"),
                                      Item("DEEPSEEK_API_KEY", b"sk-from-label\n")])]
    monkeypatch.setitem(sys.modules, "secretstorage", types.SimpleNamespace(
        dbus_init=lambda: object(), get_all_collections=lambda bus: collections))
    monkeypatch.setattr(secrets, "_from_keyring", lambda n: None)
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    assert secrets._from_keyring_label("DEEPSEEK_API_KEY") == "sk-from-label"
    assert secrets._from_keyring_label("SPRITES_TOKEN") is None
    assert secrets.get_secret("DEEPSEEK_API_KEY") == "sk-from-label"


def test_missing_secret_message_names_the_keyring_backend(monkeypatch):
    from control import secrets
    monkeypatch.setattr(secrets, "keyring_backend", lambda: "x.Keyring")
    assert "keyring backend: x.Keyring" in secrets.missing_message(["DEEPSEEK_API_KEY"])
    monkeypatch.setattr(secrets, "keyring_backend", lambda: None)
    assert "no keyring backend" in secrets.missing_message(["DEEPSEEK_API_KEY"])
