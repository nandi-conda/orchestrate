"""Offline tests: SQLite store, heuristic router, and the whole loop against the
echo runner (control.devrunner), served over real HTTP so the peer API is used."""

import asyncio
import socket
import threading
import time

import httpx
import pytest
import uvicorn

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
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     LocalProvider(workdir=str(tmp_path / "workers"), use_devrunner=True),
                     HeuristicRouter(), public_url=url)
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

        # Peer API rejects callers without a worker token.
        assert (await c.get(f"/api/peer/projects/{pid}/threads")).status_code == 401

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
    subprocess.run(["initdb", "-D", str(data), "-U", "postgres", "--auth=trust"], check=True,
                   capture_output=True)
    subprocess.run(["pg_ctl", "-D", str(data), "-o", f"-p {port} -k {tmp_path} -c listen_addresses=127.0.0.1",
                    "-w", "start"], check=True, capture_output=True)
    yield f"postgresql://postgres@127.0.0.1:{port}/postgres"
    subprocess.run(["pg_ctl", "-D", str(data), "-m", "fast", "stop"], capture_output=True)


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
