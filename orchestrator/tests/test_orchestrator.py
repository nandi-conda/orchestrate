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

from control import app as app_mod
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


async def test_store_adds_activity_to_an_older_database(tmp_path):
    import aiosqlite

    path = str(tmp_path / "old.sqlite")
    async with aiosqlite.connect(path) as db:  # threads as created before the activity column
        await db.execute("""CREATE TABLE threads (
            id TEXT PRIMARY KEY, project_id TEXT NOT NULL, parent_id TEXT,
            title TEXT NOT NULL, request TEXT NOT NULL DEFAULT '', summary TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL DEFAULT 'queued', worker_id TEXT, files TEXT NOT NULL DEFAULT '[]',
            token TEXT NOT NULL DEFAULT '', created_at DOUBLE PRECISION NOT NULL,
            updated_at DOUBLE PRECISION NOT NULL)""")
        await db.execute("INSERT INTO threads (id, project_id, title, created_at, updated_at) "
                         "VALUES ('t_old', 'p_1', 'Old', 0, 0)")
        await db.commit()
    s = SqliteStore(path)
    await s.open()
    assert (await s.get_thread("t_old"))["activity"] == ""
    assert (await s.update_thread("t_old", activity="thinking"))["activity"] == "thinking"
    await s.close()
    s = SqliteStore(path)
    await s.open()  # a second open finds the column and leaves it be
    assert (await s.get_thread("t_old"))["activity"] == "thinking"
    await s.close()


def test_activity_of_events():
    act = service.activity_of
    assert act("state", {"state": "working"}) == "thinking"
    assert act("tool_call", {"name": "bash", "arguments": {"command": "pytest -q\n  tests"}}) == \
        "running bash: pytest -q tests"
    assert act("tool_call", {"name": "read_file", "arguments": {"path": "a.py"}}) == "running read_file: a.py"
    assert act("tool_call", {"name": "bash", "arguments": {"command": "x" * 400}}).endswith(" …")
    assert act("tool_result", {"name": "bash", "result": "ok"}) == "thinking"
    assert act("assistant", {"content": "on it"}) == "thinking"
    for state in ("idle", "failed", "stopped", "done", "waiting"):
        assert act("state", {"state": state}) == ""
    assert act("delta", {"kind": "reasoning", "text": "hm"}) == "thinking"
    assert act("delta", {"kind": "content", "text": "Sure"}) == "writing a reply"
    assert act("delta", {"kind": "tool", "name": "write_file", "text": ""}) == "calling write_file"
    assert act("reasoning", {"content": "hm"}) == "thinking"
    assert act("message_in", {"from": "user", "body": "hi"}) is None
    assert act("error", {"error": "boom"}) is None


async def test_ingest_tracks_what_a_thread_is_doing(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    t = await s.create_thread(p["id"], "Fix it", "fix the bug")
    orch = service.Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True), HeuristicRouter())

    async def doing(typ, data):
        await orch._ingest(p["id"], {"thread": t["id"], "type": typ, "data": data})
        return (await s.get_thread(t["id"]))["activity"]

    assert await doing("state", {"state": "working"}) == "thinking"
    assert await doing("tool_call", {"name": "bash", "arguments": {"command": "pytest"}}) == "running bash: pytest"
    assert await doing("message_in", {"from": "t_x", "body": "fyi"}) == "running bash: pytest"
    assert await doing("tool_result", {"name": "bash", "result": "1 passed"}) == "thinking"
    assert await doing("state", {"state": "idle"}) == ""
    assert (await s.get_thread(t["id"]))["state"] == "idle"
    await s.close()


async def test_streamed_reply_shows_live_then_folds_into_its_events(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    t = await s.create_thread(p["id"], "Fix it", "fix the bug")
    orch = service.Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True), HeuristicRouter())

    async def ingest(typ, data):
        await orch._ingest(p["id"], {"thread": t["id"], "type": typ, "data": data})

    async def shown():
        return app_mod.fold(await s.list_events(p["id"], t["id"], 0, limit=2000))

    await ingest("state", {"state": "working"})
    await ingest("delta", {"kind": "reasoning", "text": "Look at "})
    await ingest("delta", {"kind": "reasoning", "text": "the file."})
    await ingest("delta", {"kind": "content", "text": "Fixing"})
    await ingest("delta", {"kind": "tool", "name": "write_file", "text": ""})
    await ingest("delta", {"kind": "tool", "name": "write_file", "text": '{"path": "a.py"'})
    rows = await shown()
    live = [(r["kind"], r["label"], r["text"]) for r in rows if r.get("live")]
    assert live == [(app_mod.K_THINKING, "Thinking…", "Look at the file."),
                    (app_mod.K_ASSISTANT, "Agent (writing…)", "Fixing"),
                    (app_mod.K_TOOL_CALL, "write_file", '{"path": "a.py" …')]
    assert (await s.get_thread(t["id"]))["activity"] == "calling write_file"

    await ingest("reasoning", {"content": "Look at the file."})
    await ingest("assistant", {"content": "Fixing"})
    await ingest("tool_call", {"id": "c1", "name": "write_file", "arguments": {"path": "a.py"}})
    rows = await shown()
    assert not any(r.get("live") for r in rows)
    assert [r["type"] for r in rows] == ["state", "reasoning", "assistant", "tool_call"]
    assert rows[1]["kind"] == app_mod.K_THINKING and rows[1]["text"] == "Look at the file."
    assert not [e for e in await s.list_events(p["id"], t["id"], 0) if e["type"] == "delta"]
    await s.close()


async def test_worker_logs_come_from_the_provider(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True)
    orch = service.Orchestrator(s, provider, HeuristicRouter())
    try:
        t = await orch.spawn(p["id"], "Echo", "say hi")
        await asyncio.wait({orch.starting[t["id"]]})  # spawn returns before the worker is up
        log = await orch.worker_logs(t["id"], 50)
        assert "devrunner: echo runner on port" in log
        with pytest.raises(KeyError):
            await orch.worker_logs("t_nope")
    finally:
        await orch.close()
        await s.close()


def test_describe_names_the_request_a_timeout_was_for():
    req = httpx.Request("POST", "http://w:8080/threads")
    assert service.describe(httpx.ReadTimeout("", request=req)) == "ReadTimeout on POST http://w:8080/threads"
    assert service.describe(ValueError("bad")) == "ValueError: bad"


def test_fold_drops_deltas_already_ended_by_a_later_event():
    def ev(seq, typ, payload):
        return {"seq": seq, "project_id": "p", "thread_id": "t", "type": typ, "payload": payload, "created_at": 0}
    rows = app_mod.fold([ev(1, "delta", {"kind": "content", "text": "hi"}),
                         ev(2, "assistant", {"content": "hi"}),
                         ev(3, "usage", {"prompt_tokens": 1})])
    assert [(r["type"], r.get("live")) for r in rows] == [("assistant", None)]


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

        r = await c.post(f"/api/projects/{pid}/requests", json={"text": "- login page\n- export bug"})
        assert r.status_code == 202 and r.json()["queued"]

        async def routed():
            log = (await c.get(f"/api/projects/{pid}/log")).json()
            return next((e for e in log if e["type"] == "route"), None)
        route = await wait_for(routed)
        assert route["payload"]["summary"] == "spawned 2"
        ids = [s["id"] for s in route["payload"]["spawned"]]

        async def all_idle():
            ts = (await c.get(f"/api/projects/{pid}/threads")).json()
            return len(ts) == 2 and all(t["state"] == "idle" and t["summary"] for t in ts)
        await wait_for(all_idle)
        # An idle thread is doing nothing, so it shows no current activity.
        assert all(t["activity"] == "" for t in (await c.get(f"/api/projects/{pid}/threads")).json())

        tr = (await c.get(f"/api/threads/{ids[0]}/transcript")).json()
        logs = await c.get(f"/api/threads/{ids[0]}/worker-logs?lines=20")
        assert logs.status_code == 200 and logs.headers["content-type"].startswith("text/plain")
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


class GatedRouter(HeuristicRouter):
    """A router whose model has not answered until `gate` is set."""

    def __init__(self) -> None:
        self.gate = asyncio.Event()

    async def plan(self, request, directory):
        await self.gate.wait()
        return await super().plan(request, directory)


class GatedProvider(LocalProvider):
    """A provider whose workers take until `gate` is set to come up (a sprite being made)."""

    def __init__(self, **kw) -> None:
        super().__init__(**kw)
        self.gate = asyncio.Event()

    async def create(self, spec):
        await self.gate.wait()
        return await super().create(spec)


async def test_sending_returns_before_routing_and_workers(tmp_path, monkeypatch):
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.05")
    router = GatedRouter()
    provider = GatedProvider(workdir=str(tmp_path / "workers"), use_devrunner=True)
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")), provider, router)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]

            async def post(body):
                t = time.perf_counter()
                r = await c.post(f"/api/projects/{pid}/requests", json=body)
                assert time.perf_counter() - t < 1.0, "a send waited on routing or a worker"
                return r

            # The router has not answered: the request is taken and shown at once.
            r = await post({"text": "fix login"})
            assert r.status_code == 202
            log = (await c.get(f"/api/projects/{pid}/log")).json()
            assert [(e["type"], e["label"], e["text"]) for e in log] == [("request", "You", "fix login")]

            # No worker is up: the thread exists, says it is starting, and takes messages.
            r = await post({"text": "fix export", "mode": "new"})
            assert r.status_code == 200 and r.json()["summary"] == "spawned 1"
            tid = r.json()["spawned"][0]["id"]
            t = (await c.get(f"/api/threads/{tid}")).json()
            assert (t["state"], t["activity"]) == ("queued", "starting on a worker")
            assert (await post({"text": "check csv too", "mode": "thread", "target": tid})).status_code == 200
            tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
            assert [(e["type"], e["text"]) for e in tr] == [("created", "fix export"), ("message_in", "check csv too")]

            # A thread stopped before its worker is up never starts.
            gone = (await post({"text": "drop me", "mode": "new"})).json()["spawned"][0]["id"]
            await c.post(f"/api/threads/{gone}/stop")

            assert (await post({"text": "x", "mode": "thread", "target": "t_nope"})).status_code == 404

            provider.gate.set()

            async def delivered():
                tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
                return any(e["type"] == "assistant" and "check csv too" in e["text"] for e in tr) and tr
            tr = await wait_for(delivered)
            # Logged when it was sent; the runner's echo of it is dropped.
            assert [e["text"] for e in tr if e["type"] == "message_in"] == ["check csv too"]

            router.gate.set()

            async def routed():
                log = (await c.get(f"/api/projects/{pid}/log")).json()
                return next((e for e in log if e["type"] == "route"), None)
            assert (await wait_for(routed))["payload"]["summary"] == "spawned 1"
            g = (await c.get(f"/api/threads/{gone}")).json()
            assert (g["state"], g["worker_id"]) == ("stopped", None)


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
    await a.update_thread(t["id"], files=["f.py"], state="idle", activity="thinking")
    got = await b.get_thread(t["id"])
    assert got["files"] == ["f.py"] and got["activity"] == "thinking"
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
