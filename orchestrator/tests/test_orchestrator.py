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


async def test_store_lists_archived_threads_apart(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    keep = await s.create_thread(p["id"], "Keep", "k")
    old = await s.create_thread(p["id"], "Old", "o")
    assert keep["archived_at"] is None
    await s.update_thread(old["id"], archived_at=123.0)
    assert [t["id"] for t in await s.list_threads(p["id"])] == [keep["id"]]
    assert [t["id"] for t in await s.list_threads(p["id"], archived=True)] == [old["id"]]
    assert (await s.get_thread(old["id"]))["archived_at"] == 123.0
    await s.update_thread(old["id"], archived_at=None)
    assert {t["id"] for t in await s.list_threads(p["id"])} == {keep["id"], old["id"]}
    assert await s.list_threads(p["id"], archived=True) == []
    await s.close()


async def test_store_lists_working_threads_first(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    a = await s.create_thread(p["id"], "A", "a")
    b = await s.create_thread(p["id"], "B", "b")
    c = await s.create_thread(p["id"], "C", "c")
    for t in (a, b, c):
        await s.update_thread(t["id"], state="idle")

    async def order():
        return [t["title"] for t in await s.list_threads(p["id"])]

    assert await order() == ["C", "B", "A"]
    await s.update_thread(a["id"], state="working")
    await s.update_thread(b["id"], state="queued")
    assert await order() == ["B", "A", "C"]
    await s.update_thread(b["id"], state="idle")
    assert await order() == ["A", "C", "B"]
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
    assert (await s.get_thread("t_old"))["archived_at"] is None
    assert [t["id"] for t in await s.list_threads("p_1")] == ["t_old"]
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


def test_turn_summary_is_the_result_and_state_changes_have_their_own_kind():
    def ev(seq, typ, payload):
        return {"seq": seq, "project_id": "p", "thread_id": "t", "type": typ, "payload": payload, "created_at": 0}
    rows = app_mod.fold([ev(1, "state", {"state": "working"}),
                         ev(2, "turn_done", {"summary": "Built `hello`."})])
    assert [(r["kind"], r["label"], r["text"]) for r in rows] == [(app_mod.K_STATE, "state", "working"),
                                                                  (app_mod.K_RESULT, "Result", "Built `hello`.")]


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
        assert route["payload"]["summary"] == "started “login page”, “export bug”"
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
            assert r.status_code == 200 and r.json()["summary"] == "started “fix export”"
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
                return next((e for e in log if e["type"] == "route" and e["payload"]["mode"] == "auto"), None)
            assert (await wait_for(routed))["payload"]["summary"] == "started “fix login”"
            g = (await c.get(f"/api/threads/{gone}")).json()
            assert (g["state"], g["worker_id"]) == ("stopped", None)


async def test_archiving_stops_hides_and_unarchives(tmp_path, monkeypatch):
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.05")
    provider = GatedProvider(workdir=str(tmp_path / "workers"), use_devrunner=True)
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")), provider, HeuristicRouter())
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]
            tid = (await c.post(f"/api/projects/{pid}/requests",
                                json={"text": "fix login", "mode": "new"})).json()["spawned"][0]["id"]
            assert (await c.get(f"/api/threads/{tid}")).json()["state"] == "queued"

            # Archiving a thread that has not finished stops it first.
            r = await c.post(f"/api/threads/{tid}/archive")
            assert r.status_code == 200
            assert (r.json()["state"], r.json()["archived_at"] is not None) == ("stopped", True)
            tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
            assert [e["type"] for e in tr][-1] == "archived"
            assert (await c.get(f"/api/projects/{pid}/threads")).json() == []
            assert [t["id"] for t in (await c.get(f"/api/projects/{pid}/threads?archived=1")).json()] == [tid]

            # Archived threads take no messages, and the router does not see them:
            # naming one spawns a new thread instead of sending to it.
            r = await c.post(f"/api/projects/{pid}/requests",
                             json={"text": "more", "mode": "thread", "target": tid})
            assert r.status_code == 409 and "archived" in r.json()["error"]
            assert (await c.post(f"/api/threads/{tid}/messages", json={"body": "hi"})).status_code == 409
            r = await c.post(f"/api/projects/{pid}/requests", json={"text": f"@{tid} more"})
            assert r.status_code == 202

            async def routed():
                log = (await c.get(f"/api/projects/{pid}/log")).json()
                return next((e for e in log if e["type"] == "route"), None)
            route = (await wait_for(routed))["payload"]
            assert route["sent"] == [] and len(route["spawned"]) == 1

            r = await c.post(f"/api/threads/{tid}/unarchive")
            assert r.status_code == 200 and r.json()["archived_at"] is None
            live = {t["id"] for t in (await c.get(f"/api/projects/{pid}/threads")).json()}
            assert tid in live and (await c.get(f"/api/projects/{pid}/threads?archived=1")).json() == []
            assert (await c.post("/api/threads/t_nope/archive")).status_code == 404
            provider.gate.set()


async def test_files_of_a_thread_browse_and_download(tmp_path, monkeypatch):
    """The Files tab: the control plane reads a thread's worktree from its
    worker and serves a file as a download."""
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.05")
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     LocalProvider(workdir=str(tmp_path / "workers"), use_devrunner=True), HeuristicRouter())
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]
            text = "build hello\nwrite out/hello world: compiled\nwrite README.md: hi"
            tid = (await c.post(f"/api/projects/{pid}/requests",
                                json={"text": text, "mode": "new"})).json()["spawned"][0]["id"]

            async def idle():
                return (await c.get(f"/api/threads/{tid}")).json()["state"] == "idle"
            await wait_for(idle)

            top = (await c.get(f"/api/threads/{tid}/files")).json()
            assert top == {"path": "", "entries": [{"name": "out", "dir": True, "size": 0},
                                                   {"name": "README.md", "dir": False, "size": 3}]}
            sub = (await c.get(f"/api/threads/{tid}/files", params={"path": "out/x/.."})).json()
            assert sub["path"] == "out" and [e["name"] for e in sub["entries"]] == ["hello world"]

            r = await c.get(f"/api/threads/{tid}/files/raw", params={"path": "out/hello world"})
            assert r.status_code == 200 and r.content == b"compiled\n"
            assert r.headers["content-type"] == "application/octet-stream"
            assert r.headers["content-disposition"] == "attachment; filename*=UTF-8''hello%20world"

            # The runner's refusals keep their status and words.
            r = await c.get(f"/api/threads/{tid}/files/raw", params={"path": "../../etc/passwd"})
            assert r.status_code == 400 and "escapes" in r.json()["error"]
            assert (await c.get(f"/api/threads/{tid}/files/raw", params={"path": "nope"})).status_code == 404
            assert (await c.get(f"/api/threads/{tid}/files", params={"path": "README.md"})).status_code == 400
            assert (await c.get("/api/threads/t_nope/files")).status_code == 404


async def test_archiving_a_finished_thread_leaves_its_state(tmp_path):
    from control.service import Orchestrator

    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    t = await s.create_thread(p["id"], "Done", "d")
    await s.update_thread(t["id"], state="done")
    orch = Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True), HeuristicRouter())
    got = await orch.archive(t["id"])
    assert got["state"] == "done" and got["archived_at"] is not None
    assert await orch.archive(t["id"]) == got  # archiving twice changes nothing
    assert [e["type"] for e in await s.list_events(p["id"], t["id"])] == ["archived"]
    await s.close()


async def test_messaging_a_thread_whose_worker_is_gone_brings_it_back(tmp_path, monkeypatch):
    """After a control plane restart no worker runs the thread; a message brings it
    back on a new worker with its conversation, then is delivered."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")

    async def idle(tid):
        return (await s.get_thread(tid))["state"] == "idle" or None

    async def replies(tid):
        return [e["payload"]["content"] for e in await s.list_events(p["id"], tid) if e["type"] == "assistant"]

    first = Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w1"), use_devrunner=True), HeuristicRouter())
    t = await first.spawn(p["id"], "Hello", "build hello")
    await wait_for(lambda: replies(t["id"]))
    await wait_for(lambda: idle(t["id"]))
    await first.close()

    second = Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w2"), use_devrunner=True), HeuristicRouter())
    await second.send(t["id"], "can I download it?")
    got = await wait_for(lambda: _second_reply(replies, t["id"]))
    assert got == "Echo: can I download it? (resumed with 3 earlier events)"  # request, reasoning, reply
    types = [e["type"] for e in await s.list_events(p["id"], t["id"])]
    assert "revived" in types and "error" not in types
    assert types.index("revived") > types.index("message_in")
    assert [x for x in types if x == "message_in"] == ["message_in"]  # the runner's echo is dropped
    row = await s.get_thread(t["id"])
    assert row["worker_id"] == second.by_thread[t["id"]].worker.id and row["state"] == "idle"
    assert await s.list_messages(t["id"], unread_only=True) == []
    await second.close()
    await s.close()


async def _second_reply(replies, tid):
    r = await replies(tid)
    return r[1] if len(r) > 1 else None


async def test_a_worker_that_dies_is_dropped_and_its_threads_come_back(tmp_path, monkeypatch):
    """A worker dies while the control plane runs: the watch notices it, its
    working thread fails, and both its threads come back on a new worker with
    their next message."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    monkeypatch.setattr(service, "HEALTH_EVERY", 0.2)
    monkeypatch.setattr(service, "HEALTH_GAP", 0.05)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True)
    orch = Orchestrator(s, provider, HeuristicRouter())
    idle_one = await orch.spawn(p["id"], "Idle", "first")

    async def state(tid, want):
        return (await s.get_thread(tid))["state"] == want or None
    await wait_for(lambda: state(idle_one["id"], "idle"))
    old = orch.by_thread[idle_one["id"]]
    await s.update_thread(idle_one["id"], state="idle")
    busy = await orch.spawn(p["id"], "Busy", "second")
    await wait_for(lambda: state(busy["id"], "idle"))
    await s.update_thread(busy["id"], state="working")  # as if killed mid-turn
    provider.procs[old.worker.id].kill()

    async def lost():
        types = [e["type"] for e in await s.list_events(p["id"], busy["id"])]
        return "worker_lost" in types or None
    await wait_for(lost)
    assert (await s.get_thread(busy["id"]))["state"] == "failed"
    assert (await s.get_thread(idle_one["id"]))["state"] == "idle"
    assert orch.workers[p["id"]] == [] and busy["id"] not in orch.by_thread

    for t in (idle_one, busy):
        await orch.send(t["id"], f"still there, {t['title']}?")

    async def answered(t):
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith(f"Echo: still there, {t['title']}?")
                   for e in await s.list_events(p["id"], t["id"])) or None
    for t in (idle_one, busy):
        await wait_for(lambda: answered(t))
    new = orch.by_thread[busy["id"]]
    assert new is orch.by_thread[idle_one["id"]] and new.worker.id != old.worker.id
    await orch.close()
    await s.close()


async def test_a_message_finds_a_dead_worker_before_the_watch_does(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    monkeypatch.setattr(service, "HEALTH_EVERY", 3600)
    monkeypatch.setattr(service, "HEALTH_GAP", 0.05)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True)
    orch = Orchestrator(s, provider, HeuristicRouter())
    t = await orch.spawn(p["id"], "T", "first")

    async def idle():
        return (await s.get_thread(t["id"]))["state"] == "idle" or None
    await wait_for(idle)
    provider.procs[orch.by_thread[t["id"]].worker.id].kill()
    await orch.send(t["id"], "again")

    async def answered():
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: again")
                   for e in await s.list_events(p["id"], t["id"])) or None
    await wait_for(answered)
    types = [e["type"] for e in await s.list_events(p["id"], t["id"])]
    assert "error" not in types and types.index("worker_lost") < types.index("revived")
    await orch.close()
    await s.close()


async def test_history_is_what_the_thread_received_and_said(tmp_path):
    from control.service import Orchestrator

    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    t = await s.create_thread(p["id"], "T", "do it", parent_id="t_parent")
    orch = Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True), HeuristicRouter())
    await s.append_event(p["id"], t["id"], "created", {"title": "T", "request": "do it", "parent": "t_parent"})
    await s.append_event(p["id"], t["id"], "state", {"state": "working"})
    await s.append_event(p["id"], t["id"], "assistant", {"content": "done"})
    await s.append_event(p["id"], t["id"], "message_in", {"from": "runner", "body": "conflict"})
    delivered = await s.add_message(p["id"], "user", t["id"], "next")
    await s.mark_read([delivered["id"]])
    await s.append_event(p["id"], t["id"], "message_in", {"from": "user", "body": "next", "message": delivered["id"]})
    lost = await s.add_message(p["id"], "router", t["id"], "never arrived")
    await s.append_event(p["id"], t["id"], "message_in", {"from": "router", "body": "never arrived",
                                                         "message": lost["id"]})
    await s.append_event(p["id"], t["id"], "error", {"error": "message not delivered: boom"})
    assert await orch.history(await s.get_thread(t["id"])) == [
        {"type": "message_in", "data": {"from": "t_parent", "body": "do it"}},
        {"type": "assistant", "data": {"content": "done"}},
        {"type": "message_in", "data": {"from": "runner", "body": "conflict"}},
        {"type": "message_in", "data": {"from": "user", "body": "next", "message": delivered["id"]}},
    ]
    await s.close()


async def test_a_stopped_thread_takes_its_next_message(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    orch = Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True), HeuristicRouter())
    t = await orch.spawn(p["id"], "T", "first")

    async def idle():
        return (await s.get_thread(t["id"]))["state"] == "idle" or None
    await wait_for(idle)
    await orch.stop(t["id"])
    ran_on = (await s.get_thread(t["id"]))["worker_id"]

    async def no_other_worker(project):
        raise AssertionError("revived away from the live worker that holds its worktree")
    monkeypatch.setattr(orch, "_pick_worker", no_other_worker)
    await orch.send(t["id"], "again")

    async def answered():
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: again")
                   for e in await s.list_events(p["id"], t["id"])) or None
    await wait_for(answered)
    revived = [e for e in await s.list_events(p["id"], t["id"]) if e["type"] == "revived"]
    assert [e["payload"]["worker"] for e in revived] == [ran_on]
    await orch.close()
    await s.close()


async def test_a_thread_keeps_its_uncommitted_files_across_a_control_plane_restart(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    first = Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True), HeuristicRouter())
    t = await first.spawn(p["id"], "T", "write build/app: binary")

    async def idle():
        return (await s.get_thread(t["id"]))["state"] == "idle" or None
    await wait_for(idle)
    ran_on = (await s.get_thread(t["id"]))["worker_id"]
    token = first.by_thread[t["id"]].worker.token
    await first.close()
    await first.provider.close()

    # The next control plane attaches to the same worker, files and all.
    provider = LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True)
    second = Orchestrator(s, provider, HeuristicRouter())
    assert "devrunner: echo runner on port" in await second.worker_logs(t["id"], 50)  # before it is attached
    listing = await second.list_files(t["id"], "build")
    assert [e["name"] for e in listing["entries"]] == ["app"]
    revived = [e for e in await s.list_events(p["id"], t["id"]) if e["type"] == "revived"]
    assert [e["payload"]["worker"] for e in revived] == [ran_on]
    assert second.by_thread[t["id"]].worker.token == token  # a runner still going answers to it
    # A new thread goes to that worker too rather than a new one.
    u = await second.spawn(p["id"], "U", "hello")
    await wait_for(lambda: idle_of(s, u["id"]))
    assert (await s.get_thread(u["id"]))["worker_id"] == ran_on
    await second.close()
    await provider.close()
    await s.close()


async def test_threads_on_a_worker_stored_without_its_index_share_one_new_worker(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    # A row from before workers kept their index (idx 0), and two threads on it.
    await s.upsert_worker({"id": f"{p['id']}-w1", "project_id": p["id"], "provider": "local",
                           "handle": "1", "url": "http://127.0.0.1:1", "token": "x"})
    ts = [await s.create_thread(p["id"], title, "") for title in ("A", "B")]
    for t in ts:
        await s.update_thread(t["id"], worker_id=f"{p['id']}-w1", state="idle")
    provider = LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True)
    orch = Orchestrator(s, provider, HeuristicRouter())
    lws = [await orch.live_worker(t["id"]) for t in ts]
    assert lws[0] is lws[1] and len(orch.workers[p["id"]]) == 1 and lws[0].worker.index == 1
    await orch.close()
    await provider.close()
    await s.close()


async def idle_of(s, tid):
    return (await s.get_thread(tid))["state"] == "idle" or None


def test_only_what_the_person_typed_is_a_user_row():
    def kind(t, d):
        return app_mod.display({"type": t, "payload": d})["kind"]
    assert kind("request", {"text": "zip it"}) == app_mod.K_USER
    assert kind("message_in", {"from": "user", "body": "more"}) == app_mod.K_USER
    assert kind("created", {"request": "Create a zip archive"}) == app_mod.K_REQUEST
    assert kind("message_in", {"from": "router", "body": "x"}) == app_mod.K_PEER


async def test_routed_messages_are_from_the_router(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    orch = Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True), HeuristicRouter())
    t = await orch.spawn(p["id"], "T", "first")
    await orch.handle_request(p["id"], f"@{t['id']} more", "auto")
    await orch.handle_request(p["id"], "direct", "thread", t["id"])
    got = [(e["payload"]["from"], app_mod.display(e)["label"]) for e in await s.list_events(p["id"], t["id"])
           if e["type"] == "message_in"]
    assert got == [("router", "from router"), ("user", "You")]
    await orch.close()
    await s.close()


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
    await a.update_thread(t["id"], archived_at=5.0)
    assert await b.list_threads(p["id"]) == []
    assert [x["archived_at"] for x in await b.list_threads(p["id"], archived=True)] == [5.0]
    await a.update_thread(t["id"], archived_at=None)
    assert [x["id"] for x in await b.list_threads(p["id"])] == [t["id"]]
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


async def test_heuristic_revision_spawns_only_new_items():
    r = HeuristicRouter()
    plan = await r.revise("- fix login\n- fix export", "- fix login\n- fix signup", ["t_a"], [])
    assert [s["request"] for s in plan["spawn"]] == ["fix export"] and not plan["send"]
    plan = await r.revise("fix login, not signup", "fix signup", ["t_a"], [])
    assert not plan["spawn"] and not plan["send"]
    # An edit that reached no thread is planned like a new request.
    plan = await r.revise("fix login", "fix signup", [], [])
    assert [s["request"] for s in plan["spawn"]] == ["fix login"]


async def test_editing_a_message_corrects_the_threads_it_reached(tmp_path, monkeypatch):
    """An edited request shows its new text, every thread it reached gets a
    correction from the person, and the router plans only what is new."""
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.05")
    router = GatedRouter()
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     LocalProvider(workdir=str(tmp_path / "workers"), use_devrunner=True), router)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]
            r = await c.post(f"/api/projects/{pid}/requests", json={"text": "- fix login\n- fix signup"})
            seq = r.json()["seq"]

            # Edited before the router answered: the edit is taken at once, and
            # re-assessed only after the original's routing.
            t0 = time.perf_counter()
            r = await c.post(f"/api/projects/{pid}/messages/{seq}/edit",
                             json={"text": "- fix login\n- fix export"})
            assert time.perf_counter() - t0 < 1.0, "an edit waited on routing"
            assert r.status_code == 202 and r.json()["changed"]
            log = (await c.get(f"/api/projects/{pid}/log")).json()
            mine = [e for e in log if e["seq"] == seq][0]
            assert (mine["text"], mine["label"], mine["edited"], mine["editable"]) == \
                ("- fix login\n- fix export", "You · edited", 1, 1)
            assert not any(e["type"] == "edited" for e in log)
            router.gate.set()

            async def routes():
                log = (await c.get(f"/api/projects/{pid}/log")).json()
                rs = [e["payload"] for e in log if e["type"] == "route"]
                return rs if len(rs) == 2 else None
            first, again = await wait_for(routes)
            assert first["of"] == seq and len(first["spawned"]) == 2
            reached = [t["id"] for t in first["spawned"]]
            assert again["mode"] == "edit" and again["of"] == seq and again["corrected"] == reached
            assert [t["title"] for t in again["spawned"]] == ["fix export"]
            assert again["summary"].startswith("edit: corrected “fix login”, “fix signup”")

            # Each reached thread got the person's correction, with both versions.
            for tid in reached:
                tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
                fix = [e for e in tr if e["type"] == "message_in" and e["payload"].get("edit") == seq]
                assert len(fix) == 1 and fix[0]["label"] == "You · edit" and fix[0]["editable"] == 0
                assert "> - fix export" in fix[0]["text"] and "> - fix signup" in fix[0]["text"]

            # The same text again changes nothing; machine rows can't be edited.
            r = await c.post(f"/api/projects/{pid}/messages/{seq}/edit",
                             json={"text": "- fix login\n- fix export"})
            assert r.status_code == 200 and not r.json()["changed"]
            route_seq = [e for e in (await c.get(f"/api/projects/{pid}/log")).json() if e["type"] == "route"][0]["seq"]
            r = await c.post(f"/api/projects/{pid}/messages/{route_seq}/edit", json={"text": "x"})
            assert r.status_code == 400
            assert (await c.post(f"/api/projects/{pid}/messages/999999/edit", json={"text": "x"})).status_code == 404

            # A message sent straight to a thread: its thread is corrected.
            tid = reached[0]
            await c.post(f"/api/projects/{pid}/requests", json={"text": "use OAuth", "mode": "thread", "target": tid})
            tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
            msg = [e for e in tr if e["type"] == "message_in" and e["text"] == "use OAuth"][0]
            assert msg["editable"] == 1
            r = await c.post(f"/api/projects/{pid}/messages/{msg['seq']}/edit", json={"text": "use passkeys"})
            assert r.status_code == 202

            async def corrected():
                tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
                return [e for e in tr if e["payload"].get("edit") == msg["seq"]] or None
            fix = (await wait_for(corrected))[0]
            assert fix["text"].startswith("I edited my message to you. It now reads:\n\n> use passkeys")
            tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
            assert [e["text"] for e in tr if e["seq"] == msg["seq"]] == ["use passkeys"]
