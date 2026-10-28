"""Offline tests: SQLite store, heuristic router, and the whole loop against the
echo runner (control.devrunner), served over real HTTP so the peer API is used."""

import asyncio
import base64
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import types

import httpx
import pytest
import uvicorn

from control import app as app_mod
from control import secrets, service
from control.service import NotOnWorker
from control.app import create_app
from control.providers import LocalProvider, WorkerBroken
from control.router import clean_plan
from control.providers import PlaceRefused
from tests.doubles import ECHO_RUNNER, HeuristicRouter, local
from control.server import Server
from control.store import SqliteStore

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


# A 1x1 PNG, small enough to paste in a test.
PNG_1PX = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")
PNG_URL = "data:image/png;base64," + base64.b64encode(PNG_1PX).decode()


async def binds(s, pid, tid):
    """The thread's moves to bound (states.BINDING), first one first: the
    first puts a new thread on a worker, each later one brings it back."""
    return [e["payload"] for e in await s.list_events(pid, tid)
            if e["type"] == "binding" and e["payload"]["to"] == "bound"]


async def prepared(s, pid, tid):
    """What the runner said each time it had made the thread's worktree and
    session (its `prepared` event, after it answered the post)."""
    return [e["payload"] for e in await s.list_events(pid, tid) if e["type"] == "prepared"]


async def unbinds(s, pid, tid):
    return [e["payload"]["reason"] for e in await s.list_events(pid, tid)
            if e["type"] == "binding" and e["payload"]["to"] == "unbound"]


async def test_store_roundtrip(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo", "deepseek-v4-pro")
    assert p["repo_url"] == "" and p["model"] == "deepseek-v4-pro"
    p = await s.update_project(p["id"], name="renamed", repo_url="https://example.com/r.git")
    assert (p["name"], p["repo_url"]) == ("renamed", "https://example.com/r.git")
    with pytest.raises(ValueError):
        await s.update_project(p["id"], id="p_x")
    t = await s.create_thread(p["id"], "Fix it", "fix the bug", "local")
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


async def test_store_roundtrips_pasted_images(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    ref = await s.add_image(p["id"], "image/png", PNG_1PX, name="shot.png")
    assert (ref["media_type"], ref["size"], ref["name"]) == ("image/png", len(PNG_1PX), "shot.png")
    row = await s.get_image(ref["id"])
    assert row["data"] == PNG_1PX and row["project_id"] == p["id"]
    assert await s.get_image("i_nope") is None
    await s.close()


async def test_store_lists_archived_threads_apart(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    keep = await s.create_thread(p["id"], "Keep", "k", "local")
    old = await s.create_thread(p["id"], "Old", "o", "local")
    assert keep["archived_at"] is None
    await s.update_thread(old["id"], archived_at=123.0)
    assert [t["id"] for t in await s.list_threads(p["id"])] == [keep["id"]]
    assert [t["id"] for t in await s.list_threads(p["id"], archived=True)] == [old["id"]]
    assert (await s.get_thread(old["id"]))["archived_at"] == 123.0
    await s.update_thread(old["id"], archived_at=None)
    assert {t["id"] for t in await s.list_threads(p["id"])} == {keep["id"], old["id"]}
    assert await s.list_threads(p["id"], archived=True) == []
    await s.close()


async def test_store_lists_archived_projects_apart(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    keep = await s.create_project("keep")
    old = await s.create_project("old")
    assert keep["archived_at"] is None
    assert (await s.update_project(old["id"], archived_at=7.0))["archived_at"] == 7.0
    assert [p["id"] for p in await s.list_projects()] == [keep["id"]]
    assert [p["id"] for p in await s.list_projects(archived=True)] == [old["id"]]
    await s.update_project(old["id"], archived_at=None)
    assert [p["id"] for p in await s.list_projects()] == [keep["id"], old["id"]]
    with pytest.raises(ValueError):
        await s.update_project(old["id"], bogus="x")
    await s.close()


async def test_store_adds_project_archived_at_to_an_older_database(tmp_path):
    import aiosqlite

    path = str(tmp_path / "db.sqlite")
    async with aiosqlite.connect(path) as db:
        await db.execute("CREATE TABLE projects (id TEXT PRIMARY KEY, name TEXT NOT NULL, "
                         "repo_url TEXT NOT NULL DEFAULT '', model TEXT NOT NULL DEFAULT '', "
                         "settings TEXT NOT NULL DEFAULT '{}', created_at DOUBLE PRECISION NOT NULL)")
        await db.execute("INSERT INTO projects (id, name, created_at) VALUES ('p_old', 'old', 1.0)")
        await db.commit()
    s = SqliteStore(path)
    await s.open()
    assert [(p["id"], p["archived_at"]) for p in await s.list_projects()] == [("p_old", None)]
    await s.close()


async def test_store_lists_working_threads_first(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    a = await s.create_thread(p["id"], "A", "a", "local")
    b = await s.create_thread(p["id"], "B", "b", "local")
    c = await s.create_thread(p["id"], "C", "c", "local")
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


async def test_store_takes_the_newest_events_for_a_pane(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    t = await s.create_thread(p["id"], "A", "a", "local")
    seqs = []
    for i in range(6):  # alternating: the thread, then the project
        e = await s.append_event(p["id"], t["id"] if i % 2 == 0 else None, "assistant", {"content": str(i)})
        seqs.append(e["seq"])
    # A pane is about what just happened: the newest events, oldest first.
    assert [e["seq"] for e in await s.recent_events(p["id"], limit=3)] == seqs[-3:]
    assert [e["seq"] for e in await s.recent_events(p["id"], t["id"], limit=2)] == [seqs[2], seqs[4]]
    assert [e["seq"] for e in await s.recent_events(p["id"], limit=50)] == seqs
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
    assert (await s.get_thread("t_old"))["objectives"] == []
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
    t = await s.create_thread(p["id"], "Fix it", "fix the bug", "local")
    orch = service.Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)), HeuristicRouter(), tmp_path / "repos")

    async def doing(typ, data):
        await orch._ingest({"thread": t["id"], "type": typ, "data": data})
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
    t = await s.create_thread(p["id"], "Fix it", "fix the bug", "local")
    orch = service.Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)), HeuristicRouter(), tmp_path / "repos")

    async def ingest(typ, data):
        await orch._ingest({"thread": t["id"], "type": typ, "data": data})

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
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = service.Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    try:
        t = await orch.spawn(p["id"], "Echo", "say hi")
        await asyncio.wait({orch.binding[t["id"]]})  # spawn returns before the worker is up
        log = await orch.worker_logs(t["id"], 50)
        assert "devrunner: echo runner on port" in log
        with pytest.raises(KeyError):
            await orch.worker_logs("t_nope")
    finally:
        await orch.close()
        await provider.close()
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
    one = await r.plan("add a login page", [], HERE)
    assert len(one["spawn"]) == 1 and not one["send"]
    many = await r.plan("Do these:\n1. login page\n2. export bug\n- docs", [], HERE)
    assert [s["request"] for s in many["spawn"]] == ["login page", "export bug", "docs"]
    sent = await r.plan("@t_abc also handle the empty case", [{"id": "t_abc"}], HERE)
    assert sent["send"] == [{"thread": "t_abc", "message": "also handle the empty case", "kind": "work",
                              "objectives": ["also handle the empty case"], "workers": "", "where": ""}]
    assert (await r.plan("fix it locally", [], HERE))["spawn"][0]["workers"] == "local"
    assert (await r.plan("fix it on sprites", [], HERE))["spawn"][0]["workers"] == ""  # not available


def test_clean_plan_drops_unknown_threads():
    plan = clean_plan({"spawn": [{"request": "x", "objectives": [" do x ", "", 3], "workers": "sprites"},
                                 {"title": "no request"}],
                       "send": [{"thread": "t_nope", "message": "m"},
                                {"thread": "t_1", "message": "m", "kind": "steer", "workers": "local",
                                 "where": "its logs are here"}]},
                      [{"id": "t_1"}], HERE)
    # Sprites is not available here: that thread is placed when it is put on a worker.
    assert [(s["workers"], s["where"], s["objectives"]) for s in plan["spawn"]] == [("", "", ["do x"])]
    assert plan["send"] == [{"thread": "t_1", "message": "m", "kind": "steer", "objectives": [], "workers": "local",
                             "where": "its logs are here"}]


def test_a_planned_thread_with_no_objectives_is_an_error():
    with pytest.raises(ValueError, match="no objectives"):
        clean_plan({"spawn": [{"title": "T", "request": "x", "objectives": []}]}, [], HERE)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def server(tmp_path, monkeypatch):
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.05")
    # One running thread per worker: the second thread fills a second slot.
    monkeypatch.setattr(service, "CAPACITY", 1)
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    # Served as production does (control/server.py): the shutdown event ends the
    # SSE streams, so the server stops at once instead of waiting on a client
    # that still holds one open.
    stop = asyncio.Event()
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)),
                     HeuristicRouter(), shutdown_event=stop, repos=str(tmp_path / "repos"))
    srv = Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning",
                                timeout_graceful_shutdown=5), stop)
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

        # A child that finishes after its parent was archived reports to no one.
        child = next(t for t in (await c.get(f"/api/projects/{pid}/threads")).json() if t["parent_id"] == ids[0])
        before = (await c.get(f"/api/threads/{child['id']}")).json()["summary"]
        assert (await c.post(f"/api/threads/{ids[0]}/archive")).status_code == 200
        await c.post(f"/api/projects/{pid}/requests",
                     json={"text": "a longer follow-up for the child", "mode": "thread", "target": child["id"]})

        async def child_finished_again():
            return (await c.get(f"/api/threads/{child['id']}")).json()["summary"] != before
        await wait_for(child_finished_again)
        log = (await c.get(f"/api/projects/{pid}/log")).json()
        assert not any(e["type"] == "error" and "archived" in str(e["payload"]) for e in log)
        assert (await c.post(f"/api/threads/{ids[0]}/unarchive")).status_code == 200

        # Workers never call the control plane, so it serves no peer API.
        assert (await c.get(f"/api/peer/projects/{pid}/threads")).status_code == 404

        # Peers: list, read and message another thread, wherever it runs.
        on = {t["id"]: t["worker_id"] for t in (await c.get(f"/api/projects/{pid}/threads")).json()}
        assert on[ids[0]] and on[ids[1]]
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

        # The state can lag the stop: a state event from the turn that was just
        # running may still be on its way when the control plane takes it.
        async def stopped():
            return (await c.get(f"/api/threads/{ids[1]}")).json()["state"] == "stopped" or None
        await wait_for(stopped)


async def test_a_pane_shows_the_newest_events_of_a_long_log(server, monkeypatch):
    """A project worked on for a while has more events than a pane shows. Project
    activity and a thread's transcript take the newest ones: a request sent in the
    activity view used to fall off the end of the page and never show there."""
    monkeypatch.setattr(app_mod, "PAGE_EVENTS", 4)
    async with httpx.AsyncClient(base_url=server, timeout=20) as c:
        pid = (await c.post("/api/projects", json={"name": "long"})).json()["id"]
        tid = (await c.post(f"/api/projects/{pid}/requests",
                            json={"text": "first request", "mode": "new"})).json()["spawned"][0]["id"]

        async def idle():
            return (await c.get(f"/api/threads/{tid}")).json()["state"] == "idle"
        await wait_for(idle)

        log = (await c.get(f"/api/projects/{pid}/log")).json()
        assert len(log) == 4, "a pane holds one page of the log"
        assert [e["seq"] for e in log] == sorted(e["seq"] for e in log), "oldest first"
        assert not any(e["type"] in ("request", "created") for e in log), "the oldest events slid off"

        # A request sent now is in the window, not beyond the end of it.
        assert (await c.post(f"/api/projects/{pid}/requests", json={"text": "second request"})).status_code == 202

        async def shown():
            return any(e["type"] == "request" and e["text"] == "second request"
                       for e in (await c.get(f"/api/projects/{pid}/log")).json())
        await wait_for(shown)

        # A thread's transcript is a page of its own events, from the same end.
        tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
        assert 0 < len(tr) <= 4 and [e["seq"] for e in tr] == sorted(e["seq"] for e in tr)


async def test_a_reply_in_flight_is_one_row_that_grows_on_the_stream(server):
    """A pane patches the reply it is watching instead of refetching the whole
    pane: the stream sends that reply as one row, under the seq it started at,
    whose text grows with each delta."""
    async with httpx.AsyncClient(base_url=server, timeout=30) as c:
        pid = (await c.post("/api/projects", json={"name": "live"})).json()["id"]
        tid = (await c.post(f"/api/projects/{pid}/requests",
                            json={"text": "first", "mode": "new"})).json()["spawned"][0]["id"]

        async def idle():
            return (await c.get(f"/api/threads/{tid}")).json()["state"] == "idle"
        await wait_for(idle)

        growing: list[dict] = []
        ended = None
        after = (await c.get(f"/api/projects/{pid}/log")).json()[-1]["seq"]
        async with c.stream("GET", f"/api/projects/{pid}/events", params={"after": after}) as s:
            # The reply streams while the stream is open, as a pane sees it.
            await c.post(f"/api/threads/{tid}/messages", json={"body": "y" * 1200})
            event = ""
            async for line in s.aiter_lines():
                if line.startswith("event: "):
                    event = line.removeprefix("event: ")
                elif line.startswith("data: "):
                    row = json.loads(line.removeprefix("data: "))
                    if event == "delta":
                        growing.append(row)
                    elif event == "assistant":  # the end of the model call
                        ended = row
                        break

        reply = [r for r in growing if r["kind"] == app_mod.K_ASSISTANT]
        assert len(reply) > 1, "a long reply streams in several deltas"
        assert len({r["seq"] for r in reply}) == 1, "one row, not one per delta"
        assert all(r.get("live") == 1 for r in reply)
        # Only the delta that started the part says so: a pane refreshes the
        # thread list for that one, and patches the row for the rest.
        assert [r.get("began") for r in reply] == [1] + [None] * (len(reply) - 1)
        assert [len(r["text"]) for r in reply] == sorted(len(r["text"]) for r in reply)
        assert len(reply[-1]["text"]) > len(reply[0]["text"]), "the row grew"
        assert reply[-1]["label"] == "Agent (writing…)"

        # The event that ends the call is the whole reply, and the deltas are
        # gone: what the pane's next snapshot shows.
        assert ended["text"].startswith("Echo: ")
        rows = (await c.get(f"/api/projects/{pid}/log")).json()
        assert not any(r.get("live") for r in rows)
        assert [r["text"] for r in rows if r["type"] == "assistant"][-1] == ended["text"]


# Places.offered of a control plane that runs workers on this computer only.
HERE = [{"id": "local", "label": "This computer", "why": ""},
        {"id": "sprites", "label": "Sprites", "why": "it needs SPRITES_TOKEN"}]


class GatedRouter(HeuristicRouter):
    """A router whose model has not answered until `gate` is set."""

    def __init__(self) -> None:
        self.gate = asyncio.Event()

    async def plan(self, request, directory, places, images=0):
        await self.gate.wait()
        return await super().plan(request, directory, places, images)


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
    provider = GatedProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")), local(provider), router,
                     repos=str(tmp_path / "repos"))
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
            assert t["load"]["level"] == "busy"
            assert (await post({"text": "check csv too", "mode": "thread", "target": tid})).status_code == 200
            tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
            assert [(e["type"], e["text"]) for e in tr if e["type"] not in ("binding", "objectives")] == [
                ("created", "fix export"), ("message_in", "check csv too")]

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
    provider = GatedProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")), local(provider), HeuristicRouter(),
                     repos=str(tmp_path / "repos"))
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


async def test_archiving_a_project_stops_its_threads_and_takes_it_off_the_workers(tmp_path, monkeypatch):
    """An archived project leaves the list, takes no work and has nothing left
    on the pool's workers, which stay for the other projects; unarchived, its
    threads come back on the pool with their next message."""
    monkeypatch.setenv("DEVRUNNER_DELAY", "1.5")  # a turn is long enough to archive during it
    provider = LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)
    destroyed = []
    destroy = provider.destroy

    async def recording_destroy(worker):
        destroyed.append(worker.id)
        await destroy(worker)
    provider.destroy = recording_destroy
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")), local(provider), HeuristicRouter(),
                     repos=str(tmp_path / "repos"))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]
            other = (await c.post("/api/projects", json={"name": "other"})).json()["id"]
            done = (await c.post(f"/api/projects/{pid}/requests",
                                 json={"text": "build hello", "mode": "new"})).json()["spawned"][0]["id"]
            theirs = (await c.post(f"/api/projects/{other}/requests",
                                   json={"text": "build theirs", "mode": "new"})).json()["spawned"][0]["id"]

            async def idle():
                return {(await c.get(f"/api/threads/{t}")).json()["state"] for t in (done, theirs)} == {"idle"}
            await wait_for(idle)
            busy = (await c.post(f"/api/projects/{pid}/requests",
                                 json={"text": "sleep 30", "mode": "new"})).json()["spawned"][0]["id"]

            async def working():
                return (await c.get(f"/api/threads/{busy}")).json()["state"] == "working"
            await wait_for(working)
            w1 = tmp_path / "workers" / "w1"
            assert (w1 / ".repos" / pid).exists() and (w1 / done).exists() and (w1 / theirs).exists()

            r = await c.post(f"/api/projects/{pid}/archive")
            assert r.status_code == 200 and r.json()["archived_at"] is not None
            assert [p["id"] for p in (await c.get("/api/projects")).json()] == [other]
            assert [p["id"] for p in (await c.get("/api/projects?archived=1")).json()] == [pid]
            assert {(await c.get(f"/api/threads/{t}")).json()["state"] for t in (done, busy)} == {"stopped"}
            # The worker stays, shared; only the project is gone from it.
            assert destroyed == [] and [lw.worker.id for lw in app.state.orch.workers] == ["w1"]
            assert not (w1 / ".repos" / pid).exists() and not (w1 / done).exists() and not (w1 / busy).exists()
            assert (w1 / ".repos" / other).exists() and (w1 / theirs).exists()
            log = (await c.get(f"/api/projects/{pid}/log")).json()
            assert [e["type"] for e in log if e["type"].startswith("project_")] == ["project_archived"]

            # It takes no work, from any door.
            for r in (await c.post(f"/api/projects/{pid}/requests", json={"text": "more"}),
                      await c.post(f"/api/projects/{pid}/threads", json={"request": "more"}),
                      await c.post(f"/api/threads/{done}/messages", json={"body": "hi"}),
                      await c.get(f"/api/threads/{done}/files")):
                assert r.status_code == 409 and "archived" in r.json()["error"]
            # Archiving it again changes nothing.
            assert (await c.post(f"/api/projects/{pid}/archive")).status_code == 200
            log = (await c.get(f"/api/projects/{pid}/log")).json()
            assert [e["type"] for e in log if e["type"].startswith("project_")] == ["project_archived"]

            r = await c.post(f"/api/projects/{pid}/unarchive")
            assert r.status_code == 200 and r.json()["archived_at"] is None
            assert {p["id"] for p in (await c.get("/api/projects")).json()} == {pid, other}
            await c.post(f"/api/threads/{done}/messages", json={"body": "again"})

            async def revived():
                return (await binds(app.state.orch.store, pid, done))[1:] or None
            back = (await wait_for(revived))[0]
            assert back["worker"] == "w1"  # back on the pool, from the hub
            assert (await unbinds(app.state.orch.store, pid, done))[0] == "its project was archived"

            async def answered():
                return (await c.get(f"/api/threads/{done}")).json()["state"] == "idle" or None
            await wait_for(answered)
            assert (w1 / done).exists()  # its worktree, made again from the hub
            assert (await c.post("/api/projects/p_nope/archive")).status_code == 404
            assert (await c.post("/api/projects/p_nope/unarchive")).status_code == 404


async def test_files_of_a_thread_browse_and_download(tmp_path, monkeypatch):
    """The Files tab: the control plane reads a thread's worktree from its
    worker and serves a file as a download."""
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.05")
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)), HeuristicRouter(),
                     repos=str(tmp_path / "repos"))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]
            text = ("build hello\nwrite out/hello world: compiled\nwrite README.md: hi\n"
                    "write out/dot.svg: <svg xmlns='http://www.w3.org/2000/svg'><script>x()</script></svg>")
            tid = (await c.post(f"/api/projects/{pid}/requests",
                                json={"text": text, "mode": "new"})).json()["spawned"][0]["id"]

            async def idle():
                return (await c.get(f"/api/threads/{tid}")).json()["state"] == "idle"
            await wait_for(idle)

            top = (await c.get(f"/api/threads/{tid}/files")).json()
            assert top["path"] == "" and top["entries"][0] == {"name": "out", "dir": True, "size": 0}
            assert [e["name"] for e in top["entries"]] == ["out", ".gitattributes", ".gitignore", "README.md",
                                                           "pixi.toml"]  # every project is a pixi project
            assert top["entries"][3] == {"name": "README.md", "dir": False, "size": 3}
            sub = (await c.get(f"/api/threads/{tid}/files", params={"path": "out/x/.."})).json()
            assert sub["path"] == "out" and [e["name"] for e in sub["entries"]] == ["dot.svg", "hello world"]

            r = await c.get(f"/api/threads/{tid}/files/raw", params={"path": "out/hello world"})
            assert r.status_code == 200 and r.content == b"compiled\n"
            assert r.headers["content-type"] == "application/octet-stream"
            assert r.headers["content-disposition"] == "attachment; filename*=UTF-8''hello%20world"
            # An image is served inline as its type, for the window to show;
            # nothing served runs (an SVG's script stays dead).
            r = await c.get(f"/api/threads/{tid}/files/raw", params={"path": "out/dot.svg"})
            assert r.status_code == 200 and r.headers["content-type"] == "image/svg+xml"
            assert r.headers["content-disposition"] == "inline; filename*=UTF-8''dot.svg"
            assert "sandbox" in r.headers["content-security-policy"]
            assert r.headers["x-content-type-options"] == "nosniff"

            # The runner's refusals keep their status and words.
            r = await c.get(f"/api/threads/{tid}/files/raw", params={"path": "../../etc/passwd"})
            assert r.status_code == 400 and "escapes" in r.json()["error"]
            assert (await c.get(f"/api/threads/{tid}/files/raw", params={"path": "nope"})).status_code == 404
            assert (await c.get(f"/api/threads/{tid}/files", params={"path": "README.md"})).status_code == 400
            assert (await c.get("/api/threads/t_nope/files")).status_code == 404


async def test_project_settings_change_name_model_and_remote(tmp_path, monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "k")
    monkeypatch.delenv("OPENGATEWAY_API_TOKEN", raising=False)
    monkeypatch.setattr(secrets, "_from_keyring", lambda name: None)
    monkeypatch.setattr(secrets, "_from_keyring_label", lambda name: None)
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)), HeuristicRouter(),
                     repos=str(tmp_path / "repos"))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            # A new project has no remote: what it is given is ignored.
            p = (await c.post("/api/projects", json={"name": "demo", "repo_url": "https://x/y.git"})).json()
            pid = p["id"]
            assert p["repo_url"] == "" and p["repo_path"].endswith(f"{pid}.git")

            r = await c.post(f"/api/projects/{pid}/settings", json={"name": " renamed ", "model": "deepseek-v4-pro"})
            assert r.status_code == 200 and (r.json()["name"], r.json()["model"]) == ("renamed", "deepseek-v4-pro")
            # A model is one of control/models.py's, and this computer has its provider's key.
            r = await c.post(f"/api/projects/{pid}/settings", json={"model": "m"})
            assert r.status_code == 400 and "no model 'm'" in r.json()["error"]
            ultrafast = {"model": "deepseek/deepseek-v4.1-flash-ultrafast"}
            r = await c.post(f"/api/projects/{pid}/settings", json=ultrafast)
            assert r.status_code == 400 and "OPENGATEWAY_API_TOKEN" in r.json()["error"]
            assert (await c.get(f"/api/projects/{pid}")).json()["model"] == "deepseek-v4-pro"
            monkeypatch.setenv("OPENGATEWAY_API_TOKEN", "og")
            r = await c.post(f"/api/projects/{pid}/settings", json=ultrafast)
            assert r.status_code == 200 and r.json()["model"] == ultrafast["model"]
            assert (await c.post(f"/api/projects/{pid}/settings", json={"model": ""})).json()["model"] == ""
            assert (await c.post(f"/api/projects/{pid}/settings", json={"name": ""})).status_code == 400

            empty = tmp_path / "remote.git"
            subprocess.run(["git", "init", "-q", "--bare", str(empty)], check=True)
            r = await c.post(f"/api/projects/{pid}/settings", json={"repo_url": str(empty)})
            assert r.status_code == 200 and r.json()["repo_url"] == str(empty)
            r = await c.post(f"/api/projects/{pid}/settings", json={"repo_url": str(tmp_path / "nope.git")})
            assert r.status_code == 400 and "nope.git" in r.json()["error"]
            assert (await c.get(f"/api/projects/{pid}")).json()["repo_url"] == str(empty)
            r = await c.post(f"/api/projects/{pid}/settings", json={"repo_url": ""})
            assert r.status_code == 200 and r.json()["repo_url"] == ""
            assert (await c.post("/api/projects/p_nope/settings", json={"name": "x"})).status_code == 404


async def test_a_project_opens_from_a_folder_named_after_it(tmp_path):
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)), HeuristicRouter(),
                     repos=str(tmp_path / "repos"))
    folder = tmp_path / "my-app"
    folder.mkdir()
    (folder / "README.md").write_text("hi\n")
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            r = await c.post("/api/projects", json={"folder": "my-app"})
            assert r.status_code == 400 and "full path" in r.json()["error"]
            r = await c.post("/api/projects", json={"folder": str(tmp_path / "nope")})
            assert r.status_code == 400 and "no folder" in r.json()["error"]
            assert (await c.get("/api/projects")).json() == []

            r = await c.post("/api/projects", json={"folder": f" {tmp_path}/x/../my-app/ "})
            assert r.status_code == 201
            p = r.json()
            assert (p["name"], p["repo_url"]) == ("my-app", str(folder.resolve()))
            assert (folder / "pixi.toml").exists()
            r = await c.post("/api/projects", json={"folder": str(folder), "name": "named"})
            assert r.status_code == 201 and r.json()["name"] == "named"


async def test_the_folder_picker_lists_folders_from_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    for d in ("b-app", "A-notes", ".hidden"):
        (tmp_path / d).mkdir()
    (tmp_path / "file.txt").write_text("x")
    subprocess.run(["git", "init", "-q", str(tmp_path / "b-app")], check=True)
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)), HeuristicRouter(),
                     repos=str(tmp_path / "repos"))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            home = (await c.get("/api/folders")).json()
            assert home["path"] == str(tmp_path.resolve()) and home["parent"] == str(tmp_path.resolve().parent)
            assert [(e["name"], e["git"]) for e in home["entries"] if e["name"] in ("A-notes", "b-app")] == [
                ("A-notes", False), ("b-app", True)]
            assert all(not e["name"].startswith(".") for e in home["entries"])
            inner = (await c.get("/api/folders", params={"path": "~/b-app"})).json()
            assert (inner["path"], inner["entries"]) == (str((tmp_path / "b-app").resolve()), [])
            assert (await c.get("/api/folders", params={"path": "/"})).json()["parent"] == ""
            r = await c.get("/api/folders", params={"path": "relative"})
            assert r.status_code == 400 and "full path" in r.json()["error"]
            r = await c.get("/api/folders", params={"path": str(tmp_path / "nope")})
            assert r.status_code == 400 and "no folder" in r.json()["error"]


async def test_the_folder_picker_lists_a_folder_it_cannot_look_into(tmp_path, monkeypatch):
    """A folder only root may read (a system folder at the root of the disk, a
    mount of someone else's) is listed like any other and marked not a
    repository: the picker used to fail the whole listing of the folder it is
    in, on looking for its .git."""
    monkeypatch.setenv("HOME", str(tmp_path))
    (tmp_path / "open").mkdir()
    (tmp_path / "locked").mkdir()
    (tmp_path / "locked").chmod(0o000)
    try:
        try:
            os.listdir(tmp_path / "locked")
        except PermissionError:
            pass
        else:
            pytest.skip("a folder with no permissions is readable here (root)")
        app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                         local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)),
                         HeuristicRouter(), repos=str(tmp_path / "repos"))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                         timeout=20) as c:
                home = (await c.get("/api/folders")).json()
        assert sorted((e["name"], e["git"]) for e in home["entries"]) == [("locked", False), ("open", False)]
    finally:
        (tmp_path / "locked").chmod(0o755)


async def test_pasted_images_are_stored_and_served_not_logged(tmp_path, monkeypatch):
    """An image's bytes live in the store: events and the transcript carry only
    its id, /api/images/<id> serves the bytes, and the runner gets them base64
    inside the conversation it is rebuilt from."""
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    store = SqliteStore(str(tmp_path / "db.sqlite"))
    app = create_app(store, local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)),
                     NoRoutingRouter(), repos=str(tmp_path / "repos"))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]
            r = await c.post(f"/api/projects/{pid}/requests",
                             json={"text": "what is this", "images": [PNG_URL], "mode": "new"})
            assert r.status_code == 200, r.text
            tid = r.json()["spawned"][0]["id"]

            # The request shows in the transcript with the image's id, not its bytes.
            tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
            ref = next(e for e in tr if e["type"] == "created")["images"][0]
            assert (ref["media_type"], ref["size"]) == ("image/png", len(PNG_1PX))
            assert "data" not in ref
            assert base64.b64encode(PNG_1PX).decode() not in json.dumps(
                (await c.get(f"/api/projects/{pid}/log")).json())

            # The bytes come from their own endpoint, cacheable for good.
            img = await c.get(f"/api/images/{ref['id']}")
            assert img.status_code == 200 and img.content == PNG_1PX
            assert img.headers["content-type"] == "image/png"
            assert "immutable" in img.headers["cache-control"]
            assert (await c.get("/api/images/i_nope")).status_code == 404

            # A message to the thread carries its image the same way, as its own paste.
            r = await c.post(f"/api/threads/{tid}/messages", json={"body": "", "images": [PNG_URL]})
            assert r.status_code == 202, r.text
            tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
            said = next(e for e in tr if e["type"] == "message_in")
            assert said["images"][0]["id"] != ref["id"]

            # Let the turn end before the store closes under its delivery.
            async def answered():
                tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
                return any(e["type"] == "assistant" and e["seq"] > said["seq"] for e in tr)
            await wait_for(answered)


class NoRoutingRouter:
    """A router that must not be asked: an image-only request needs no plan."""

    async def plan(self, request, directory, places, images=0):
        raise AssertionError("the router was asked to plan a request with no text")

    async def place(self, request, places):
        return {"workers": "local", "where": ""}

    async def objectives(self, request, directory):
        return {"objectives": ["look at the image"], "why": "an image alone"}


async def test_an_image_alone_starts_one_thread_without_the_router(tmp_path, monkeypatch):
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)),
                     NoRoutingRouter(), repos=str(tmp_path / "repos"))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]
            r = await c.post(f"/api/projects/{pid}/requests", json={"images": [PNG_URL, PNG_URL]})
            assert r.status_code == 202, r.text
            plan = r.json()
            # An image-only request skips the router: it always starts one new thread.
            assert plan["mode"] == "new" and [t["title"] for t in plan["spawned"]] == ["2 pasted images"]
            # The logged request names the images by id; the row shows them.
            log = (await c.get(f"/api/projects/{pid}/log")).json()
            req = next(e for e in log if e["type"] == "request")
            assert len(req["images"]) == 2
            assert base64.b64encode(PNG_1PX).decode() not in json.dumps(req)

            r = await c.post(f"/api/projects/{pid}/requests", json={"images": [PNG_URL], "mode": "new"})
            assert r.status_code == 200 and r.json()["spawned"][0]["title"] == "Pasted image"


async def test_the_image_api_refuses_what_it_cannot_take(tmp_path):
    store = SqliteStore(str(tmp_path / "db.sqlite"))
    app = create_app(store, local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)),
                     NoRoutingRouter(), repos=str(tmp_path / "repos"))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]

            async def refused(body, why):
                r = await c.post(f"/api/projects/{pid}/requests", json=body)
                assert r.status_code == 400, r.text
                assert why in r.json()["error"], r.text

            await refused({"text": "x", "images": "nope"}, "list")
            await refused({"text": "x", "images": [PNG_URL] * 5}, "at most 4")
            await refused({"text": "x", "images": ["nope"]}, "data: URL")
            await refused({"text": "x", "images": ["data:text/plain;base64,aGk="]}, "png, jpeg, webp or gif")
            await refused({"text": "x", "images": ["data:image/png;base64,!!!!"]}, "base64")
            await refused({"text": "x", "images": ["data:image/png;base64,"]}, "empty")
            await refused({"text": "   "}, "text or an image")

            # A message to a thread needs text or an image too.
            t = await store.create_thread(pid, "demo", "go", "local")
            r = await c.post(f"/api/threads/{t['id']}/messages", json={"body": ""})
            assert r.status_code == 400 and "body or an image" in r.json()["error"]


async def test_archiving_a_finished_thread_leaves_its_state(tmp_path):
    from control.service import Orchestrator

    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    t = await s.create_thread(p["id"], "Done", "d", "local")
    await s.update_thread(t["id"], state="done")
    orch = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)), HeuristicRouter(), tmp_path / "repos")
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

    first = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w1"), command=ECHO_RUNNER)), HeuristicRouter(), tmp_path / "repos")
    t = await first.spawn(p["id"], "Hello", "build hello")
    await wait_for(lambda: replies(t["id"]))
    await wait_for(lambda: idle(t["id"]))
    await first.close()
    await first.places.close()

    second = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w2"), command=ECHO_RUNNER)), HeuristicRouter(), tmp_path / "repos")
    await second.send(t["id"], "can I download it?")
    got = await wait_for(lambda: _second_reply(replies, t["id"]))
    await wait_for(lambda: idle(t["id"]))  # its work is saved before it goes idle
    assert got == "Echo: can I download it? (resumed after 1 earlier messages)"  # its session came along
    events = await s.list_events(p["id"], t["id"])
    types = [e["type"] for e in events]
    assert "error" not in types
    back = [i for i, e in enumerate(events) if e["type"] == "binding" and e["payload"]["to"] == "bound"][1]
    assert back > types.index("message_in")
    assert "the control plane that had it" in (await unbinds(s, p["id"], t["id"]))[0]
    assert [x for x in types if x == "message_in"] == ["message_in"]  # the runner's echo is dropped
    row = await s.get_thread(t["id"])
    assert row["worker_id"] == second.by_thread[t["id"]].worker.id and row["state"] == "idle"
    assert await s.list_messages(t["id"], unread_only=True) == []
    await second.close()
    await second.places.close()
    await s.close()


async def _second_reply(replies, tid):
    r = await replies(tid)
    return r[1] if len(r) > 1 else None


class BreakableProvider(LocalProvider):
    """A provider that says its workers are broken while `broken` is set (a
    sprite whose services database Fly reports corrupted)."""

    def __init__(self, **kw) -> None:
        super().__init__(**kw)
        self.broken = False
        self.destroyed: list[str] = []

    async def create(self, spec):
        if self.broken:
            raise WorkerBroken("looking for its runner service failed: 500: database disk image is malformed")
        return await super().create(spec)

    async def destroy(self, worker):
        self.destroyed.append(worker.id)
        await super().destroy(worker)


async def test_a_broken_worker_holds_its_threads_until_respawn_replaces_it(tmp_path, monkeypatch):
    """A worker its provider says is broken is kept in its slot, broken, with
    the provider's words: a message to a thread on it fails at once and says
    Respawn mends it, and the slot is not attached again for it. Respawn
    destroys the worker and makes it afresh in its slot; the thread comes
    back on it from its last saved turn and answers."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    first = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)),
                         HeuristicRouter(), tmp_path / "repos")
    t = await first.spawn(p["id"], "T", "write notes.txt: kept")
    await wait_for(lambda: idle_of(s, t["id"]))
    wid = first.by_thread[t["id"]].worker.id
    await first.close()
    await first.places.close()

    provider = BreakableProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    provider.broken = True
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    await orch.send(t["id"], "hello")

    async def refused():
        return [r for r in await unbinds(s, p["id"], t["id"]) if "is broken" in r] or None
    reason = (await wait_for(refused))[0]
    assert "database disk image is malformed" in reason and "Respawn" in reason
    [lw] = orch.workers
    assert (lw.worker.id, lw.state) == (wid, "broken")
    assert (await orch.with_load(await s.get_thread(t["id"])))["load"]["level"] in ("idle", "stalled")
    await orch.send(t["id"], "still there?")  # held, not attached again: no second worker, no destroy
    await wait_for(lambda: _unbound_count(s, p["id"], t["id"], 3))
    assert orch.workers == [lw] and provider.destroyed == []

    provider.broken = False
    await orch.respawn(t["id"])

    async def rebound():
        return len(await binds(s, p["id"], t["id"])) >= 2 or None
    await wait_for(rebound)
    await orch.send(t["id"], "after")

    async def answered():
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: after")
                   for e in await s.list_events(p["id"], t["id"])) or None
    await wait_for(answered)
    assert provider.destroyed == [wid] and lw.state == "ready" and orch.by_thread[t["id"]] is lw
    states = [(e["payload"]["from"], e["payload"]["to"]) for e in await s.list_events(p["id"])
              if e["type"] == "worker_state"]
    assert states == [("", "broken"), ("broken", "replacing"), ("replacing", "ready")]
    assert (await binds(s, p["id"], t["id"]))[-1]["worker"] == wid
    await orch.close()
    await provider.close()
    await s.close()


async def _unbound_count(s, pid, tid, n):
    return len(await unbinds(s, pid, tid)) >= n or None


async def test_a_worker_that_dies_is_dropped_and_its_threads_come_back(tmp_path, monkeypatch):
    """A worker dies while the control plane runs: the watch notices it, its
    working thread fails, and both its threads come back on a new worker with
    their next message."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    monkeypatch.setattr(service, "HEALTH_EVERY", 0.2)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
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
        return any(f"worker {old.worker.id} is lost" in r for r in await unbinds(s, p["id"], busy["id"])) or None
    await wait_for(lost)
    assert (await s.get_thread(busy["id"]))["state"] == "failed"
    assert (await s.get_thread(idle_one["id"]))["state"] == "idle"
    assert orch.workers == [] and busy["id"] not in orch.by_thread

    for t in (idle_one, busy):
        await orch.send(t["id"], f"still there, {t['title']}?")

    async def answered(t):
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith(f"Echo: still there, {t['title']}?")
                   for e in await s.list_events(p["id"], t["id"])) or None
    for t in (idle_one, busy):
        await wait_for(lambda: answered(t))
    new = orch.by_thread[busy["id"]]
    # A new worker, in the lost one's slot: the pool never grows.
    assert new is orch.by_thread[idle_one["id"]] and new is not old and new.worker.id == old.worker.id
    await orch.close()
    await provider.close()
    await s.close()


async def test_a_message_finds_a_dead_worker_before_the_watch_does(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    monkeypatch.setattr(service, "HEALTH_EVERY", 3600)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
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
    assert "error" not in types
    assert "is lost" in (await unbinds(s, p["id"], t["id"]))[0] and len(await binds(s, p["id"], t["id"])) == 2
    await orch.close()
    await provider.close()
    await s.close()


async def test_a_worker_that_does_not_answer_for_a_while_is_kept(tmp_path, monkeypatch):
    """A worker whose runner stops answering (a sprite waking, this machine's
    network down) is not lost while its provider still has it: a thread no
    work came for meanwhile stays on it and carries on once it answers again."""
    import signal

    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    monkeypatch.setattr(service, "HEALTH_EVERY", 0.1)
    monkeypatch.setattr(service, "HEALTH_TIMEOUT", 0.2)
    monkeypatch.setattr(service, "READY_TIMEOUT", 60)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "first")
    await wait_for(lambda: idle_of(s, t["id"]))
    lw = orch.by_thread[t["id"]]
    proc = provider.procs[lw.worker.id]
    proc.send_signal(signal.SIGSTOP)
    await asyncio.sleep(1.5)  # several watches that get no answer
    proc.send_signal(signal.SIGCONT)

    async def back():
        return lw.state == "ready" or None
    await wait_for(back)
    await orch.send(t["id"], "again")

    async def answered():
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: again")
                   for e in await s.list_events(p["id"], t["id"])) or None
    await wait_for(answered)
    assert await unbinds(s, p["id"], t["id"]) == [] and len(await binds(s, p["id"], t["id"])) == 1
    states = [(e["payload"]["from"], e["payload"]["to"]) for e in await s.list_events(p["id"])
              if e["type"] == "worker_state"]
    assert ("ready", "unreachable") in states and states[-1] == ("unreachable", "ready")
    assert orch.by_thread[t["id"]] is lw and orch.workers == [lw]
    await orch.close()
    await provider.close()
    await s.close()


async def restart_runner(provider, w, tmp_path) -> None:
    """Kill a local worker's runner and start the same command again, on the
    same port and disk: what a sprite's service does when it restarts."""
    import os

    from control import providers

    old = provider.procs[w.id]
    old.kill()
    await old.wait()
    env = {**os.environ, "RUNNER_PORT": w.url.rsplit(":", 1)[1], "RUNNER_HOST": "127.0.0.1",
           "RUNNER_TOKEN": w.token, "RUNNER_WORKDIR": str((tmp_path / "w" / w.id).resolve()),
           "RUNNER_CAPACITY": str(w.capacity)}
    with open(provider.log_path(w.id), "ab") as log:
        provider.procs[w.id] = await asyncio.create_subprocess_exec(
            *provider.command(), env=env, cwd=str(providers.Path(providers.__file__).resolve().parents[1]),
            stdout=log, stderr=subprocess.STDOUT)
    await providers.wait_healthy(w.url, timeout=30)


async def test_a_restarted_runner_lets_its_idle_threads_go_and_they_come_back_on_it(tmp_path, monkeypatch):
    """A runner that restarts (it crashed, a sprite woke cold) runs none of
    its threads any more: an idle thread is unbound, and comes back on the
    same worker, in its worktree there, with its next message."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    monkeypatch.setattr(service, "HEALTH_EVERY", 3600)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "write notes.txt: kept")
    await wait_for(lambda: idle_of(s, t["id"]))
    lw = orch.by_thread[t["id"]]
    w = lw.worker
    await restart_runner(provider, w, tmp_path)

    async def restarted():
        return [r for r in await unbinds(s, p["id"], t["id"]) if "runner restarted" in r] or None
    await wait_for(restarted)  # the relay reconnects to the new runner and notices
    th = await s.get_thread(t["id"])
    assert th["state"] == "idle" and th["worker_id"] == w.id and t["id"] not in orch.by_thread
    await orch.send(t["id"], "again")

    async def answered():
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: again")
                   for e in await s.list_events(p["id"], t["id"])) or None
    await wait_for(answered)
    assert [b["worker"] for b in await binds(s, p["id"], t["id"])] == [w.id, w.id]
    assert orch.by_thread[t["id"]] is lw and orch.workers == [lw]
    listing = await orch.list_files(t["id"], "")
    assert "notes.txt" in [e["name"] for e in listing["entries"]]
    await orch.close()
    await provider.close()
    await s.close()


async def test_a_turn_a_runner_restart_cuts_short_resumes_on_that_worker(tmp_path, monkeypatch):
    """A thread mid-turn when its runner restarts is not failed: it is put back
    on the same worker at once, and that runner resumes the turn it cut short
    (runner/API.md, "Interrupted turns") instead of taking the request again."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "30")  # the first turn is still running when the runner dies
    monkeypatch.setattr(service, "HEALTH_EVERY", 3600)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "write notes.txt: never")

    async def working():
        return (await s.get_thread(t["id"]))["state"] == "working" or None
    await wait_for(working)
    w = orch.by_thread[t["id"]].worker
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    await restart_runner(provider, w, tmp_path)

    async def resumed():
        return [e for e in await s.list_events(p["id"], t["id"])
                if e["type"] == "assistant" and "[from runner] Your last turn was cut short" in e["payload"]["content"]]
    await wait_for(resumed)
    await wait_for(lambda: idle_of(s, t["id"]))
    events = await s.list_events(p["id"], t["id"])
    assert [e["payload"]["from"] for e in events if e["type"] == "message_in"] == ["runner"]
    assert not [e for e in events if e["type"] == "assistant" and "write notes" in e["payload"]["content"]]
    assert "error" not in [e["type"] for e in events] and await unbinds(s, p["id"], t["id"]) == []
    moves = [(e["payload"]["from"], e["payload"]["to"]) for e in events if e["type"] == "binding"]
    assert moves == [("unbound", "binding"), ("binding", "bound"), ("bound", "binding"), ("binding", "bound")]
    bound = await binds(s, p["id"], t["id"])
    assert [b["worker"] for b in bound] == [w.id, w.id]
    assert (await prepared(s, p["id"], t["id"]))[-1] == {"resumed": True, "idle": False}
    await orch.close()
    await provider.close()
    await s.close()


async def test_a_turn_under_way_goes_on_after_a_control_plane_restart(tmp_path, monkeypatch):
    """A control plane that takes over a thread mid-turn puts it back on its
    worker at once. Attaching to that worker starts its runner afresh here
    (a local provider's runners are its own), and the runner resumes the cut
    turn; a sprite already running as it would be started is left running,
    and the thread is found again there (test_providers)."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "30")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    first = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    t = await first.spawn(p["id"], "T", "write notes.txt: never")

    async def working():
        return (await s.get_thread(t["id"]))["state"] == "working" or None
    await wait_for(working)
    wid = first.by_thread[t["id"]].worker.id
    await first.close()  # its workers are left running, as a stopped control plane leaves them
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    await orch.open_project(p["id"])

    async def resumed():
        return [e for e in await s.list_events(p["id"], t["id"])
                if e["type"] == "assistant" and "Your last turn was cut short" in e["payload"]["content"]]
    await wait_for(resumed)
    events = await s.list_events(p["id"], t["id"])
    assert "error" not in [e["type"] for e in events] and await unbinds(s, p["id"], t["id"]) == []
    bound = await binds(s, p["id"], t["id"])
    assert [b["worker"] for b in bound] == [wid, wid]
    assert (await prepared(s, p["id"], t["id"]))[-1] == {"resumed": True, "idle": False}
    await orch.close()
    await provider.close()
    await s.close()


async def test_a_worker_unreachable_for_too_long_is_reattached_and_its_thread_comes_back_on_it(tmp_path, monkeypatch):
    """unreachable -> reattaching -> ready: a worker that has not answered for
    READY_TIMEOUT while its provider still has it gets its runner started
    again on the same disk; its threads are unbound (the new runner runs none
    of them) and come back on it, worktree and all, with their next message."""
    import signal

    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    monkeypatch.setattr(service, "HEALTH_EVERY", 0.1)
    monkeypatch.setattr(service, "HEALTH_TIMEOUT", 0.2)
    monkeypatch.setattr(service, "READY_TIMEOUT", 0.5)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "write notes.txt: kept")
    await wait_for(lambda: idle_of(s, t["id"]))
    lw = orch.by_thread[t["id"]]
    provider.procs[lw.worker.id].send_signal(signal.SIGSTOP)

    async def reattached():
        states = [(e["payload"]["from"], e["payload"]["to"]) for e in await s.list_events(p["id"])
                  if e["type"] == "worker_state"]
        return states if ("reattaching", "ready") in states else None
    states = await wait_for(reattached)
    assert states[:3] == [("ready", "unreachable"), ("unreachable", "reattaching"), ("reattaching", "ready")]
    assert lw.state == "ready" and "reattached" in lw.reason
    assert any("runner restarted" in r for r in await unbinds(s, p["id"], t["id"]))
    await orch.send(t["id"], "again")

    async def answered():
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: again")
                   for e in await s.list_events(p["id"], t["id"])) or None
    await wait_for(answered)
    assert orch.by_thread[t["id"]] is lw and orch.workers == [lw]
    assert [b["worker"] for b in await binds(s, p["id"], t["id"])] == [lw.worker.id] * 2
    assert "notes.txt" in [e["name"] for e in (await orch.list_files(t["id"], ""))["entries"]]
    await orch.close()
    await provider.close()
    await s.close()


async def test_a_message_to_a_thread_whose_worker_does_not_answer_moves_it_to_a_ready_worker(tmp_path, monkeypatch):
    """Work never waits for a worker to answer again: a message to a thread
    on an unreachable worker moves the thread to another worker of the pool
    (from its last saved turn in the hub) and is delivered there. Once the
    first worker answers again, the thread is stopped there, and nothing it
    sends for that thread is taken."""
    import signal

    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    monkeypatch.setattr(service, "HEALTH_EVERY", 0.1)
    monkeypatch.setattr(service, "HEALTH_TIMEOUT", 0.2)
    monkeypatch.setattr(service, "READY_TIMEOUT", 60)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos", pool=2)
    t = await orch.spawn(p["id"], "T", "write notes.txt: kept")
    await wait_for(lambda: idle_of(s, t["id"]))
    first = orch.by_thread[t["id"]]
    proc = provider.procs[first.worker.id]
    proc.send_signal(signal.SIGSTOP)
    await orch.send(t["id"], "again")

    async def answered():
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: again")
                   for e in await s.list_events(p["id"], t["id"])) or None
    await wait_for(answered)
    now = orch.by_thread[t["id"]]
    assert now is not first and now.state == "ready" and first.state == "unreachable"
    assert t["id"] in first.moved_off
    assert [b["worker"] for b in await binds(s, p["id"], t["id"])] == [first.worker.id, now.worker.id]
    assert "notes.txt" in [e["name"] for e in (await orch.list_files(t["id"], ""))["entries"]]
    stopped = []
    stop_if_there = first.client.stop_if_there

    async def recording_stop(tid):
        stopped.append(tid)
        await stop_if_there(tid)
    first.client.stop_if_there = recording_stop
    proc.send_signal(signal.SIGCONT)
    async def back():
        return first.state == "ready" or None
    await wait_for(back)
    assert stopped == [t["id"]] and orch.by_thread[t["id"]] is now
    assert not first.unstopped
    # It is stopped there once: the next time that worker stops answering and
    # comes back, the thread is not stopped there again.
    proc.send_signal(signal.SIGSTOP)
    async def gone():
        return first.state == "unreachable" or None
    await wait_for(gone)
    proc.send_signal(signal.SIGCONT)
    await wait_for(back)
    assert stopped == [t["id"]]
    await wait_for(lambda: idle_of(s, t["id"]))
    await orch._ingest({"thread": t["id"], "type": "state", "data": {"state": "stopped"}}, first)
    assert (await s.get_thread(t["id"]))["state"] == "idle"  # w1's copy of it does not count
    await orch.close()
    await provider.close()
    await s.close()


class HangingOnceProvider(LocalProvider):
    """A provider whose first worker never comes up (a sprite stuck booting)."""

    def __init__(self, **kw) -> None:
        super().__init__(**kw)
        self.hung = False

    async def create(self, spec):
        if not self.hung:
            self.hung = True
            await asyncio.Event().wait()
        return await super().create(spec)


async def test_respawn_starts_over_a_thread_stuck_being_put_on_a_worker(tmp_path, monkeypatch):
    """binding -> unbound -> binding -> bound: Respawn calls off a start that
    hangs and starts again; a message sent meanwhile waits for the new start
    and is delivered."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = HangingOnceProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "first")
    assert (await s.get_thread(t["id"]))["binding"] == "binding"
    await orch.send(t["id"], "second")
    await asyncio.sleep(0.2)
    assert (await s.get_thread(t["id"]))["binding"] == "binding"  # stuck
    await orch.respawn(t["id"])

    async def answered():
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: second")
                   for e in await s.list_events(p["id"], t["id"])) or None
    await wait_for(answered)
    moves = [(e["payload"]["from"], e["payload"]["to"]) for e in await s.list_events(p["id"], t["id"])
             if e["type"] == "binding"]
    assert moves == [("unbound", "binding"), ("binding", "unbound"), ("unbound", "binding"), ("binding", "bound")]
    assert "Respawn called off" in (await unbinds(s, p["id"], t["id"]))[0]
    errors = [e["payload"] for e in await s.list_events(p["id"], t["id"]) if e["type"] == "error"]
    assert errors == []
    await orch.close()
    await provider.close()
    await s.close()


async def test_respawn_puts_a_bound_thread_on_its_worker_again(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "first")
    await wait_for(lambda: idle_of(s, t["id"]))
    lw = orch.by_thread[t["id"]]
    await orch.respawn(t["id"])

    async def twice():
        return len(await binds(s, p["id"], t["id"])) == 2 or None
    await wait_for(twice)
    assert orch.by_thread[t["id"]] is lw and (await s.get_thread(t["id"]))["binding"] == "bound"
    await orch.send(t["id"], "again")

    async def answered():
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: again")
                   for e in await s.list_events(p["id"], t["id"])) or None
    await wait_for(answered)
    await orch.close()
    await provider.close()
    await s.close()


async def test_an_idle_thread_of_an_earlier_control_plane_is_unbound_when_its_project_is_opened(tmp_path):
    from control.service import Orchestrator

    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    t = await s.create_thread(p["id"], "T", "x", "local")
    await s.update_thread(t["id"], binding="bound", worker_id=f"{p['id']}-w1", state="idle")
    orch = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)), HeuristicRouter(), tmp_path / "repos")
    await orch.open_project(p["id"])
    row = await s.get_thread(t["id"])
    assert (row["binding"], row["state"], row["worker_id"]) == ("unbound", "idle", f"{p['id']}-w1")
    assert "the control plane that had it on worker" in row["binding_reason"]
    await orch.open_project(p["id"])  # once per control plane
    assert len(await unbinds(s, p["id"], t["id"])) == 1
    await s.close()


def test_every_state_has_a_way_back_and_illegal_moves_raise():
    from control.states import BINDING, WORKER, IllegalTransition, Machine

    for m in (WORKER, BINDING):
        for state in m.states - m.retired:
            assert m.reaches_happy(state), (m.name, state)
    with pytest.raises(IllegalTransition):
        BINDING.check("thread t", "unbound", "bound")
    with pytest.raises(IllegalTransition):
        WORKER.check("worker w", "lost", "ready")
    with pytest.raises(ValueError, match="no path back"):
        Machine("m", "ok", {"ok": {"stuck"}, "stuck": set()})


class FailingOnceProvider(LocalProvider):
    """A provider whose first worker cannot be made (a sprite that would not come up)."""

    def __init__(self, **kw) -> None:
        super().__init__(**kw)
        self.failed = False

    async def create(self, spec):
        if not self.failed:
            self.failed = True
            raise RuntimeError("the sprite did not come up")
        return await super().create(spec)


async def test_respawning_a_thread_that_never_started_gives_it_its_request(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = FailingOnceProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "first")

    async def failed():
        return (await s.get_thread(t["id"]))["state"] == "failed" or None
    await wait_for(failed)
    assert (await s.get_thread(t["id"]))["worker_id"] is None
    await orch.respawn(t["id"])

    async def answered():
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: first")
                   for e in await s.list_events(p["id"], t["id"])) or None
    await wait_for(answered)
    events = await s.list_events(p["id"], t["id"])
    revived = await binds(s, p["id"], t["id"])
    assert len(revived) == 1 and await prepared(s, p["id"], t["id"]) == [{"resumed": False, "idle": False}]
    assert "could not put it on a worker: RuntimeError: the sprite did not come up" in \
        (await unbinds(s, p["id"], t["id"]))[0]
    assert [e for e in events if e["type"] == "message_in"] == []  # the request's echo was expected
    # Once a turn is saved, respawning brings it back idle, its request not run again.
    await wait_for(lambda: idle_of(s, t["id"]))
    await orch.stop(t["id"])
    await wait_for(lambda: idle_or_stopped(s, t["id"]))
    await orch.respawn(t["id"])

    async def twice():
        return len(await binds(s, p["id"], t["id"])) == 2 or None
    await wait_for(twice)
    await wait_for(lambda: idle_of(s, t["id"]))
    echoes = [e for e in await s.list_events(p["id"], t["id"])
              if e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: first")]
    assert len(echoes) == 1
    await orch.close()
    await provider.close()
    await s.close()


async def test_respawning_an_idle_thread_with_no_worker_on_record_brings_it_back(tmp_path, monkeypatch):
    """An idle thread bound to no worker, with nothing saying why (a row from
    an older control plane), comes back with Respawn, its saved turn kept."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "first")
    await wait_for(lambda: idle_of(s, t["id"]))
    lw = orch.by_thread.pop(t["id"])
    lw.threads.discard(t["id"])
    await s.update_thread(t["id"], worker_id=None, binding="unbound", binding_reason="")
    assert f"thread {t['id']} is unbound: nothing recorded yet" in await orch.worker_logs(t["id"], 50)
    await orch.respawn(t["id"])

    async def revived():
        return (await binds(s, p["id"], t["id"]))[1:] or None
    (ev,) = await wait_for(revived)
    # Its runner still ran it: the control plane only lost track of where.
    assert ev == {"from": "binding", "to": "bound", "worker": lw.worker.id,
                  "reason": f"found again on worker {lw.worker.id}, which still runs it"}
    assert (await s.get_thread(t["id"]))["worker_id"] == lw.worker.id and orch.by_thread[t["id"]] is lw
    await orch.send(t["id"], "again")

    async def answered():
        return any(e["type"] == "assistant" and e["payload"]["content"].startswith("Echo: again")
                   for e in await s.list_events(p["id"], t["id"])) or None
    await wait_for(answered)
    await orch.close()
    await provider.close()
    await s.close()


async def idle_or_stopped(s, tid):
    return (await s.get_thread(tid))["state"] in ("idle", "stopped") or None


async def test_a_stopped_thread_takes_its_next_message(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    orch = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)), HeuristicRouter(), tmp_path / "repos")
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
    assert [b["worker"] for b in await binds(s, p["id"], t["id"])][1:] == [ran_on]
    await orch.close()
    await orch.places.close()
    await s.close()


async def test_a_thread_keeps_its_uncommitted_files_across_a_control_plane_restart(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    first = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)), HeuristicRouter(), tmp_path / "repos")
    t = await first.spawn(p["id"], "T", "write build/app: binary")

    async def idle():
        return (await s.get_thread(t["id"]))["state"] == "idle" or None
    await wait_for(idle)
    ran_on = (await s.get_thread(t["id"]))["worker_id"]
    token = first.by_thread[t["id"]].worker.token
    await first.close()
    await first.places.close()

    # The next control plane attaches to the same worker, files and all.
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    second = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    assert "devrunner: echo runner on port" in await second.worker_logs(t["id"], 50)  # before it is attached
    # Reading its files waits for nothing and binds nothing: it says where
    # the thread stands; Respawn puts it back on its worker.
    with pytest.raises(NotOnWorker, match="on no worker"):
        await second.list_files(t["id"], "build")
    await second.respawn(t["id"])
    await wait_for(lambda: bound_of(s, t["id"]))
    listing = await second.list_files(t["id"], "build")
    assert [e["name"] for e in listing["entries"]] == ["app"]
    assert [b["worker"] for b in await binds(s, p["id"], t["id"])][1:] == [ran_on]
    assert second.by_thread[t["id"]].worker.token == token  # a runner still going answers to it
    # A new thread goes to that worker too rather than a new one.
    u = await second.spawn(p["id"], "U", "hello")
    await wait_for(lambda: idle_of(s, u["id"]))
    assert (await s.get_thread(u["id"]))["worker_id"] == ran_on
    await second.close()
    await provider.close()
    await s.close()


async def test_threads_on_a_projects_own_worker_from_before_the_pool_come_back_on_the_pool(tmp_path, monkeypatch):
    """Each project had its own workers before they were a pool: starting
    destroys what the provider still has of them, the store says they are
    lost, and their threads come back on the pool's first worker."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    old = f"{p['id']}-w1"
    await s.upsert_worker({"id": old, "provider": "local", "handle": "1", "url": "http://127.0.0.1:1",
                           "token": "x", "index": 1})
    await s._exec(s._q("UPDATE workers SET project_id = ? WHERE id = ?"), p["id"], old)
    (tmp_path / "w" / old).mkdir(parents=True)  # its disk
    ts = [await s.create_thread(p["id"], title, "", "local") for title in ("A", "B")]
    for t in ts:
        await s.update_thread(t["id"], worker_id=old, state="idle")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    await orch.start()
    await asyncio.gather(*orch.sweeping)  # in the background: starting never waits for it
    assert not (tmp_path / "w" / old).exists()
    row = {w["id"]: w for w in await s.list_workers()}[old]
    assert row["lost_at"] is not None and "pool of 3" in row["lost_reason"]
    lws = [await orch.live_worker(t["id"]) for t in ts]
    assert lws[0] is lws[1] and orch.workers == [lws[0]] and lws[0].worker.id == "w1"
    await orch.close()
    await provider.close()
    await s.close()


async def bound_of(s, tid):
    return (await s.get_thread(tid))["binding"] == "bound" or None


async def idle_of(s, tid):
    return (await s.get_thread(tid))["state"] == "idle" or None


def ids_ok(items):
    return all(re.fullmatch(r"o_[0-9a-f]{8}", o["id"]) for o in items)


def without_ids(items):
    return [{k: v for k, v in o.items() if k != "id"} for o in items]


async def test_a_thread_owns_the_objectives_the_orchestrator_gives_it_and_is_archived_when_done(tmp_path, monkeypatch):
    """The orchestrator makes the objectives a thread is spawned with (ids,
    pending) and posts the thread with them; its runner says them as they
    move; the store keeps them and the UI's thread shows them. Once a turn
    ends with all done its runner ends it `done`, and the control plane
    archives it: it takes no more messages. Unarchived, it comes back with
    its objectives as they were, and a message that is new work brings the
    objectives the router gave it."""
    from control.service import ArchivedError, Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    orch = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)),
                        HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "objective 0 done: off by one", objectives=["find the bug", "fix it"])
    assert ids_ok(t["objectives"]) and [o["status"] for o in t["objectives"]] == ["pending", "pending"]
    await wait_for(lambda: idle_of(s, t["id"]))
    row = await s.get_thread(t["id"])
    assert [o["id"] for o in row["objectives"]] == [o["id"] for o in t["objectives"]]
    assert without_ids(row["objectives"]) == [{"text": "find the bug", "status": "done", "note": "off by one"},
                                              {"text": "fix it", "status": "pending", "note": ""}]
    assert row["archived_at"] is None
    assert app_mod.thread_view(await orch.with_load(row), orch)["objectives"] == row["objectives"]
    shown = [app_mod.display(e) for e in await s.list_events(p["id"], t["id"]) if e["type"] == "objectives"]
    assert [(e["kind"], e["text"]) for e in shown] == [(app_mod.K_STATE, "1 of 2 done")]

    await orch.send(t["id"], "objective 1 done: fixed")  # assessed: it steers, bringing no objectives

    async def archived():
        row = await s.get_thread(t["id"])
        return row if row["archived_at"] is not None else None
    row = await wait_for(archived)
    assert row["state"] == "done" and [o["status"] for o in row["objectives"]] == ["done", "done"]
    ev = [e for e in await s.list_events(p["id"], t["id"]) if e["type"] == "archived"][-1]
    assert app_mod.display(ev)["text"] == "thread archived: it accomplished all its objectives"
    assert [x["id"] for x in await s.list_threads(p["id"], archived=True)] == [t["id"]]
    with pytest.raises(ArchivedError):
        await orch.send(t["id"], "one more thing")

    await orch.unarchive(t["id"])
    await orch.send(t["id"], "new job: objectives: tidy up")
    await wait_for(lambda: idle_of(s, t["id"]))  # posted with its objectives, then given the new one

    async def three():
        row = await s.get_thread(t["id"])
        return row if len(row["objectives"]) == 3 else None
    row = await wait_for(three)
    assert row["archived_at"] is None and ids_ok(row["objectives"])
    assert [(o["text"], o["status"]) for o in row["objectives"]] == \
        [("find the bug", "done"), ("fix it", "done"), ("tidy up", "pending")]
    await orch.close()
    await orch.places.close()
    await s.close()


async def test_the_router_gives_a_thread_nobody_planned_objectives_for_them(tmp_path, monkeypatch):
    """A thread spawned with no objectives (forced new, by another thread) is
    given them by the router as it is first put on a worker, before it is
    posted; a request the router plans spawns threads with the objectives
    it planned."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    orch = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)),
                        HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "objectives: build it | ship it")
    assert t["objectives"] == []
    await wait_for(lambda: idle_of(s, t["id"]))
    row = await s.get_thread(t["id"])
    assert ids_ok(row["objectives"]) and [o["text"] for o in row["objectives"]] == ["build it", "ship it"]
    given = [e["payload"] for e in await s.list_events(p["id"], t["id"]) if e["type"] == "objectives"][0]
    assert given["objectives"] == row["objectives"] and given["why"] == "what its request asks"

    routed = await orch.handle_request(p["id"], "- write docs\n- add tests")
    rows = [await s.get_thread(x["id"]) for x in routed["spawned"]]
    assert [[o["text"] for o in r["objectives"]] for r in rows] == [["write docs"], ["add tests"]]
    await orch.close()
    await orch.places.close()
    await s.close()


async def test_objectives_from_before_ids_get_them(tmp_path):
    path = str(tmp_path / "db.sqlite")
    s = SqliteStore(path)
    await s.open()
    p = await s.create_project("demo")
    t = await s.create_thread(p["id"], "T", "x", "local")
    await s.update_thread(t["id"], objectives=[{"text": "old", "status": "done", "note": ""}])
    await s.close()
    s = SqliteStore(path)
    await s.open()
    got = (await s.get_thread(t["id"]))["objectives"]
    assert ids_ok(got) and without_ids(got) == [{"text": "old", "status": "done", "note": ""}]
    await s.close()


def test_only_what_the_person_typed_is_a_user_row():
    def kind(t, d):
        return app_mod.display({"type": t, "payload": d})["kind"]
    assert kind("request", {"text": "zip it"}) == app_mod.K_USER
    assert kind("message_in", {"from": "user", "body": "more"}) == app_mod.K_USER
    assert kind("created", {"request": "Create a zip archive"}) == app_mod.K_REQUEST
    assert kind("message_in", {"from": "router", "body": "x"}) == app_mod.K_PEER


async def test_a_message_between_threads_is_one_event_both_list(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    orch = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)), HeuristicRouter(), tmp_path / "repos")
    a = await orch.spawn(p["id"], "A", "first")
    b = await orch.spawn(p["id"], "B", "second")
    await orch.send(b["id"], "hello b", a["id"], 1)
    sent = [e for e in await s.list_events(p["id"]) if e["payload"].get("body") == "hello b"]
    assert [(e["type"], e["thread_id"], e["peer_id"]) for e in sent] == [("message_in", b["id"], a["id"])]
    for tid in (a["id"], b["id"]):
        assert [e["seq"] for e in await s.list_events(p["id"], tid) if e["payload"].get("body") == "hello b"] \
            == [sent[0]["seq"]]
        assert [e["seq"] for e in await s.recent_events(p["id"], tid, limit=50)
                if e["payload"].get("body") == "hello b"] == [sent[0]["seq"]]
    row = app_mod.display(sent[0])
    assert (row["kind"], row["from"], row["label"]) == (app_mod.K_PEER, a["id"], f"{a['id']} → {b['id']}")
    await orch.close()
    await orch.places.close()
    await s.close()


async def test_old_two_event_peer_messages_merge_into_one(tmp_path):
    import aiosqlite

    path = str(tmp_path / "db.sqlite")
    s = SqliteStore(path)
    await s.open()
    p = await s.create_project("demo")
    a = await s.create_thread(p["id"], "A", "", "local")
    b = await s.create_thread(p["id"], "B", "", "local")
    await s.close()
    # A log from before events had peer_id: the message is logged on both sides.
    async with aiosqlite.connect(path) as db:
        await db.execute("ALTER TABLE events DROP COLUMN peer_id")
        for tid, typ, payload in ((a["id"], "message_out", {"to": b["id"], "body": "hi"}),
                                  (b["id"], "message_in", {"from": a["id"], "body": "hi"}),
                                  (b["id"], "message_in", {"from": "user", "body": "yo"})):
            await db.execute("INSERT INTO events (project_id, thread_id, type, payload, created_at) "
                             "VALUES (?, ?, ?, ?, 0)", (p["id"], tid, typ, json.dumps(payload)))
        await db.commit()
    s = SqliteStore(path)
    await s.open()
    got = [(e["type"], e["thread_id"], e["peer_id"], e["payload"]["body"]) for e in await s.list_events(p["id"])]
    assert got == [("message_in", b["id"], a["id"], "hi"), ("message_in", b["id"], None, "yo")]
    assert [e["payload"]["body"] for e in await s.list_events(p["id"], a["id"])] == ["hi"]
    await s.close()


async def test_routed_messages_are_from_the_router(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    orch = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)), HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "first")
    await orch.handle_request(p["id"], f"@{t['id']} more", "auto")
    await orch.handle_request(p["id"], "direct", "thread", t["id"])
    got = [(e["payload"]["from"], app_mod.display(e)["label"]) for e in await s.list_events(p["id"], t["id"])
           if e["type"] == "message_in"]
    assert got == [("router", f"router → {t['id']}"), ("user", "You")]
    await orch.close()
    await orch.places.close()
    await s.close()


async def test_a_quick_question_the_router_answers_starts_no_thread(tmp_path):
    """The router's own answer is the route: no thread is spawned and no
    worker is put to work, and the answer shows as markdown like a reply."""
    from control.service import Orchestrator

    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    got = await orch.handle_request(p["id"], "question: what is running?", "auto")
    assert got["answer"] == "Answer: what is running?" and got["spawned"] == [] and got["sent"] == []
    assert got["summary"] == "answered"
    assert await s.list_threads(p["id"]) == [] and orch.by_thread == {} and orch.binding == {}
    route = next(e for e in await s.list_events(p["id"]) if e["type"] == "route")
    shown = app_mod.display(route)
    assert shown["kind"] == app_mod.K_ANSWER and shown["text"] == "Answer: what is running?"
    # An answer beside work done says what went to threads under it.
    both = app_mod.display({"type": "route", "payload": {"answer": "It is idle.", "summary": "answered, started “X”",
                                                        "spawned": [{"id": "t_1", "title": "X"}], "sent": []}})
    assert both["text"] == "It is idle.\n\n*answered, started “X”*"
    await orch.close()
    await orch.places.close()
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
    t = await a.create_thread(p["id"], "T", "req", "local")
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
    await a.update_project(p["id"], archived_at=6.0)
    assert await b.list_projects() == []
    assert [x["archived_at"] for x in await b.list_projects(archived=True)] == [6.0]
    await a.update_project(p["id"], archived_at=None)
    assert [x["id"] for x in await b.list_projects()] == [p["id"]]
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
    plan = await r.revise("- fix login\n- fix export", "- fix login\n- fix signup", ["t_a"], [], HERE)
    assert [s["request"] for s in plan["spawn"]] == ["fix export"] and not plan["send"]
    plan = await r.revise("fix login, not signup", "fix signup", ["t_a"], [], HERE)
    assert not plan["spawn"] and not plan["send"]
    # An edit that reached no thread is planned like a new request.
    plan = await r.revise("fix login", "fix signup", [], [], HERE)
    assert [s["request"] for s in plan["spawn"]] == ["fix login"]


async def test_editing_a_message_corrects_the_threads_it_reached(tmp_path, monkeypatch):
    """An edited request shows its new text, every thread it reached gets a
    correction from the person, and the router plans only what is new."""
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.05")
    router = GatedRouter()
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)), router,
                     repos=str(tmp_path / "repos"))
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


async def test_a_reply_to_a_selection_names_where_its_quote_came_from(tmp_path, monkeypatch):
    """A reply to selected text records the event the quote was selected from,
    with its thread, so the UI can go back to it; a quote of no event in the
    project is refused."""
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)), GatedRouter(),
                     repos=str(tmp_path / "repos"))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]
            tid = (await c.post(f"/api/projects/{pid}/requests",
                                json={"text": "say IFn.invoke", "mode": "new"})).json()["spawned"][0]["id"]
            tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
            src = tr[0]["seq"]

            # Sent straight to the thread: its message carries the quote's source.
            r = await c.post(f"/api/projects/{pid}/requests", json={
                "text": "> IFn.invoke\n\nwhat does this do?", "mode": "thread", "target": tid, "quote_of": src})
            assert r.status_code == 200
            tr = (await c.get(f"/api/threads/{tid}/transcript")).json()
            mine = [e for e in tr if e["type"] == "message_in" and e["text"].startswith("> IFn.invoke")][0]
            assert (mine["quote_of"], mine["quote_thread"]) == (src, tid)

            # Left to the router: the request in the project's log carries it.
            seq = (await c.post(f"/api/projects/{pid}/requests",
                                json={"text": "> IFn.invoke\n\nand this?", "quote_of": src})).json()["seq"]
            log = (await c.get(f"/api/projects/{pid}/log")).json()
            req = [e for e in log if e["seq"] == seq][0]
            assert (req["quote_of"], req["quote_thread"]) == (src, tid)

            # A message without a quote has none; a quote of nothing is refused.
            assert all("quote_of" not in e for e in log if e["type"] == "created")
            r = await c.post(f"/api/projects/{pid}/requests", json={"text": "x", "quote_of": 999999})
            assert r.status_code == 404
            r = await c.post(f"/api/projects/{pid}/requests", json={"text": "x", "quote_of": "7"})
            assert r.status_code == 400


async def test_worker_logs_go_to_stdout_and_a_lost_worker_says_why(tmp_path, monkeypatch, capsys):
    """Each worker's runner log is printed as it is written, tagged with the
    worker. Once the worker is lost, its thread's worker log says which worker
    it was, why it was dropped, and how its log ended."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    monkeypatch.setattr(service, "HEALTH_EVERY", 0.2)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    try:
        t = await orch.spawn(p["id"], "Echo", "say hi")

        async def idle():
            return (await s.get_thread(t["id"]))["state"] == "idle" or None
        await wait_for(idle)
        lw = orch.by_thread[t["id"]]
        wid = lw.worker.id

        async def printed():
            return f"{wid} | devrunner: echo runner on port" in capsys.readouterr().out or None
        await wait_for(printed)
        provider.procs[wid].kill()

        async def lost():
            return (await s.get_thread(t["id"]))["binding"] == "unbound" or None
        await wait_for(lost)
        text = await orch.worker_logs(t["id"], 50)
        assert f"thread {t['id']} is unbound: worker {wid} is lost" in text and f"worker {wid} is lost:" in text
        assert "provider no longer has it" in text
        assert "devrunner: echo runner on port" in text  # its log, as it was followed
    finally:
        await orch.close()
        await provider.close()
        await s.close()


async def test_a_thread_that_never_started_says_why(tmp_path):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")

    class Broken(LocalProvider):
        async def create(self, spec):
            raise RuntimeError("no capacity on sprites")

    provider = Broken(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = service.Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    try:
        t = await orch.spawn(p["id"], "Echo", "say hi")
        await asyncio.wait({orch.binding[t["id"]]})
        text = await orch.worker_logs(t["id"], 50)
        assert f"thread {t['id']} is unbound: could not put it on a worker" in text
        assert "no capacity on sprites" in text and "It has not been on a worker" in text
    finally:
        await orch.close()
        await s.close()


def test_a_threads_load_comes_to_one_level():
    from control.load import load_of

    t = {"state": "idle", "binding": "bound", "busy_since": None}
    assert load_of(t, "ready", 100.0) == {"level": "idle", "busy_for": 0}
    # Just handed a job: it is on it before the store shows its turn.
    assert load_of(t, "ready", 100.0, taken=True)["level"] == "busy"
    working = {**t, "state": "working", "busy_since": 40.0}
    assert load_of(working, "ready", 100.0) == {"level": "busy", "busy_for": 60}
    # Being put on a worker counts as busy; a worker that is away stalls it.
    assert load_of({**t, "binding": "binding"}, "", 100.0)["level"] == "busy"
    assert load_of(working, "unreachable", 100.0)["level"] == "stalled"
    assert load_of(t, "reattaching", 100.0)["level"] == "stalled"


async def test_store_keeps_when_a_turn_began(tmp_path, monkeypatch):
    from control import store as store_mod

    clock = iter(range(1, 1000))
    monkeypatch.setattr(store_mod, "now", lambda: float(next(clock)))
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    t = await s.create_thread(p["id"], "T", "do it", "local")
    assert t["busy_since"] is not None  # a new thread is queued: its first turn has begun
    idle = await s.update_thread(t["id"], state="idle")
    assert idle["busy_since"] is None
    began = (await s.update_thread(t["id"], state="queued"))["busy_since"]
    assert (await s.update_thread(t["id"], state="working"))["busy_since"] == began  # the same turn
    assert (await s.update_thread(t["id"], state="idle"))["busy_since"] is None
    await s.close()


class RecordingRouter(HeuristicRouter):
    """The heuristic router, keeping the directory each plan was given."""

    def __init__(self) -> None:
        self.directories: list[list[dict]] = []

    async def plan(self, request, directory, places, images=0):
        self.directories.append(directory)
        return await super().plan(request, directory, places, images)


async def test_the_router_sees_how_loaded_each_thread_is(tmp_path, monkeypatch):
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "2")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    router = RecordingRouter()
    orch = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)), router, tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "first")

    async def working():
        return (await s.get_thread(t["id"]))["state"] == "working"
    await wait_for(working)
    await orch.handle_request(p["id"], "something else", "auto")
    seen = {d["id"]: d["load"] for d in router.directories[-1]}[t["id"]]
    assert seen["level"] == "busy" and seen["busy_for"] >= 0
    new = next(d for d in await orch.directory(p["id"]) if d["id"] != t["id"])
    assert new["load"]["level"] == "busy"  # a new thread starts on its request

    async def idle():
        load = {d["id"]: d["load"] for d in await orch.directory(p["id"])}[t["id"]]
        return load == {"level": "idle", "busy_for": 0}
    await wait_for(idle, timeout=30)
    await orch.close()
    await orch.places.close()
    await s.close()


async def test_a_busy_thread_is_steered_but_new_work_for_it_goes_to_a_new_thread(tmp_path, monkeypatch):
    """One job per thread, nothing queued: a message about its job reaches a
    busy thread's turn; new work sent to it (the router says which) starts a
    new thread where it runs, with its job as context, and its log says so."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "1.5")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    orch = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)),
                        HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "first")

    async def working():
        return (await s.get_thread(t["id"]))["state"] == "working" or None
    await wait_for(working)
    await orch.send(t["id"], "use tabs, not spaces")
    await orch.send(t["id"], "new job: write the docs")

    async def handed():
        return next((e for e in await s.list_events(p["id"], t["id"]) if e["type"] == "handed_off"), None)
    ev = await wait_for(handed)
    new = await s.get_thread(ev["payload"]["to"])
    assert new["workers"] == "local" and new["parent_id"] is None
    assert new["request"].startswith("new job: write the docs") and "“T”" in new["request"]
    await wait_for(lambda: idle_of(s, t["id"]))
    bodies = [e["payload"]["body"] for e in await s.list_events(p["id"], t["id"]) if e["type"] == "message_in"]
    assert "use tabs, not spaces" in bodies
    echoes = [e["payload"]["content"] for e in await s.list_events(p["id"], t["id"]) if e["type"] == "assistant"]
    assert not any("write the docs" in c for c in echoes)
    await orch.close()
    await orch.places.close()
    await s.close()


async def test_a_full_pool_puts_a_new_thread_on_the_least_loaded_worker_at_once(tmp_path, monkeypatch):
    """The pool never holds more than its slots, and a thread never waits for
    room: with every worker full, a new thread goes on the least loaded one
    at once (CAPACITY only spreads threads) and says so in its Worker log."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "1.0")
    monkeypatch.setattr(service, "CAPACITY", 1)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    made = []
    create = provider.create

    async def recording_create(spec):
        made.append(spec.index)
        return await create(spec)
    provider.create = recording_create
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos", pool=1)
    first = await orch.spawn(p["id"], "First", "one")

    async def working():
        return (await s.get_thread(first["id"]))["state"] == "working" or None
    await wait_for(working)
    second = await orch.spawn(p["id"], "Second", "two")

    async def bound():
        return (await s.get_thread(second["id"]))["binding"] == "bound" or None
    await wait_for(bound)
    assert (await s.get_thread(first["id"]))["state"] == "working"  # it went on beside it, not after it
    assert made == [1] and [lw.worker.id for lw in orch.workers] == ["w1"]
    assert (await s.get_thread(second["id"]))["worker_id"] == "w1"
    assert "it goes on the least loaded one" in await orch.worker_logs(second["id"])
    await orch.close()
    await provider.close()
    await s.close()


async def test_projects_share_a_worker_and_see_only_their_own_threads(tmp_path, monkeypatch):
    """Two projects' threads on one worker: each thread's peer calls are
    answered within its own project, and a call made as another thread is refused."""
    from control.service import Orchestrator, PeerCallError

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    a, b = await s.create_project("a"), await s.create_project("b")
    provider = LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos")
    ta = await orch.spawn(a["id"], "Mine", "first")
    tb = await orch.spawn(b["id"], "Theirs", "second")
    await wait_for(lambda: idle_of(s, ta["id"]))
    await wait_for(lambda: idle_of(s, tb["id"]))
    assert orch.by_thread[ta["id"]] is orch.by_thread[tb["id"]] and len(orch.workers) == 1
    await orch.send(ta["id"], f"list: \nread {tb['id']}")

    async def results():
        got = {e["payload"]["name"]: e["payload"]["result"] for e in await s.list_events(a["id"], ta["id"])
               if e["type"] == "tool_result"}
        return got if {"list_threads", "read_thread"} <= set(got) else None
    got = await wait_for(results)
    assert [t["id"] for t in json.loads(got["list_threads"])] == [ta["id"]]  # its own project's only
    assert json.loads(got["read_thread"]) == {"error": f"no thread {tb['id']}"}
    lw = orch.by_thread[ta["id"]]
    caller = await orch._caller(lw, ta["id"])
    with pytest.raises(PeerCallError, match="cannot act as"):
        await orch.peer_set_summary(lw, caller, tb["id"], "not mine")
    with pytest.raises(PeerCallError, match="is not on worker"):
        await orch._caller(lw, "t_nope")
    await orch.close()
    await provider.close()
    await s.close()


async def test_a_worker_slow_to_come_up_holds_up_no_thread(tmp_path, monkeypatch):
    """Making a worker (a sprite installing) takes no lock and keeps no
    thread waiting while a worker is ready: with the first worker full, a
    new thread goes on it at once and the second slot's worker is made in
    the background, its steps in the Worker log, for the threads after it."""
    from control.service import Orchestrator

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.3")
    monkeypatch.setattr(service, "CAPACITY", 1)
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")

    class SlowSecondSlot(LocalProvider):
        gate = asyncio.Event()

        async def create(self, spec):
            if spec.index == 2:
                await self.gate.wait()
            return await super().create(spec)

    provider = SlowSecondSlot(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)
    orch = Orchestrator(s, local(provider), HeuristicRouter(), tmp_path / "repos", pool=2)
    a = await orch.spawn(p["id"], "A", "one")

    async def working():
        return (await s.get_thread(a["id"]))["state"] == "working" or None
    await wait_for(working)
    b = await orch.spawn(p["id"], "B", "two")
    await wait_for(lambda: idle_of(s, b["id"]))
    assert (await s.get_thread(b["id"]))["worker_id"] == "w1"
    text = await orch.worker_logs(b["id"])
    assert "it goes on the least loaded one while slot 2 is filled" in text
    assert ("local", 2) in orch.attaching and [lw.worker.id for lw in orch.workers] == ["w1"]

    provider.gate.set()

    async def filled():
        return [lw.worker.id for lw in orch.workers] == ["w1", "w2"] or None
    await wait_for(filled)
    assert "attached at http://" in orch.attach_steps[("local", 2)].text()
    await orch.close()
    await provider.close()
    await s.close()


async def test_reading_files_never_waits_for_a_binding(tmp_path, monkeypatch):
    """The Files tab of a thread still being put on a worker answers at once
    (409, with where it stands) instead of holding the request open."""
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    provider = GatedProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")), local(provider), HeuristicRouter(),
                     repos=str(tmp_path / "repos"))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]
            r = await c.post(f"/api/projects/{pid}/requests", json={"text": "hello", "mode": "new"})
            tid = r.json()["spawned"][0]["id"]
            t0 = time.perf_counter()
            r = await c.get(f"/api/threads/{tid}/files?path=")
            assert time.perf_counter() - t0 < 1.0
            assert r.status_code == 409 and "being put on a worker" in r.json()["error"]
            provider.gate.set()

            async def listed():
                r = await c.get(f"/api/threads/{tid}/files?path=")
                return r.status_code == 200 or None
            await wait_for(listed)


class StandInSprites(LocalProvider):
    """Runs echo runners on this machine under the Sprites place's name, so a
    test can have two places without the Sprites API."""
    name = "sprites"

    @staticmethod
    def worker_name(index: int, pool: str = "") -> str:
        return f"s{index}"


async def test_the_router_puts_a_thread_where_its_request_says(tmp_path):
    """Threads go where the router puts them (the heuristic router: Sprites
    unless the request says to run locally), with its reason; a place this
    control plane runs no workers in is never picked, and saying so to an
    existing thread moves it, with the reason in its `moved` event."""
    from control.providers import Places

    places = Places({"local": LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER),
                     "sprites": StandInSprites(workdir=str(tmp_path / "s"), command=ECHO_RUNNER)})
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")), places, HeuristicRouter(),
                     repos=str(tmp_path / "repos"))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t",
                                     timeout=20) as c:
            offered = (await c.get("/api/places")).json()
            assert [p["id"] for p in offered] == ["local", "sprites"]
            assert all(p["reaches"] for p in offered)  # what the router places threads by
            pid = (await c.post("/api/projects", json={"name": "demo"})).json()["id"]
            cloud = (await c.post(f"/api/projects/{pid}/requests",
                                  json={"text": "add a login page", "mode": "new"})).json()["spawned"][0]["id"]
            here = (await c.post(f"/api/projects/{pid}/requests",
                                 json={"text": "fix the docs, run it locally", "mode": "auto"})).json()

            async def bound(tid):
                t = (await c.get(f"/api/threads/{tid}")).json()
                return t if t["binding"] == "bound" else None
            assert (await wait_for(lambda: bound(cloud)))["worker_id"] == "s1"

            async def routed():
                return [e for e in (await c.get(f"/api/projects/{pid}/log")).json()
                        if e.get("type") == "route" and e["payload"].get("spawned")
                        and e["payload"]["mode"] == "auto"] or None
            local_tid = (await wait_for(routed))[0]["payload"]["spawned"][0]["id"]
            t = await wait_for(lambda: bound(local_tid))
            assert (t["workers"], t["worker_id"], t["workers_why"]) == ("local", "w1", "the request says where")

            # "run locally" in a request that goes to the cloud thread moves it.
            await c.post(f"/api/projects/{pid}/requests",
                         json={"text": f"@{cloud} run this locally too", "mode": "auto"})

            async def moved():
                t = (await c.get(f"/api/threads/{cloud}")).json()
                return t if t["worker_id"] == "w1" and t["binding"] == "bound" else None
            assert (await wait_for(moved))["workers"] == "local"
            log = (await c.get(f"/api/projects/{pid}/log")).json()
            assert [e["payload"] for e in log if e["type"] == "moved"] == [
                {"from": "sprites", "to": "local", "why": "the request says where"}]


async def test_sprites_without_a_token_is_never_picked(tmp_path):
    from control.service import Orchestrator

    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo")
    orch = Orchestrator(s, local(LocalProvider(workdir=str(tmp_path / "w"), command=ECHO_RUNNER)),
                        HeuristicRouter(), tmp_path / "repos")
    t = await orch.spawn(p["id"], "T", "do it")
    await wait_for(lambda: idle_of(s, t["id"]))
    assert (await s.get_thread(t["id"]))["workers"] == "local"
    with pytest.raises(PlaceRefused, match="cannot run on Sprites"):
        await orch.spawn(p["id"], "U", "do it", workers="sprites")
    with pytest.raises(PlaceRefused, match="cannot run on Sprites"):
        await orch.move(t["id"], "sprites")
    await orch.close()
    await orch.places.close()
    await s.close()


async def test_a_store_names_its_pool_once(tmp_path):
    """Each control plane's workers are its own pool, named by its store:
    the same name every time it is asked, across restarts."""
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    name = await s.pool_id()
    assert re.fullmatch(r"[0-9a-f]{8}", name) and await s.pool_id() == name
    await s.close()
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    assert await s.pool_id() == name
    await s.close()
    other = SqliteStore(str(tmp_path / "other.sqlite"))
    await other.open()
    assert await other.pool_id() != name
    await other.close()


async def test_app_settings_show_keys_and_keep_them_in_the_keyring(tmp_path, monkeypatch):
    """The project list's Settings: each key with where it is found, kept in
    the keyring from there, except one the environment sets (it wins)."""
    import keyring
    from keyring.backend import KeyringBackend
    from control import secrets

    class Memory(KeyringBackend):
        priority = 1

        def __init__(self):
            super().__init__()
            self.kept = {}

        def get_password(self, service, username):
            return self.kept.get((service, username))

        def set_password(self, service, username, password):
            self.kept[(service, username)] = password

        def delete_password(self, service, username):
            from keyring.errors import PasswordDeleteError
            if self.kept.pop((service, username), None) is None:
                raise PasswordDeleteError(username)

    memory = Memory()
    before = keyring.get_keyring()
    keyring.set_keyring(memory)
    try:
        monkeypatch.setattr(secrets, "_COPIED", set())
        monkeypatch.setattr(secrets, "_from_keyring_label", lambda n: None)
        monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-env")
        monkeypatch.delenv("SPRITES_TOKEN", raising=False)
        app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                         local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)),
                         HeuristicRouter(), repos=str(tmp_path / "repos"))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
                got = (await c.get("/api/settings")).json()
                keys = {k["name"]: k["source"] for k in got["keys"]}
                assert keys["DEEPSEEK_API_KEY"] == "environment" and keys["SPRITES_TOKEN"] == ""
                assert got["sync"] is None and got["keyring"].endswith("Memory")

                r = await c.post("/api/settings/keys", json={"name": "SPRITES_TOKEN", "value": " tok "})
                assert r.status_code == 200
                assert {k["name"]: k["source"] for k in r.json()["keys"]}["SPRITES_TOKEN"] == "keyring"
                assert memory.kept[("orchestrate", "SPRITES_TOKEN")] == "tok"

                r = await c.post("/api/settings/keys", json={"name": "DEEPSEEK_API_KEY", "value": "x"})
                assert r.status_code == 400 and "environment" in r.json()["error"]
                r = await c.post("/api/settings/keys", json={"name": "NOPE", "value": "x"})
                assert r.status_code == 400

                r = await c.post("/api/settings/keys", json={"name": "SPRITES_TOKEN", "value": ""})
                assert r.status_code == 200 and ("orchestrate", "SPRITES_TOKEN") not in memory.kept
                assert (await c.post("/api/settings/keys", json={"name": "SPRITES_TOKEN", "value": ""})).status_code == 200
    finally:
        keyring.set_keyring(before)


async def test_ship_sends_a_projects_hub_to_a_worker_once_at_a_time():
    """Two threads bound to one worker together do not each send it the whole hub:
    the second asks what the worker has once the first's bundle landed."""
    landed: list[str] = []
    sent: list[list[str]] = []

    class Client:
        async def repo_refs(self, pid):
            return {f"refs/remotes/{c}": c for c in landed}

        async def push_repo(self, pid, data, base, refs):
            await asyncio.sleep(0.05)  # the upload takes a while
            landed.append("base")

    class Hub:
        base = "main"

        async def sync(self, steps):
            pass

        async def pack(self, tid, have):
            sent.append(list(have))
            return (b"" if have else b"x" * 10), {}

    async def hub(project):
        return Hub()

    lw = service.LiveWorker(types.SimpleNamespace(id="w1"), Client())
    me = types.SimpleNamespace(hub=hub)
    await asyncio.gather(*(service.Orchestrator._ship(me, lw, {"id": "p_1"}, tid, service.Steps(tid))
                           for tid in ("t_1", "t_2")))
    assert sent == [[], ["base"]]
