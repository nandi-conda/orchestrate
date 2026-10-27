"""Two control planes sharing projects through the sync server (control/sync.py),
served locally by wrangler from ../sync with its own Durable Object and R2
storage, and the echo runner as both computers' workers."""

import asyncio
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import httpx
import pytest

from control import sync as sync_mod
from control.providers import LocalProvider
from control.service import Orchestrator
from control.store import SqliteStore
from control.sync import Sync, SyncedStore
from tests.doubles import ECHO_RUNNER, HeuristicRouter, local

pytestmark = pytest.mark.anyio

SYNC_DIR = Path(__file__).resolve().parents[2] / "sync"
SPACE = "5" * 64


@pytest.fixture
def anyio_backend():
    return "asyncio"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def sync_server(tmp_path_factory):
    """The sync server under `wrangler dev`, its state in a fresh directory."""
    if not (SYNC_DIR / "node_modules").is_dir():
        subprocess.run(["npm", "ci"], cwd=SYNC_DIR, check=True, capture_output=True)
    port = free_port()
    state = tmp_path_factory.mktemp("sync-state")
    log = open(state / "wrangler.log", "w")
    proc = subprocess.Popen(
        ["npx", "wrangler", "dev", "--local", "--port", str(port), "--ip", "127.0.0.1",
         "--persist-to", str(state), "--show-interactive-dev-session=false"],
        cwd=SYNC_DIR, stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "CI": "1"})
    url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 90
    while True:
        try:
            if httpx.get(f"{url}/leases", headers={"x-sync-space": SPACE}, timeout=2).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        if time.time() > deadline or proc.poll() is not None:
            proc.kill()
            raise RuntimeError("wrangler dev did not start:\n" + (state / "wrangler.log").read_text()[-3000:])
        time.sleep(0.3)
    yield url
    proc.terminate()
    try:
        proc.wait(10)
    except subprocess.TimeoutExpired:
        proc.kill()
    log.close()


async def wait_for(fn, timeout=30.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        v = await fn()
        if v:
            return v
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("timed out")
        await asyncio.sleep(0.1)


class Computer:
    """One computer's control plane: its own store, data directory, hubs and workers."""

    def __init__(self, root: Path, url: str, name: str) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.root = root
        self.sync = Sync(url, SPACE, root / "data", name=name)
        self.store = SyncedStore(SqliteStore(str(root / "db.sqlite")), self.sync)
        self.provider = LocalProvider(workdir=str(root / "workers"), command=ECHO_RUNNER)
        self.orch = Orchestrator(self.store, local(self.provider), HeuristicRouter(), root / "repos", sync=self.sync)

    async def start(self) -> "Computer":
        await self.store.open()
        await self.orch.start()
        await self.sync.caught_up.wait()  # in the background: starting never waits for it
        return self

    async def close(self) -> None:
        await self.orch.close()
        await self.provider.close()
        await self.store.close()

    async def events(self, pid, tid=None):
        return await self.store.list_events(pid, tid, 0, limit=5000)


@pytest.fixture
def fast(monkeypatch):
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")
    monkeypatch.setattr(sync_mod, "RENEW_EVERY", 0.5)
    monkeypatch.setattr(sync_mod, "WAIT", 2.0)
    monkeypatch.setattr(sync_mod, "DRIVER_FRESH", 0.2)


async def test_a_project_is_shown_live_on_another_computer_which_forwards_to_its_driver(
        tmp_path, sync_server, fast):
    a = await Computer(tmp_path / "a", sync_server, "desktop").start()
    b = await Computer(tmp_path / "b", sync_server, "phone").start()
    try:
        p = await a.orch.create_project("shared")
        pid = p["id"]
        await a.orch.submit(pid, "write notes.txt: hello")

        async def idle_on(c):
            ts = await c.store.list_threads(pid)
            return ts if ts and all(t["state"] == "idle" for t in ts) else None
        [t] = await wait_for(lambda: idle_on(a))
        # The phone shows the project, its thread and the thread's turn, as the desktop has them.
        assert (await wait_for(lambda: b.store.get_project(pid)))["name"] == "shared"
        await wait_for(lambda: idle_on(b))
        async def b_has_reply():
            return [e for e in await b.events(pid, t["id"]) if e["type"] == "assistant"]
        await wait_for(b_has_reply)
        assert a.orch.sync.driver(pid) == {"name": "desktop", "me": True}

        # The phone does not drive it: a request typed there goes to the desktop, which routes it.
        assert not await b.orch.sync.claim(pid)
        assert b.orch.sync.driver(pid) == {"name": "desktop", "me": False}
        r = await b.orch.submit(pid, "write more.txt: again")
        assert r["forwarded"]

        async def two_threads(c):
            ts = await c.store.list_threads(pid)
            return len(ts) == 2 and all(t["state"] == "idle" for t in ts) or None
        await wait_for(lambda: two_threads(a))
        await wait_for(lambda: two_threads(b))
        # The request shows once on each computer, logged by the desktop.
        for c in (a, b):
            assert [e["payload"]["text"] for e in await c.events(pid) if e["type"] == "request"] == \
                ["write notes.txt: hello", "write more.txt: again"]
        # Every thread ran on the desktop's workers; the phone has none.
        assert await b.store.list_workers() == []
    finally:
        await b.close()
        await a.close()


async def test_taking_over_moves_the_threads_and_the_hub_to_the_new_driver(tmp_path, sync_server, fast):
    a = await Computer(tmp_path / "a", sync_server, "desktop").start()
    b = await Computer(tmp_path / "b", sync_server, "laptop").start()
    try:
        p = await a.orch.create_project("moving")
        pid = p["id"]
        t = await a.orch.spawn(pid, "Notes", "write notes.txt: first")

        async def idle(c):
            got = await c.store.get_thread(t["id"])
            return got if got and got["state"] == "idle" and got["summary"] else None
        await wait_for(lambda: idle(a))
        await wait_for(lambda: idle(b))

        # The laptop takes over: the desktop lets go, the laptop has the hub, and
        # the thread's next message brings it back on the laptop's own worker.
        await b.orch.take_over(pid)
        assert b.orch.sync.driver(pid)["me"]
        await wait_for(lambda: asyncio.sleep(0, result=not a.orch.sync.driving(pid)))
        assert (b.root / "repos" / f"{pid}.git" / "HEAD").exists()
        await b.orch.send(t["id"], "write later.txt: second")

        async def second_turn():
            return [e for e in await b.events(pid, t["id"])
                    if e["type"] == "assistant" and "later.txt" in e["payload"]["content"]]
        await wait_for(second_turn)
        bound = [e["payload"] for e in await b.events(pid, t["id"])
                 if e["type"] == "binding" and e["payload"]["to"] == "bound"]
        workers = {w["id"] for w in await b.store.list_workers()}
        assert bound[-1]["worker"] in workers
        # Its worktree on the laptop's worker came from the hub: the first turn's file is there.
        files = await b.orch.list_files(t["id"], "")
        assert {"notes.txt", "later.txt"} <= {f["name"] for f in files["entries"]}

        # The desktop shows the second turn too, from the laptop.
        async def a_sees():
            return [e for e in await a.events(pid, t["id"])
                    if e["type"] == "assistant" and "later.txt" in e["payload"]["content"]]
        await wait_for(a_sees)
    finally:
        await b.close()
        await a.close()


async def test_an_image_pasted_on_one_computer_reaches_the_other(tmp_path, sync_server, fast):
    a = await Computer(tmp_path / "a", sync_server, "desktop").start()
    b = await Computer(tmp_path / "b", sync_server, "phone").start()
    try:
        p = await a.orch.create_project("pictures")
        ref = await b.store.add_image(p["id"], "image/png", b"\x89PNG fake", "shot.png")
        row = await a.store.get_image(ref["id"])
        assert row["data"] == b"\x89PNG fake" and row["media_type"] == "image/png" and row["project_id"] == p["id"]
    finally:
        await b.close()
        await a.close()


async def test_projects_made_before_syncing_are_shared_and_a_restart_picks_up_where_it_was(
        tmp_path, sync_server, fast):
    # A project made while the desktop did not sync yet.
    plain = SqliteStore(str(tmp_path / "a" / "db.sqlite"))
    await plain.open()
    old = await plain.create_project("from before")
    await plain.append_event(old["id"], None, "request", {"text": "an old request"})
    await plain.close()

    a = await Computer(tmp_path / "a", sync_server, "desktop").start()
    b = await Computer(tmp_path / "b", sync_server, "phone").start()
    try:
        assert a.orch.sync.driving(old["id"])

        async def b_has_it():
            return [e for e in await b.events(old["id"]) if e["type"] == "request"]
        [req] = await wait_for(b_has_it)
        assert req["payload"]["text"] == "an old request"

        # An edit made on the phone names the message by its uid; the desktop
        # logs the edit, and each computer's copy of it names its own seq.
        await b.orch.edit(old["id"], req["seq"], "an edited request")

        async def edited(c):
            return [e for e in await c.events(old["id"]) if e["type"] == "edited"]
        for c in (a, b):
            [e] = await wait_for(lambda: edited(c))
            mine = [r for r in await c.events(old["id"]) if r["type"] == "request"][0]
            assert e["payload"]["of"] == mine["seq"] and e["payload"]["text"] == "an edited request"
    finally:
        await b.close()
        await a.close()

    # Closing let go of the desktop's leases: the phone drives the project at once.
    b = await Computer(tmp_path / "b", sync_server, "phone").start()
    try:
        assert await b.orch.sync.claim(old["id"])
    finally:
        await b.close()
    # The desktop starts again from its replica: nothing is logged twice.
    a = await Computer(tmp_path / "a", sync_server, "desktop").start()
    try:
        types = [e["type"] for e in await a.events(old["id"])]
        assert types.count("request") == 1 and types.count("edited") == 1
    finally:
        await a.close()


async def test_a_computer_in_another_space_sees_none_of_its_projects(tmp_path, sync_server, fast):
    a = await Computer(tmp_path / "a", sync_server, "desktop").start()
    try:
        p = await a.orch.create_project("mine")
        other = Sync(sync_server, "6" * 64, tmp_path / "c" / "data", name="stranger")
        c = SyncedStore(SqliteStore(str(tmp_path / "c" / "db.sqlite")), other)
        await c.open()
        await other.start()
        try:
            assert await c.list_projects() == [] and await c.get_project(p["id"]) is None
            # Its own projects are its own: it drives one with the same id as nobody does in its space.
            assert await other.claim(p["id"])
        finally:
            await other.close()
            await c.close()
    finally:
        await a.close()


async def test_a_computer_starts_without_the_server_and_catches_up_once_it_answers(tmp_path, sync_server, fast,
                                                                                  monkeypatch):
    monkeypatch.setattr(sync_mod, "RETRY", 0.2)
    port = free_port()
    a = Computer(tmp_path / "a", f"http://127.0.0.1:{port}", "desktop")
    await a.store.open()
    await a.orch.start()  # nothing listens there: starting goes on without it
    try:
        assert not a.sync.caught_up.is_set()
        a.sync.client.http.base_url = sync_server  # the server answers from now on
        await wait_for(lambda: asyncio.sleep(0, result=a.sync.caught_up.is_set()))
        p = await a.orch.create_project("after all")
        b = await Computer(tmp_path / "b", sync_server, "phone").start()
        try:
            assert (await wait_for(lambda: b.store.get_project(p["id"])))["name"] == "after all"
        finally:
            await b.close()
    finally:
        await a.close()


def test_the_space_key_is_made_once_and_kept(tmp_path, monkeypatch):
    monkeypatch.delenv(sync_mod.SPACE_KEY, raising=False)
    monkeypatch.setattr(sync_mod, "get_secret", lambda name: None)
    first = Sync.from_env(tmp_path)
    assert sync_mod.is_space(first.space) and Sync.from_env(tmp_path).space == first.space
    assert (tmp_path / "space").stat().st_mode & 0o077 == 0
    assert first.client.url == sync_mod.DEFAULT_URL
    # A key from another computer (environment or keyring) names the space instead.
    monkeypatch.setattr(sync_mod, "get_secret", lambda name: {"ORCHESTRATE_SYNC_SPACE": "a" * 64}.get(name))
    assert Sync.from_env(tmp_path).space == "a" * 64
    monkeypatch.setattr(sync_mod, "get_secret", lambda name: {"ORCHESTRATE_SYNC_SPACE": " " + "A" * 64 + "\n"}.get(name))
    assert Sync.from_env(tmp_path).space == "a" * 64
    monkeypatch.setattr(sync_mod, "get_secret", lambda name: {"ORCHESTRATE_SYNC_SPACE": "nope"}.get(name))
    with pytest.raises(RuntimeError, match="ORCHESTRATE_SYNC_SPACE"):
        Sync.from_env(tmp_path)


async def test_pairing_a_computer_that_had_its_own_space(tmp_path, sync_server, fast):
    a = await Computer(tmp_path / "a", sync_server, "desktop").start()
    try:
        pa = await a.orch.create_project("from the desktop")
        # The laptop ran in its own space first, with a project of its own.
        lone = Computer(tmp_path / "b", sync_server, "laptop")
        lone.sync = Sync(sync_server, "7" * 64, tmp_path / "b" / "data", name="laptop")
        lone.store = SyncedStore(SqliteStore(str(tmp_path / "b" / "db.sqlite")), lone.sync)
        lone.orch = Orchestrator(lone.store, local(lone.provider), HeuristicRouter(), tmp_path / "b" / "repos",
                                 sync=lone.sync)
        await lone.start()
        pb = await lone.orch.create_project("from the laptop")
        await lone.close()
        # Then it is given the desktop's key.
        b = await Computer(tmp_path / "b", sync_server, "laptop").start()
        try:
            for c in (a, b):
                for p in (pa, pb):
                    await wait_for(lambda: c.store.get_project(p["id"]))
        finally:
            await b.close()
    finally:
        await a.close()


async def test_a_big_log_crosses_in_pages(tmp_path, sync_server, fast):
    # More entries than a page holds, more bytes than a page and a POST carry,
    # and one event bigger than a part of an entry on the server.
    plain = SqliteStore(str(tmp_path / "a" / "db.sqlite"))
    await plain.open()
    p = await plain.create_project("big")
    for i in range(2500):
        await plain.append_event(p["id"], None, "request", {"text": f"{i} " + "x" * 3000})
    await plain.append_event(p["id"], None, "request", {"text": "huge " + "é" * 1_200_000})
    await plain.close()

    a = await Computer(tmp_path / "a", sync_server, "desktop").start()
    b = await Computer(tmp_path / "b", sync_server, "phone").start()
    try:
        async def all_there():
            evs = [e for e in await b.events(p["id"]) if e["type"] == "request"]
            return evs if len(evs) == 2501 else None
        evs = await wait_for(all_there, timeout=120)
        assert [e["payload"]["text"].split(" ", 1)[0] for e in evs[:3]] == ["0", "1", "2"]
        assert evs[-1]["payload"]["text"] == "huge " + "é" * 1_200_000
    finally:
        await b.close()
        await a.close()


async def test_logs_kept_as_documents_before_are_dropped_from_the_replica(tmp_path):
    s = Sync("http://127.0.0.1:1", SPACE, tmp_path / "data")
    store = SqliteStore(str(tmp_path / "db.sqlite"))
    await store.open()
    await s.open(store)
    await s.replica.put("log:p_1:main", {"e_1": "{}"})
    await s.replica.put("projects", {"p_1": "{}"})
    await s.replica.set_state("format", 1)
    await s.replica.close()
    s = Sync("http://127.0.0.1:1", SPACE, tmp_path / "data")
    await s.open(store)
    try:
        assert await s.replica.names() == ["projects"]
        assert await s.replica.state("format") == sync_mod.FORMAT
    finally:
        await s.replica.close()
        await s.client.close()
        await store.close()
