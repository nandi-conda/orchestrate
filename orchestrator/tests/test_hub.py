"""One git repository per project (control/hub.py): threads' work reaches it
every turn, finished threads land there (and on the project's repository
first, when it has one), and a thread brought back on a fresh worker gets its
files from it. Against the echo runner, which keeps the real runner's git flow."""

import asyncio
import subprocess

import pytest

from control.hub import Hub
from control.providers import LocalProvider
from control.router import HeuristicRouter
from control.service import Orchestrator
from control.store import SqliteStore

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setenv("DEVRUNNER_DELAY", "0.02")


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


async def wait_for(fn, timeout=20.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        v = await fn()
        if v:
            return v
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("timed out")
        await asyncio.sleep(0.1)


async def setup(tmp_path, repo_url="", workers="w"):
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    p = await s.create_project("demo", repo_url)
    orch = Orchestrator(s, LocalProvider(workdir=str(tmp_path / workers), use_devrunner=True),
                        HeuristicRouter(), tmp_path / "repos")
    return s, p, orch


async def event(s, p, tid, typ):
    async def find():
        return next((e["payload"] for e in await s.list_events(p["id"], tid) if e["type"] == typ), None)
    return await wait_for(find)


async def idle(s, tid):
    return (await s.get_thread(tid))["state"] == "idle" or None


def remote(tmp_path):
    """A project repository (as on GitHub) whose default branch `trunk` holds notes.txt."""
    bare, seed = tmp_path / "remote.git", tmp_path / "seed"
    git(tmp_path, "init", "-q", "--bare", "-b", "trunk", str(bare))
    git(tmp_path, "clone", "-q", str(bare), str(seed))
    (seed / "notes.txt").write_text("hello\n")
    git(seed, "add", "-A")
    git(seed, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "-m", "init")
    git(seed, "push", "-q", "origin", "HEAD:trunk")
    return bare, seed


async def test_a_project_without_a_repository_starts_from_an_empty_commit(tmp_path):
    hub = Hub(tmp_path / "p.git")
    await hub.open()
    assert hub.base == "main"
    assert git(hub.path, "log", "--format=%s", "main") == "Start the project"
    assert git(hub.path, "ls-tree", "main") == ""
    await Hub(tmp_path / "p.git").open()  # a second open finds it
    assert git(hub.path, "rev-list", "--count", "main") == "1"


async def test_every_turn_reaches_the_hub_and_finish_lands(tmp_path):
    s, p, orch = await setup(tmp_path)
    t = await orch.spawn(p["id"], "T", "write a.txt: one")
    saved = await event(s, p, t["id"], "saved")
    hub = orch.hub_path(p["id"])
    assert git(hub, "rev-parse", f"thread/{t['id']}") == saved["commit"]
    assert git(hub, "show", f"thread/{t['id']}:a.txt") == "one"
    assert "a.txt" not in git(hub, "ls-tree", "--name-only", "main")
    await wait_for(lambda: idle(s, t["id"]))

    await orch.send(t["id"], "write b.txt: two\nfinish")
    landed = await event(s, p, t["id"], "landed")
    assert landed["base"] == "main" and git(hub, "rev-parse", "main") == landed["commit"]
    assert set(git(hub, "ls-tree", "--name-only", "main").split()) == {"a.txt", "b.txt"}
    assert git(hub, "rev-list", "--merges", "--count", "main") == "0"
    await orch.close()
    await s.close()


async def test_landing_rebases_onto_what_landed_before_and_conflicts_go_back(tmp_path):
    s, p, orch = await setup(tmp_path)
    a = await orch.spawn(p["id"], "A", "write a.txt: a")
    b = await orch.spawn(p["id"], "B", "write same.txt: b")
    c = await orch.spawn(p["id"], "C", "write same.txt: c")
    for t in (a, b, c):
        await wait_for(lambda: idle(s, t["id"]))
    for t, text in ((a, "finish"), (b, "finish")):
        await orch.send(t["id"], text)
        await event(s, p, t["id"], "landed")
    hub = orch.hub_path(p["id"])
    assert set(git(hub, "ls-tree", "--name-only", "main").split()) == {"a.txt", "same.txt"}

    await orch.send(c["id"], "finish")
    conflict = await event(s, p, c["id"], "land_conflict")
    assert conflict["files"] == ["same.txt"] and conflict["base"] == "main"
    assert git(hub, "show", "main:same.txt") == "b"
    # The worker has the base the thread has to rebase onto.
    wt = tmp_path / "w" / f"{p['id']}-w1" / c["id"]
    assert git(wt, "rev-parse", "origin/main") == git(hub, "rev-parse", "main")
    await orch.close()
    await s.close()


async def test_a_project_repository_gets_the_landing_first_and_nothing_else(tmp_path):
    bare, seed = remote(tmp_path)
    s, p, orch = await setup(tmp_path, str(bare))
    t = await orch.spawn(p["id"], "T", "write a.txt: one")
    await wait_for(lambda: idle(s, t["id"]))
    hub = orch.hub_path(p["id"])
    assert git(hub, "symbolic-ref", "HEAD") == "refs/heads/trunk"

    # Someone pushes to the repository meanwhile; the landing goes on top.
    (seed / "theirs.txt").write_text("x\n")
    git(seed, "add", "-A")
    git(seed, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "-m", "theirs")
    git(seed, "push", "-q", "origin", "HEAD:trunk")

    await orch.send(t["id"], "finish")
    landed = await event(s, p, t["id"], "landed")
    assert git(bare, "rev-parse", "trunk") == landed["commit"] == git(hub, "rev-parse", "trunk")
    assert set(git(bare, "ls-tree", "--name-only", "trunk").split()) == {"notes.txt", "theirs.txt", "a.txt"}
    assert git(bare, "for-each-ref", "--format=%(refname)") == "refs/heads/trunk"  # no thread branches
    await orch.close()
    await s.close()


async def test_a_thread_on_a_fresh_worker_gets_its_ignored_files_from_the_hub(tmp_path):
    s, p, first = await setup(tmp_path, workers="w1")
    t = await first.spawn(p["id"], "T", "write .gitignore: build/\nwrite build/app: binary\nwrite src.txt: code")
    await wait_for(lambda: idle(s, t["id"]))
    await event(s, p, t["id"], "saved")
    hub = first.hub_path(p["id"])
    assert "build" not in git(hub, "ls-tree", "--name-only", f"thread/{t['id']}").split()
    assert git(hub, "show", f"refs/snapshots/{t['id']}:build/app") == "binary"
    await first.close()
    await first.provider.close()

    # Its worker's disk is gone: the next control plane makes the worker again in a new directory.
    second = Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w2"), use_devrunner=True),
                          HeuristicRouter(), tmp_path / "repos")
    listing = await second.list_files(t["id"], "build")
    assert [e["name"] for e in listing["entries"]] == ["app"]
    r = await second.open_file(t["id"], "src.txt")
    assert (await r.aread()) == b"code\n"
    await r.aclose()
    await second.close()
    await second.provider.close()
    await s.close()


async def test_a_worker_may_only_set_its_threads_refs(tmp_path):
    hub = Hub(tmp_path / "p.git")
    await hub.open()
    with pytest.raises(Exception, match="may not set"):
        await hub.receive("t_1", b"", {"refs/heads/main": git(hub.path, "rev-parse", "main")})


async def test_a_hub_at_a_relative_path_is_made_where_it_says(tmp_path, monkeypatch):
    """The control plane's default is `.orchestrator/repos`, relative to where it runs."""
    monkeypatch.chdir(tmp_path)
    hub = Hub(".orchestrator/repos/p.git")
    await hub.open()
    assert (tmp_path / ".orchestrator" / "repos" / "p.git" / "HEAD").exists()
    assert git(tmp_path / ".orchestrator" / "repos" / "p.git", "log", "--format=%s", "main") == "Start the project"
