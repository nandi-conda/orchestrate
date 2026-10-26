"""One git repository per project (control/hub.py): threads' work reaches it
every turn and land there (and on the project's repository
first, when it has one), and a thread brought back on a fresh worker gets its
files from it. Against the echo runner, which keeps the real runner's git flow."""

import asyncio
import subprocess
import tomllib

import pytest

from control.gitio import GitError
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
    p = await s.create_project("demo")
    orch = Orchestrator(s, LocalProvider(workdir=str(tmp_path / workers), use_devrunner=True),
                        HeuristicRouter(), tmp_path / "repos")
    if repo_url:
        p = await orch.update_project(p["id"], repo_url=repo_url)
    return s, p, orch


def commit(seed, name, text, message):
    (seed / name).write_text(text)
    git(seed, "add", "-A")
    git(seed, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "-m", message)
    git(seed, "push", "-q", "origin", "HEAD:trunk")


async def landing(s, p, tid, n):
    """The thread's n-th `landed` event (from 1), once there is one."""
    async def find():
        found = [e["payload"] for e in await s.list_events(p["id"], tid) if e["type"] == "landed"]
        return found[n - 1] if len(found) >= n else None
    return await wait_for(find)


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


async def test_a_project_starts_as_a_pixi_project(tmp_path):
    hub = Hub(tmp_path / "p.git", 'my "p"')
    await hub.open()
    assert hub.base == "main"
    assert git(hub.path, "log", "--format=%s", "main") == "Start the project"
    assert git(hub.path, "ls-tree", "--name-only", "main").split() == [".gitattributes", ".gitignore", "pixi.toml"]
    manifest = tomllib.loads(git(hub.path, "show", "main:pixi.toml"))
    assert manifest["workspace"] == {"name": 'my "p"', "channels": ["conda-forge"],
                                     "platforms": ["linux-64", "linux-aarch64"]}
    assert ".pixi/*" in git(hub.path, "show", "main:.gitignore").splitlines()
    assert "pixi.lock merge=binary" in git(hub.path, "show", "main:.gitattributes")
    await Hub(tmp_path / "p.git", "p").open()  # a second open finds it
    assert git(hub.path, "rev-list", "--count", "main") == "1"


def legacy_hub(path):
    """A hub from before every project was a pixi project: an empty first commit."""
    git(path.parent, "init", "-q", "--bare", "-b", "main", str(path))
    empty = git(path, "hash-object", "-t", "tree", "-w", "/dev/null")
    start = git(path, "-c", "user.email=a@b", "-c", "user.name=a", "commit-tree", empty, "-m", "Start the project")
    git(path, "update-ref", "refs/heads/main", start)
    git(path, "config", "orchestrate.base", "main")
    return start


async def test_a_hub_from_before_pixi_becomes_a_pixi_project_when_it_opens(tmp_path):
    start = legacy_hub(tmp_path / "p.git")
    hub = Hub(tmp_path / "p.git", "p")
    await hub.open()
    assert git(hub.path, "log", "--format=%s", "main").splitlines() == ["Make the project a pixi project",
                                                                       "Start the project"]
    assert git(hub.path, "rev-parse", "main^") == start
    assert "pixi.toml" in git(hub.path, "ls-tree", "--name-only", "main").split()
    await Hub(tmp_path / "p.git", "p").open()  # once
    assert git(hub.path, "rev-list", "--count", "main") == "2"


async def test_every_turn_lands_on_the_hub(tmp_path):
    s, p, orch = await setup(tmp_path)
    t = await orch.spawn(p["id"], "T", "write a.txt: one")
    landed = await event(s, p, t["id"], "landed")
    hub = orch.hub_path(p["id"])
    pixi = {".gitattributes", ".gitignore", "pixi.toml"}
    assert git(hub, "rev-parse", f"thread/{t['id']}") == landed["commit"] == git(hub, "rev-parse", "main")
    assert git(hub, "show", "main:a.txt") == "one"
    await wait_for(lambda: idle(s, t["id"]))

    await orch.send(t["id"], "write b.txt: two")
    landed = await landing(s, p, t["id"], 2)
    assert landed["base"] == "main" and git(hub, "rev-parse", "main") == landed["commit"]
    assert set(git(hub, "ls-tree", "--name-only", "main").split()) == {"a.txt", "b.txt", *pixi}
    assert git(hub, "rev-list", "--merges", "--count", "main") == "0"
    await orch.close()
    await orch.provider.close()
    await s.close()


async def test_landing_rebases_onto_what_landed_before_and_conflicts_go_back(tmp_path):
    s, p, orch = await setup(tmp_path)
    a = await orch.spawn(p["id"], "A", "write a.txt: a")
    await event(s, p, a["id"], "landed")
    c = await orch.spawn(p["id"], "C", "hold on")  # its worker base is a's main
    await event(s, p, c["id"], "landed")
    await wait_for(lambda: idle(s, c["id"]))
    b = await orch.spawn(p["id"], "B", "write same.txt: b")
    await event(s, p, b["id"], "landed")
    hub = orch.hub_path(p["id"])
    assert set(git(hub, "ls-tree", "--name-only", "main").split()) == {
        "a.txt", "same.txt", ".gitattributes", ".gitignore", "pixi.toml"}

    await orch.send(c["id"], "write same.txt: c")
    conflict = await event(s, p, c["id"], "land_conflict")
    assert conflict["files"] == ["same.txt"] and conflict["base"] == "main"
    assert git(hub, "show", "main:same.txt") == "b"
    # The worker has the base the thread has to rebase onto.
    wt = tmp_path / "w" / "w1" / c["id"]
    assert git(wt, "rev-parse", "origin/main") == git(hub, "rev-parse", "main")
    await orch.close()
    await orch.provider.close()
    await s.close()


async def test_a_project_repository_gets_the_landing_first_and_nothing_else(tmp_path):
    bare, seed = remote(tmp_path)
    s, p, orch = await setup(tmp_path, str(bare))
    t = await orch.spawn(p["id"], "T", "write a.txt: one")
    await wait_for(lambda: idle(s, t["id"]))
    hub = orch.hub_path(p["id"])
    assert git(hub, "symbolic-ref", "HEAD") == "refs/heads/trunk"

    await landing(s, p, t["id"], 1)
    # Someone pushes to the repository meanwhile; the next landing goes on top.
    git(seed, "pull", "-q", "--ff-only", "origin", "trunk")
    commit(seed, "theirs.txt", "x\n", "theirs")

    await orch.send(t["id"], "write b.txt: two")
    landed = await landing(s, p, t["id"], 2)
    assert git(bare, "rev-parse", "trunk") == landed["commit"] == git(hub, "rev-parse", "trunk")
    assert set(git(bare, "ls-tree", "--name-only", "trunk").split()) == {
        "notes.txt", "theirs.txt", "a.txt", "b.txt", "pixi.toml", ".gitignore", ".gitattributes"}
    assert git(bare, "for-each-ref", "--format=%(refname)") == "refs/heads/trunk"  # no thread branches
    await orch.close()
    await orch.provider.close()
    await s.close()


async def test_a_thread_on_a_fresh_worker_gets_its_ignored_files_from_the_hub(tmp_path):
    s, p, first = await setup(tmp_path, workers="w1")
    t = await first.spawn(p["id"], "T", "write .gitignore: build/\nwrite build/app: binary\nwrite src.txt: code")
    await wait_for(lambda: idle(s, t["id"]))
    await event(s, p, t["id"], "landed")
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
    hub = Hub(tmp_path / "p.git", "p")
    await hub.open()
    with pytest.raises(Exception, match="may not set"):
        await hub.receive("t_1", b"", {"refs/heads/main": git(hub.path, "rev-parse", "main")})


async def test_a_hub_at_a_relative_path_is_made_where_it_says(tmp_path, monkeypatch):
    """The control plane's default is `.orchestrator/repos`, relative to where it runs."""
    monkeypatch.chdir(tmp_path)
    hub = Hub(".orchestrator/repos/p.git", "p")
    await hub.open()
    assert (tmp_path / ".orchestrator" / "repos" / "p.git" / "HEAD").exists()
    assert git(tmp_path / ".orchestrator" / "repos" / "p.git", "log", "--format=%s", "main") == "Start the project"


async def test_an_empty_remote_gets_the_projects_history(tmp_path):
    s, p, orch = await setup(tmp_path)
    t = await orch.spawn(p["id"], "T", "write a.txt: one")
    landed = await event(s, p, t["id"], "landed")
    empty = tmp_path / "empty.git"
    git(tmp_path, "init", "-q", "--bare", str(empty))

    await orch.update_project(p["id"], repo_url=str(empty))
    assert git(empty, "rev-parse", "main") == landed["commit"]
    await orch.close()
    await orch.provider.close()
    await s.close()


async def test_a_remote_with_another_history_is_refused(tmp_path):
    bare, seed = remote(tmp_path)
    s, p, orch = await setup(tmp_path)
    t = await orch.spawn(p["id"], "T", "write a.txt: one")
    landed = await event(s, p, t["id"], "landed")

    with pytest.raises(GitError, match="each gone their own way"):
        await orch.update_project(p["id"], repo_url=str(bare))
    assert (await s.get_project(p["id"]))["repo_url"] == ""
    hub = orch.hub_path(p["id"])
    assert git(hub, "rev-parse", "main") == landed["commit"]
    assert git(hub, "for-each-ref", "--format=%(refname)", "refs/orchestrate") == ""
    assert git(bare, "log", "--format=%s", "trunk") == "init"  # not made a pixi project either
    await orch.close()
    await orch.provider.close()
    await s.close()


async def test_a_remote_ahead_or_behind_follows_and_removing_it_keeps_the_hub(tmp_path):
    bare, seed = remote(tmp_path)
    s, p, orch = await setup(tmp_path, str(bare))
    hub = orch.hub_path(p["id"])
    a = await orch.spawn(p["id"], "A", "write a.txt: a")
    landed = await event(s, p, a["id"], "landed")

    # Off the remote, the next landing stays here; the remote is left as it was.
    await orch.update_project(p["id"], repo_url="")
    b = await orch.spawn(p["id"], "B", "write b.txt: b")
    ours = (await event(s, p, b["id"], "landed"))["commit"]
    assert git(bare, "rev-parse", "trunk") == landed["commit"]
    assert git(hub, "rev-parse", "trunk") == ours

    # Back on it: the remote is behind, so it gets the landing it missed.
    await orch.update_project(p["id"], repo_url=str(bare))
    assert git(bare, "rev-parse", "trunk") == ours

    # A remote that moved on while the project was off it: the base follows.
    await orch.update_project(p["id"], repo_url="")
    git(seed, "pull", "-q", "origin", "trunk")
    commit(seed, "theirs.txt", "x\n", "theirs")
    await orch.update_project(p["id"], repo_url=str(bare))
    assert git(hub, "rev-parse", "trunk") == git(bare, "rev-parse", "trunk")
    await orch.close()
    await orch.provider.close()
    await s.close()


async def test_a_hub_remembers_its_remote(tmp_path):
    bare, seed = remote(tmp_path)
    hub = Hub(tmp_path / "p.git", "p")
    await hub.open()
    await hub.connect(str(bare))
    again = Hub(tmp_path / "p.git", "p")
    await again.open()
    assert (again.url, again.base) == (str(bare), "trunk")
    assert git(bare, "log", "--format=%s", "trunk").splitlines() == ["Make the project a pixi project", "init"]
    await again.connect("")
    last = Hub(tmp_path / "p.git", "p")
    await last.open()
    assert last.url == ""


async def test_a_remote_that_is_a_pixi_project_is_left_as_it_is(tmp_path):
    bare, seed = remote(tmp_path)
    (seed / ".gitignore").write_text("build/\n")
    commit(seed, "pyproject.toml", '[project]\nname = "r"\n\n[tool.pixi.workspace]\nchannels = []\n', "pixi")
    tip = git(bare, "rev-parse", "trunk")
    hub = Hub(tmp_path / "p.git", "p")
    await hub.open()
    await hub.connect(str(bare))
    assert git(bare, "rev-parse", "trunk") == tip == git(hub.path, "rev-parse", "trunk")


async def test_a_remote_that_is_not_a_pixi_project_gets_pixis_files_beside_its_own(tmp_path):
    bare, seed = remote(tmp_path)
    commit(seed, ".gitignore", "build/\n", "ignore build")
    hub = Hub(tmp_path / "p.git", "p")
    await hub.open()
    await hub.connect(str(bare))
    assert git(bare, "rev-parse", "trunk") == git(hub.path, "rev-parse", "trunk")
    assert git(bare, "log", "-1", "--format=%s", "trunk") == "Make the project a pixi project"
    assert git(bare, "show", "trunk:.gitignore").splitlines() == ["build/", "# pixi environments", ".pixi/*",
                                                                  "!.pixi/config.toml"]


async def test_a_thread_begun_before_the_remote_lands_on_it_without_the_hubs_start(tmp_path):
    s, p, orch = await setup(tmp_path)
    t = await orch.spawn(p["id"], "T", "hold on")
    await event(s, p, t["id"], "landed")
    await wait_for(lambda: idle(s, t["id"]))
    bare, seed = remote(tmp_path)
    commit(seed, ".gitignore", "build/\n", "ignore build")  # not the .gitignore the hub started with
    await orch.update_project(p["id"], repo_url=str(bare))  # nothing landed here: the base follows the remote

    await orch.send(t["id"], "write a.txt: one")
    landed = await landing(s, p, t["id"], 2)
    assert git(bare, "rev-parse", "trunk") == landed["commit"]
    assert git(bare, "log", "--format=%s", "trunk").splitlines()[1:] == ["Make the project a pixi project",
                                                                         "ignore build", "init"]
    assert git(bare, "show", "trunk:a.txt") == "one"
    await orch.close()
    await orch.provider.close()
    await s.close()
