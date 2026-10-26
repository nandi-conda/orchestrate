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
    await second.respawn(t["id"])

    async def bound():
        return (await s.get_thread(t["id"]))["binding"] == "bound" or None
    await wait_for(bound)
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
    monkeypatch.chdir(tmp_path)
    hub = Hub("repos/p.git", "p")
    await hub.open()
    assert (tmp_path / "repos" / "p.git" / "HEAD").exists()
    assert git(tmp_path / "repos" / "p.git", "log", "--format=%s", "main") == "Start the project"


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


async def opened(tmp_path, folder):
    """A project opened from `folder` on disk, and its orchestrator."""
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    orch = Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True),
                        HeuristicRouter(), tmp_path / "repos")
    p = await orch.create_project("demo", "", str(folder))
    return s, p, orch


async def test_a_project_opened_from_a_checkout_lands_in_its_files(tmp_path):
    bare, seed = remote(tmp_path)  # seed: a checkout of trunk, holding notes.txt
    s, p, orch = await opened(tmp_path, seed)
    hub = orch.hub_path(p["id"])
    assert p["repo_url"] == str(seed.resolve())
    # Its history is the project's, made a pixi project: in its files too.
    assert git(hub, "log", "--format=%s", "trunk").splitlines() == ["Make the project a pixi project", "init"]
    assert git(seed, "rev-parse", "HEAD") == git(hub, "rev-parse", "trunk")
    assert (seed / "pixi.toml").exists() and git(seed, "status", "--porcelain") == ""

    t = await orch.spawn(p["id"], "T", "write a.txt: one")
    landed = await event(s, p, t["id"], "landed")
    assert git(seed, "rev-parse", "HEAD") == landed["commit"]
    assert (seed / "a.txt").read_text() == "one\n" and git(seed, "status", "--porcelain") == ""
    await wait_for(lambda: idle(s, t["id"]))

    # What its owner commits there reaches the threads; a landing goes on top.
    commit(seed, "mine.txt", "m\n", "mine")
    await orch.send(t["id"], "write b.txt: two")
    landed = await landing(s, p, t["id"], 2)
    assert git(seed, "log", "-2", "--format=%s").splitlines()[1] == "mine"
    assert (seed / "b.txt").read_text() == "two\n" and (seed / "mine.txt").exists()
    await wait_for(lambda: idle(s, t["id"]))

    # Changes not committed there are never overwritten: the landing is refused.
    (seed / "notes.txt").write_text("editing\n")
    before = git(hub, "rev-parse", "trunk")
    await orch.send(t["id"], "write c.txt: three")
    error = await event(s, p, t["id"], "error")
    assert "unstaged changes" in error["error"]
    assert git(hub, "rev-parse", "trunk") == before and not (seed / "c.txt").exists()
    assert (seed / "notes.txt").read_text() == "editing\n"
    await orch.close()
    await orch.provider.close()
    await s.close()


async def test_a_folder_that_is_not_a_repository_becomes_one_with_its_files(tmp_path):
    folder = tmp_path / "plain"
    (folder / "src").mkdir(parents=True)
    (folder / "src" / "app.py").write_text("print(1)\n")
    (folder / ".gitignore").write_text("secret.txt\n")
    (folder / "secret.txt").write_text("x\n")
    s, p, orch = await opened(tmp_path, folder)
    hub = orch.hub_path(p["id"])
    assert git(folder, "symbolic-ref", "HEAD") == "refs/heads/main"
    assert git(hub, "log", "--format=%s", "main").splitlines() == ["Make the project a pixi project",
                                                                   "Add the folder's files"]
    assert set(git(hub, "ls-tree", "-r", "--name-only", "main").split()) == {
        "src/app.py", ".gitignore", ".gitattributes", "pixi.toml"}
    assert git(hub, "show", "main:.gitignore").splitlines()[0] == "secret.txt"
    assert (folder / "pixi.toml").exists() and (folder / "secret.txt").exists()
    assert git(folder, "status", "--porcelain") == ""
    await orch.close()
    await orch.provider.close()
    await s.close()


async def test_a_folder_starts_from_what_it_has_not_committed(tmp_path):
    # A checkout with an untracked .gitignore, a change to a tracked file, and
    # a new file: opening commits them, so making it a pixi project edits its
    # .gitignore (never pushes over it) and threads see every file.
    bare, seed = remote(tmp_path)
    (seed / ".gitignore").write_text("build/\n")
    (seed / "notes.txt").write_text("hello again\n")
    (seed / "todo.txt").write_text("x\n")
    (seed / "build").mkdir()
    (seed / "build" / "out.o").write_text("o\n")
    s, p, orch = await opened(tmp_path, seed)
    hub = orch.hub_path(p["id"])
    assert git(hub, "log", "--format=%s", "trunk").splitlines() == [
        "Make the project a pixi project", "Add the folder's files", "init"]
    assert git(hub, "show", "trunk~1:.gitignore") == "build/"
    assert git(hub, "show", "trunk:.gitignore").splitlines()[:2] == ["build/", "# pixi environments"]
    assert git(hub, "show", "trunk:notes.txt") == "hello again" and git(hub, "show", "trunk:todo.txt") == "x"
    assert git(seed, "rev-parse", "HEAD") == git(hub, "rev-parse", "trunk")
    assert git(seed, "status", "--porcelain") == "" and (seed / "build" / "out.o").exists()

    t = await orch.spawn(p["id"], "T", "write a.txt: one")
    landed = await event(s, p, t["id"], "landed")
    assert git(seed, "rev-parse", "HEAD") == landed["commit"] and (seed / "a.txt").exists()
    await wait_for(lambda: idle(s, t["id"]))
    await orch.close()
    await orch.provider.close()
    await s.close()


async def test_a_folder_with_nothing_to_commit_gets_no_commit(tmp_path):
    bare, seed = remote(tmp_path)
    commit(seed, "pixi.toml", '[workspace]\nname = "x"\n', "pixi")
    head = git(seed, "rev-parse", "HEAD")
    s, p, orch = await opened(tmp_path, seed)
    assert git(seed, "rev-parse", "HEAD") == head == git(orch.hub_path(p["id"]), "rev-parse", "trunk")
    await orch.close()
    await orch.provider.close()
    await s.close()


async def test_a_gitignore_that_ignores_itself_is_kept(tmp_path):
    # Ignored files are written over by a push; the pixi files never are,
    # because opening commits the folder's own.
    folder = tmp_path / "plain"
    folder.mkdir()
    (folder / ".gitignore").write_text(".*\n")
    s, p, orch = await opened(tmp_path, folder)
    hub = orch.hub_path(p["id"])
    assert git(hub, "show", "main~1:.gitignore") == ".*"
    assert (folder / ".gitignore").read_text().splitlines()[:2] == [".*", "# pixi environments"]
    await orch.close()
    await orch.provider.close()
    await s.close()


async def test_a_folder_that_cannot_be_opened_leaves_nothing_behind(tmp_path):
    bare, seed = remote(tmp_path)
    (seed / "sub").mkdir()
    git(seed, "checkout", "-q", "--detach")
    s = SqliteStore(str(tmp_path / "db.sqlite"))
    await s.open()
    orch = Orchestrator(s, LocalProvider(workdir=str(tmp_path / "w"), use_devrunner=True),
                        HeuristicRouter(), tmp_path / "repos")
    for folder, reason in ((tmp_path / "nope", "there is no folder"),
                           (seed / "sub", "inside the git repository"),
                           (seed / ".git", "inside a repository's .git folder"),
                           (seed, "no branch checked out")):
        with pytest.raises(GitError, match=reason):
            await orch.create_project("demo", "", str(folder))
    assert await s.list_projects() == []
    assert list((tmp_path / "repos").iterdir()) == []
    await orch.close()
    await orch.provider.close()
    await s.close()
