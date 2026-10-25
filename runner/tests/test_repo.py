"""Threads' commits: worktrees off the base branch, snapshots, bundles to and from the hub, landing."""

import asyncio
import subprocess

import pytest

from runner import ModelResponse, ScriptedModel, ThreadHost, ToolCall
from runner.peers import OrchestratorPeers, PeerOutbox
from runner.repo import GitError, Repo


def call(name, **args):
    return ToolCall(id=f"c_{name}", name=name, arguments=args)


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def source(tmp_path):
    """A repository whose default branch `main` holds notes.txt."""
    src = tmp_path / "source"
    git(tmp_path, "init", "-q", "-b", "main", str(src))
    (src / "notes.txt").write_text("one\ntwo\nthree\n")
    git(src, "add", "-A")
    git(src, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "-m", "init")
    return src


async def seeded(tmp_path, name="threads"):
    repo = Repo(tmp_path / name / ".repo")
    await repo.seed(str(tmp_path / "source"))
    return repo


async def host(tmp_path, scripts, **kw):
    pending, order = dict(scripts), list(scripts)
    return ThreadHost(model_factory=lambda spec: ScriptedModel(pending.pop(order.pop(0))),
                      workdir=str(tmp_path / "threads"), repo=await seeded(tmp_path), **kw)


def types(t):
    return [e["type"] for e in t.events]


async def move(src: Repo, dst: Repo, tid: str):
    """What the orchestrator does when a thread goes to another worker: take
    its branch and snapshot from one worker, bring them to the other."""
    data, refs = await src.send(tid, [])
    names = {f"refs/heads/thread/{tid}": f"refs/remotes/origin/thread/{tid}",
             f"refs/snapshots/{tid}": f"refs/remotes/origin-snapshots/{tid}"}
    await dst.receive(data, dst.base, {names[k]: v for k, v in refs.items()})


async def test_worktree_per_thread_off_the_base_branch(tmp_path):
    source(tmp_path)
    h = await host(tmp_path, {"a": [ModelResponse("ok")], "b": [ModelResponse("ok")]})
    a = await h.start(project="p", title="a", request="x")
    b = await h.start(project="p", title="b", request="y")
    assert a.sandbox.root != b.sandbox.root and a.branch == f"thread/{a.id}"
    assert (await a.sandbox.exec("git branch --show-current")).stdout.strip() == a.branch
    assert (await a.sandbox.exec("cat notes.txt")).stdout.startswith("one")
    await asyncio.gather(h.wait_idle(a.id), h.wait_idle(b.id))


async def test_no_worktree_before_the_orchestrator_sends_the_repository(tmp_path):
    repo = Repo(tmp_path / ".repo")
    with pytest.raises(GitError, match="has not sent"):
        await repo.worktree(tmp_path / "T", "T")


async def test_a_thread_sees_its_worktree_not_the_others(tmp_path):
    """Other threads' worktrees and the repository's checkout are hidden; the
    thread can still commit, since its worktree's git dir is in the view."""
    source(tmp_path)
    h = await host(tmp_path, {"a": [ModelResponse("ok")], "b": [ModelResponse("ok")]})
    a = await h.start(project="p", title="a", request="x")
    b = await h.start(project="p", title="b", request="y")
    await asyncio.gather(h.wait_idle(a.id), h.wait_idle(b.id))
    threads = tmp_path / "threads"
    r = await a.sandbox.exec(f"ls -A {threads}; echo ---; ls -A {threads / '.repo'}; echo ---; "
                             "echo hi > mine.txt && git add mine.txt && git commit -qm mine && git log --format=%s -1")
    listed, clone, log = r.stdout.split("---\n")
    assert b.id not in listed.split() and a.id in listed.split()
    assert clone.split() == [".git"]
    assert log.strip() == "mine", r.stderr


async def test_turn_end_commits_leftovers_and_snapshots_the_worktree(tmp_path):
    source(tmp_path)
    h = await host(tmp_path, {"t": [
        ModelResponse("", [call("bash", command="printf 'build/\\n' > .gitignore; mkdir -p build; "
                                                "echo bin > build/app; echo draft > new.txt")]),
        ModelResponse("stopping here"),
    ]})
    t = await h.start(project="p", title="t", request="draft")
    await h.wait_idle(t.id)
    assert "saved" in types(t) and "landed" not in types(t) and "error" not in types(t)
    repo = h.repo.path
    assert git(repo, "show", f"{t.branch}:new.txt") == "draft"
    assert "build" not in git(repo, "ls-tree", "--name-only", t.branch).split()
    snap = f"refs/snapshots/{t.id}"
    assert git(repo, "show", f"{snap}:build/app") == "bin"
    assert git(repo, "rev-parse", f"{snap}^") == git(repo, "rev-parse", t.branch)
    assert "new.txt" not in git(repo, "ls-tree", "--name-only", "origin/main")


async def test_finish_lands_concurrent_threads_linearly(tmp_path):
    source(tmp_path)
    h = await host(tmp_path, {
        "a": [ModelResponse("", [call("write_file", path="a.txt", content="a\n")]),
              ModelResponse("", [call("finish", summary="added a")])],
        "b": [ModelResponse("", [call("bash", command="echo b > b.txt && git add b.txt && git commit -qm 'add b'")]),
              ModelResponse("", [call("finish", summary="added b")])],
    })
    a = await h.start(project="p", title="a", request="x")
    b = await h.start(project="p", title="b", request="y")
    await asyncio.gather(h.wait_idle(a.id), h.wait_idle(b.id))
    assert "landed" in types(a) and "landed" in types(b)
    repo = h.repo.path
    assert set(git(repo, "ls-tree", "--name-only", "origin/main").split()) == {"notes.txt", "a.txt", "b.txt"}
    assert git(repo, "rev-list", "--merges", "--count", "origin/main") == "0"
    assert "add b" in git(repo, "log", "--format=%s", "origin/main")
    assert "Thread: " in git(repo, "log", "--format=%B", "origin/main")


async def test_conflict_goes_back_to_the_thread(tmp_path):
    source(tmp_path)
    fix = ("git rebase origin/main; printf 'one\\nTWO-b\\nthree\\n' > notes.txt && "
           "git add notes.txt && git rebase --continue")
    h = await host(tmp_path, {
        "a": [ModelResponse("", [call("edit_file", path="notes.txt", old="two", new="TWO-a")]),
              ModelResponse("", [call("finish", summary="a edits two")])],
        "b": [ModelResponse("", [call("edit_file", path="notes.txt", old="two", new="TWO-b")]),
              ModelResponse("", [call("bash", command="sleep 1")]),  # let a land first
              ModelResponse("", [call("finish", summary="b edits two")]),
              ModelResponse("", [call("bash", command=fix)]),
              ModelResponse("", [call("finish", summary="b resolved")])],
    })
    a = await h.start(project="p", title="a", request="x")
    b = await h.start(project="p", title="b", request="y")
    await h.wait_idle(a.id)
    for _ in range(300):
        if "landed" in types(b):
            break
        await asyncio.sleep(0.02)
    conflict = next(e["data"] for e in b.events if e["type"] == "land_conflict")
    assert conflict["files"] == ["notes.txt"] and conflict["base"] == "main"
    assert any(e["type"] == "message_in" and e["data"]["from"] == "runner" for e in b.events)
    assert "landed" in types(b)
    repo = h.repo.path
    assert git(repo, "show", "origin/main:notes.txt").splitlines()[1] == "TWO-b"
    assert git(repo, "rev-list", "--merges", "--count", "origin/main") == "0"


async def test_the_orchestrator_takes_the_branch_and_lands_it_in_its_hub(tmp_path):
    """Under an orchestrator `save` is a peer call: here a stand-in hub (a bare
    repository) takes the branch and snapshot over a bundle, rebases the
    branch onto its main, and sends both back; the worktree follows."""
    src = source(tmp_path)
    hub = tmp_path / "hub.git"
    git(tmp_path, "clone", "-q", "--bare", str(src), str(hub))
    outbox = PeerOutbox()
    repo = Repo(tmp_path / "threads" / ".repo")
    h = ThreadHost(model_factory=lambda spec: ScriptedModel([
        ModelResponse("", [call("bash", command="echo x > x.txt; printf 'out/\\n' > .gitignore; "
                                                "mkdir out; echo o > out/o")]),
        ModelResponse("", [call("finish", summary="added x")])]),
        peers=OrchestratorPeers(outbox), workdir=str(tmp_path / "threads"), repo=repo)

    async def ship():
        tip = git(hub, "rev-parse", "main")
        data = subprocess.run(["git", "bundle", "create", "-", "main"], cwd=hub, capture_output=True).stdout
        await repo.receive(data, "main", {"refs/remotes/origin/main": tip})

    # The other side lands a commit first, so the thread's branch is rebased.
    other = tmp_path / "other"
    git(tmp_path, "clone", "-q", str(hub), str(other))
    (other / "y.txt").write_text("y\n")
    git(other, "add", "-A")
    git(other, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "-m", "y")
    await ship()
    git(other, "push", "-q", "origin", "HEAD:main")

    async def orchestrator():
        while True:
            for c in await outbox.take(1):
                if c["op"] != "save":
                    outbox.answer(c["id"], {"ok": True, "result": None})
                    continue
                tid = c["args"]["thread_id"]
                data, refs = await repo.send(tid, [git(hub, "rev-parse", "main")])
                (tmp_path / "in.bundle").write_bytes(data)
                git(hub, "bundle", "unbundle", str(tmp_path / "in.bundle"))
                for ref, sha in refs.items():
                    git(hub, "update-ref", ref, sha)
                wt = tmp_path / "landing"
                git(hub, "worktree", "add", "-q", "--detach", str(wt), f"refs/heads/thread/{tid}")
                git(wt, "-c", "user.email=a@b", "-c", "user.name=a", "rebase", "-q", "main")
                new = git(wt, "rev-parse", "HEAD")
                git(hub, "worktree", "remove", "--force", str(wt))
                git(hub, "update-ref", "refs/heads/main", new)
                git(hub, "update-ref", f"refs/heads/thread/{tid}", new)
                data = subprocess.run(["git", "bundle", "create", "-", "main"], cwd=hub,
                                      capture_output=True).stdout
                await repo.receive(data, "main", {"refs/remotes/origin/main": new,
                                                  f"refs/remotes/origin/thread/{tid}": new})
                outbox.answer(c["id"], {"ok": True, "result": {"landed": True, "base": "main", "commit": new}})

    o = asyncio.create_task(orchestrator())
    try:
        t = await h.start(project="p", title="t", request="x")
        await h.wait_idle(t.id)
    finally:
        o.cancel()
    assert "landed" in types(t) and "error" not in types(t), t.events
    assert set(git(hub, "ls-tree", "--name-only", "main").split()) == {"notes.txt", "y.txt", "x.txt", ".gitignore"}
    assert git(hub, "show", f"refs/snapshots/{t.id}:out/o") == "o"
    wt = tmp_path / "threads" / t.id
    assert git(wt, "rev-parse", "HEAD") == git(hub, "rev-parse", "main")
    assert (wt / "y.txt").exists() and (wt / "out" / "o").read_text() == "o\n"


async def test_resumed_thread_continues_on_a_new_worker_with_its_files(tmp_path):
    source(tmp_path)
    first = ThreadHost(model_factory=lambda spec: ScriptedModel([
        ModelResponse("", [call("bash", command="echo v1 > draft.txt; printf 'build/\\n' > .gitignore; "
                                                "mkdir build; echo bin > build/app")]),
        ModelResponse("saved a draft")]),
        workdir=str(tmp_path / "w1"), repo=await seeded(tmp_path, "w1"))
    t = await first.start(project="p", title="t", request="draft", id="t_resume")
    await first.wait_idle(t.id)
    # The first worker is gone; another one picks the thread up with its events.
    model = ScriptedModel([ModelResponse("still here")])
    second = ThreadHost(model_factory=lambda spec: model, workdir=str(tmp_path / "w2"),
                        repo=await seeded(tmp_path, "w2"))
    await move(first.repo, second.repo, "t_resume")
    r = await second.start(project="p", title="t", id="t_resume", history=list(t.events))
    assert r.state == "idle" and (await r.sandbox.exec("cat draft.txt")).stdout == "v1\n"
    assert (await r.sandbox.exec("cat build/app")).stdout == "bin\n"  # ignored, from the snapshot
    assert (await r.sandbox.exec("git status --porcelain")).stdout == ""
    assert (await r.sandbox.exec("git branch --show-current")).stdout.strip() == "thread/t_resume"
    second.deliver(r.id, "and now?")
    await second.wait_idle(r.id)
    roles = [m["role"] for m in model.seen[0]]
    assert roles == ["system", "user", "assistant", "tool", "assistant", "user"]
    assert model.seen[0][-1]["content"] == "and now?"


async def test_stopped_thread_restarts_in_the_worktree_it_had(tmp_path):
    source(tmp_path)
    h = await host(tmp_path, {"a": [ModelResponse("", [call("bash", command="echo wip > wip.txt; sleep 30")])],
                              "b": [ModelResponse("back")]})
    t = await h.start(project="p", title="t", request="x", id="t_stop")
    for _ in range(300):
        if (tmp_path / "threads" / "t_stop" / "wip.txt").exists():
            break
        await asyncio.sleep(0.02)
    await h.stop(t.id)
    r = await h.start(project="p", title="t", id="t_stop", history=list(t.events))
    assert r is not t and (await r.sandbox.exec("cat wip.txt")).stdout == "wip\n"  # never saved, still here
    h.deliver(r.id, "go on")
    await h.wait_idle(r.id)
    assert r.model.seen[0][3] == {"role": "tool", "tool_call_id": "c_bash",
                                  "content": "error: the thread stopped before this tool returned"}


async def test_worktree_left_behind_by_a_thread_that_moved_on_is_made_again(tmp_path):
    source(tmp_path)
    old = await seeded(tmp_path, "w1")
    ws = await old.worktree(tmp_path / "w1" / "T", "T")
    (ws.path / "a.txt").write_text("a\n")
    await ws.commit("a")
    await ws.snapshot()
    # The thread went on on another worker, which saved more.
    other = await seeded(tmp_path, "w2")
    await move(old, other, "T")
    ws2 = await other.worktree(tmp_path / "w2" / "T", "T")
    (ws2.path / "b.txt").write_text("b\n")
    await ws2.commit("b")
    await ws2.snapshot()
    await move(other, old, "T")
    back = await old.worktree(tmp_path / "w1" / "T", "T")
    assert sorted(p.name for p in back.path.iterdir() if p.name != ".git") == ["a.txt", "b.txt", "notes.txt"]


async def test_worktree_that_holds_what_the_hub_has_keeps_its_ignored_files(tmp_path):
    source(tmp_path)
    repo = await seeded(tmp_path, "w1")
    ws = await repo.worktree(tmp_path / "w1" / "T", "T")
    (ws.path / ".gitignore").write_text("build/\n")
    await ws.commit("ignore build")
    await ws.snapshot()
    await move(repo, repo, "T")
    (ws.path / "build").mkdir()
    (ws.path / "build" / "app").write_text("binary\n")
    again = await repo.worktree(tmp_path / "w1" / "T", "T")
    assert (again.path / "build" / "app").read_text() == "binary\n"


async def test_bundles_leave_out_what_the_other_side_has(tmp_path):
    source(tmp_path)
    repo = await seeded(tmp_path)
    ws = await repo.worktree(tmp_path / "threads" / "T", "T")
    await ws.commit("nothing new")
    await ws.snapshot()
    data, refs = await repo.send("T", list(refs_of(repo).values()))
    assert set(refs) == {"refs/heads/thread/T", "refs/snapshots/T"}
    # The snapshot is new (an empty tree change still makes a commit), so a bundle comes.
    assert data
    data, _ = await repo.send("T", list(refs.values()))
    assert data == b""


def refs_of(repo):
    out = git(repo.path, "for-each-ref", "--format=%(refname) %(objectname)", "refs/remotes")
    return dict(line.split(" ", 1) for line in out.splitlines())


async def test_the_orchestrator_may_set_remote_tracking_refs_only(tmp_path):
    source(tmp_path)
    repo = await seeded(tmp_path)
    tip = git(repo.path, "rev-parse", "origin/main")
    with pytest.raises(GitError, match="may not set"):
        await repo.receive(b"", "main", {"refs/heads/main": tip})
    await repo.receive(b"", "main", {"refs/remotes/origin/thread/x": tip})
    assert git(repo.path, "rev-parse", "origin/thread/x") == tip
