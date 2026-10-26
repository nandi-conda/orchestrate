"""Threads' commits: worktrees off the base branch, snapshots, bundles to and from the hub, landing."""

import asyncio
import subprocess

import pytest
from conftest import Scripted, Web, in_view, py, sh, started, types, until

from runner import ThreadHost
from runner.peers import OrchestratorPeers, PeerOutbox
from runner.repo import GitError, Repo


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


async def host(tmp_path, *scripts, name="threads", **kw):
    return ThreadHost(agent=Scripted(*scripts), web=Web(), workdir=str(tmp_path / name),
                      repo=await seeded(tmp_path, name), **kw)


async def run(t, command):
    """A command in the thread's sandbox, once it is up (its team socket is in it)."""
    await started(t)
    return in_view(t.place.view, command)


async def stop_all(h):
    for tid in list(h.threads):
        await h.stop(tid)


async def move(src: Repo, dst: Repo, tid: str):
    """What the orchestrator does when a thread goes to another worker: take
    its branch, snapshot and session from one worker, bring them to the other."""
    data, refs = await src.send(tid, [])
    names = {f"refs/heads/thread/{tid}": f"refs/remotes/origin/thread/{tid}",
             f"refs/snapshots/{tid}": f"refs/remotes/origin-snapshots/{tid}",
             f"refs/sessions/{tid}": f"refs/remotes/origin-sessions/{tid}"}
    await dst.receive(data, dst.base, {names[k]: v for k, v in refs.items()})


async def test_worktree_per_thread_off_the_base_branch(tmp_path):
    source(tmp_path)
    h = await host(tmp_path, ["ok"], ["ok"])
    a = await h.start(project="p", title="a", request="x")
    b = await h.start(project="p", title="b", request="y")
    assert a.root != b.root and a.branch == f"thread/{a.id}"
    assert (await run(a, "git branch --show-current")).stdout.strip() == a.branch
    assert (await run(a, "cat notes.txt")).stdout.startswith("one")
    await asyncio.gather(h.wait_idle(a.id), h.wait_idle(b.id))
    await stop_all(h)


async def test_no_worktree_before_the_orchestrator_sends_the_repository(tmp_path):
    repo = Repo(tmp_path / ".repo")
    with pytest.raises(GitError, match="has not sent"):
        await repo.worktree(tmp_path / "T", "T")


async def test_a_thread_sees_its_worktree_not_the_others(tmp_path):
    """Other threads' worktrees and the repository's checkout are hidden; the
    thread can still commit, since its worktree's git dir is in the view."""
    source(tmp_path)
    h = await host(tmp_path, ["ok"], ["ok"])
    a = await h.start(project="p", title="a", request="x")
    b = await h.start(project="p", title="b", request="y")
    await asyncio.gather(h.wait_idle(a.id), h.wait_idle(b.id))
    threads = tmp_path / "threads"
    r = (await run(a, f"ls -A {threads}; echo ---; ls -A {threads / '.repo'}; echo ---; "
               "echo hi > mine.txt && git add mine.txt && git commit -qm mine && git log --format=%s -1"))
    listed, clone, log = r.stdout.split("---\n")
    assert b.id not in listed.split() and a.id in listed.split()
    assert clone.split() == [".git"]
    assert log.strip() == "mine", r.stderr
    await stop_all(h)


async def test_turn_end_commits_leftovers_snapshots_the_worktree_and_lands(tmp_path):
    source(tmp_path)
    h = await host(tmp_path, [sh("printf 'build/\\n' > .gitignore; mkdir -p build; "
                                 "echo bin > build/app; echo draft > new.txt"), "stopping here"])
    t = await h.start(project="p", title="t", request="draft")
    await h.wait_idle(t.id)
    await h.stop(t.id)
    assert "landed" in types(t) and "saved" not in types(t) and "error" not in types(t), t.events
    assert sorted(t.files) == [".gitignore", "new.txt"]
    repo = h.repo.path
    assert git(repo, "show", f"{t.branch}:new.txt") == "draft"
    assert "build" not in git(repo, "ls-tree", "--name-only", t.branch).split()
    snap = f"refs/snapshots/{t.id}"
    assert git(repo, "show", f"{snap}:build/app") == "bin"
    assert git(repo, "rev-parse", f"{snap}^") == git(repo, "rev-parse", t.branch)
    assert git(repo, "show", "origin/main:new.txt") == "draft"
    assert "build" not in git(repo, "ls-tree", "--name-only", "origin/main").split()
    session = f"refs/sessions/{t.id}"
    files = git(repo, "ls-tree", "-r", "--name-only", session).splitlines()
    assert any(f.startswith("sessions/") and f.endswith(".jsonl") for f in files), files
    assert '"draft"' in git(repo, "show", f"{session}:{next(f for f in files if f.endswith('.jsonl'))}")


async def test_a_turn_a_model_call_failed_in_is_saved_not_landed(tmp_path):
    source(tmp_path)
    h = await host(tmp_path, [sh("echo half > half.txt"),
                              {"content": [{"type": "text", "text": ""}], "stopReason": "error",
                               "errorMessage": "HTTP 402: Insufficient Balance"}])
    t = await h.start(project="p", title="t", request="draft")
    await h.wait_idle(t.id)
    await h.stop(t.id)
    assert "saved" in types(t) and "landed" not in types(t), t.events
    assert git(h.repo.path, "show", f"{t.branch}:half.txt") == "half"
    assert "half.txt" not in git(h.repo.path, "ls-tree", "--name-only", "origin/main")


async def test_every_turn_lands_concurrent_threads_linearly(tmp_path):
    source(tmp_path)
    h = await host(tmp_path,
                   [py("open('a.txt', 'w').write('a\\n')"), py("await team.set_summary('added a')"), "done"],
                   [sh("echo b > b.txt && git add b.txt && git commit -qm 'add b'"),
                    py("await team.set_summary('added b')"), "done"])
    a = await h.start(project="p", title="a", request="x")
    b = await h.start(project="p", title="b", request="y")
    await asyncio.gather(h.wait_idle(a.id), h.wait_idle(b.id))
    await stop_all(h)
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
    h = await host(tmp_path,
                   [py("await edit('notes.txt', 'two', 'TWO-a')"), py("await team.set_summary('a edits two')"), "done"],
                   [py("await edit('notes.txt', 'two', 'TWO-b')"),
                    sh("sleep 2"),  # let a land first
                    py("await team.set_summary('b edits two')"), "done",
                    sh(fix), py("await team.set_summary('b resolved')"), "done"])
    a = await h.start(project="p", title="a", request="x")
    b = await h.start(project="p", title="b", request="y")
    await h.wait_idle(a.id)
    await until(lambda: "landed" in types(b))
    await stop_all(h)
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
    h = ThreadHost(agent=Scripted([sh("echo x > x.txt; printf 'out/\\n' > .gitignore; mkdir out; echo o > out/o"),
                                   py("await team.set_summary('added x')"), "done"]),
                   web=Web(), peers=OrchestratorPeers(outbox), workdir=str(tmp_path / "threads"), repo=repo)

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
        await h.stop(t.id)
    finally:
        o.cancel()
    assert "landed" in types(t) and "error" not in types(t), t.events
    assert set(git(hub, "ls-tree", "--name-only", "main").split()) == {"notes.txt", "y.txt", "x.txt", ".gitignore"}
    assert git(hub, "show", f"refs/snapshots/{t.id}:out/o") == "o"
    assert git(hub, "rev-parse", "--verify", f"refs/sessions/{t.id}")
    wt = tmp_path / "threads" / t.id
    assert git(wt, "rev-parse", "HEAD") == git(hub, "rev-parse", "main")
    assert (wt / "y.txt").exists() and (wt / "out" / "o").read_text() == "o\n"


async def test_resumed_thread_continues_on_a_new_worker_with_its_files_and_session(tmp_path):
    source(tmp_path)
    first = await host(tmp_path, [sh("echo v1 > draft.txt; printf 'build/\\n' > .gitignore; mkdir build; "
                                     "echo bin > build/app"),
                                  py("note = 'kept in the kernel'"), "saved a draft"], name="w1")
    t = await first.start(project="p", title="t", request="draft", id="t_resume")
    await first.wait_idle(t.id)
    await first.stop(t.id)
    # The first worker is gone; another one picks the thread up from what the hub has.
    second = await host(tmp_path, ["still here"], name="w2")
    await move(first.repo, second.repo, "t_resume")
    r = await second.start(project="p", title="t", id="t_resume")
    assert r.state == "idle" and (await run(r, "cat draft.txt")).stdout == "v1\n"
    assert (await run(r, "cat build/app")).stdout == "bin\n"  # ignored, from the snapshot
    assert (await run(r, "git status --porcelain")).stdout == ""
    assert (await run(r, "git branch --show-current")).stdout.strip() == "thread/t_resume"
    second.deliver(r.id, "and now?")
    await second.wait_idle(r.id)
    messages = (await r.harness.command("get_messages"))["messages"]
    said = [(m["role"], b.get("text")) for m in messages if m["role"] in ("user", "assistant")
            for b in m["content"] if b.get("type") == "text"]
    assert said == [("user", "draft"), ("assistant", "saved a draft"), ("user", "and now?"), ("assistant", "still here")]
    await second.stop(r.id)


async def test_stopped_thread_restarts_in_the_worktree_and_session_it_had(tmp_path):
    source(tmp_path)
    h = await host(tmp_path, [sh("echo wip > wip.txt; sleep 30")], ["back"])
    t = await h.start(project="p", title="t", request="x", id="t_stop")
    await until(lambda: (tmp_path / "threads" / "t_stop" / "wip.txt").exists())
    await h.stop(t.id)
    r = await h.start(project="p", title="t", id="t_stop")
    assert r is not t and (await run(r, "cat wip.txt")).stdout == "wip\n"  # never saved, still here
    h.deliver(r.id, "go on")
    await h.wait_idle(r.id)
    messages = (await r.harness.command("get_messages"))["messages"]
    users = [b["text"] for m in messages if m["role"] == "user" for b in m["content"] if b.get("type") == "text"]
    assert users == ["x", "go on"]
    await h.stop(r.id)


async def test_session_left_behind_by_a_thread_that_moved_on_is_replaced(tmp_path):
    source(tmp_path)
    old = await seeded(tmp_path, "w1")
    d1 = tmp_path / "w1" / "agent"
    (d1 / "sessions").mkdir(parents=True)
    (d1 / "sessions" / "s.jsonl").write_text("one\n")
    (d1 / "skills").mkdir()
    await old.save_session("T", d1, ("sessions", "session-artifacts"))
    other = await seeded(tmp_path, "w2")
    ws = await other.worktree(tmp_path / "w2" / "T", "T")  # the branch, so the bundle has refs to send
    await ws.commit("start")
    await other.receive((await old.send("T", []))[0], other.base, {
        "refs/remotes/origin-sessions/T": git(old.path, "rev-parse", "refs/sessions/T")})
    d2 = tmp_path / "w2" / "agent"
    await other.session("T", d2, ("sessions", "session-artifacts"))
    (d2 / "sessions" / "s.jsonl").write_text("one\ntwo\n")
    await other.save_session("T", d2, ("sessions", "session-artifacts"))
    await old.receive((await other.send("T", []))[0], old.base, {
        "refs/remotes/origin-sessions/T": git(other.path, "rev-parse", "refs/sessions/T")})
    await old.session("T", d1, ("sessions", "session-artifacts"))
    assert (d1 / "sessions" / "s.jsonl").read_text() == "one\ntwo\n"
    assert (d1 / "skills").is_dir()  # the rest of the agent dir is the worker's
    # Up to date: kept as it is.
    (d1 / "sessions" / "s.jsonl").write_text("local\n")
    await old.session("T", d1, ("sessions", "session-artifacts"))
    assert (d1 / "sessions" / "s.jsonl").read_text() == "local\n"


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
    assert set(refs) == {"refs/heads/thread/T", "refs/snapshots/T"}  # no session yet
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
