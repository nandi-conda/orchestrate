"""Threads' commits: worktrees off origin's default branch, saved every turn, landed on finish."""

import asyncio
import base64
import os
import subprocess

from runner import ModelResponse, ScriptedModel, ThreadHost, ToolCall
from runner.repo import Repo
from runner.server import repo_from_env


def call(name, **args):
    return ToolCall(id=f"c_{name}", name=name, arguments=args)


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def origin(tmp_path):
    """A bare `origin` whose default branch `main` holds notes.txt."""
    bare, seed = tmp_path / "origin.git", tmp_path / "seed"
    git(tmp_path, "init", "-q", "--bare", "-b", "main", str(bare))
    git(tmp_path, "clone", "-q", str(bare), str(seed))
    (seed / "notes.txt").write_text("one\ntwo\nthree\n")
    git(seed, "add", "-A")
    git(seed, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "-m", "init")
    git(seed, "push", "-q", "origin", "HEAD:main")
    return bare


def host(tmp_path, bare, scripts):
    pending, order = dict(scripts), list(scripts)
    return ThreadHost(model_factory=lambda spec: ScriptedModel(pending.pop(order.pop(0))),
                      workdir=str(tmp_path / "threads"), repo=Repo(str(bare), tmp_path / "threads" / ".repo"))


def types(t):
    return [e["type"] for e in t.events]


async def test_worktree_per_thread_off_default_branch(tmp_path):
    bare = origin(tmp_path)
    h = host(tmp_path, bare, {"a": [ModelResponse("ok")], "b": [ModelResponse("ok")]})
    a = await h.start(project="p", title="a", request="x")
    b = await h.start(project="p", title="b", request="y")
    assert a.sandbox.root != b.sandbox.root and a.branch == f"thread/{a.id}"
    assert (await a.sandbox.exec("git branch --show-current")).stdout.strip() == a.branch
    assert (await a.sandbox.exec("cat notes.txt")).stdout.startswith("one")
    await asyncio.gather(h.wait_idle(a.id), h.wait_idle(b.id))


async def test_turn_end_commits_leftovers_and_pushes_branch(tmp_path):
    bare = origin(tmp_path)
    h = host(tmp_path, bare, {"t": [
        ModelResponse("", [call("write_file", path="new.txt", content="draft\n")]),
        ModelResponse("stopping here"),
    ]})
    t = await h.start(project="p", title="t", request="draft")
    await h.wait_idle(t.id)
    assert "saved" in types(t) and "landed" not in types(t)
    assert git(bare, "show", f"{t.branch}:new.txt") == "draft"
    assert "new.txt" not in git(bare, "ls-tree", "--name-only", "main")


async def test_finish_lands_concurrent_threads_linearly(tmp_path):
    bare = origin(tmp_path)
    h = host(tmp_path, bare, {
        "a": [ModelResponse("", [call("write_file", path="a.txt", content="a\n")]),
              ModelResponse("", [call("finish", summary="added a")])],
        "b": [ModelResponse("", [call("bash", command="echo b > b.txt && git add b.txt && git commit -qm 'add b'")]),
              ModelResponse("", [call("finish", summary="added b")])],
    })
    a = await h.start(project="p", title="a", request="x")
    b = await h.start(project="p", title="b", request="y")
    await asyncio.gather(h.wait_idle(a.id), h.wait_idle(b.id))
    assert "landed" in types(a) and "landed" in types(b)
    assert set(git(bare, "ls-tree", "--name-only", "main").split()) == {"notes.txt", "a.txt", "b.txt"}
    assert git(bare, "rev-list", "--merges", "--count", "main") == "0"
    assert "add b" in git(bare, "log", "--format=%s", "main")
    assert "Thread: " in git(bare, "log", "--format=%B", "main")


async def test_conflict_goes_back_to_the_thread(tmp_path):
    bare = origin(tmp_path)
    fix = ("git rebase origin/main; printf 'one\\nTWO-b\\nthree\\n' > notes.txt && "
           "git add notes.txt && git rebase --continue")
    h = host(tmp_path, bare, {
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
    assert git(bare, "show", "main:notes.txt").splitlines()[1] == "TWO-b"
    assert git(bare, "rev-list", "--merges", "--count", "main") == "0"


async def test_token_reaches_git_only(tmp_path, monkeypatch):
    monkeypatch.setenv("RUNNER_REPO", "https://github.com/acme/app.git")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_x")
    repo = repo_from_env(str(tmp_path))
    assert "GITHUB_TOKEN" not in os.environ
    assert repo.env["GIT_CONFIG_KEY_0"] == "http.https://github.com/acme/app.git.extraHeader"
    assert repo.env["GIT_CONFIG_VALUE_0"] == "Authorization: Basic " + base64.b64encode(b"x-access-token:ghp_x").decode()
    assert "GITHUB_TOKEN" not in repo.env


async def test_resumed_thread_continues_its_pushed_branch_on_a_new_worker(tmp_path):
    bare = origin(tmp_path)
    first = ThreadHost(model_factory=lambda spec: ScriptedModel([
        ModelResponse("", [call("write_file", path="draft.txt", content="v1\n")]),
        ModelResponse("saved a draft")]),
        workdir=str(tmp_path / "w1"), repo=Repo(str(bare), tmp_path / "w1" / ".repo"))
    t = await first.start(project="p", title="t", request="draft", id="t_resume")
    await first.wait_idle(t.id)
    # The first worker is gone; another one picks the thread up with its events.
    model = ScriptedModel([ModelResponse("still here")])
    second = ThreadHost(model_factory=lambda spec: model, workdir=str(tmp_path / "w2"),
                        repo=Repo(str(bare), tmp_path / "w2" / ".repo"))
    r = await second.start(project="p", title="t", id="t_resume", history=list(t.events))
    assert r.state == "idle" and (await r.sandbox.exec("cat draft.txt")).stdout == "v1\n"
    assert (await r.sandbox.exec("git branch --show-current")).stdout.strip() == "thread/t_resume"
    second.deliver(r.id, "and now?")
    await second.wait_idle(r.id)
    roles = [m["role"] for m in model.seen[0]]
    assert roles == ["system", "user", "assistant", "tool", "assistant", "user"]
    assert model.seen[0][-1]["content"] == "and now?"


async def test_stopped_thread_restarts_in_the_worktree_it_had(tmp_path):
    bare = origin(tmp_path)
    h = host(tmp_path, bare, {"a": [ModelResponse("", [call("bash", command="echo wip > wip.txt; sleep 30")])],
                              "b": [ModelResponse("back")]})
    t = await h.start(project="p", title="t", request="x", id="t_stop")
    for _ in range(300):
        if (tmp_path / "threads" / "t_stop" / "wip.txt").exists():
            break
        await asyncio.sleep(0.02)
    await h.stop(t.id)
    r = await h.start(project="p", title="t", id="t_stop", history=list(t.events))
    assert r is not t and (await r.sandbox.exec("cat wip.txt")).stdout == "wip\n"  # never pushed, still here
    h.deliver(r.id, "go on")
    await h.wait_idle(r.id)
    assert r.model.seen[0][3] == {"role": "tool", "tool_call_id": "c_bash",
                                  "content": "error: the thread stopped before this tool returned"}
