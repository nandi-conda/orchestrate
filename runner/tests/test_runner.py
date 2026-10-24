import asyncio
import subprocess

import httpx
import pytest

from runner import LocalSandbox, ModelResponse, ScriptedModel, ThreadHost, ToolCall
from runner.server import create_app


def call(name, **args):
    return ToolCall(id=f"c_{name}", name=name, arguments=args)


def host_with(scripts: dict, tmp_path, repo=None):
    """scripts maps thread title -> list of responses; one ScriptedModel per thread."""
    pending = dict(scripts)
    order = list(scripts)

    def factory(spec):
        return ScriptedModel(pending.pop(order.pop(0)))

    return ThreadHost(model_factory=factory, workdir=str(tmp_path / "threads"), repo=repo)


async def test_sandbox_confines_paths(tmp_path):
    sb = LocalSandbox(tmp_path / "ws")
    await sb.write_file("a/b.txt", b"hi")
    assert await sb.read_file("a/b.txt") == b"hi"
    with pytest.raises(Exception):
        await sb.read_file("../outside")
    r = await sb.exec("echo $((2+3)) && ls a")
    assert r.exit_code == 0 and "5" in r.stdout and "b.txt" in r.stdout
    r = await sb.exec("sleep 5", timeout=0.2)
    assert r.timed_out


async def test_thread_uses_tools_and_finishes(tmp_path):
    host = host_with({"t": [
        ModelResponse("", [call("write_file", path="hello.py", content="print('hi')\n")]),
        ModelResponse("", [call("bash", command="python3 hello.py")]),
        ModelResponse("done", [call("finish", summary="added hello.py")]),
    ]}, tmp_path)
    t = await host.start(project="p", title="t", request="add hello.py")
    await host.wait_idle(t.id)
    types = [e["type"] for e in t.events]
    assert "turn_done" in types and t.state == "idle"
    results = [e["data"]["result"] for e in t.events if e["type"] == "tool_result"]
    assert "hi" in results[1]
    assert t.summary == "added hello.py" and "hello.py" in t.files


async def test_worktree_per_thread(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    for cmd in (["git", "init", "-q"], ["git", "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "--allow-empty", "-m", "init"]):
        subprocess.run(cmd, cwd=repo, check=True)
    host = host_with({"a": [ModelResponse("ok")], "b": [ModelResponse("ok")]}, tmp_path, repo=str(repo))
    a = await host.start(project="p", title="a", request="x")
    b = await host.start(project="p", title="b", request="y")
    assert a.sandbox.root != b.sandbox.root and a.branch == f"thread/{a.id}"
    r = await a.sandbox.exec("git branch --show-current")
    assert r.stdout.strip() == a.branch


async def test_spawn_and_child_reports_back(tmp_path):
    host = host_with({
        "parent": [
            ModelResponse("", [call("spawn_thread", title="child", request="do the child work")]),
            ModelResponse("waiting for child", [call("finish", summary="spawned child")]),
            ModelResponse("child reported", []),
        ],
        "child": [ModelResponse("", [call("finish", summary="child work done")])],
    }, tmp_path)
    p = await host.start(project="p", title="parent", request="split this")
    for _ in range(100):
        if len(host.threads) == 2:
            break
        await asyncio.sleep(0.02)
    child = next(t for t in host.threads.values() if t.id != p.id)
    await host.wait_idle(child.id)
    await host.wait_idle(p.id)
    for _ in range(100):  # parent's second turn, triggered by the child's report
        if sum(e["type"] == "turn_done" for e in p.events) >= 2:
            break
        await asyncio.sleep(0.02)
    ins = [e["data"] for e in p.events if e["type"] == "message_in"]
    assert any(m["from"] == child.id and "child work done" in m["body"] for m in ins)
    assert child.parent == p.id


async def test_peer_messages_and_directory(tmp_path):
    host = host_with({
        "a": [ModelResponse("", [call("set_summary", summary="owns the export page")]), ModelResponse("ok")],
        "b": [ModelResponse("", [call("list_threads")]), ModelResponse("", [call("send_message", to="PLACEHOLDER", body="hello")]), ModelResponse("ok")],
    }, tmp_path)
    a = await host.start(project="p", title="a", request="x")
    await host.wait_idle(a.id)
    # point b's message at a
    b = await host.start(project="p", title="b", request="y")
    b.model.responses[1].tool_calls[0].arguments["to"] = a.id  # start() has not yielded yet
    b.model.responses[1].message = {}
    await host.wait_idle(b.id)
    listed = [e["data"]["result"] for e in b.events if e["type"] == "tool_result"][0]
    assert "owns the export page" in listed
    await asyncio.sleep(0.1)
    assert any(e["type"] == "message_in" and e["data"]["from"] == b.id for e in a.events)


async def test_http_api(tmp_path):
    host = host_with({"hello": [ModelResponse("hi there")]}, tmp_path)
    app = create_app(host, token="secret")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w") as c:
        assert (await c.get("/health")).json()["capacity"] == 16
        assert (await c.post("/threads", json={"request": "hi"})).status_code == 401
        h = {"Authorization": "Bearer secret"}
        r = await c.post("/threads", json={"request": "say hi", "title": "hello", "project": "p"}, headers=h)
        assert r.status_code == 201
        tid = r.json()["id"]
        await host.wait_idle(tid)
        threads = (await c.get("/threads", headers=h)).json()
        assert threads[0]["id"] == tid and threads[0]["state"] == "idle"
        assert (await c.post(f"/threads/{tid}/messages", json={"body": "again"}, headers=h)).status_code == 202


def test_secrets_env_then_keyring_then_exit(monkeypatch, capsys):
    from runner import secrets
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.setattr(secrets, "_from_keyring", lambda n: "from-ring" if n == "DEEPSEEK_API_KEY" else None)
    assert secrets.get_secret("DEEPSEEK_API_KEY") == "from-ring"
    monkeypatch.setenv("DEEPSEEK_API_KEY", "from-env")
    assert secrets.get_secret("DEEPSEEK_API_KEY") == "from-env"
    monkeypatch.delenv("RUNNER_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        secrets.require(["DEEPSEEK_API_KEY", "RUNNER_TOKEN"])
    err = capsys.readouterr().err
    assert "RUNNER_TOKEN" in err and "DEEPSEEK_API_KEY" not in err.split("\n\n")[1]
