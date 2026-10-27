import asyncio
import os
import signal
import subprocess
import sys
import types

import httpx
import pytest
import uvicorn
from conftest import Scripted, Web, in_view, py, results, sh, until

from runner import ThreadHost, View
from runner.sandbox import SandboxError, pixi, runner_checkout, worker_private
from runner.server import create_app, serve


def host_with(*scripts, tmp_path, **kw):
    return ThreadHost(agent=Scripted(*scripts), web=Web(), workdir=str(tmp_path / "threads"), **kw)


def view(tmp_path, **kw):
    v = View(root=str(tmp_path / "ws"), home=str(tmp_path / "home"), tmp=str(tmp_path / "tmp"), **kw)
    for d in (v.root, v.home, v.tmp):
        os.makedirs(d, exist_ok=True)
    return v


def test_commands_see_the_workspace_and_nothing_of_the_runner(tmp_path, monkeypatch):
    """The runner's checkout, environment and processes are not in a thread's view."""
    monkeypatch.setenv("RUNNER_TOKEN", "runner-secret")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "model-secret")
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "other-thread.txt").write_text("not yours")
    checkout = str(runner_checkout())
    v = view(tmp_path, hidden=(checkout, sys.prefix, str(tmp_path / "state")))
    (tmp_path / "ws" / "mine.txt").write_text("yours")
    r = in_view(v, "env; echo ---; pwd; cat mine.txt; echo ---; "
                   f"ls -A {checkout}; ls -A {tmp_path / 'state'}; echo ---; "
                   "echo kept > /tmp/scratch; ls /proc | grep -c '^[0-9]'")
    env, here, hidden, procs = r.stdout.split("---\n")
    assert "runner-secret" not in env and "model-secret" not in env and "RUNNER_" not in env
    assert "PIXI" not in env and "CONDA_PREFIX" not in env and sys.prefix not in env
    assert f"HOME={tmp_path / 'home'}" in env
    assert here.split() == [str(tmp_path / "ws"), "yours"]
    assert hidden == ""  # the runner's checkout and the worker's state are empty
    assert int(procs) < 5  # its own pid namespace: the runner is not in /proc
    assert (tmp_path / "tmp" / "scratch").read_text() == "kept\n"
    assert in_view(v, "cat /tmp/scratch").stdout == "kept\n"  # /tmp lasts across commands


def test_readonly_paths_come_back_inside_hidden_ones(tmp_path):
    (tmp_path / "state" / "env" / "bin").mkdir(parents=True)
    (tmp_path / "state" / "env" / "bin" / "tool").write_text("tool\n")
    (tmp_path / "state" / "secret").write_text("no")
    v = view(tmp_path, hidden=(str(tmp_path / "state"),), readonly=(str(tmp_path / "state" / "env"),))
    r = in_view(v, f"ls -A {tmp_path / 'state'}; cat {tmp_path / 'state/env/bin/tool'}; "
                   f"touch {tmp_path / 'state/env/x'} 2>/dev/null || echo read-only")
    assert r.stdout.split() == ["env", "tool", "read-only"], r.stderr


def test_commands_get_pixi_read_only_with_its_cache_in_the_thread_home(tmp_path):
    """Every project is a pixi project: a thread's commands run the pixi the
    runner runs under, from wherever the worker has it (its home, hidden), and
    keep what pixi makes in the thread's own home."""
    v = view(tmp_path, hidden=worker_private(tmp_path / "threads"), tools=(pixi(),))
    r = in_view(v, "command -v pixi; ls -A \"$(dirname \"$(command -v pixi)\")\"; "
                   "pixi info --json | python3 -c 'import json, sys; print(json.load(sys.stdin)[\"cache_dir\"])'; "
                   "touch \"$(command -v pixi)\" 2>/dev/null || echo read-only")
    assert r.stdout.split() == [pixi(), "pixi", f"{tmp_path / 'home'}/.cache/rattler/cache", "read-only"], r.stderr


async def test_a_thread_runs_pixi_in_its_workspace(tmp_path):
    host = host_with([sh("pixi init . 2>/dev/null && pixi task add hello 'echo hello from pixi' 2>/dev/null && "
                         "pixi task list --machine-readable"), "done"], tmp_path=tmp_path)
    t = await host.start(project="p", title="t", request="make it a pixi project")
    await host.wait_idle(t.id)
    assert results(t)[0].split() == ["0", "hello"], results(t)
    assert (tmp_path / "threads" / t.id / "pixi.toml").exists()
    await host.stop(t.id)


async def test_a_workdir_too_deep_for_a_thread_socket_is_refused(tmp_path):
    """A thread's team socket is a file under the worker's workdir, and a unix
    socket address is at most 107 bytes: a workdir that deep is refused up
    front, not left to fail as an OSError in the thread's start."""
    host = ThreadHost(agent=Scripted(), web=Web(), workdir=str(tmp_path / ("d" * 100)))
    with pytest.raises(SandboxError, match="longer than a unix socket address"):
        await host.start(project="p", title="t", request="x")


async def test_the_kernel_runs_in_the_sandbox_with_no_secrets(tmp_path, monkeypatch):
    """Everything the model runs is in the sandbox: the kernel's environment
    has none of the runner's secrets (the binary holds the model's key), and
    the runner's checkout and other threads are not there."""
    monkeypatch.setenv("RUNNER_TOKEN", "runner-secret")
    host = ThreadHost(agent=Scripted([py("import os, sys\n"
                                         "print(sorted(os.environ))\n"
                                         "print(os.environ.get('DEEPSEEK_API_KEY'), os.environ.get('RUNNER_TOKEN'))\n"
                                         f"print(os.path.exists({str(runner_checkout() / 'runner' / 'src')!r}), "
                                         f"os.path.exists({str(runner_checkout() / 'runner' / 'pixi.toml')!r}))\n"
                                         "print(sys.prefix, len([p for p in os.listdir('/proc') if p.isdigit()]))"),
                                      "done"]),
                      web=Web(), workdir=str(tmp_path / "threads"))
    object.__setattr__(host.agent, "env", {"DEEPSEEK_API_KEY": "model-secret"})
    t = await host.start(project="p", title="t", request="look around")
    await host.wait_idle(t.id)
    out = results(t)[0]
    assert "model-secret" not in out and "runner-secret" not in out, out
    lines = out.splitlines()
    # Only the kernel's environment is there of the checkout, read-only.
    assert lines[1] == "None None" and lines[2] == "False False", out
    prefix, procs = lines[3].split()
    assert prefix == str(tmp_path / "threads" / ".home" / t.id / ".venv") and int(procs) < 10
    await host.stop(t.id)


async def test_thread_runs_python_and_sets_its_summary(tmp_path):
    host = host_with([
        py("open('hello.py', 'w').write(\"print('hi')\\n\")"),
        sh("python3 hello.py"),
        py("await team.set_summary('added hello.py')", text="done"),
        "all done",
    ], tmp_path=tmp_path)
    t = await host.start(project="p", title="t", request="add hello.py")
    await host.wait_idle(t.id)
    assert "turn_done" in [e["type"] for e in t.events] and t.state == "idle"
    assert results(t)[1].strip() == "0 hi"
    assert t.summary == "added hello.py"
    await host.stop(t.id)


async def test_thread_streams_deltas_before_the_final_events(tmp_path):
    host = host_with([py("print(1 + 1)", text="Let me add."), "all done"], tmp_path=tmp_path)
    t = await host.start(project="p", title="t", request="add")
    await host.wait_idle(t.id)
    kinds = [(e["type"], (e["data"] or {}).get("kind")) for e in t.events]
    first_call = kinds.index(("tool_call", None))
    assert kinds.index(("delta", "content")) < kinds.index(("assistant", None)) < first_call
    assert kinds.index(("delta", "tool")) < first_call
    tool_deltas = [e["data"] for e in t.events if e["type"] == "delta" and e["data"]["kind"] == "tool"]
    assert tool_deltas[0] == {"kind": "tool", "name": "python_repl", "text": ""}
    assert "print(1 + 1)" in "".join(d["text"] for d in tool_deltas).replace("\\n", "\n")
    call = next(e["data"] for e in t.events if e["type"] == "tool_call")
    assert call["name"] == "python_repl" and call["arguments"] == {"code": "print(1 + 1)"} and call["step"] == 0
    assert results(t) == ["2\n"]
    content = [e for e in t.events if e["type"] == "delta" and e["data"]["kind"] == "content"]
    assert content[-1]["data"]["text"].endswith("done")
    assert any(e["type"] == "usage" for e in t.events)
    await host.stop(t.id)


async def test_a_message_during_a_turn_steers_it(tmp_path):
    host = host_with([{**py("import asyncio\nawait asyncio.sleep(2)"), "delayMs": 0}, "noted", "done"],
                     tmp_path=tmp_path)
    t = await host.start(project="p", title="t", request="wait")
    await until(lambda: any(e["type"] == "tool_call" for e in t.events))
    t.deliver("also do this", sender="t_other")
    await host.wait_idle(t.id)
    assert sum(e["type"] == "turn_done" for e in t.events) == 1
    messages = (await t.harness.command("get_messages"))["messages"]
    users = [m for m in messages if m["role"] == "user"]
    assert users[-1]["content"][0]["text"] == "[from t_other] also do this"
    await host.stop(t.id)


async def test_spawn_and_child_reports_back(tmp_path):
    host = host_with(
        [py("print(await team.spawn_thread('child', 'do the child work'))"),
         py("await team.set_summary('spawned child')"), "waiting for child", "child reported"],
        [py("await team.set_summary('child work done')"), "done"],
        tmp_path=tmp_path)
    p = await host.start(project="p", title="parent", request="split this")
    await until(lambda: len(host.threads) == 2)
    child = next(t for t in host.threads.values() if t.id != p.id)
    await host.wait_idle(child.id)
    await until(lambda: sum(e["type"] == "turn_done" for e in p.events) >= 2)
    ins = [e["data"] for e in p.events if e["type"] == "message_in"]
    assert any(m["from"] == child.id and "child work done" in m["body"] for m in ins)
    assert child.parent == p.id
    for t in list(host.threads.values()):
        await host.stop(t.id)


async def test_peer_messages_and_directory(tmp_path):
    host = host_with(
        [py("await team.set_summary('owns the export page')"), "ok", "got it"],
        [py("rows = await team.list_threads()\nprint(rows)\n"
            "print(await team.send_message(rows[0]['id'], 'hello'))"), "ok"],
        tmp_path=tmp_path)
    a = await host.start(project="p", title="a", request="x")
    await host.wait_idle(a.id)
    b = await host.start(project="p", title="b", request="y")
    await host.wait_idle(b.id)
    assert "owns the export page" in results(b)[0] and a.id in results(b)[0] and b.id not in results(b)[0]
    await until(lambda: any(e["type"] == "message_in" and e["data"]["from"] == b.id for e in a.events))
    await host.stop(a.id)
    await host.stop(b.id)


async def test_team_calls_that_fail_raise_in_the_kernel(tmp_path):
    host = host_with([py("try:\n    await team.read_thread('t_nope', bogus=1)\nexcept TypeError as e:\n    print('bad', e)\n"
                         "try:\n    await team._call('format_disk')\nexcept team.TeamError as e:\n    print(e)"),
                      "done"], tmp_path=tmp_path)
    t = await host.start(project="p", title="t", request="x")
    await host.wait_idle(t.id)
    out = results(t)[0]
    assert out.startswith("bad ") and "format_disk: unknown call" in out, out
    await host.stop(t.id)


async def test_thread_searches_and_fetches_the_web(tmp_path):
    web = Web()
    host = ThreadHost(agent=Scripted([py("r = await team.web_search('bubblewrap', count=3, question='what is it')\n"
                                         "print(r[0]['url'])\npage = await team.web_fetch(r[0]['url'])\n"
                                         "print(page['content'])"), "done"]),
                      web=web, workdir=str(tmp_path / "threads"))
    t = await host.start(project="p", title="t", request="look it up")
    await host.wait_idle(t.id)
    assert results(t)[0].split("\n")[:2] == ["https://github.com/containers/bubblewrap", "Unprivileged sandboxing tool"]
    assert web.calls == [("search", "bubblewrap", 3, "what is it"), ("fetch", "https://github.com/containers/bubblewrap")]
    await host.stop(t.id)


def test_serve_gives_uvicorn_its_config_and_the_thread_host_the_signal(tmp_path, monkeypatch):
    """The service starts the runner with `serve(app, thread_host, host=…,
    port=…)`: uvicorn's config keys are not serve's own parameters. A thread
    host named `host` there crashed every start of the service (`TypeError:
    serve() got multiple values for argument 'host'`), so no worker came up;
    and the host is the one a stopping signal reaches, so its threads mark
    their turns interrupted."""
    seen = {}

    class Stub(uvicorn.Server):
        def __init__(self, config):
            super().__init__(config)
            seen["config"], seen["class"] = config, type(self)

        def run(self):
            seen["ran"] = True

    monkeypatch.setattr(uvicorn, "Server", Stub)
    host = host_with(tmp_path=tmp_path)
    serve(create_app(host, "tok"), host, host="127.0.0.1", port=8123, timeout_graceful_shutdown=3)
    assert seen["ran"]
    assert (seen["config"].host, seen["config"].port) == ("127.0.0.1", 8123)
    server = seen["class"](seen["config"])
    server.handle_exit(signal.SIGTERM, None)
    assert server.should_exit
    assert host.stopping == "the runner got SIGTERM"


async def test_http_api(tmp_path):
    host = host_with(["hi there", "again"], tmp_path=tmp_path)
    app = create_app(host, token="secret")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w") as c:
        assert (await c.get("/health")).json()["capacity"] == 16
        assert (await c.post("/threads", json={"request": "hi"})).status_code == 401
        h = {"Authorization": "Bearer secret"}
        r = await c.post("/threads", json={"request": "say hi", "title": "hello", "project": "p"}, headers=h)
        assert r.status_code == 201
        tid = r.json()["id"]
        await host.wait_idle(tid)
        done = [e for e in host.threads[tid].events if e["type"] == "turn_done"][0]
        assert done["data"]["summary"] == "hi there"  # no set_summary call: last reply becomes the summary
        threads = (await c.get("/threads", headers=h)).json()
        assert threads[0]["id"] == tid and threads[0]["state"] == "idle"
        assert (await c.post(f"/threads/{tid}/messages", json={"body": "again"}, headers=h)).status_code == 202
    await host.stop(tid)


async def test_files_of_a_thread_over_http(tmp_path):
    host = host_with(["built"], tmp_path=tmp_path)
    app = create_app(host, token="secret")
    h = {"Authorization": "Bearer secret"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w") as c:
        tid = (await c.post("/threads", json={"request": "build", "title": "build", "project": "p"},
                            headers=h)).json()["id"]
        await host.wait_idle(tid)
        root = host.threads[tid].root
        os.makedirs(f"{root}/out")
        open(f"{root}/out/hello", "wb").write(b"\x7fELF\x00binary")
        open(f"{root}/README.md", "w").write("hi")
        os.makedirs(f"{root}/.git")
        os.symlink("/etc/passwd", f"{root}/leak")
        assert (await c.get(f"/threads/{tid}/files")).status_code == 401
        top = (await c.get(f"/threads/{tid}/files", headers=h)).json()
        assert top["path"] == ""
        assert [(e["name"], e["dir"]) for e in top["entries"]] == [("out", True), ("README.md", False), ("leak", False)]
        sub = (await c.get(f"/threads/{tid}/files", params={"path": "out"}, headers=h)).json()
        assert sub == {"path": "out", "entries": [{"name": "hello", "dir": False, "size": 11}]}
        raw = await c.get(f"/threads/{tid}/files/raw", params={"path": "out/hello"}, headers=h)
        assert raw.status_code == 200 and raw.content == b"\x7fELF\x00binary"
        assert raw.headers["content-type"] == "application/octet-stream"
        for path, status in (("../x", 400), ("leak", 400), (".git", 400), ("nope", 404), ("out", 400)):
            r = await c.get(f"/threads/{tid}/files/raw", params={"path": path}, headers=h)
            assert r.status_code == status, path
        assert (await c.get(f"/threads/{tid}/files", params={"path": "README.md"}, headers=h)).status_code == 400
        assert (await c.get("/threads/t_nope/files", headers=h)).status_code == 404
    await host.stop(tid)


async def test_peer_calls_over_http(tmp_path):
    from runner.peers import OrchestratorPeers, PeerError

    host = ThreadHost(agent=Scripted(), web=Web(), workdir=str(tmp_path / "t"), peers=OrchestratorPeers())
    app = create_app(host, token="secret")
    h = {"Authorization": "Bearer secret"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w") as c:
        assert (await c.get("/peer/calls", params={"wait": 0})).status_code == 401
        assert (await c.get("/peer/calls", params={"wait": 0}, headers=h)).json() == []
        pending = asyncio.ensure_future(host.peers.list_threads("t_a", "p", "export"))
        calls = (await c.get("/peer/calls", params={"wait": 2}, headers=h)).json()
        assert [(x["thread"], x["op"], x["args"]) for x in calls] == [
            ("t_a", "list_threads", {"project": "p", "query": "export"})]
        # leased: not handed out again while the orchestrator works on it
        assert (await c.get("/peer/calls", params={"wait": 0}, headers=h)).json() == []
        r = await c.post(f"/peer/calls/{calls[0]['id']}", json={"ok": True, "result": [{"id": "t_1"}]}, headers=h)
        assert r.status_code == 200
        assert await pending == [{"id": "t_1"}]
        assert (await c.post(f"/peer/calls/{calls[0]['id']}", json={"ok": True}, headers=h)).status_code == 404

        failing = asyncio.ensure_future(host.peers.send_message("t_a", "t_b", "hi"))
        call = (await c.get("/peer/calls", params={"wait": 2}, headers=h)).json()[0]
        assert (call["thread"], call["op"], call["args"]["to"]) == ("t_a", "send_message", "t_b")
        await c.post(f"/peer/calls/{call['id']}", json={"ok": False, "error": "no such thread"}, headers=h)
        with pytest.raises(PeerError, match="no such thread"):
            await failing


async def test_unanswered_peer_call_is_handed_out_again():
    from runner.peers import PeerOutbox

    box = PeerOutbox(lease=0.05)
    pending = asyncio.ensure_future(box.call("read_thread", "t_0", {"thread_id": "t_1"}, timeout=5))
    first = await box.take(wait=1)
    again = await box.take(wait=1)
    assert first[0]["id"] == again[0]["id"]
    box.answer(first[0]["id"], {"ok": True, "result": {}})
    assert await pending == {"ok": True, "result": {}}


def test_secrets_env_then_keyring_then_exit(monkeypatch, capsys):
    from runner import secrets
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.setattr(secrets, "_from_keyring", lambda n: "from-ring" if n == "DEEPSEEK_API_KEY" else None)
    monkeypatch.setattr(secrets, "_from_keyring_label", lambda n: None)
    assert secrets.get_secret("DEEPSEEK_API_KEY") == "from-ring"
    monkeypatch.setenv("DEEPSEEK_API_KEY", "from-env")
    assert secrets.get_secret("DEEPSEEK_API_KEY") == "from-env"
    monkeypatch.delenv("RUNNER_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        secrets.require(["DEEPSEEK_API_KEY", "RUNNER_TOKEN"])
    err = capsys.readouterr().err
    assert "RUNNER_TOKEN" in err and "DEEPSEEK_API_KEY" not in err.split("\n\n")[1]
    assert "keyring backend" in err or "no keyring backend" in err


def test_secrets_accept_an_item_added_in_a_keyring_gui(monkeypatch):
    """A keyring-GUI item has no service/username attributes, only a label, so
    it is found by label: exactly, unlocked collections only, surrounding
    whitespace dropped."""
    from runner import secrets

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


def test_secrets_without_a_secret_service(monkeypatch):
    from runner import secrets
    monkeypatch.setitem(sys.modules, "secretstorage", None)  # import raises ImportError
    assert secrets._from_keyring_label("DEEPSEEK_API_KEY") is None


def test_secrets_env_then_keyring_then_exit(monkeypatch, capsys):
    from runner import secrets
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.setattr(secrets, "_from_keyring", lambda n: "from-ring" if n == "DEEPSEEK_API_KEY" else None)
    monkeypatch.setattr(secrets, "_from_keyring_label", lambda n: None)
    assert secrets.get_secret("DEEPSEEK_API_KEY") == "from-ring"
    monkeypatch.setenv("DEEPSEEK_API_KEY", "from-env")
    assert secrets.get_secret("DEEPSEEK_API_KEY") == "from-env"
    monkeypatch.delenv("RUNNER_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        secrets.require(["DEEPSEEK_API_KEY", "RUNNER_TOKEN"])
    err = capsys.readouterr().err
    assert "RUNNER_TOKEN" in err and "DEEPSEEK_API_KEY" not in err.split("\n\n")[1]
    assert "keyring backend" in err or "no keyring backend" in err


def test_secrets_accept_an_item_added_in_a_keyring_gui(monkeypatch):
    """A keyring-GUI item has no service/username attributes, only a label, so
    it is found by label: exactly, unlocked collections only, surrounding
    whitespace dropped."""
    from runner import secrets

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


def test_secrets_without_a_secret_service(monkeypatch):
    from runner import secrets
    monkeypatch.setitem(sys.modules, "secretstorage", None)  # import raises ImportError
    assert secrets._from_keyring_label("DEEPSEEK_API_KEY") is None


async def test_http_bringing_a_thread_back_needs_its_id_or_a_request(tmp_path):
    host = host_with([], tmp_path=tmp_path)
    app = create_app(host, None)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w") as c:
        assert (await c.post("/threads", json={"title": "t", "project": "p"})).status_code == 400
        assert (await c.post("/threads", json={"id": "t_x", "title": "t"})).status_code == 400  # no project
        r = await c.post("/threads", json={"id": "t_x", "title": "t", "summary": "was here", "project": "p"})
        # It answers at once, queued; its worktree and session are made after (ThreadHost._prepare).
        assert r.status_code == 201 and r.json()["state"] == "queued" and r.json()["summary"] == "was here"
        assert (await c.post("/threads", json={"id": "t_x", "project": "p"})).status_code == 409
        t = host.threads["t_x"]
        await until(lambda: any(e["type"] == "prepared" for e in t.events))
        assert [e["data"] for e in t.events if e["type"] == "prepared"] == [{"resumed": False, "idle": True}]
        assert {"state": "idle"} in [e["data"] for e in t.events if e["type"] == "state"]
    await host.stop("t_x")


def test_a_poll_that_came_back_fine_is_not_a_log_line(caplog):
    """The worker log is for what happened, not for the polls that keep
    coming back 200; a poll that fails is still there."""
    import logging

    from runner.server import configure_logging

    configure_logging()
    access = logging.getLogger("uvicorn.access")
    access.setLevel(logging.INFO)

    def poll(method, path, status):
        # what uvicorn's access logger gets for one request
        access.info('%s - "%s %s HTTP/%s" %d', "10.0.0.2:33294", method, path, "1.1", status)

    with caplog.at_level(logging.INFO):
        poll("GET", "/health", 200)
        poll("GET", "/peer/calls?wait=25.0", 200)
        assert caplog.records == []
        poll("GET", "/threads", 200)
        poll("GET", "/health", 500)
        poll("POST", "/peer/calls/c_1", 404)
        assert [r.args[2] for r in caplog.records] == ["/threads", "/health", "/peer/calls/c_1"]


@pytest.mark.skipif(os.geteuid() != 0, reason="making a user with ambient capabilities needs root")
def test_sandbox_starts_for_a_user_with_ambient_capabilities():
    """A Sprite's services run as a non-root user with ambient capabilities,
    which bwrap refuses unless they are cleared before it starts."""
    code = "from runner import sandbox; sandbox.check()"
    r = subprocess.run(["setpriv", "--reuid=nobody", "--regid=nogroup", "--clear-groups",
                        "--inh-caps=+net_bind_service", "--ambient-caps=+net_bind_service",
                        sys.executable, "-c", code], capture_output=True, text=True,
                       env={**os.environ, "PYTHONPATH": str(runner_checkout() / "runner" / "src")})
    assert r.returncode == 0, r.stderr


def test_image_turns_go_to_a_model_that_takes_images(tmp_path):
    """The binary runs --offline, so it knows the models the agent dir
    declares: the thread's default takes text only, and the settings send an
    image turn to deepseek-flash, which takes images."""
    from runner.harness import HARNESS, IMAGE_MODEL, Place, PrimeAgent

    place = Place(view=view(tmp_path), socket=str(tmp_path / "team.sock"), kernel=str(tmp_path / "kernel"),
                  turn=str(tmp_path / "turn"))
    PrimeAgent().prepare(place)
    out = subprocess.run([str(HARNESS / "prime-agent"), "--offline", "model", "list", "deepseek"],
                         env={"PATH": os.environ["PATH"], "HOME": place.view.home, "DEEPSEEK_API_KEY": "unused",
                              "PRIME_AGENT_CODING_AGENT_DIR": str(place.agent_dir)},
                         stdin=subprocess.DEVNULL, capture_output=True, text=True, check=True).stdout
    images = {row.split()[1]: row.split()[-1] for row in out.splitlines()[1:]}
    assert IMAGE_MODEL == "deepseek/deepseek-flash"
    assert images["deepseek-flash"] == "yes" and images["deepseek-v4-pro"] == "no"
    assert '"imageModel": "deepseek/deepseek-flash"' in (place.agent_dir / "settings.json").read_text()
