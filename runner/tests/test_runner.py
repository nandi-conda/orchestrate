import asyncio
import json
import os
import subprocess
import sys
import types

import httpx
import pytest

from runner import LocalSandbox, ModelResponse, OpenAICompatModel, ScriptedModel, ThreadHost, ToolCall, View
from runner.model import ModelError
from runner.sandbox import runner_checkout
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


def view(tmp_path, **kw):
    return View(root=str(tmp_path / "ws"), home=str(tmp_path / "home"), tmp=str(tmp_path / "tmp"), **kw)


async def test_sandbox_confines_paths(tmp_path):
    sb = LocalSandbox(view(tmp_path))
    await sb.write_file("a/b.txt", b"hi")
    assert await sb.read_file("a/b.txt") == b"hi"
    with pytest.raises(Exception):
        await sb.read_file("../outside")
    r = await sb.exec("echo $((2+3)) && ls a")
    assert r.exit_code == 0 and "5" in r.stdout and "b.txt" in r.stdout
    r = await sb.exec("sleep 5", timeout=0.2)
    assert r.timed_out


async def test_commands_see_the_workspace_and_nothing_of_the_runner(tmp_path, monkeypatch):
    """The runner's checkout, environment and processes are not in a thread's view."""
    monkeypatch.setenv("RUNNER_TOKEN", "runner-secret")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "model-secret")
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "other-thread.txt").write_text("not yours")
    checkout = str(runner_checkout())
    sb = LocalSandbox(view(tmp_path, hidden=(checkout, sys.prefix, str(tmp_path / "state"))))
    await sb.write_file("mine.txt", b"yours")
    r = await sb.exec("env; echo ---; pwd; cat mine.txt; echo ---; "
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
    assert (await sb.exec("cat /tmp/scratch")).stdout == "kept\n"  # /tmp lasts across commands
    with pytest.raises(Exception):
        await sb.read_file(str(runner_checkout() / "runner" / "pixi.toml"))


def test_sandbox_falls_back_to_an_empty_proc(monkeypatch, tmp_path):
    """A kernel that refuses procfs in a user namespace (Sprites') still gets a
    sandbox: /proc is an empty tmpfs, and the check says so."""
    import runner.sandbox as sandbox

    monkeypatch.setattr(sandbox, "_proc_mount", "proc")  # restored after the test

    def probe(kind):
        return subprocess.CompletedProcess([], 0 if kind == "tmpfs" else 1, "",
                                           "bwrap: Can't mount proc on /proc: Operation not permitted")
    monkeypatch.setattr(sandbox, "_probe", probe)

    sandbox.check()
    assert sandbox._proc_mount == "tmpfs"
    args = sandbox.bwrap_args(view(tmp_path))
    assert any(a == "--tmpfs" and b == "/proc" for a, b in zip(args, args[1:]))


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
        done = [e for e in host.threads[tid].events if e["type"] == "turn_done"][0]
        assert done["data"]["summary"] == "hi there"  # no finish call: last reply becomes the summary
        threads = (await c.get("/threads", headers=h)).json()
        assert threads[0]["id"] == tid and threads[0]["state"] == "idle"
        assert (await c.post(f"/threads/{tid}/messages", json={"body": "again"}, headers=h)).status_code == 202


async def test_files_of_a_thread_over_http(tmp_path):
    host = host_with({"build": [ModelResponse("built")]}, tmp_path)
    app = create_app(host, token="secret")
    h = {"Authorization": "Bearer secret"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w") as c:
        tid = (await c.post("/threads", json={"request": "build", "title": "build"}, headers=h)).json()["id"]
        await host.wait_idle(tid)
        root = host.threads[tid].sandbox.root
        import os
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


async def test_peer_calls_over_http(tmp_path):
    from runner.peers import OrchestratorPeers, PeerError

    host = ThreadHost(model_factory=lambda spec: ScriptedModel([]), workdir=str(tmp_path / "t"),
                      peers=OrchestratorPeers())
    app = create_app(host, token="secret")
    h = {"Authorization": "Bearer secret"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w") as c:
        assert (await c.get("/peer/calls", params={"wait": 0})).status_code == 401
        assert (await c.get("/peer/calls", params={"wait": 0}, headers=h)).json() == []
        pending = asyncio.ensure_future(host.peers.list_threads("p", "export"))
        calls = (await c.get("/peer/calls", params={"wait": 2}, headers=h)).json()
        assert [(x["op"], x["args"]) for x in calls] == [("list_threads", {"project": "p", "query": "export"})]
        # leased: not handed out again while the orchestrator works on it
        assert (await c.get("/peer/calls", params={"wait": 0}, headers=h)).json() == []
        r = await c.post(f"/peer/calls/{calls[0]['id']}", json={"ok": True, "result": [{"id": "t_1"}]}, headers=h)
        assert r.status_code == 200
        assert await pending == [{"id": "t_1"}]
        assert (await c.post(f"/peer/calls/{calls[0]['id']}", json={"ok": True}, headers=h)).status_code == 404

        failing = asyncio.ensure_future(host.peers.send_message("t_a", "t_b", "hi"))
        call = (await c.get("/peer/calls", params={"wait": 2}, headers=h)).json()[0]
        assert call["op"] == "send_message" and call["args"]["to"] == "t_b"
        await c.post(f"/peer/calls/{call['id']}", json={"ok": False, "error": "no such thread"}, headers=h)
        with pytest.raises(PeerError, match="no such thread"):
            await failing


async def test_unanswered_peer_call_is_handed_out_again():
    from runner.peers import PeerOutbox

    box = PeerOutbox(lease=0.05)
    pending = asyncio.ensure_future(box.call("read_thread", {"thread_id": "t_1"}, timeout=5))
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


def sse_body(*chunks, keepalives=0):
    lines = [": keep-alive\n\n"] * keepalives
    lines += [f"data: {json.dumps(c)}\n\n" for c in chunks]
    lines.append("data: [DONE]\n\n")
    return "".join(lines).encode()


async def test_openai_compat_streams_reasoning_text_and_tool_calls():
    seen = {}

    def handler(req: httpx.Request):
        seen["body"] = json.loads(req.content)
        seen["accept-encoding"], seen["accept"] = req.headers["accept-encoding"], req.headers["accept"]
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=sse_body(
            {"choices": [{"delta": {"reasoning_content": "Need a "}}]},
            {"choices": [{"delta": {"reasoning_content": "file."}}]},
            {"choices": [{"delta": {"content": "Writing it."}}]},
            {"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_1", "type": "function",
                                                    "function": {"name": "write_file", "arguments": ""}}]}}]},
            {"choices": [{"delta": {"tool_calls": [{"index": 0, "function": {"arguments": "{\"path\": "}}]}}]},
            {"choices": [{"delta": {"tool_calls": [{"index": 0, "function": {"arguments": "\"a.py\"}"}}]}}]},
            {"choices": [], "usage": {"prompt_tokens": 3, "completion_tokens": 9}},
            keepalives=2))

    m = OpenAICompatModel("deepseek-v4-pro", "k", "https://x", client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    got = []
    r = await m.complete([{"role": "user", "content": "hi"}], [], lambda k, t, n: got.append((k, t, n)))
    assert seen["body"]["stream"] is True
    assert seen["accept-encoding"] == "identity" and seen["accept"] == "text/event-stream"
    assert r.content == "Writing it." and r.reasoning == "Need a file."
    assert [(c.id, c.name, c.arguments) for c in r.tool_calls] == [("call_1", "write_file", {"path": "a.py"})]
    assert r.message["reasoning_content"] == "Need a file."
    assert r.message["tool_calls"][0]["function"]["arguments"] == '{"path": "a.py"}'
    assert r.usage == {"prompt_tokens": 3, "completion_tokens": 9}
    assert got[:3] == [("reasoning", "Need a ", ""), ("reasoning", "file.", ""), ("content", "Writing it.", "")]
    assert ("tool", "\"a.py\"}", "write_file") in got


async def test_openai_compat_reports_http_errors():
    def handler(req):
        return httpx.Response(400, json={"error": {"message": "bad model"}})
    m = OpenAICompatModel("nope", "k", "https://x", client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    with pytest.raises(ModelError, match="HTTP 400.*bad model"):
        await m.complete([{"role": "user", "content": "hi"}], [])


async def test_openai_compat_fails_when_only_keepalives_arrive():
    async def stream():
        while True:
            yield b": keep-alive\n\n"
            await asyncio.sleep(0.02)

    def handler(req):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=stream())
    m = OpenAICompatModel("m", "k", "https://x", stall=0.1, client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    with pytest.raises(ModelError, match="only keep-alives from POST https://x/chat/completions for 0s"):
        await m.complete([{"role": "user", "content": "hi"}], [])


async def test_thread_streams_deltas_before_the_final_events(tmp_path):
    host = host_with({"t": [
        ModelResponse("", [call("bash", command="echo hi")], reasoning="I should run it."),
        ModelResponse("all done"),
    ]}, tmp_path)
    t = await host.start(project="p", title="t", request="run it")
    await host.wait_idle(t.id)
    kinds = [(e["type"], (e["data"] or {}).get("kind")) for e in t.events]
    first_call = kinds.index(("tool_call", None))
    assert kinds.index(("delta", "reasoning")) < kinds.index(("reasoning", None)) < first_call
    assert kinds.index(("delta", "tool")) < first_call
    tool_deltas = [e["data"] for e in t.events if e["type"] == "delta" and e["data"]["kind"] == "tool"]
    assert tool_deltas[0] == {"kind": "tool", "name": "bash", "text": ""}
    assert "".join(d["text"] for d in tool_deltas) == '{"command": "echo hi"}'
    content = [e for e in t.events if e["type"] == "delta" and e["data"]["kind"] == "content"]
    assert content[-1]["data"]["text"] == "all done"
    assert content[-1]["seq"] < next(e["seq"] for e in t.events if e["type"] == "assistant")


async def test_openai_compat_names_the_url_and_timeout_when_reads_time_out():
    def handler(req):
        raise httpx.ReadTimeout("", request=req)
    m = OpenAICompatModel("m", "k", "https://x", stall=300, client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    with pytest.raises(ModelError, match=r"m: ReadTimeout: nothing from POST https://x/chat/completions for 300s; no response headers"):
        await m.complete([{"role": "user", "content": "hi"}], [])


async def test_stream_is_never_compressed_so_keepalives_arrive_as_sent():
    """A server (or proxy) that gzips a response buffers small writes until the
    stream ends. This one does exactly that when asked for gzip, and streams
    plainly otherwise; the model client must get each line as it is written."""
    import gzip

    lines = [b": keep-alive\n\n", b'data: {"choices": [{"delta": {"content": "hi"}}]}\n\n', b"data: [DONE]\n\n"]

    async def handle(reader, writer):
        head = (await reader.readuntil(b"\r\n\r\n")).decode().lower()
        n = int(head.split("content-length:")[1].split("\r\n")[0])
        await reader.readexactly(n)
        if "gzip" in head.split("accept-encoding:")[1].split("\r\n")[0]:
            await asyncio.sleep(1.0)  # a buffering compressor: nothing until the end
            body = gzip.compress(b"".join(lines))
            writer.write(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-encoding: gzip\r\n"
                         b"content-length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
        else:
            writer.write(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n")
            for line in lines:
                writer.write(b"%x\r\n%s\r\n" % (len(line), line))
                await writer.drain()
                await asyncio.sleep(0.05)
            writer.write(b"0\r\n\r\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        m = OpenAICompatModel("m", "k", f"http://127.0.0.1:{port}", stall=0.5)
        r = await m.complete([{"role": "user", "content": "hi"}], [])
        assert r.content == "hi"
    finally:
        server.close()


def test_replay_rebuilds_steps_tool_results_and_senders():
    from runner.thread import replay
    ev = lambda type, **data: {"type": type, "data": data}
    msgs = replay([
        ev("message_in", **{"from": "user", "body": "hi"}),
        ev("reasoning", content="think"),
        ev("assistant", content="looking"),
        ev("tool_call", id="a", name="bash", arguments={"command": "ls"}, step=0),
        ev("tool_result", id="a", name="bash", result="x"),
        ev("tool_call", id="b", name="bash", arguments={"command": "pwd"}, step=0),
        ev("tool_result", id="b", name="bash", result="/w"),
        ev("tool_call", id="c", name="read_file", arguments={"path": "x"}, step=1),
        ev("tool_result", id="c", name="read_file", result="data"),
        ev("assistant", content="done"),
        ev("turn_done", summary="s"),
        ev("message_in", **{"from": "router", "body": "more"}),
    ])
    assert [m["role"] for m in msgs] == ["user", "assistant", "tool", "tool", "assistant", "tool", "assistant", "user"]
    assert msgs[1]["reasoning_content"] == "think" and msgs[1]["content"] == "looking"
    assert [c["id"] for c in msgs[1]["tool_calls"]] == ["a", "b"]
    assert json.loads(msgs[1]["tool_calls"][0]["function"]["arguments"]) == {"command": "ls"}
    assert msgs[4]["content"] == "" and [c["id"] for c in msgs[4]["tool_calls"]] == ["c"]
    assert msgs[6] == {"role": "assistant", "content": "done"}
    assert msgs[7]["content"] == "[from router] more"


async def test_http_resume_needs_request_or_history(tmp_path):
    host = host_with({"t": [ModelResponse("ok")]}, tmp_path)
    app = create_app(host, None)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w") as c:
        assert (await c.post("/threads", json={"id": "t_x"})).status_code == 400
        r = await c.post("/threads", json={"id": "t_x", "title": "t", "history": [], "summary": "was here"})
        assert r.status_code == 201 and r.json()["state"] == "idle" and r.json()["summary"] == "was here"
        assert (await c.post("/threads", json={"id": "t_x", "history": []})).status_code == 409


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
