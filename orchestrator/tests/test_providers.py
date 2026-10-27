"""Where workers run.

No network and no keyring: `control.secrets.get_secret` is patched, so these
run anywhere.
"""

import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
import types
from pathlib import Path

import httpx
import pytest

from control import providers, secrets
from tests.doubles import ECHO_RUNNER
from control.service import worker_spec

pytestmark = pytest.mark.anyio


def _wait_port(port: int, up: bool, patience: float) -> bool:
    """Whether the port starts (up) or stops answering within `patience`."""
    deadline = time.time() + patience
    while True:
        try:
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=0.5)
            answered = True
        except Exception:  # refused, reset, or slower than a dead runner is worth
            answered = False
        if answered is up:
            return True
        if time.time() >= deadline:
            return False
        time.sleep(0.1)


@pytest.mark.skipif(sys.platform == "win32",
                    reason="only Unix reparents a child, which is how it sees its parent go")
def test_a_devrunner_goes_when_the_process_that_started_it_is_gone(tmp_path):
    """A devrunner only serves the control plane that started it, so it must not
    outlive it: one that does is a port nobody will call, listening until the
    machine goes down. A control plane that stops its workers leaves nothing
    behind (control/app.py's lifespan, LocalProvider.close), but one that is
    killed without the chance -- a test run that dies, a window killed
    mid-start -- used to leave its runner behind for days
    (tests/devrunner.py, watch_parent)."""
    port = providers.free_port()
    # A parent of our own that starts the runner and then waits, so this test
    # can kill it outright and watch what the runner does about it.
    parent = subprocess.Popen(
        [sys.executable, "-c",
         "import subprocess, sys, time\n"
         "subprocess.Popen([sys.executable, '-m', 'tests.devrunner'])\n"
         "time.sleep(60)\n"],
        cwd=str(Path(providers.__file__).resolve().parents[1]),
        env={**os.environ, "RUNNER_PORT": str(port), "RUNNER_WORKDIR": str(tmp_path)})
    try:
        assert _wait_port(port, up=True, patience=30.0), "the devrunner never answered on its port"
        parent.kill()  # no chance to stop its workers, like a test run that is killed
        parent.wait(timeout=10)
        assert _wait_port(port, up=False, patience=15.0), "the devrunner outlived its parent"
    finally:
        if parent.poll() is None:
            parent.kill()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def env(monkeypatch):
    for name in ("DEEPSEEK_API_KEY", "SPRITES_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(secrets, "get_secret", lambda name: None)
    return monkeypatch


class Recording:
    """Stands in for LocalProvider/SpritesProvider and remembers what it got."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.created: list = []
        self.destroyed: list = []

    async def create(self, spec):
        self.created.append(spec)
        return providers.Worker(id=f"{self.name}-w", provider=self.name, handle="", url="http://example.invalid")

    async def destroy(self, worker) -> None:
        self.destroyed.append(worker)

    def worker_name(self, index, pool=""):
        return f"{self.name}-{pool}-w{index}"

    async def sweep(self, slots, pool):
        self.swept = getattr(self, "swept", []) + [pool]
        return [f"{self.name}-swept-{pool}-beyond-{slots}"]

    async def close(self) -> None:
        pass


def test_without_a_token_sprites_are_refused_by_name(env):
    """Sprites without SPRITES_TOKEN is refused, saying which credential and
    how it is sent; no other place stands in for it."""
    env.setattr(secrets, "get_secret", lambda name: "deepseek" if name == "DEEPSEEK_API_KEY" else None)
    places = providers.default_places()
    assert "sprites" not in places.providers
    with pytest.raises(providers.PlaceRefused, match="SPRITES_TOKEN.*Authorization: Bearer"):
        places.provider("sprites")
    offered = {p["id"]: p for p in places.offered()}
    assert offered["sprites"]["label"] == "Sprites" and "SPRITES_TOKEN" in offered["sprites"]["why"]


def test_sprites_refused_says_what_the_keyring_said(env):
    """The reason names what was looked in, so a keyring that failed when
    the app started is told apart from one without the entry."""
    import keyring
    from keyring.backend import KeyringBackend

    class Locked(KeyringBackend):
        priority = 1

        def get_password(self, service, username):
            raise keyring.errors.KeyringLocked("the collection is locked")

        def set_password(self, service, username, password):
            raise NotImplementedError

        def delete_password(self, service, username):
            raise NotImplementedError

    before = keyring.get_keyring()
    keyring.set_keyring(Locked())
    try:
        why = {p["id"]: p for p in providers.default_places().offered()}["sprites"]["why"]
    finally:
        keyring.set_keyring(before)
    assert "SPRITES_TOKEN is not in the environment" in why
    assert "failed: KeyringLocked: the collection is locked" in why


def test_a_token_offers_sprites_and_this_computer(env):
    env.setattr(secrets, "get_secret", lambda name: {"SPRITES_TOKEN": "tok", "DEEPSEEK_API_KEY": "k"}.get(name))
    places = providers.default_places()
    assert set(places.providers) == {"local", "sprites"}
    assert [(p["id"], p["why"]) for p in places.offered()] == [("local", ""), ("sprites", "")]


def test_a_place_nobody_offers_says_why():
    places = providers.Places({"sprites": Recording("sprites")}, {"local": "not on a phone"})
    with pytest.raises(providers.PlaceRefused, match="This computer: not on a phone"):
        places.provider("local")
    with pytest.raises(providers.PlaceRefused, match="none of"):
        places.provider("moon")


def test_passthrough_env_reads_a_keyring_only_secret(env):
    env.setattr(secrets, "get_secret", lambda name: "deepseek" if name == "DEEPSEEK_API_KEY" else None)
    assert providers.passthrough_env() == {"DEEPSEEK_API_KEY": "deepseek"}


def test_passthrough_env_gives_workers_the_opengateway_token(env):
    keys = {"DEEPSEEK_API_KEY": "deepseek", "OPENGATEWAY_API_TOKEN": "og"}
    env.setattr(secrets, "get_secret", keys.get)
    assert providers.passthrough_env() == keys


def test_a_project_model_names_its_provider_to_the_runner():
    from control.models import MODELS, worker_model

    assert worker_model("") is None
    assert worker_model("deepseek-v4-pro") == {"provider": "deepseek", "model": "deepseek-v4-pro"}
    assert worker_model("deepseek/deepseek-v4.1-flash-ultrafast") == {
        "provider": "opengateway", "model": "deepseek/deepseek-v4.1-flash-ultrafast"}
    with pytest.raises(ValueError, match="no model 'gpt-4o'"):
        worker_model("gpt-4o")
    # Each is one runner/models.json declares under that provider.
    declared = json.loads((Path(__file__).resolve().parents[2] / "runner" / "models.json").read_text())["providers"]
    for name, (provider, _) in MODELS.items():
        assert name in [m["id"] for m in declared[provider]["models"]]


def test_passthrough_env_never_includes_the_sprites_token(env):
    env.setattr(secrets, "get_secret", lambda name: f"value-{name}")
    kept = providers.passthrough_env()
    assert "SPRITES_TOKEN" not in kept
    assert "GITHUB_TOKEN" not in kept  # workers reach no repository; the control plane does


def test_worker_spec_carries_no_control_plane_address():
    spec = worker_spec(2)
    assert spec.index == 2 and not hasattr(spec, "repo_url")  # the hub syncs the worker
    assert not hasattr(spec, "orchestrator_url")


def _checkout(tmp_path):
    def git(*args):
        subprocess.run(["git", "-C", str(tmp_path), *args], check=True, capture_output=True)
    git("init", "-q")
    git("remote", "add", "origin", "https://github.com/acme/fork.git")
    git("checkout", "-q", "-b", "feature/x")
    return tmp_path


def _commit(root, message):
    subprocess.run(["git", "-C", str(root), "-c", "user.email=a@b.c", "-c", "user.name=n",
                    "commit", "-q", "--allow-empty", "-m", message], check=True, capture_output=True)


def test_source_defaults_to_this_checkouts_origin_and_branch(tmp_path):
    assert providers.repo_source_default(_checkout(tmp_path)) == ("https://github.com/acme/fork.git", "feature/x")


def test_source_falls_back_to_the_public_repository(tmp_path):
    assert providers.repo_source_default(tmp_path) == (providers.DEFAULT_SOURCE, providers.DEFAULT_REF)


def test_an_install_inside_another_repository_uses_the_public_repository(tmp_path):
    # The conda package under a home directory that is itself a git repository.
    install = _checkout(tmp_path) / "envs" / "orchestrate"
    install.mkdir(parents=True)
    assert providers.repo_source_default(install) == (providers.DEFAULT_SOURCE, providers.DEFAULT_REF)


def test_checkout_note_is_quiet_when_origin_names_this_checkouts_head(tmp_path):
    """origin's commit is what remote_commit asked origin for: a missing or
    stale remote-tracking ref here (none in this checkout) says nothing."""
    root = _checkout(tmp_path)
    _commit(root, "one")
    head = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    assert providers.checkout_note(root, "feature/x", head) == ""


def test_checkout_note_warns_about_unpushed_commits(tmp_path):
    root = _checkout(tmp_path)
    _commit(root, "one")
    pushed = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    _commit(root, "two")
    assert "not this checkout's HEAD" in providers.checkout_note(root, "feature/x", pushed)


async def test_sprites_provider_takes_source_and_ref_and_names_sprites(env):
    env.setattr(secrets, "get_secret", lambda name: "tok")
    p = providers.SpritesProvider(source="https://example.com/fork.git", ref="dev")
    try:
        assert (p.source, p.ref) == ("https://example.com/fork.git", "dev")
        assert p.worker_name(1, "3f9a1c2b") == "orch-3f9a1c2b-w1"
        with pytest.raises(ValueError, match="pool"):
            p.worker_name(1, "")
    finally:
        await p.close()


def test_without_the_model_key_there_are_no_local_workers_and_no_router(env):
    """No echo runner and no rule-based router stand in for the model: a
    missing DEEPSEEK_API_KEY is refused, by name."""
    from control.router import default_router

    with pytest.raises(RuntimeError, match="DEEPSEEK_API_KEY"):
        providers.LocalProvider()
    with pytest.raises(RuntimeError, match="DEEPSEEK_API_KEY"):
        default_router()


async def test_a_router_model_that_plans_nothing_is_an_error(monkeypatch):
    from control.router import ModelRouter

    async def empty(*_):
        return {"spawn": [], "send": [], "answer": "", "why": ""}

    r = ModelRouter("key")
    monkeypatch.setattr(r, "_ask", empty)
    with pytest.raises(ValueError, match="planned nothing"):
        await r.plan("do it", [], [])


def router_answering(message: dict, seen: list, finish_reason: str = "stop"):
    """A ModelRouter whose model answers every request with `message`."""
    from control.router import ModelRouter

    def handle(req: httpx.Request) -> httpx.Response:
        seen.append(json.loads(req.content))
        return httpx.Response(200, json={"choices": [{"message": message, "finish_reason": finish_reason}]})
    r = ModelRouter("key")
    r.http = httpx.AsyncClient(base_url="https://api.deepseek.com/beta", transport=httpx.MockTransport(handle))
    return r


def route_call(plan: dict) -> dict:
    return {"role": "assistant", "content": "",
            "tool_calls": [{"id": "c1", "type": "function",
                            "function": {"name": "route", "arguments": json.dumps(plan)}}]}


async def test_the_router_model_answers_with_one_forced_strict_route_call():
    """The plan is the arguments of a `route` call the model is made to make,
    in strict mode (so they have the plan's schema), with thinking off."""
    places = [{"id": "local", "label": "This computer", "why": ""},
              {"id": "sprites", "label": "Sprites", "why": "it needs SPRITES_TOKEN"}]
    seen: list = []
    r = router_answering(route_call({"spawn": [{"title": "Fix it", "request": "fix it", "workers": "local",
                                                "where": "Sprites are not available"}],
                                     "send": [{"thread": "t_1", "message": "heads up", "kind": "steer",
                                               "workers": "", "where": "it stays"}],
                                     "why": "new work"}), seen)
    plan = await r.plan("fix it", [{"id": "t_1", "title": "Old", "load": {"level": "busy", "busy_for": 9}}],
                        places)
    # A send that moves nothing keeps no reason for a move.
    assert plan == {"spawn": [{"title": "Fix it", "request": "fix it", "workers": "local",
                               "where": "Sprites are not available"}],
                    "send": [{"thread": "t_1", "message": "heads up", "kind": "steer", "workers": "", "where": ""}],
                    "answer": "", "why": "new work"}
    body = seen[0]
    assert body["thinking"] == {"type": "disabled"}
    assert body["tool_choice"] == {"type": "function", "function": {"name": "route"}}
    fn = body["tools"][0]["function"]
    assert fn["strict"] is True and fn["name"] == "route"
    props = fn["parameters"]["properties"]
    assert props["spawn"]["items"]["properties"]["workers"]["enum"] == ["local"]
    assert "where" in props["spawn"]["items"]["required"] and "where" in props["send"]["items"]["required"]
    assert props["send"]["items"]["properties"]["thread"]["enum"] == ["t_1"]
    assert props["send"]["items"]["properties"]["kind"]["enum"] == ["steer", "work"]
    assert props["send"]["items"]["properties"]["workers"]["enum"] == ["", "local"]
    assert "answer" in fn["parameters"]["required"] and props["answer"]["type"] == "string"
    # With no threads there is nothing to send to, so the schema has no send.
    await r.plan("fix it", [], places)
    assert "send" not in seen[1]["tools"][0]["function"]["parameters"]["properties"]


async def test_the_router_model_answers_a_quick_question_itself():
    """A plan that only answers is a plan: the request needs no thread. The
    router is told how many images came with the request, which it cannot see."""
    seen: list = []
    r = router_answering(route_call({"spawn": [], "answer": "  **t_1** is busy fixing login.  ",
                                     "why": "the directory answers it"}), seen)
    plan = await r.plan("what is t_1 doing?", [{"id": "t_1", "title": "Fix login", "summary": "fixing login"}],
                        [{"id": "sprites", "label": "Sprites", "why": ""}], images=2)
    assert plan == {"spawn": [], "send": [], "answer": "**t_1** is busy fixing login.",
                    "why": "the directory answers it"}
    content = json.loads(seen[0]["messages"][1]["content"])
    assert content["images"] == 2 and content["threads"][0]["summary"] == "fixing login"
    assert "A quick question needs no thread" in seen[0]["messages"][0]["content"]


async def test_the_router_model_says_whether_a_message_steers_a_busy_thread_or_is_new_work():
    seen: list = []
    call = {"role": "assistant", "content": "",
            "tool_calls": [{"id": "c1", "type": "function", "function": {
                "name": "kind", "arguments": json.dumps({"kind": "work", "why": "a new job"})}}]}
    r = router_answering(call, seen)
    got = await r.kind("now write the docs", "user", {"id": "t_1", "title": "Fix login", "request": "fix login",
                                                       "summary": "", "state": "working"})
    assert got == {"kind": "work", "why": "a new job"}
    fn = seen[0]["tools"][0]["function"]
    assert fn["name"] == "kind" and fn["parameters"]["properties"]["kind"]["enum"] == ["steer", "work"]
    content = json.loads(seen[0]["messages"][1]["content"])
    assert content["message"] == "now write the docs" and content["thread"]["title"] == "Fix login"


async def test_a_router_answer_that_is_not_a_route_call_is_an_error_saying_what_came_back():
    r = router_answering({"role": "assistant", "content": "I would spawn a thread"}, [])
    with pytest.raises(ValueError, match="did not call route once.*I would spawn a thread"):
        await r.plan("fix it", [], [{"id": "sprites", "label": "Sprites", "why": ""}])
    r = router_answering(route_call({"spawn": [], "why": "nothing asked"}), [])
    with pytest.raises(ValueError, match="planned nothing.*nothing asked"):
        await r.plan("fix it", [], [{"id": "sprites", "label": "Sprites", "why": ""}])


async def test_a_route_call_the_model_was_stopped_in_is_an_error_naming_why():
    """A call cut off mid-argument (here by the server running out of
    resources) is refused for its finish_reason, before its arguments are read."""
    places = [{"id": "sprites", "label": "Sprites", "why": ""}]
    cut = route_call({})
    cut["tool_calls"][0]["function"]["arguments"] = '{"spawn":[{"title":"Fix it","request":"fix the rou'
    for reason in ("insufficient_system_resource", "content_filter", "aborted", "length", None):
        r = router_answering(cut, [], finish_reason=reason)
        with pytest.raises(ValueError, match=f"stopped before finishing its route call.*{reason!r}.*fix the rou"):
            await r.plan("fix it", [], places)


async def test_route_arguments_that_are_not_json_are_an_error_saying_what_came_back():
    cut = route_call({})
    cut["tool_calls"][0]["function"]["arguments"] = '{"spawn":[{"title":"Fix it","request":"fix the rou'
    r = router_answering(cut, [], finish_reason="tool_calls")
    with pytest.raises(ValueError, match="arguments that are not JSON.*Unterminated string.*fix the rou"):
        await r.plan("fix it", [], [{"id": "sprites", "label": "Sprites", "why": ""}])


async def test_sprites_create_checks_for_the_model_key_before_creating_a_sprite(env):
    env.setattr(secrets, "get_secret", lambda name: "tok" if name == "SPRITES_TOKEN" else None)
    p = providers.SpritesProvider(source="https://example.com/fork.git", ref="dev")
    try:
        with pytest.raises(RuntimeError, match="DEEPSEEK_API_KEY"):
            await p.create(providers.WorkerSpec(index=1, pool="ab12"))
    finally:
        await p.close()


class FakeSprites:
    """The Sprites API surface `SpritesProvider.create` uses, for a sprite that
    already exists, with or without a runner service defined.

    The service keeps the definition it was given, as the API does: a PUT
    whose command is the one it already has is answered "Service already
    running with that command" and changes nothing, running or not. Only a
    DELETE forgets it, and a PUT after that makes it again."""

    # The command every definition carries: what main() starts the runner with.
    COMMAND = {"cmd": "bash", "args": ["/opt/orchestrate/runner/deploy/start.sh"]}

    def __init__(self, defined: bool) -> None:
        self.defined = defined
        self.deleted = 0
        self.service: dict = ({**self.COMMAND, "env": {"RUNNER_TOKEN": "old"}, "http_port": 8080}
                              if defined else {})

    async def post(self, url, **kw):
        assert url == "/sprites"
        return httpx.Response(409, request=httpx.Request("POST", url))

    async def get(self, url, **kw):
        assert url.startswith("/sprites/")
        if url.endswith("/services/runner") and not self.defined:
            return httpx.Response(404, request=httpx.Request("GET", url))
        return httpx.Response(200, json={"url": "https://worker.invalid", "env": {"RUNNER_TOKEN": "old"}},
                              request=httpx.Request("GET", url))

    async def put(self, url, **kw):
        if url.endswith("/services/runner"):
            if self.defined and all(self.service.get(k) == kw["json"].get(k) for k in ("cmd", "args")):
                return httpx.Response(200, json={"message": "Service already running with that command, use "
                                                            "POST /v1/services/runner/restart if you want to "
                                                            "restart it", "name": "runner"},
                                      request=httpx.Request("PUT", url))
            self.service, self.defined = kw["json"], True  # it makes the service, and starts it
        else:
            self.labels = kw["json"]["labels"]
        return httpx.Response(200, request=httpx.Request("PUT", url))

    async def delete(self, url, **kw):
        assert url.endswith("/services/runner")
        if not self.defined:
            return httpx.Response(404, request=httpx.Request("DELETE", url))
        self.deleted += 1
        self.service, self.defined = {}, False
        return httpx.Response(204, request=httpx.Request("DELETE", url))

    async def aclose(self):
        pass


async def _sprites_create(env, defined: bool, serving: str = "c0ffee", spec: str = "", fake=None):
    env.setattr(secrets, "get_secret", lambda name: "key")
    p = providers.SpritesProvider(token="tok", source="https://example.com/fork.git", ref="dev")
    fake = fake or FakeSprites(defined)
    fake.bootstrap = None
    p.http = fake

    async def no_install(name, commit, steps):
        fake.bootstrap = {"ORCHESTRATE_REPO": p.source, "ORCHESTRATE_COMMIT": commit}

    async def healthy(url, timeout=120.0):
        return {"ok": True, "commit": serving, "spec": spec}

    async def accepts(worker):
        return True

    async def resolve(source, ref):
        return "c0ffee"

    async def logs(worker, lines):
        return "runner log"

    env.setattr(providers, "remote_commit", resolve)
    env.setattr(p, "logs", logs)
    env.setattr(p, "install", no_install)
    env.setattr(providers, "wait_healthy", healthy)
    env.setattr(p, "accepts_token", accepts)
    try:
        return await p.create(providers.WorkerSpec(index=1, pool="ab12", token="stored")), fake
    finally:
        await fake.aclose()


async def test_sprites_create_replaces_a_service_an_earlier_control_plane_defined(env):
    """A defined service cannot be redefined: a PUT whose command is the one it
    already has is answered "Service already running with that command" and
    keeps the definition it has. The env is part of that definition (this
    control plane's token, model key, commit and fingerprint), so a service
    left by an earlier control plane is removed and put again: otherwise the
    runner kept the old token and key, reported no fingerprint, and every
    attach restarted a worker it would otherwise leave running."""
    worker, fake = await _sprites_create(env, defined=True)
    assert worker.id == "orch-ab12-w1" and fake.labels == ["orchestrator-ab12"]  # the slot's sprite, labeled
    assert fake.deleted == 1
    assert worker.token == "stored" and fake.service["env"]["RUNNER_TOKEN"] == "stored"
    assert fake.service["env"]["DEEPSEEK_API_KEY"] == "key"


async def test_sprites_create_leaves_running_a_runner_it_would_start_the_same_way(env):
    """Restarting a runner ends the turns under way on it. A control plane
    attaching to a sprite whose runner reports the fingerprint of the very
    service it would define (same commit, key, token) leaves it running."""
    _, fake = await _sprites_create(env, defined=True)
    want = fake.service["env"]["RUNNER_SPEC"]
    assert want == providers.service_spec({**fake.service, "env": {
        k: v for k, v in fake.service["env"].items() if k != "RUNNER_SPEC"}})
    assert fake.service["env"]["ORCHESTRATE_COMMIT"] == "c0ffee"
    left, deleted = dict(fake.service), fake.deleted
    worker, fake = await _sprites_create(env, defined=True, spec=want, fake=fake)
    assert (fake.service, fake.deleted) == (left, deleted) and fake.bootstrap is None  # nothing more
    assert worker.token == "stored"


async def test_sprites_create_replaces_a_runner_started_otherwise(env):
    """Another fingerprint (another commit, key or token) is another runner:
    the service is removed and put again from this control plane's definition."""
    _, fake = await _sprites_create(env, defined=True, spec="not-this-one")
    assert fake.deleted == 1 and fake.bootstrap is not None
    assert fake.service["env"]["RUNNER_TOKEN"] == "stored"


async def test_sprites_create_makes_a_new_service(env):
    """A sprite with no runner service gets one: the PUT makes it, and starts it."""
    _, fake = await _sprites_create(env, defined=False)
    assert fake.deleted == 0
    assert fake.bootstrap == {"ORCHESTRATE_REPO": "https://example.com/fork.git", "ORCHESTRATE_COMMIT": "c0ffee"}


class LocalSprite:
    """The Sprites API's exec and filesystem calls, served on this machine.
    POST /exec runs `bash` with the given args and env, the request held until
    it ends, and answers as the API does: stdout, stderr and the exit code in
    frames with a stream byte and no length, which exec must not try to read.
    The install's scripts run for real (in tmp dirs; there is no
    /.sprite/api.sock here, so its hold only logs that it could not be made)."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.calls = 0

    async def post(self, url, params=None, **kw):
        assert url == "/sprites/orch-w1/exec"
        self.calls += 1
        args = [v for k, v in params if k == "cmd"]
        env = {**os.environ, **dict(v.split("=", 1) for k, v in params if k == "env"),
               "INSTALL_ROOT": str(self.root)}
        proc = await asyncio.create_subprocess_exec(*args, env=env, stdout=subprocess.PIPE,
                                                    stderr=subprocess.PIPE)
        out, err = await proc.communicate()
        body = b"\x01" + out + b"\x02" + err + bytes([3, proc.returncode])
        return httpx.Response(200, content=body, request=httpx.Request("POST", url))

    async def get(self, url, params=None, **kw):
        assert url == "/sprites/orch-w1/fs/read"
        path = Path(params["path"])
        if not path.exists():
            return httpx.Response(404, json={"error": "not found"}, request=httpx.Request("GET", url))
        return httpx.Response(200, content=path.read_bytes(), request=httpx.Request("GET", url))

    async def request(self, method, url, json=None, **kw):
        assert (method, url, json["recursive"]) == ("DELETE", "/sprites/orch-w1/fs/delete", True)
        shutil.rmtree(json["path"])
        return httpx.Response(200, request=httpx.Request(method, url))

    async def aclose(self):
        pass


def _local_sprites(env, tmp_path):
    env.setattr(providers, "EXEC_ROOT", str(tmp_path / "exec"))
    p = providers.SpritesProvider(token="tok", source="https://example.com/fork.git", ref="dev")
    p.http = LocalSprite(tmp_path / "install")
    return p


async def _install(env, tmp_path, script):
    env.setattr(providers, "INSTALL_LOOK_EVERY", 0.05)
    p = _local_sprites(env, tmp_path)
    steps = providers.Steps("worker orch-w1")
    await p.install("orch-w1", "c0ffee" * 6, steps, script=script)
    return p.http, [line.split("s  ", 1)[1] for line in steps.lines]


async def test_exec_reads_the_output_and_exit_code_from_files_not_the_framed_body(env, tmp_path):
    p = _local_sprites(env, tmp_path)
    assert await p.exec("orch-w1", 'echo "running $X"; echo oops >&2', env={"X": "now"}) == "running now\noops\n"
    with pytest.raises(RuntimeError, match="exited 4; its output:\nnope"):
        await p.exec("orch-w1", "echo nope; exit 4")
    assert not list((tmp_path / "exec").iterdir())  # each call's files are removed


async def test_the_install_runs_detached_and_is_watched_until_it_ends(env, tmp_path):
    """The install outlives the exec that starts it (a held exec came back as a
    bare 502 mid-install), so each exec is short: one starts it, the others
    look at it, noting the lines it wrote since the last look."""
    script = 'echo "fetching $ORCHESTRATE_COMMIT"\nsleep 0.3\nprintf "pixi install\\nready"\n'
    sprite, lines = await _install(env, tmp_path, script)
    assert sprite.calls >= 3  # started, looked at while it ran, looked at once it ended
    assert "  install | fetching " + "c0ffee" * 6 in lines
    assert lines[-2:] == ["  install | pixi install", "  install | ready"]  # the last line, without its newline


async def test_a_failed_install_says_how_it_ended(env, tmp_path):
    with pytest.raises(RuntimeError, match=r"failed \(exit 3\); its output ends:\nno pixi here"):
        await _install(env, tmp_path, "echo 'no pixi here'\nexit 3\n")


async def test_an_install_that_dies_is_gone_not_running(env, tmp_path):
    """A run whose process is not there and wrote no exit code (the sprite
    rebooted under it) is reported, not waited on."""
    with pytest.raises(RuntimeError, match="stopped without finishing"):
        await _install(env, tmp_path, 'echo started\nkill -9 "$(cat "$INSTALL_ROOT/$INSTALL_RUN/pid")"\n')


async def test_a_failed_sprites_call_says_what_the_api_answered():
    """A gateway's 502 has no body: the error says so, with the request id."""
    r = httpx.Response(502, headers={"fly-request-id": "01ABC"},
                       request=httpx.Request("POST", "https://api.sprites.dev/v1/sprites/orch-w1/exec"))
    assert providers.failure("exec on sprite orch-w1", r) == (
        "exec on sprite orch-w1 failed: 502 Bad Gateway (POST /v1/sprites/orch-w1/exec; fly-request-id 01ABC): "
        "the API answered with an empty body")


async def test_looking_for_the_runner_service_fails_on_an_api_error(env):
    """Only a 404 means "no runner service": a 502 is not taken for one."""
    class Broken(FakeSprites):
        async def get(self, url, **kw):
            if url.endswith("/services/runner"):
                return httpx.Response(502, request=httpx.Request("GET", url))
            return await super().get(url, **kw)

    with pytest.raises(RuntimeError, match="looking for sprite orch-ab12-w1's runner service failed: 502"):
        await _sprites_create(env, defined=True, fake=Broken(True))


async def test_a_cold_sprites_services_manager_is_waited_for(env):
    """A cold sprite's services API answers 500 "services manager not started"
    until it has booted it: that is waited out, then the answer read."""
    env.setattr(providers, "SERVICES_LOOK_EVERY", 0.0)

    class Booting(FakeSprites):
        booting = 2

        async def get(self, url, **kw):
            if url.endswith("/services/runner") and self.booting:
                self.booting -= 1
                return httpx.Response(500, text="services manager not started - call Start() first",
                                      request=httpx.Request("GET", url))
            return await super().get(url, **kw)

    _, fake = await _sprites_create(env, defined=False, fake=Booting(False))
    assert fake.booting == 0 and fake.bootstrap is not None  # then found no service, and installed


async def test_a_services_manager_that_never_starts_is_an_error(env):
    env.setattr(providers, "SERVICES_LOOK_EVERY", 0.0)
    env.setattr(providers, "SERVICES_WAIT", 0.05)

    class Stuck(FakeSprites):
        async def get(self, url, **kw):
            if url.endswith("/services/runner"):
                return httpx.Response(500, text="services manager not started - call Start() first",
                                      request=httpx.Request("GET", url))
            return await super().get(url, **kw)

    with pytest.raises(RuntimeError, match=r"services manager not started.*\(still, after 0s\)"):
        await _sprites_create(env, defined=True, fake=Stuck(True))


async def test_a_sprite_serving_other_code_is_refused(env):
    """A worker must run the commit its branch names now; one serving older
    code (a runner that outlived its definition) is an error, not a worker."""
    with pytest.raises(RuntimeError, match="runs orchestrate deadbeef, not c0ffee"):
        await _sprites_create(env, defined=True, serving="deadbeef")


async def test_a_local_runner_that_exits_at_startup_says_why(tmp_path, monkeypatch):
    import sys

    p = providers.LocalProvider(workdir=str(tmp_path), command=ECHO_RUNNER)
    monkeypatch.setattr(p, "command", lambda: [sys.executable, "-c",
                                               "print('bubblewrap cannot make a sandbox here'); raise SystemExit(1)"])
    with pytest.raises(RuntimeError, match="exited with code 1.*\n.*bubblewrap cannot make a sandbox here"):
        await p.create(providers.WorkerSpec(index=1, pool="ab12"))


async def test_local_follow_yields_the_backlog_then_each_new_line(tmp_path):
    p = providers.LocalProvider(workdir=str(tmp_path), command=ECHO_RUNNER)
    w = providers.Worker(id="p-w0", provider="local", handle="", url="")
    (tmp_path / "p-w0").mkdir()
    path = p.log_path("p-w0")
    path.write_bytes(b"one\ntwo\nthree\npart")
    env_lines = p.follow(w, 2)
    got = [await env_lines.__anext__(), await env_lines.__anext__()]
    with open(path, "ab") as f:
        f.write(b"ial\nfour\n")
    got += [await env_lines.__anext__(), await env_lines.__anext__()]
    await env_lines.aclose()
    assert got == ["two", "three", "partial", "four"]


async def test_sprites_follow_reopens_the_log_stream_without_repeating_lines(env, monkeypatch):
    """Each log stream ends when its follow time is up; the next one starts with
    lines the last one read, and those are not yielded twice."""
    monkeypatch.setattr(providers, "FOLLOW_AGAIN", 0)
    ev = lambda ts, data, typ="stdout": json.dumps({"type": typ, "data": data, "timestamp": ts})
    complete = json.dumps({"type": "complete", "timestamp": 9})
    streams = [
        [ev(1, "a\n"), ev(2, "b\nc\n"), complete],
        [ev(1, "a\n"), ev(2, "b\nc\n"), ev(2, "d\n", "stderr"),
         json.dumps({"type": "exit", "exit_code": 1, "timestamp": 3}), complete],
    ]
    asked = []

    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/v1/sprites/orch-p-w0/services/runner/logs"
        asked.append(dict(req.url.params))
        body = "\n".join(streams.pop(0)) + "\n" if streams else ""
        return httpx.Response(200, content=body.encode())

    p = providers.SpritesProvider(token="tok", source="https://example.com/r.git", ref="main")
    await p.http.aclose()
    p.http = httpx.AsyncClient(base_url=p.API, transport=httpx.MockTransport(handler))
    w = providers.Worker(id="orch-p-w0", provider="sprites", handle="orch-p-w0", url="")
    lines = p.follow(w, 50)
    got = [await lines.__anext__() for _ in range(5)]
    await lines.aclose()
    await p.http.aclose()
    assert got == ["a", "b", "c", "d", "[sprite] runner service exited with code 1"]
    assert asked[0]["lines"] == "50" and asked[1]["lines"] == "51"
    assert asked[0]["duration"] == "1h"


class SpriteLookup:
    """GET /sprites/<name> as the Sprites API answers it for a sprite that is there, or not."""

    def __init__(self, status: int) -> None:
        self.status = status

    async def get(self, url, **kw):
        return httpx.Response(self.status, json={}, request=httpx.Request("GET", url))

    async def aclose(self):
        pass


@pytest.mark.parametrize("status,there", [(200, True), (404, False)])
async def test_a_sprite_exists_until_the_api_no_longer_has_it(env, status, there):
    """A paused sprite is still there (it wakes on the next request); only
    one the API does not know any more is gone."""
    p = providers.SpritesProvider(token="tok", source="https://example.com/fork.git", ref="dev")
    await p.http.aclose()
    p.http = SpriteLookup(status)
    w = providers.Worker(id="orch-p-1-w1", provider="sprites", handle="orch-p-1-w1",
                         url="https://worker.invalid")
    assert await p.exists(w) is there


async def test_a_sprite_lookup_that_fails_is_not_an_answer(env):
    p = providers.SpritesProvider(token="tok", source="https://example.com/fork.git", ref="dev")
    await p.http.aclose()
    p.http = SpriteLookup(503)
    w = providers.Worker(id="w", provider="sprites", handle="w", url="https://worker.invalid")
    with pytest.raises(httpx.HTTPStatusError):
        await p.exists(w)


async def test_a_new_sprite_is_made_with_the_label(env):
    env.setattr(secrets, "get_secret", lambda name: "key")
    p = providers.SpritesProvider(token="tok", source="https://example.com/fork.git", ref="dev")
    made = []

    async def post(url, **kw):
        made.append(kw["json"])
        raise RuntimeError("stop after the create request")

    p.http = types.SimpleNamespace(post=post, aclose=lambda: asyncio.sleep(0))
    with pytest.raises(RuntimeError, match="stop after"):
        await p.create(providers.WorkerSpec(index=2, pool="ab12"))
    assert made == [{"name": "orch-ab12-w2", "url_settings": {"auth": "public"}, "labels": ["orchestrator-ab12"]}]


async def test_sprites_sweep_destroys_ours_outside_the_slots_only(env):
    """This pool's sprites beyond its slots go, and so do those from before
    pools had names (the one shared pool, each project's own workers); its
    slots, another control plane's pool and sprites that are not ours stay."""
    p = providers.SpritesProvider(token="tok", source="https://example.com/fork.git", ref="dev")
    pages = {None: {"sprites": [{"name": "orch-ab12-w1", "labels": ["orchestrator-ab12"]},
                                {"name": "orch-ab12-w4", "labels": ["orchestrator-ab12"]},
                                {"name": "orch-cd34-w4", "labels": ["orchestrator-cd34"]},
                                {"name": "orch-w1", "labels": ["orchestrator"]},
                                {"name": "orch-p-ab12cd34-w1", "labels": []}],
                    "has_more": True, "next_continuation_token": "next"},
             "next": {"sprites": [{"name": "orch-ab12-w3", "labels": ["orchestrator-ab12"]},
                                  {"name": "orch-notes", "labels": []}],
                      "has_more": False}}
    asked, deleted = [], []

    async def get(url, params=None, **kw):
        asked.append(params)
        return httpx.Response(200, json=pages[params.get("continuation_token")], request=httpx.Request("GET", url))

    async def delete(url, **kw):
        deleted.append(url)
        return httpx.Response(204, request=httpx.Request("DELETE", url))

    p.http = types.SimpleNamespace(get=get, delete=delete)
    assert await p.sweep(3, "ab12") == ["orch-ab12-w4", "orch-p-ab12cd34-w1", "orch-w1"]
    assert sorted(deleted) == ["/sprites/orch-ab12-w4", "/sprites/orch-p-ab12cd34-w1", "/sprites/orch-w1"]
    assert all(a["prefix"] == "orch-" for a in asked) and len(asked) == 2


async def test_local_sweep_deletes_worker_directories_outside_the_slots(tmp_path):
    for d in ("w1", "w2", "w3", "p_1-w1"):
        (tmp_path / d).mkdir()
    p = providers.LocalProvider(workdir=str(tmp_path), command=ECHO_RUNNER)
    assert await p.sweep(2, "ab12") == ["p_1-w1", "w3"]
    assert sorted(d.name for d in tmp_path.iterdir()) == ["w1", "w2"]


async def test_threads_from_before_places_get_where_they_ran(tmp_path):
    """A thread from before threads had a place gets the one its worker was
    in; one never on a worker is left for the router."""
    from control.service import Orchestrator
    from control.store import SqliteStore
    from tests.doubles import HeuristicRouter

    store = SqliteStore(str(tmp_path / "db.sqlite"))
    await store.open()
    p = await store.create_project("old")
    old = await store.create_thread(p["id"], "Old", "o", "")
    new = await store.create_thread(p["id"], "New", "n", "")
    await store.upsert_worker({"id": "orch-w1", "provider": "sprites", "handle": "orch-w1", "url": "u",
                               "token": "t", "capacity": 16, "index": 1})
    await store.update_thread(old["id"], worker_id="orch-w1")
    orch = Orchestrator(store, providers.Places({"local": Recording("local"), "sprites": Recording("sprites")}),
                        HeuristicRouter(), tmp_path / "repos")
    await orch.start()
    assert (await store.get_thread(old["id"]))["workers"] == "sprites"
    assert (await store.get_thread(new["id"]))["workers"] == ""
    await orch.close()
    await store.close()


async def test_the_router_model_never_places_a_thread_where_workers_cannot_run(monkeypatch):
    from control.router import ModelRouter

    places = [{"id": "local", "label": "This computer", "why": ""},
              {"id": "sprites", "label": "Sprites", "why": "it needs SPRITES_TOKEN"}]
    r = ModelRouter("key")

    async def answer(system, content, tool):
        assert content["places"] == places and "run it locally" in system
        assert tool["parameters"]["properties"]["workers"]["enum"] == ["local"]
        assert tool["parameters"]["required"] == ["workers", "where"]
        return {"workers": "sprites", "where": "it is code"}
    monkeypatch.setattr(r, "_call", answer)
    with pytest.raises(ValueError, match="picked no place"):
        await r.place("do it", places)
    monkeypatch.setattr(r, "_call", lambda *_: asyncio.sleep(0, {"workers": "local", "where": "Chrome runs here"}))
    assert await r.place("why does Chrome crash", places) == {"workers": "local", "where": "Chrome runs here"}


async def test_starting_never_waits_for_a_place_whose_sweep_hangs(tmp_path):
    """A provider that does not answer (Sprites over a bad network) holds no
    start up: its sweep runs in the background, and the other place's
    sweep and the store's bookkeeping go on without it."""
    from control.service import Orchestrator
    from control.store import SqliteStore
    from tests.doubles import HeuristicRouter

    class Hanging(Recording):
        async def sweep(self, slots, pool):
            await asyncio.Event().wait()

    store = SqliteStore(str(tmp_path / "db.sqlite"))
    await store.open()
    await store.upsert_worker({"id": "orch-w9", "provider": "local", "handle": "9", "url": "u",
                               "token": "t", "capacity": 16, "index": 9})
    orch = Orchestrator(store, providers.Places({"local": Recording("local"), "sprites": Hanging("sprites")}),
                        HeuristicRouter(), tmp_path / "repos")
    await asyncio.wait_for(orch.start(), 2)
    local_sweep, sprites_sweep = orch.sweeping
    await asyncio.wait_for(local_sweep, 2)
    assert not sprites_sweep.done()
    assert (await store.list_workers())[0]["lost_at"] is not None, "outside the pool: the local sweep lost it"
    await orch.close()
    assert sprites_sweep.cancelled()
    await store.close()


async def test_a_worker_of_the_shared_pool_is_lost_before_any_thread_can_use_it(tmp_path):
    """Each control plane's workers are a pool of its own. A stored worker of
    the pool every control plane shared (orch-w1) is lost as the control
    plane starts, before a thread could be put on it, and each place is
    swept with this pool's name (the sweep destroys it)."""
    from control.service import Orchestrator
    from control.store import SqliteStore
    from tests.doubles import HeuristicRouter

    store = SqliteStore(str(tmp_path / "db.sqlite"))
    await store.open()
    await store.upsert_worker({"id": "orch-w1", "provider": "sprites", "handle": "orch-w1", "url": "u",
                               "token": "t", "capacity": 16, "index": 1})
    local, sprites = Recording("local"), Recording("sprites")
    orch = Orchestrator(store, providers.Places({"local": local, "sprites": sprites}),
                        HeuristicRouter(), tmp_path / "repos")
    await orch.start()
    name = await store.pool_id()
    assert orch.pool_id == name
    row = (await store.list_workers())[0]
    assert row["lost_at"] is not None and name in row["lost_reason"]
    await asyncio.gather(*orch.sweeping)
    assert local.swept == [name] and sprites.swept == [name]
    await orch.close()
    await store.close()


async def test_a_local_runner_left_serving_its_worker_directory_is_stopped_before_another_starts(tmp_path):
    """A runner an earlier control plane left behind (killed before it could
    stop its workers, or `pixi run` not passing its SIGTERM on) still holds
    the worker's directory, and with it its threads' sessions. Starting the
    worker again stops that runner first, by the lock it holds there
    (runner/src/runner/thread.py, claim_workdir), so the new one is the only
    runner on the directory."""
    wdir = tmp_path / "w1"
    left = subprocess.Popen(ECHO_RUNNER, cwd=str(Path(providers.__file__).resolve().parents[1]),
                            start_new_session=True,  # not a child the new provider knows of
                            env={**os.environ, "RUNNER_PORT": str(providers.free_port()),
                                 "RUNNER_WORKDIR": str(wdir)})
    try:
        for _ in range(200):
            if providers.runner_holding(wdir) == left.pid:
                break
            time.sleep(0.05)
        assert providers.runner_holding(wdir) == left.pid
        p = providers.LocalProvider(workdir=str(tmp_path), command=ECHO_RUNNER)
        w = await p.create(providers.WorkerSpec(index=1, pool="ab12"))
        try:
            assert left.wait(5) is not None
            assert providers.runner_holding(wdir) not in (None, left.pid)
            async with httpx.AsyncClient() as c:
                assert (await c.get(f"{w.url}/health")).status_code == 200
        finally:
            await p.close()
        assert providers.runner_holding(wdir) is None
    finally:
        if left.poll() is None:
            left.kill()
