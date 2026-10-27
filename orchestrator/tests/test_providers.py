"""Where workers run.

No network and no keyring: `control.secrets.get_secret` is patched, so these
run anywhere.
"""

import asyncio
import json
import os
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

    async def sweep(self, slots, pool):
        return [f"{self.name}-swept-{pool}-beyond-{slots}"]

    async def close(self) -> None:
        pass


async def test_a_token_means_sprites(env):
    env.setattr(secrets, "get_secret", lambda name: "tok" if name == "SPRITES_TOKEN" else None)
    local, sprites = Recording("local"), Recording("sprites")
    p = providers.AutoProvider(local=local, sprites=sprites)
    worker = await p.create(worker_spec(1))
    assert worker.provider == "sprites" and sprites.created and not local.created


async def test_no_token_means_local_workers(env, capsys):
    local = Recording("local")
    p = providers.AutoProvider(local=local)
    await p.create(worker_spec(1))
    assert local.created
    assert "SPRITES_TOKEN is not stored" in capsys.readouterr().err


async def test_destroy_goes_to_the_provider_that_made_the_worker(env):
    local, sprites = Recording("local"), Recording("sprites")
    p = providers.AutoProvider(local=local, sprites=sprites)
    worker = providers.Worker(id="x", provider="sprites", handle="x", url="u")
    await p.destroy(worker)
    assert sprites.destroyed == [worker] and not local.destroyed


def test_passthrough_env_reads_a_keyring_only_secret(env):
    env.setattr(secrets, "get_secret", lambda name: "deepseek" if name == "DEEPSEEK_API_KEY" else None)
    assert providers.passthrough_env() == {"DEEPSEEK_API_KEY": "deepseek"}


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


def test_checkout_note_warns_when_the_branch_was_never_pushed(tmp_path):
    root = _checkout(tmp_path)
    assert "clone will fail" in providers.checkout_note(root, "feature/x")


def test_checkout_note_is_quiet_when_the_branch_matches(tmp_path):
    root = _checkout(tmp_path)
    _commit(root, "one")
    subprocess.run(["git", "-C", str(root), "update-ref", "refs/remotes/origin/feature/x", "HEAD"], check=True)
    assert providers.checkout_note(root, "feature/x") == ""


def test_checkout_note_warns_about_unpushed_commits(tmp_path):
    root = _checkout(tmp_path)
    _commit(root, "one")
    subprocess.run(["git", "-C", str(root), "update-ref", "refs/remotes/origin/feature/x", "HEAD"], check=True)
    _commit(root, "two")
    assert "not this checkout's HEAD" in providers.checkout_note(root, "feature/x")


async def test_sprites_provider_takes_source_and_ref_and_names_sprites(env):
    env.setattr(secrets, "get_secret", lambda name: "tok")
    p = providers.SpritesProvider(source="https://example.com/fork.git", ref="dev")
    try:
        assert (p.source, p.ref) == ("https://example.com/fork.git", "dev")
        assert p.sprite_name(1, "3f9a1c2b") == "orch-3f9a1c2b-w1"
        with pytest.raises(ValueError, match="pool"):
            p.sprite_name(1, "")
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
        return {"spawn": [], "send": [], "why": ""}

    r = ModelRouter("key")
    monkeypatch.setattr(r, "_ask", empty)
    with pytest.raises(ValueError, match="planned nothing"):
        await r.plan("do it", [])


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
    already exists, with or without a runner service defined."""

    def __init__(self, defined: bool) -> None:
        self.defined = defined
        self.actions: list[str] = []
        self.service: dict = {}

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
            self.service = kw["json"]
        else:
            self.labels = kw["json"]["labels"]
        return httpx.Response(200, request=httpx.Request("PUT", url))

    def stream(self, method, url, **kw):
        self.actions.append(url.rsplit("/", 1)[-1])
        return FakeStream()

    async def aclose(self):
        pass


class FakeStream:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def raise_for_status(self):
        pass

    async def aiter_lines(self):
        yield '{"type": "complete"}'


async def _sprites_create(env, defined: bool, serving: str = "c0ffee", spec: str = "", fake=None):
    env.setattr(secrets, "get_secret", lambda name: "key")
    p = providers.SpritesProvider(token="tok", source="https://example.com/fork.git", ref="dev")
    fake = fake or FakeSprites(defined)
    fake.bootstrap = None
    p.http = fake

    async def no_exec(name, script, env=None):
        fake.bootstrap = env
        return ""

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
    env.setattr(p, "exec", no_exec)
    env.setattr(providers, "wait_healthy", healthy)
    env.setattr(p, "accepts_token", accepts)
    try:
        return await p.create(providers.WorkerSpec(index=1, pool="ab12", token="stored")), fake
    finally:
        await fake.aclose()


async def test_sprites_create_restarts_a_runner_an_earlier_control_plane_started(env):
    """`start` leaves a running runner as it was: its old code and its old
    model key. A service already defined is restarted from the new definition."""
    worker, fake = await _sprites_create(env, defined=True)
    assert worker.id == "orch-ab12-w1" and fake.labels == ["orchestrator-ab12"]  # the slot's sprite, labeled
    assert fake.actions == ["restart"]
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
    worker, fake = await _sprites_create(env, defined=True, spec=want, fake=fake)
    assert fake.actions == ["restart"] and fake.bootstrap is None  # nothing more: no bootstrap, no restart
    assert worker.token == "stored"


async def test_sprites_create_restarts_a_runner_started_otherwise(env):
    """Another fingerprint (another commit, key or token) is another runner:
    the service is restarted from this control plane's definition."""
    _, fake = await _sprites_create(env, defined=True, spec="not-this-one")
    assert fake.actions == ["restart"] and fake.bootstrap is not None


async def test_sprites_create_starts_a_new_service(env):
    _, fake = await _sprites_create(env, defined=False)
    assert fake.actions == ["start"]
    assert fake.bootstrap == {"ORCHESTRATE_REPO": "https://example.com/fork.git", "ORCHESTRATE_COMMIT": "c0ffee"}


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


async def test_auto_sweeps_the_provider_the_pool_is_on(env):
    env.setattr(secrets, "get_secret", lambda name: "tok" if name == "SPRITES_TOKEN" else None)
    p = providers.AutoProvider(local=Recording("local"), sprites=Recording("sprites"))
    assert await p.sweep(3, "ab12") == ["sprites-swept-ab12-beyond-3"]
