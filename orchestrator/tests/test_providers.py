"""Where workers run.

No network and no keyring: `control.secrets.get_secret` is patched, so these
run anywhere.
"""

import json
import subprocess

import httpx
import pytest

from control import providers, secrets
from control.service import worker_spec

pytestmark = pytest.mark.anyio


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
        return providers.Worker(id=f"{self.name}-w", project_id=spec.project_id, provider=self.name,
                                handle="", url="http://example.invalid")

    async def destroy(self, worker) -> None:
        self.destroyed.append(worker)

    async def close(self) -> None:
        pass


async def test_a_token_means_sprites(env):
    env.setattr(secrets, "get_secret", lambda name: "tok" if name == "SPRITES_TOKEN" else None)
    local, sprites = Recording("local"), Recording("sprites")
    p = providers.AutoProvider(local=local, sprites=sprites)
    worker = await p.create(worker_spec({"id": "p_1"}, 1))
    assert worker.provider == "sprites" and sprites.created and not local.created


async def test_no_token_means_local_workers(env, capsys):
    local = Recording("local")
    p = providers.AutoProvider(local=local)
    await p.create(worker_spec({"id": "p_1"}, 1))
    assert local.created
    assert "SPRITES_TOKEN is not stored" in capsys.readouterr().err


async def test_destroy_goes_to_the_provider_that_made_the_worker(env):
    local, sprites = Recording("local"), Recording("sprites")
    p = providers.AutoProvider(local=local, sprites=sprites)
    worker = providers.Worker(id="x", project_id="p_1", provider="sprites", handle="x", url="u")
    await p.destroy(worker)
    assert sprites.destroyed == [worker] and not local.destroyed


def test_passthrough_env_reads_a_keyring_only_secret(env):
    env.setattr(secrets, "get_secret", lambda name: "deepseek" if name == "DEEPSEEK_API_KEY" else None)
    assert providers.passthrough_env() == {"DEEPSEEK_API_KEY": "deepseek"}


def test_passthrough_env_never_includes_the_sprites_token(env):
    env.setattr(secrets, "get_secret", lambda name: f"value-{name}")
    kept = providers.passthrough_env()
    assert "SPRITES_TOKEN" not in kept
    assert kept["GITHUB_TOKEN"] == "value-GITHUB_TOKEN"  # workers push and land with it


def test_worker_spec_carries_no_control_plane_address():
    spec = worker_spec({"id": "p_1", "repo_url": "https://example.com/r.git"}, 2)
    assert (spec.index, spec.repo_url) == (2, "https://example.com/r.git")
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
        assert p.sprite_name(providers.WorkerSpec(project_id="p_ab12cd34", index=1)) == "orch-p-ab12cd34-w1"
    finally:
        await p.close()


async def test_sprites_create_checks_for_the_model_key_before_creating_a_sprite(env):
    env.setattr(secrets, "get_secret", lambda name: "tok" if name == "SPRITES_TOKEN" else None)
    p = providers.SpritesProvider(source="https://example.com/fork.git", ref="dev")
    try:
        with pytest.raises(RuntimeError, match="DEEPSEEK_API_KEY"):
            await p.create(providers.WorkerSpec(project_id="p_1", index=1))
    finally:
        await p.close()


async def test_sprites_create_keeps_the_token_a_running_service_uses(env, monkeypatch):
    """A reattached sprite's runner still answers to the token it was started
    with; PUT + start does not replace a running process. Using the token in
    the worker spec instead made every call to the runner a 401."""
    env.setattr(secrets, "get_secret", lambda name: "tok")

    async def healthy(url, timeout=120.0):
        return None
    monkeypatch.setattr(providers, "wait_healthy", healthy)

    service = {}

    def handler(req):
        path = req.url.path
        if req.method == "POST" and path.endswith("/sprites"):
            return httpx.Response(201, json={})
        if req.method == "GET" and path.endswith("/services/runner"):
            return httpx.Response(200, json={"env": {"RUNNER_TOKEN": "running-token"}})
        if req.method == "PUT" and path.endswith("/services/runner"):
            service.update(json.loads(req.content))
            return httpx.Response(200, json={})
        if req.method == "POST" and path.endswith("/services/runner/start"):
            return httpx.Response(200, text="")
        if req.method == "POST" and path.endswith("/exec"):
            return httpx.Response(200, text="")
        if req.method == "GET" and path.endswith("/orch-p-1-w1"):
            return httpx.Response(200, json={"url": "https://w.invalid"})
        raise AssertionError(f"unexpected {req.method} {path}")

    p = providers.SpritesProvider(source="https://example.com/fork.git", ref="dev")
    try:
        await p.http.aclose()
        p.http = httpx.AsyncClient(base_url=p.API, transport=httpx.MockTransport(handler))
        worker = await p.create(providers.WorkerSpec(project_id="p_1", index=1, token="stored-token"))
        assert worker.token == "running-token"  # the running service's, not the stored one
        assert service["env"]["RUNNER_TOKEN"] == "running-token"
    finally:
        await p.close()



async def test_a_local_runner_that_exits_at_startup_says_why(tmp_path, monkeypatch):
    import sys

    p = providers.LocalProvider(workdir=str(tmp_path), use_devrunner=True)
    monkeypatch.setattr(p, "command", lambda: [sys.executable, "-c",
                                               "print('bubblewrap cannot make a sandbox here'); raise SystemExit(1)"])
    with pytest.raises(RuntimeError, match="exited with code 1.*\n.*bubblewrap cannot make a sandbox here"):
        await p.create(providers.WorkerSpec(project_id="p_1", index=1))
