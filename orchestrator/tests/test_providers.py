"""Where workers run and what address they call back on.

No network and no keyring: `control.secrets.get_secret` is patched, so these
run anywhere.
"""

import subprocess

import pytest

from control import providers, secrets
from control.context import base_url_from_scope, request_base_url
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


async def test_a_loopback_address_still_uses_sprites(env, capsys):
    env.setattr(secrets, "get_secret", lambda name: "tok" if name == "SPRITES_TOKEN" else None)
    local, sprites = Recording("local"), Recording("sprites")
    p = providers.AutoProvider(local=local, sprites=sprites)
    worker = await p.create(worker_spec({"id": "p_1"}, 1, "http://127.0.0.1:8000"))
    assert worker.provider == "sprites" and not local.created
    assert "peer API" in capsys.readouterr().err


async def test_a_loopback_callback_switches_to_pull_mode(env):
    env.setattr(secrets, "get_secret", lambda name: "tok" if name == "SPRITES_TOKEN" else None)
    sprites = Recording("sprites")
    p = providers.AutoProvider(sprites=sprites)
    await p.create(worker_spec({"id": "p_1"}, 1, "http://127.0.0.1:8000"))
    assert sprites.created[0].orchestrator_url == ""
    assert sprites.created[0].peer_mode == "pull"


def test_peer_env():
    w = providers.Worker(id="w", project_id="p", provider="sprites", handle="", url="u")
    pull = providers.WorkerSpec(project_id="p", index=1, peer_mode="pull")
    assert providers.peer_env(pull, w) == {"RUNNER_PEERS": "pull"} and w.peers == "pull"
    direct = providers.WorkerSpec(project_id="p", index=1, orchestrator_url="https://o.example.com")
    assert providers.peer_env(direct, w) == {"ORCHESTRATOR_URL": "https://o.example.com",
                                             "ORCHESTRATOR_TOKEN": w.token} and w.peers == "http"


async def test_a_public_address_moves_workers_to_sprites(env):
    env.setattr(secrets, "get_secret", lambda name: "tok" if name == "SPRITES_TOKEN" else None)
    local, sprites = Recording("local"), Recording("sprites")
    p = providers.AutoProvider(local=local, sprites=sprites)
    await p.create(worker_spec({"id": "p_1"}, 1, "https://orch.example.com"))
    assert sprites.created and not local.created


async def test_a_public_url_override_keeps_the_callback(env):
    env.setattr(secrets, "get_secret", lambda name: "tok" if name == "SPRITES_TOKEN" else None)
    sprites = Recording("sprites")
    p = providers.AutoProvider(sprites=sprites)
    token = request_base_url.set("http://127.0.0.1:8000")
    try:
        spec = worker_spec({"id": "p_1"}, 1, "https://orch.example.com")
    finally:
        request_base_url.reset(token)
    await p.create(spec)
    assert sprites.created[0].orchestrator_url == "https://orch.example.com"
    assert sprites.created[0].peer_mode == ""


async def test_a_public_address_without_a_token_stays_local(env, capsys):
    local = Recording("local")
    p = providers.AutoProvider(local=local)
    await p.create(worker_spec({"id": "p_1"}, 1, "https://orch.example.com"))
    assert local.created
    assert "SPRITES_TOKEN is not stored" in capsys.readouterr().err


async def test_destroy_goes_to_the_provider_that_made_the_worker(env):
    local, sprites = Recording("local"), Recording("sprites")
    p = providers.AutoProvider(local=local, sprites=sprites)
    worker = providers.Worker(id="x", project_id="p_1", provider="sprites", handle="x", url="u")
    await p.destroy(worker)
    assert sprites.destroyed == [worker] and not local.destroyed


@pytest.mark.parametrize("url,ok", [
    ("https://orch.example.com", True),
    ("http://93.184.216.34:8000", True),
    ("http://127.0.0.1:8000", False),
    ("http://localhost:8000", False),
    ("http://192.168.1.5:8000", False),
    ("http://0.0.0.0:8000", False),
    ("", False),
])
def test_public_url_ok(url, ok):
    assert providers.public_url_ok(url) is ok


def test_passthrough_env_reads_a_keyring_only_secret(env):
    env.setattr(secrets, "get_secret", lambda name: "deepseek" if name == "DEEPSEEK_API_KEY" else None)
    assert providers.passthrough_env() == {"DEEPSEEK_API_KEY": "deepseek"}


def test_passthrough_env_never_includes_the_sprites_token(env):
    env.setattr(secrets, "get_secret", lambda name: f"value-{name}")
    kept = providers.passthrough_env()
    assert "SPRITES_TOKEN" not in kept


@pytest.mark.parametrize("url,rule", [
    ("https://orch.example.com", {"action": "allow", "domain": "orch.example.com"}),
    ("http://93.184.216.34:8000", {"action": "allow", "address": "93.184.216.34",
                                    "protocol": "tcp", "port": 8000}),
    ("https://93.184.216.34", {"action": "allow", "address": "93.184.216.34",
                               "protocol": "tcp", "port": 443}),
    ("", None),
])
def test_allow_host_rule(url, rule):
    assert providers.allow_host_rule(url) == rule


def _scope(headers, scheme="http"):
    return {"type": "http", "scheme": scheme,
            "headers": [(k.encode(), v.encode()) for k, v in headers]}


def test_base_url_comes_from_the_request_headers():
    assert base_url_from_scope(_scope([("host", "127.0.0.1:8000")])) == "http://127.0.0.1:8000"
    assert base_url_from_scope(
        _scope([("host", "orch.example.com"), ("x-forwarded-proto", "https")])) == "https://orch.example.com"
    assert base_url_from_scope(
        _scope([("host", "0.0.0.0:8000"), ("x-forwarded-host", "orch.example.com")])) == "http://orch.example.com"


def test_base_url_rejects_a_host_that_is_not_an_address():
    assert base_url_from_scope(_scope([("host", "evil.example/../x")])) == ""
    assert base_url_from_scope(_scope([])) == ""
    assert base_url_from_scope({"type": "lifespan", "headers": []}) == ""


async def test_the_app_records_the_address_for_the_duration_of_a_request():
    from control.app import BaseUrlMiddleware

    seen = {}

    async def app(scope, receive, send):
        seen["url"] = request_base_url.get()

    middleware = BaseUrlMiddleware(app)
    await middleware(_scope([("host", "orch.example.com"), ("x-forwarded-proto", "https")]), None, None)
    assert seen["url"] == "https://orch.example.com"
    assert request_base_url.get() == ""  # reset, so the next request starts clean


def test_worker_spec_uses_the_address_of_the_current_request():
    token = request_base_url.set("https://orch.example.com")
    try:
        spec = worker_spec({"id": "p_1", "repo_url": "https://example.com/r.git"}, 2)
    finally:
        request_base_url.reset(token)
    assert (spec.orchestrator_url, spec.index, spec.repo_url) == (
        "https://orch.example.com", 2, "https://example.com/r.git")


def test_worker_spec_override_wins_over_the_request():
    token = request_base_url.set("http://127.0.0.1:8000")
    try:
        spec = worker_spec({"id": "p_1"}, 1, "https://orch.example.com")
    finally:
        request_base_url.reset(token)
    assert spec.orchestrator_url == "https://orch.example.com"


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

