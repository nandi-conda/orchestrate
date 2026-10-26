"""The control plane's data lives in ~/.orchestrate (control/paths.py), and
what an older one kept in the checkout is moved there once."""

import inspect
import subprocess
from pathlib import Path

import pytest

from control import paths
from control.app import create_app
from control.hub import Hub
from control.providers import LocalProvider
from control.store import SqliteStore, store_from_url

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def git(path, *args) -> str:
    return subprocess.run(["git", "-C", str(path), *args], check=True, capture_output=True, text=True).stdout.strip()


def test_the_defaults_are_in_the_users_data_directory():
    assert paths.DATA == Path.home() / ".orchestrate"
    assert SqliteStore().path == paths.DB == paths.DATA / "orchestrator.db"
    assert store_from_url(None).path == paths.DB
    assert LocalProvider(use_devrunner=True).workdir == paths.WORKERS == paths.DATA / "workers"
    assert inspect.signature(create_app).parameters["repos"].default == paths.REPOS == paths.DATA / "repos"


async def test_the_checkouts_data_is_moved_and_still_works(tmp_path):
    old, data = tmp_path / "orchestrator", tmp_path / "home" / ".orchestrate"
    store = SqliteStore(old / "orchestrator.db")
    await store.open()
    p = await store.create_project("kept")
    await store.close()
    hub = Hub(old / ".orchestrator" / "repos" / f"{p['id']}.git", p["id"])
    await hub.open()
    (old / ".orchestrator" / "workers" / "w1").mkdir(parents=True)
    (old / ".orchestrator" / "workers" / "w1" / "runner.log").write_text("ran\n")

    moved = paths.migrate(old, data)

    assert {dst.relative_to(data).as_posix() for _, dst in moved} >= {"orchestrator.db", "repos", "workers"}
    assert not (old / "orchestrator.db").exists() and not (old / ".orchestrator").exists()
    store = SqliteStore(data / "orchestrator.db")
    await store.open()
    assert [q["name"] for q in await store.list_projects()] == ["kept"]
    await store.close()
    hub = Hub(data / "repos" / f"{p['id']}.git", p["id"])
    await hub.open()
    assert git(hub.path, "log", "--format=%s", "main") == "Start the project"
    assert (data / "workers" / "w1" / "runner.log").read_text() == "ran\n"
    assert paths.migrate(old, data) == []  # once


def test_data_in_both_places_is_refused_before_anything_moves(tmp_path):
    old, data = tmp_path / "orchestrator", tmp_path / ".orchestrate"
    (old / ".orchestrator" / "repos").mkdir(parents=True)
    (old / "orchestrator.db").write_text("old")
    data.mkdir()
    (data / "orchestrator.db").write_text("new")
    with pytest.raises(RuntimeError, match="keep one"):
        paths.migrate(old, data)
    assert (old / "orchestrator.db").read_text() == "old" and (old / ".orchestrator" / "repos").exists()
    assert not (data / "repos").exists()
