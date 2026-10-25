"""The tests drive the window against the real control plane (../orchestrator),
served in-process on a free port with its echo runner and rule-based router,
so they need no credentials and never start a sprite or call a model."""

import asyncio
import socket
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent / "orchestrator"))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def control_plane(tmp_path, monkeypatch):
    import uvicorn

    from control.app import create_app
    from control.providers import LocalProvider
    from control.router import HeuristicRouter
    from control.store import SqliteStore

    monkeypatch.setenv("DEVRUNNER_DELAY", "0.05")
    port = free_port()
    # Set before the server stops, it ends the SSE streams the window holds open.
    stop = asyncio.Event()
    app = create_app(SqliteStore(str(tmp_path / "db.sqlite")),
                     LocalProvider(workdir=str(tmp_path / "workers"), use_devrunner=True), HeuristicRouter(),
                     shutdown_event=stop)
    srv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    loop = asyncio.new_event_loop()
    th = threading.Thread(target=loop.run_until_complete, args=(srv.serve(),), daemon=True)
    th.start()
    deadline = time.time() + 10
    while not srv.started and time.time() < deadline:
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    loop.call_soon_threadsafe(stop.set)
    srv.should_exit = True
    th.join(10)
