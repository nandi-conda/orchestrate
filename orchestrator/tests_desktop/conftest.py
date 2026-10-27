"""The desktop window, with its control plane in the same process as in
`pixi run desktop`, but on the echo runner and the rule-based router, so the
tests need no credentials and never start a sprite or call a model. The
window shows the real ultra UI from ui/dist, which `pixi run test-desktop`
builds first."""

import os
import socket
import sys
from pathlib import Path

import pytest
from PySide6.QtCore import QCoreApplication, QEvent

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
# Chromium's sandbox refuses to run as root, which test containers often are.
os.environ.setdefault("QTWEBENGINE_DISABLE_SANDBOX", "1")
# Qt's own folder dialog, which a test can drive: a theme name no plugin has
# gives Qt's plain theme. The desktop's portal dialog (prepare_qt) would pop
# up on the screen of whoever runs the tests.
os.environ["QT_QPA_PLATFORMTHEME"] = "plain"


def pytest_configure(config):
    """Draw as `pixi run desktop` does, before pytest-qt makes the QApplication."""
    from desktop.window import prepare_qt
    prepare_qt()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def echo_app(tmp_path):
    """The control plane's app on the echo runner, for ControlPlane."""
    from control.app import create_app
    from control.providers import LocalProvider
    from tests.doubles import ECHO_RUNNER, HeuristicRouter, local
    from control.store import SqliteStore

    def make(shutdown, on_startup, store=None):
        return create_app(store or SqliteStore(str(tmp_path / "db.sqlite")),
                          local(LocalProvider(workdir=str(tmp_path / "workers"), command=ECHO_RUNNER)), HeuristicRouter(),
                          shutdown_event=shutdown, repos=str(tmp_path / "repos"), on_startup=on_startup)
    return make


@pytest.fixture(autouse=True)
def delete_windows(qapp):
    """Delete each test's windows before the next test (pytest-qt only
    schedules it). A web page left for interpreter exit crashes Chromium's
    teardown."""
    yield
    QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
    qapp.processEvents()
