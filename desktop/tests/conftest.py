"""The tests start the real control plane (../orchestrator) the way the window
does, as a process it owns, but on its echo runner and rule-based router
(serve_echo.py), so they need no credentials and never start a sprite or call
a model. The window then shows the real ultra UI from ../orchestrator/ui/dist,
which `pixi run test` builds first."""

import os
import socket
import sys
from pathlib import Path

import pytest
from PySide6.QtCore import QCoreApplication, QEvent

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# Chromium's sandbox refuses to run as root, which test containers often are.
os.environ.setdefault("QTWEBENGINE_DISABLE_SANDBOX", "1")
# Offscreen there is no OpenGL, and Chromium's GPU process hangs looking for it.
os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(autouse=True)
def delete_windows(qapp):
    """Delete each test's windows before the next test (pytest-qt only
    schedules it). A web page left for interpreter exit crashes Chromium's
    teardown."""
    yield
    QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
    qapp.processEvents()
