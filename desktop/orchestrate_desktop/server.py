"""The control plane whose ultra UI the window shows.

By default the window starts the control plane itself (`pixi run serve` in
../orchestrator, which builds and serves the ultra UI) and owns it: it waits
for its /health, and closing the window stops it. Given --url, the window
connects to a control plane someone else runs and never starts one."""

from __future__ import annotations

import ctypes
import os
import shutil
import signal
import socket
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit

from PySide6.QtCore import QObject, QTimer, QUrl, Signal
from PySide6.QtNetwork import QNetworkAccessManager, QNetworkReply, QNetworkRequest

ORCHESTRATOR = Path(__file__).resolve().parents[2] / "orchestrator"
POLL_MS = 500
# How often a running control plane this window owns is checked for having exited.
WATCH_MS = 2000
# How long a control plane gets to stop (and destroy its sprites) before it is killed.
STOP_SECONDS = 15

STARTING, UP, DOWN = "starting", "up", "down"


def serve_command(port: int) -> list[str]:
    """`pixi run serve` in ../orchestrator on port; pixi installs its environment on first use."""
    pixi = shutil.which("pixi") or "pixi"
    return [pixi, "run", "--manifest-path", str(ORCHESTRATOR / "pixi.toml"), "serve", "--port", str(port)]


def port_in_use(port: int) -> bool:
    with socket.socket() as s:
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return True
    return False


def die_with_parent() -> None:
    """In the child, before exec (Linux): get SIGTERM when this window's
    process ends, however it ends, so a crashed window leaves no control
    plane behind holding the port. pixi passes SIGTERM on to the server."""
    PR_SET_PDEATHSIG = 1
    ctypes.CDLL(None, use_errno=True).prctl(PR_SET_PDEATHSIG, signal.SIGTERM)


class ControlPlane(QObject):
    """The window's control plane: starting, up, or down (with why).
    `changed` carries (state, text). With a command it is started and owned;
    without one it is only connected to."""

    changed = Signal(str, str)

    def __init__(self, base_url: str, command: list[str] | None, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self.base_url = base_url.rstrip("/")
        self.port = urlsplit(self.base_url).port or 80
        self.net = QNetworkAccessManager(self)
        self.command = command
        self.proc: subprocess.Popen | None = None
        self.state, self.text = STARTING, ""
        self.poll = QTimer(self)
        self.poll.setSingleShot(True)
        self.poll.timeout.connect(self._probe)
        self.watch = QTimer(self)
        self.watch.timeout.connect(self._watch)

    def start(self) -> None:
        if self.command is None:
            self._set(STARTING, f"Connecting to {self.base_url}…")
            self._probe()
            return
        if port_in_use(self.port):
            # Whatever holds it would answer /health in our control plane's place.
            self._set(DOWN, f"Port {self.port} is already in use, so this window can't start its control "
                            f"plane there. If it is a control plane left from an earlier run, stop it with "
                            f"`fuser -k {self.port}/tcp` (Linux) or `kill $(lsof -t -i :{self.port})`, then "
                            f"Retry. Or start on another port: `pixi run desktop --port {self.port + 100}`.")
            return
        kw: dict = {"cwd": str(ORCHESTRATOR)}
        if sys.platform == "win32":
            kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kw["start_new_session"] = True
        if sys.platform == "linux":
            kw["preexec_fn"] = die_with_parent
        try:
            self.proc = subprocess.Popen(self.command, **kw)
        except OSError as e:
            self._set(DOWN, f"Could not start the control plane ({' '.join(self.command)}): {e}")
            return
        self._set(STARTING, f"Starting the control plane at {self.base_url}. The first run installs "
                            "its environment and builds the web UI, which takes a while; its output is in "
                            "the terminal.")
        self._probe()

    def _set(self, state: str, text: str) -> None:
        self.state, self.text = state, text
        self.changed.emit(state, text)

    def _probe(self) -> None:
        reply = self.net.get(QNetworkRequest(QUrl(self.base_url + "/health")))
        reply.finished.connect(lambda: self._probed(reply))

    def _probed(self, reply: QNetworkReply) -> None:
        reply.deleteLater()
        status = reply.attribute(QNetworkRequest.Attribute.HttpStatusCodeAttribute)
        if status == 200:
            owned = "; it stops when this window closes" if self.proc else ""
            if self.proc:
                self.watch.start(WATCH_MS)
            self._set(UP, f"Control plane at {self.base_url}{owned}")
            return
        if self.proc is not None:
            code = self.proc.poll()
            if code is None:
                self.poll.start(POLL_MS)  # still starting
                return
            self.proc = None
            self._set(DOWN, f"The control plane exited with code {code} before answering at "
                            f"{self.base_url}. Its output is in the terminal.")
            return
        why = reply.errorString() if status is None else f"/health answered HTTP {status}"
        self._set(DOWN, f"No control plane at {self.base_url}: {why}.")

    def _watch(self) -> None:
        code = self.proc.poll() if self.proc else None
        if self.proc is None or code is None:
            return
        self.watch.stop()
        self.proc = None
        self._set(DOWN, f"The control plane at {self.base_url} exited with code {code}. "
                        "Its output is in the terminal.")

    def stop(self) -> None:
        """Stop the control plane this client started, gracefully (so it
        destroys its sprites), killing it only if it outlasts STOP_SECONDS."""
        self.poll.stop()
        self.watch.stop()
        proc, self.proc = self.proc, None
        if proc is None or proc.poll() is not None:
            return
        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(STOP_SECONDS)
        except subprocess.TimeoutExpired:
            if sys.platform == "win32":
                proc.kill()
            else:
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
