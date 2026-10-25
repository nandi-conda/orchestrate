"""The control plane the window talks to.

By default the client starts the control plane itself (`pixi run serve` in
../orchestrator, which also serves the web UI) and owns it: the window waits
for its /health, and closing the window stops it. Given --url, the client
connects to a control plane someone else runs and never starts one."""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
from pathlib import Path

from PySide6.QtCore import QObject, QTimer, Signal
from PySide6.QtNetwork import QNetworkReply, QNetworkRequest

from .api import Api

ORCHESTRATOR = Path(__file__).resolve().parents[2] / "orchestrator"
POLL_MS = 500
# How long a control plane gets to stop (and destroy its sprites) before it is killed.
STOP_SECONDS = 15

STARTING, UP, DOWN = "starting", "up", "down"


def serve_command(port: int) -> list[str]:
    """`pixi run serve` in ../orchestrator on port; pixi installs its environment on first use."""
    pixi = shutil.which("pixi") or "pixi"
    return [pixi, "run", "--manifest-path", str(ORCHESTRATOR / "pixi.toml"), "serve", "--port", str(port)]


class ControlPlane(QObject):
    """The window's control plane: starting, up, or down (with why).
    `changed` carries (state, text). With a command it is started and owned;
    without one it is only connected to."""

    changed = Signal(str, str)

    def __init__(self, api: Api, command: list[str] | None) -> None:
        super().__init__(api)
        self.api = api
        self.command = command
        self.proc: subprocess.Popen | None = None
        self.state, self.text = STARTING, ""
        self.poll = QTimer(self)
        self.poll.setSingleShot(True)
        self.poll.timeout.connect(self._probe)

    def start(self) -> None:
        if self.command is None:
            self._set(STARTING, f"Connecting to {self.api.base_url}…")
            self._probe()
            return
        kw: dict = {"cwd": str(ORCHESTRATOR)}
        if sys.platform == "win32":
            kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kw["start_new_session"] = True
        try:
            self.proc = subprocess.Popen(self.command, **kw)
        except OSError as e:
            self._set(DOWN, f"Could not start the control plane ({' '.join(self.command)}): {e}")
            return
        self._set(STARTING, f"Starting the control plane at {self.api.base_url}. The first run installs "
                            "its environment and builds the web UI, which takes a while; its output is in "
                            "the terminal.")
        self._probe()

    def _set(self, state: str, text: str) -> None:
        self.state, self.text = state, text
        self.changed.emit(state, text)

    def _probe(self) -> None:
        reply = self.api.net.get(self.api.request("/health"))
        reply.finished.connect(lambda: self._probed(reply))

    def _probed(self, reply: QNetworkReply) -> None:
        reply.deleteLater()
        status = reply.attribute(QNetworkRequest.Attribute.HttpStatusCodeAttribute)
        if status == 200:
            owned = "; it stops when this window closes" if self.proc else ""
            self._set(UP, f"Control plane at {self.api.base_url}{owned}")
            return
        if self.proc is not None:
            code = self.proc.poll()
            if code is None:
                self.poll.start(POLL_MS)  # still starting
                return
            self.proc = None
            self._set(DOWN, f"The control plane exited with code {code} before answering at "
                            f"{self.api.base_url}. Its output is in the terminal (a port already in use "
                            "shows there as \"address already in use\").")
            return
        why = reply.errorString() if status is None else f"/health answered HTTP {status}"
        self._set(DOWN, f"No control plane at {self.api.base_url}: {why}.")

    def stop(self) -> None:
        """Stop the control plane this client started, gracefully (so it
        destroys its sprites), killing it only if it outlasts STOP_SECONDS."""
        self.poll.stop()
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
