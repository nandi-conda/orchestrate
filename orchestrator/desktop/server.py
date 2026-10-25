"""The control plane, inside the desktop window's process.

uvicorn serves it on a thread with its own event loop, next to Qt's on the
main thread. They are one process, so the control plane ends when the window
does, however the window ends."""

from __future__ import annotations

import asyncio
import errno
import socket
import sys
import threading
from typing import Callable

import uvicorn
from PySide6.QtCore import QObject, QTimer, Signal
from starlette.applications import Starlette

from control.server import Server

POLL_MS = 100
# How long the control plane gets to stop (and destroy its sprites) when the window closes.
STOP_SECONDS = 15

STARTING, UP, DOWN = "starting", "up", "down"

# Makes the control plane's app, given the event that ends its SSE streams on shutdown.
AppFactory = Callable[[asyncio.Event], Starlette]


def listen(port: int) -> socket.socket | None:
    """The control plane's socket, or None if another process holds the port.
    Connections the last run left in TIME_WAIT don't hold it: they show in
    no process (lsof), so refusing them would be refusing nothing. That is
    SO_REUSEADDR on POSIX; Windows allows such binds already, and its
    SO_REUSEADDR would let a second process share a live port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if sys.platform == "win32":
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
    else:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError as e:
        sock.close()
        if e.errno == errno.EADDRINUSE:
            return None
        raise
    return sock


class ControlPlane(QObject):
    """The window's control plane: starting, up, or down (with why).
    `changed` carries (state, text), which also goes to the terminal."""

    changed = Signal(str, str)

    def __init__(self, port: int, make_app: AppFactory, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self.port = port
        self.base_url = f"http://127.0.0.1:{port}"
        self.make_app = make_app
        self.server: Server | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: threading.Thread | None = None
        self.error = ""
        self.state, self.text = STARTING, ""
        self.watch = QTimer(self)
        self.watch.timeout.connect(self._watch)

    def start(self) -> None:
        sock = listen(self.port)
        if sock is None:
            self._set(DOWN, f"Port {self.port} is already in use, so the control plane can't run there. "
                            f"If it is a control plane from another run, stop it with `fuser -k {self.port}/tcp` "
                            f"(Linux) or `kill $(lsof -t -i :{self.port})`, then Retry. Or use another port: "
                            f"`pixi run desktop --port {self.port + 100}`.")
            return
        self.error = ""
        self.loop = asyncio.new_event_loop()
        shutdown = asyncio.Event()
        config = uvicorn.Config(self.make_app(shutdown), host="127.0.0.1", port=self.port, log_level="info",
                                timeout_graceful_shutdown=5)
        self.server = Server(config, shutdown)
        self.thread = threading.Thread(target=self._serve, args=(sock,), name="control-plane", daemon=True)
        self.thread.start()
        self._set(STARTING, f"Starting the control plane at {self.base_url}…")
        self.watch.start(POLL_MS)

    def _serve(self, sock: socket.socket) -> None:
        """The control plane's thread. uvicorn ends a failed startup with
        SystemExit; that, like any error, is why it is down."""
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.server.serve(sockets=[sock]))
        except BaseException as e:  # noqa: BLE001 - reported on the window, then the thread ends
            self.error = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
        finally:
            self.loop.close()

    def _watch(self) -> None:
        if self.thread is not None and not self.thread.is_alive():
            self.watch.stop()
            self.thread = None
            why = f" ({self.error})" if self.error else ""
            self._set(DOWN, f"The control plane at {self.base_url} stopped{why}. Its log is in the terminal.")
        elif self.state == STARTING and self.server is not None and self.server.started:
            self._set(UP, f"Control plane at {self.base_url}, in this window's process")

    def _set(self, state: str, text: str) -> None:
        self.state, self.text = state, text
        print(f"desktop: {text}", file=sys.stderr, flush=True)
        self.changed.emit(state, text)

    def stop(self) -> None:
        """Stop the control plane gracefully, so it destroys its sprites."""
        self.watch.stop()
        thread, self.thread = self.thread, None
        if thread is None or not thread.is_alive():
            return
        self.server.should_exit = True
        thread.join(STOP_SECONDS)
