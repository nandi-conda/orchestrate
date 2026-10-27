"""The control plane inside the app, as the desktop window runs it in its process.

MainActivity starts `ControlPlane.run` on a thread of its own and shows the UI
once `started()` says so. Its data is in the app's own storage
(`$HOME/.orchestrate`, control/paths.py), its workers are sprites, and git is
the one the app carries (android/git/build.sh).

Every app on a phone can reach 127.0.0.1, so each request must carry the
`orchestrate_session` cookie with this run's secret, which the app hands its
WebView and its downloads and nothing else.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
from http.cookies import SimpleCookie
from pathlib import Path

import certifi
import uvicorn
from starlette.responses import PlainTextResponse

COOKIE = "orchestrate_session"


def setup_git(native_lib_dir: str) -> None:
    """Put git on PATH: commands named as git expects, linked to the programs
    Android installed in the native library directory, the only place it
    lets an app run programs from."""
    home = Path(os.environ["HOME"])
    bin_dir = home / "git" / "bin"
    templates = home / "git" / "templates"
    bin_dir.mkdir(parents=True, exist_ok=True)
    templates.mkdir(parents=True, exist_ok=True)
    git, remote = Path(native_lib_dir) / "libgit.so", Path(native_lib_dir) / "libgit-remote-http.so"
    # git runs its own commands by these names from its exec path (fetching
    # from and pushing to a folder runs upload-pack and receive-pack).
    links = {"git": git, "git-upload-pack": git, "git-receive-pack": git, "git-upload-archive": git,
             "git-remote-http": remote, "git-remote-https": remote}
    for name, target in links.items():
        link = bin_dir / name
        if link.is_symlink() and os.readlink(link) == str(target):
            continue
        link.unlink(missing_ok=True)
        link.symlink_to(target)
    os.environ["PATH"] = f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '/system/bin')}"
    os.environ["GIT_EXEC_PATH"] = str(bin_dir)
    os.environ["GIT_TEMPLATE_DIR"] = str(templates)
    os.environ["GIT_SSL_CAINFO"] = certifi.where()
    os.environ["GIT_CONFIG_NOSYSTEM"] = "1"


def set_secret(name: str, value: str) -> None:
    """A key from the app's Keys screen, where control/secrets.py looks first."""
    if value:
        os.environ[name] = value
    else:
        os.environ.pop(name, None)


class SessionGate:
    """Refuses any request without this run's session cookie."""

    def __init__(self, app, secret: str) -> None:
        self.app = app
        self.secret = secret

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] == "http":
            cookies = SimpleCookie()
            for name, value in scope["headers"]:
                if name == b"cookie":
                    cookies.load(value.decode("latin-1"))
            given = cookies[COOKIE].value if COOKIE in cookies else ""
            if not hmac.compare_digest(given, self.secret):
                await PlainTextResponse("not this app's session", 403)(scope, receive, send)
                return
        await self.app(scope, receive, send)


class FirstError(logging.Handler):
    """The first error logged, in one line: why a run stopped. A failed
    startup ends in uvicorn's SystemExit, after logging the traceback of what
    failed, then that it failed."""

    def __init__(self) -> None:
        super().__init__(logging.ERROR)
        self.text = ""

    def emit(self, record: logging.LogRecord) -> None:
        if self.text:
            return
        if record.exc_info and record.exc_info[1] is not None:
            e = record.exc_info[1]
            self.text = f"{type(e).__name__}: {e}"
        else:
            # A traceback as text: its exception's line follows the last frame's.
            lines = [line for line in record.getMessage().splitlines() if line.strip()]
            frames = [i for i, line in enumerate(lines) if line.startswith(" ")]
            after = frames[-1] + 1 if frames else 0
            self.text = lines[after] if after < len(lines) else ""


class ControlPlane:
    """One run of the control plane on 127.0.0.1:`port`."""

    def __init__(self, port: int, secret: str, native_lib_dir: str) -> None:
        self.port = port
        self.secret = secret
        self.native_lib_dir = native_lib_dir
        self.server: uvicorn.Server | None = None
        self.error = ""

    def run(self) -> None:
        """Serve until stop(); blocks the calling thread. A failure is kept in
        `error` for the app to show."""
        first = FirstError()
        # uvicorn's own loggers stop at "uvicorn", short of the root.
        watched = [logging.getLogger(), logging.getLogger("uvicorn")]
        try:
            logging.basicConfig(level=logging.INFO)
            logging.getLogger("httpx").setLevel(logging.WARNING)
            setup_git(self.native_lib_dir)
            from control.app import create_app
            from control.providers import DEFAULT_REF, DEFAULT_SOURCE, Places, SpritesProvider
            from control.server import Server
            from control.store import store_from_url
            from control.sync import Sync

            shutdown = asyncio.Event()
            # A phone runs no runner: it has neither pixi nor bubblewrap.
            places = Places({"sprites": SpritesProvider(source=DEFAULT_SOURCE, ref=DEFAULT_REF)},
                            {"local": "a phone cannot run workers (the runner needs pixi and bubblewrap); "
                                      "this app runs only the control plane"})
            app = SessionGate(create_app(store=store_from_url(None), places=places, shutdown_event=shutdown,
                                         sync=Sync.from_env()),
                              self.secret)
            config = uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="info",
                                    timeout_graceful_shutdown=5, loop="asyncio", http="h11")
            self.server = Server(config, shutdown)
            # After uvicorn.Config, which sets its loggers' handlers.
            for logger in watched:
                logger.addHandler(first)
            asyncio.run(self.server.serve())
        except BaseException as e:  # noqa: BLE001 - shown in the app
            if not isinstance(e, SystemExit):
                logging.exception("control plane failed")
            self.error = first.text or (f"{type(e).__name__}: {e}" if str(e) else type(e).__name__)
        finally:
            for logger in watched:
                logger.removeHandler(first)

    def started(self) -> bool:
        return self.server is not None and self.server.started

    def stop(self) -> None:
        if self.server is not None:
            self.server.should_exit = True
