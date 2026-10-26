"""Worker providers: where runner processes live.

A worker is one runner process (runner/API.md) that hosts many threads. The
control plane asks a provider for a new worker when a project has none, or
when every worker it has is at capacity.

- `LocalProvider` starts runners as subprocesses on this machine. It runs the
  real runner from ../runner when that exists, else the built-in echo runner
  (control.devrunner), so the whole loop works without any credentials.
- `SpritesProvider` creates a Fly Sprite per worker, installs the runner on
  it and runs it as a sprite service on port 8080.
- `AutoProvider` is the default: sprites whenever `SPRITES_TOKEN` is stored,
  local workers otherwise.

A worker is never told where the control plane is and never connects to it:
the control plane opens every connection, including the one that collects its
peer calls. So a provider only has to hand back a URL the control plane can reach.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import AsyncIterator, Protocol

import httpx

log = logging.getLogger("orchestrator.providers")


@dataclass
class Worker:
    id: str
    project_id: str
    provider: str
    handle: str
    url: str
    token: str = field(default_factory=lambda: secrets.token_urlsafe(24))
    capacity: int = 16
    index: int = 0  # the WorkerSpec index it was made with

    def record(self) -> dict:
        return asdict(self)


@dataclass
class WorkerSpec:
    project_id: str
    index: int
    capacity: int = 16
    env: dict = field(default_factory=dict)
    # The token of the worker this index names, when one was made before: a
    # runner still going from an earlier control plane keeps answering to it.
    token: str = ""



class WorkerProvider(Protocol):
    name: str

    async def create(self, spec: WorkerSpec) -> Worker:
        """Start a runner and return once its /health answers. A spec whose
        index was made before (and not destroyed) names the same worker: its
        disk, with the threads' worktrees, is kept, and its runner answers to
        spec.token."""

    async def destroy(self, worker: Worker) -> None: ...

    async def exists(self, worker: Worker) -> bool:
        """Whether the worker is still there, whatever its runner answers now.
        Only False loses it (Orchestrator._health); raises when it cannot tell."""

    async def logs(self, worker: Worker, lines: int) -> str:
        """The last `lines` lines the worker's runner wrote to stdout/stderr."""

    def follow(self, worker: Worker, backlog: int) -> AsyncIterator[str]:
        """The worker's runner log as it is written, line by line: the last
        `backlog` lines first, then each new one, until cancelled. The control
        plane reads it (the worker never sends it), so the follower reconnects
        on its own when its connection ends."""


# How often a local worker's log file is looked at for new lines.
FOLLOW_EVERY = 0.2
# How long one sprite log follow runs before it is opened again (seconds).
SPRITE_FOLLOW = 3600
# How long a sprite log follow waits before it is opened again.
FOLLOW_AGAIN = 1.0


def tail(data: bytes, lines: int) -> str:
    return "\n".join(data.decode(errors="replace").splitlines()[-lines:]) + "\n"


async def wait_healthy(url: str, timeout: float = 120.0) -> dict:
    """Poll /health until the runner answers. A runner on this machine answers
    in milliseconds, one on a sprite starts by installing; look again at once at
    first, then back off to a quarter second. Returns what /health answered."""
    deadline = asyncio.get_running_loop().time() + timeout
    delay = 0.01
    async with httpx.AsyncClient(timeout=5.0) as c:
        while True:
            try:
                r = await c.get(url.rstrip("/") + "/health")
                if r.status_code == 200:
                    return r.json()
            except httpx.HTTPError:
                pass
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(f"runner at {url} did not become healthy")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 0.25)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# How long a runner gets to exit after SIGTERM before it is killed: its /events
# streams and peer long polls end with the connections the control plane closes
# first, so this is only for one that is stuck.
STOP_GRACE = 5.0


def passthrough_env() -> dict:
    """Model credentials a runner needs; never the Sprites token, nor a git
    token (workers reach no repository: the control plane brings them commits).

    Look each one up with `get_secret` rather than reading the environment
    directly: a key stored only in the keyring is copied into `os.environ` when
    it is found, so the runner subprocess/sprite service inherits it just like
    an environment variable would.
    """
    from .secrets import get_secret

    keep = ("DEEPSEEK_API_KEY", "DEEPSEEK_MODEL", "DEEPSEEK_BASE_URL")
    found = {k: get_secret(k) for k in keep}
    return {k: v for k, v in found.items() if v}


DEFAULT_SOURCE = "https://github.com/nandi-conda/orchestrate"
DEFAULT_REF = "main"


def _git(root: Path, *args: str) -> str:
    try:
        out = subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return ""
    return out.stdout.strip() if out.returncode == 0 else ""


def repo_source_default(root: Path | None = None) -> tuple[str, str]:
    """What a sprite should check out: this checkout's `origin` and branch, so a
    run against sprites exercises the code in front of you. A tarball or a
    detached checkout has neither, hence the public repository as fallback."""
    root = root or Path(__file__).resolve().parents[2]
    branch = _git(root, "symbolic-ref", "--short", "HEAD")
    return (_git(root, "remote", "get-url", "origin") or DEFAULT_SOURCE,
            branch or DEFAULT_REF)


def checkout_note(root: Path, ref: str) -> str:
    """What the sprite would run instead of this checkout, or "" when it would
    run exactly it."""
    head = _git(root, "rev-parse", "HEAD")
    pushed = _git(root, "rev-parse", "--verify", "-q", f"origin/{ref}")
    if not pushed:
        return f"origin has no {ref!r} branch, so the sprite clone will fail; push it."
    if head and head != pushed:
        return (f"sprites clone origin/{ref}, not this checkout's HEAD; local commits and edits "
                "are not in that commit.")
    return ""


class LocalProvider:
    name = "local"

    def __init__(self, runner_dir: str | None = None, workdir: str = ".orchestrator/workers",
                 use_devrunner: bool | None = None) -> None:
        default = Path(__file__).resolve().parents[2] / "runner"
        self.runner_dir = Path(runner_dir) if runner_dir else default
        if use_devrunner is None:
            from .secrets import get_secret

            use_devrunner = (not (self.runner_dir / "pixi.toml").exists()
                             or not get_secret("DEEPSEEK_API_KEY"))
            if use_devrunner:
                print("LocalProvider: using the echo runner (control.devrunner). The real runner needs "
                      "runner/ and DEEPSEEK_API_KEY (env or keyring).", file=sys.stderr)
        self.use_devrunner = use_devrunner
        self.workdir = Path(workdir)
        self.procs: dict[str, asyncio.subprocess.Process] = {}

    def command(self) -> list[str]:
        if self.use_devrunner:
            return [sys.executable, "-m", "control.devrunner"]
        pixi = shutil.which("pixi") or "pixi"
        return [pixi, "run", "--manifest-path", str(self.runner_dir / "pixi.toml"), "serve"]

    async def create(self, spec: WorkerSpec) -> Worker:
        port = free_port()
        wid = f"{spec.project_id}-w{spec.index}"
        old = self.procs.pop(wid, None)
        if old is not None and old.returncode is None:  # its runner, started again as a sprite's service is
            old.kill()
            await old.wait()
        worker = Worker(id=wid, project_id=spec.project_id, provider=self.name, handle="",
                        url=f"http://127.0.0.1:{port}", capacity=spec.capacity, index=spec.index)
        worker.token = spec.token or worker.token
        wdir = (self.workdir / wid).resolve()
        wdir.mkdir(parents=True, exist_ok=True)
        env = {**os.environ, **passthrough_env(), **spec.env,
               "RUNNER_PORT": str(port), "RUNNER_HOST": "127.0.0.1",
               "RUNNER_TOKEN": worker.token, "RUNNER_WORKDIR": str(wdir),
               "RUNNER_CAPACITY": str(spec.capacity), "PYTHONUNBUFFERED": "1"}
        cwd = Path(__file__).resolve().parents[1]
        with open(self.log_path(wid), "ab") as log:
            proc = await asyncio.create_subprocess_exec(*self.command(), env=env, cwd=str(cwd),
                                                        stdout=log, stderr=subprocess.STDOUT)
        print(f"LocalProvider: worker {wid} logs to {self.log_path(wid)}", file=sys.stderr)
        worker.handle = str(proc.pid)
        self.procs[wid] = proc
        # The first run installs the runner env. A runner that exits instead
        # (it refuses to start without what it needs) says why in its log.
        healthy = asyncio.create_task(wait_healthy(worker.url, timeout=600))
        exited = asyncio.create_task(proc.wait())
        await asyncio.wait({healthy, exited}, return_when=asyncio.FIRST_COMPLETED)
        if not healthy.done():
            healthy.cancel()
            self.procs.pop(wid, None)
            raise RuntimeError(f"worker {wid}'s runner exited with code {proc.returncode} before it "
                               f"answered:\n{tail(self.log_path(wid).read_bytes(), 20)}")
        exited.cancel()
        try:
            healthy.result()
        except TimeoutError:
            proc.kill()
            raise
        return worker

    async def exists(self, worker: Worker) -> bool:
        proc = self.procs.get(worker.id)
        return proc is not None and proc.returncode is None

    def log_path(self, wid: str) -> Path:
        return (self.workdir / wid).resolve() / "runner.log"

    async def logs(self, worker: Worker, lines: int) -> str:
        return tail(self.log_path(worker.id).read_bytes(), lines)

    async def follow(self, worker: Worker, backlog: int) -> AsyncIterator[str]:
        path = self.log_path(worker.id)
        with open(path, "rb") as f:
            data = f.read()
            end = data.rfind(b"\n") + 1  # a last line still being written comes with its rest
            for line in data[:end].decode(errors="replace").splitlines()[-backlog:] if backlog else []:
                yield line
            partial = data[end:]
            while True:
                chunk = f.read()
                if not chunk:
                    await asyncio.sleep(FOLLOW_EVERY)
                    continue
                *lines, partial = (partial + chunk).split(b"\n")
                for line in lines:
                    yield line.decode(errors="replace")

    async def destroy(self, worker: Worker) -> None:
        proc = self.procs.pop(worker.id, None)
        if proc and proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), STOP_GRACE)
            except asyncio.TimeoutError:
                proc.kill()

    async def close(self) -> None:
        """Stop every runner this provider started, as destroy() does."""
        for wid in list(self.procs):
            proc = self.procs.pop(wid)
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), STOP_GRACE)
                except asyncio.TimeoutError:
                    proc.kill()


# Put the monorepo at exactly $ORCHESTRATE_COMMIT on the sprite, then let
# runner/deploy/bootstrap.sh install pixi, the runner's environments and
# prime-agent (it is idempotent, so re-running updates).
BOOTSTRAP = r"""
set -euo pipefail
[ -d /opt/orchestrate/.git ] || git init -q /opt/orchestrate
git -C /opt/orchestrate remote remove origin 2>/dev/null || true
git -C /opt/orchestrate remote add origin "$ORCHESTRATE_REPO"
git -C /opt/orchestrate fetch -q --depth 1 origin "$ORCHESTRATE_COMMIT"
git -C /opt/orchestrate checkout -q --force FETCH_HEAD
ORCHESTRATE_REF="$ORCHESTRATE_COMMIT" bash /opt/orchestrate/runner/deploy/bootstrap.sh
"""


async def remote_commit(source: str, ref: str) -> str:
    """The commit `ref` names on `source` now: what a new worker runs."""
    proc = await asyncio.create_subprocess_exec("git", "ls-remote", source, f"refs/heads/{ref}",
                                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = await proc.communicate()
    sha = out.decode().split("\t", 1)[0].strip()
    if proc.returncode != 0 or not sha:
        raise RuntimeError(f"cannot resolve branch {ref!r} of {source}: {err.decode().strip() or 'no such branch'}")
    return sha


class SpritesProvider:
    """Workers on Fly Sprites (https://api.sprites.dev/v1, Bearer $SPRITES_TOKEN).

    Each worker is a sprite named `<prefix><project>-w<n>`. The sprite's URL is
    public at the proxy level; the runner itself requires RUNNER_TOKEN, so the
    Authorization header stays free for it.
    """

    name = "sprites"
    API = "https://api.sprites.dev/v1"

    def __init__(self, token: str | None = None, source: str | None = None, ref: str | None = None,
                 prefix: str | None = None) -> None:
        from .secrets import get_secret

        self.token = token or get_secret("SPRITES_TOKEN") or ""
        if not self.token:
            raise RuntimeError("SpritesProvider needs SPRITES_TOKEN")
        default_source, default_ref = repo_source_default()
        self.source = source or default_source
        self.ref = ref or default_ref
        if not self.source.startswith(("http://", "https://")):
            print(f"SpritesProvider: {self.source} is not an http(s) URL and a sprite has no git "
                  "credentials; the clone will fail unless the URL is publicly readable.", file=sys.stderr)
        elif not source and not ref:
            note = checkout_note(Path(__file__).resolve().parents[2], self.ref)
            if note:
                print(f"SpritesProvider: {note}", file=sys.stderr)
        self.prefix = prefix if prefix is not None else "orch-"
        self.http = httpx.AsyncClient(base_url=self.API, timeout=httpx.Timeout(60.0, read=600.0),
                                      headers={"authorization": f"Bearer {self.token}"})

    def sprite_name(self, spec: WorkerSpec) -> str:
        return f"{self.prefix}{spec.project_id}-w{spec.index}".replace("_", "-").lower()

    async def create(self, spec: WorkerSpec) -> Worker:
        from .secrets import get_secret

        if not get_secret("DEEPSEEK_API_KEY"):
            raise RuntimeError("SpritesProvider needs DEEPSEEK_API_KEY (env or keyring): "
                               "the runner cannot start without it")
        name = self.sprite_name(spec)
        r = await self.http.post("/sprites", json={"name": name, "url_settings": {"auth": "public"}})
        if r.status_code not in (200, 201, 409):
            r.raise_for_status()
        info = (await self.http.get(f"/sprites/{name}")).raise_for_status().json()
        worker = Worker(id=name, project_id=spec.project_id, provider=self.name, handle=name,
                        url=info["url"].rstrip("/"), capacity=spec.capacity, index=spec.index)
        worker.token = spec.token or worker.token
        # A sprite an earlier control plane made still has its runner service,
        # and `start` does not replace a running process: that runner would go
        # on with the code and environment (the model key) it was started
        # with. So a service already there is restarted from the definition
        # put below, and the runner serving the worker is always this
        # checkout's, with this control plane's key and token.
        defined = (await self.http.get(f"/sprites/{name}/services/runner")).status_code == 200

        commit = await remote_commit(self.source, self.ref)
        await self.exec(name, BOOTSTRAP, env={"ORCHESTRATE_REPO": self.source, "ORCHESTRATE_COMMIT": commit})
        env = {**passthrough_env(), **spec.env, "RUNNER_PORT": "8080", "RUNNER_HOST": "0.0.0.0",
               "RUNNER_TOKEN": worker.token, "RUNNER_WORKDIR": "/home/sprite/threads",
               "RUNNER_CAPACITY": str(spec.capacity), "PYTHONUNBUFFERED": "1"}
        service = {"cmd": "bash", "args": ["/opt/orchestrate/runner/deploy/start.sh"],
                   "env": {**env, "ORCHESTRATE_HOME": "/opt/orchestrate"}, "http_port": 8080}
        (await self.http.put(f"/sprites/{name}/services/runner", json=service)).raise_for_status()
        await self.service_action(name, "restart" if defined else "start")
        health = await self.wait_ready(worker)
        if health.get("commit") != commit:
            raise RuntimeError(f"worker {name}'s runner runs orchestrate {health.get('commit')}, not "
                               f"{commit} ({self.ref}); its log ends:\n{await self.logs(worker, 20)}")
        if not await self.accepts_token(worker):
            raise RuntimeError(f"worker {name}'s runner does not accept the token it was started "
                               f"with; its log ends:\n{await self.logs(worker, 20)}")
        return worker

    async def service_action(self, name: str, action: str, timeout: float = 300.0) -> None:
        """Run `start`/`restart`/`stop` on the runner service to completion.

        The response is a stream of the action's log, which ends with a
        `complete` event. Bound it: a runtime that is confused about the
        service must not hang worker creation forever.
        """

        async def run() -> None:
            async with self.http.stream("POST", f"/sprites/{name}/services/runner/{action}") as s:
                s.raise_for_status()
                async for _ in s.aiter_lines():
                    pass

        what = {"start": "starting", "restart": "restarting", "stop": "stopping"}.get(action, action)
        try:
            await asyncio.wait_for(run(), timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(f"the runner service on sprite {name} did not finish {what} "
                               f"within {timeout:.0f}s") from None

    async def wait_ready(self, worker: Worker, timeout: float = 300.0) -> dict:
        try:
            return await wait_healthy(worker.url, timeout=timeout)
        except TimeoutError:
            # A runner that refuses to start (exits, and the service with it)
            # says why in the service log.
            raise RuntimeError(f"worker {worker.handle}'s runner did not answer in {timeout:.0f}s; "
                               f"its log ends:\n{await self.logs(worker, 20)}") from None

    async def accepts_token(self, worker: Worker) -> bool:
        """Whether the runner now serving the sprite answers an authenticated
        call. /health is deliberately open, so a runner started by an earlier
        control plane with an unknown token looks healthy while everything
        else is a 401."""
        async with httpx.AsyncClient(timeout=15.0,
                                     headers={"authorization": f"Bearer {worker.token}"}) as c:
            r = await c.get(f"{worker.url.rstrip('/')}/threads")
        if r.status_code == 401:
            return False
        r.raise_for_status()
        return True

    async def exec(self, name: str, script: str, env: dict | None = None) -> str:
        params = [("cmd", "bash"), ("cmd", "-c"), ("cmd", script)]
        params += [("env", f"{k}={v}") for k, v in (env or {}).items()]
        r = await self.http.post(f"/sprites/{name}/exec", params=params)
        if r.status_code >= 400:
            raise RuntimeError(f"sprite exec failed ({r.status_code}): {r.text[:500]}")
        return r.text

    # A sprite service's stdout and stderr, timestamped (Sprites docs, "Services").
    LOG = "/.sprite/logs/services/runner.log"

    async def logs(self, worker: Worker, lines: int) -> str:
        r = await self.http.get(f"/sprites/{worker.handle}/fs/read", params={"path": self.LOG, "workingDir": "/"})
        if r.status_code >= 400:
            raise RuntimeError(f"reading {self.LOG} on sprite {worker.handle} failed ({r.status_code}): {r.text[:300]}")
        return tail(r.content, lines)

    async def follow(self, worker: Worker, backlog: int) -> AsyncIterator[str]:
        """The runner service's log, from the Sprites API's log stream (NDJSON
        of stdout/stderr/exit/error events, then `complete` when the follow
        time is up). Each stream after the first starts with lines the last
        one read; those are skipped by timestamp, and by count at the last
        timestamp it read."""
        seen_at, seen = 0, 0  # the last timestamp read, and lines read at it
        lines = backlog
        while True:
            try:
                async with self.http.stream("GET", f"/sprites/{worker.handle}/services/runner/logs",
                                            params={"lines": lines, "duration": SPRITE_FOLLOW},
                                            timeout=httpx.Timeout(60.0, read=None)) as r:
                    if r.status_code >= 400:
                        body = (await r.aread()).decode(errors="replace")
                        raise RuntimeError(f"following the runner log of sprite {worker.handle} failed "
                                           f"({r.status_code}): {body[:300]}")
                    skip = seen
                    async for raw in r.aiter_lines():
                        if not raw.strip():
                            continue
                        ev = json.loads(raw)
                        if ev.get("type") == "complete":
                            continue
                        ts = int(ev.get("timestamp") or 0)
                        if ts < seen_at:
                            continue
                        if ts == seen_at and skip:
                            skip -= 1
                            continue
                        seen, seen_at = (seen + 1 if ts == seen_at else 1), ts
                        skip = 0
                        for line in service_log_lines(ev):
                            yield line
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("worker %s log follow: %s: %s", worker.id, type(e).__name__, e)
            await asyncio.sleep(FOLLOW_AGAIN)
            # Overlap the last stream: what it read at its last timestamp, and
            # some lines more, in case one event held several.
            lines = seen + 50

    async def exists(self, worker: Worker) -> bool:
        """A sprite that pauses is still there: it wakes on the next request."""
        r = await self.http.get(f"/sprites/{worker.handle}")
        if r.status_code == 404:
            return False
        r.raise_for_status()
        return True

    async def destroy(self, worker: Worker) -> None:
        await self.http.delete(f"/sprites/{shlex.quote(worker.handle)}")

    async def close(self) -> None:
        await self.http.aclose()


def service_log_lines(ev: dict) -> list[str]:
    """A Sprites service log event as log lines; `complete` ends a follow and is none."""
    typ = ev.get("type")
    if typ in ("stdout", "stderr"):
        return (ev.get("data") or "").splitlines()
    if typ == "exit":
        return [f"[sprite] runner service exited with code {ev.get('exit_code')}"]
    if typ == "error":
        return [f"[sprite] runner service error: {ev.get('data')}"]
    if typ == "started":
        return ["[sprite] runner service started"]
    return []


class AutoProvider:
    """Remote first: workers are sprites whenever a token is stored. Without a
    token the control plane falls back to local workers, so the offline echo
    demo still works. Either way the control plane opens every connection, so
    where it runs never matters to the worker."""

    name = "auto"

    def __init__(self, local: LocalProvider | None = None, sprites: SpritesProvider | None = None) -> None:
        self._local = local
        self._sprites = sprites
        self._looked_for_token = sprites is not None
        self._warned_local = False

    def local(self) -> LocalProvider:
        if self._local is None:
            self._local = LocalProvider()
        return self._local

    def sprites(self) -> SpritesProvider | None:
        if not self._looked_for_token:
            from .secrets import get_secret

            self._looked_for_token = True
            self._sprites = SpritesProvider() if get_secret("SPRITES_TOKEN") else None
        return self._sprites

    async def create(self, spec: WorkerSpec) -> Worker:
        provider = self.sprites()
        if provider is None:
            if not self._warned_local:
                self._warned_local = True
                print("AutoProvider: SPRITES_TOKEN is not stored; using local workers. "
                      "Store it with `pixi run set-key SPRITES_TOKEN` to run workers on Sprites.",
                      file=sys.stderr)
            return await self.local().create(spec)
        return await provider.create(spec)

    def _of(self, worker: Worker):
        # A worker an earlier control plane made may be asked about first.
        return self.sprites() if worker.provider == "sprites" else self.local()

    async def destroy(self, worker: Worker) -> None:
        provider = self._of(worker)
        if provider is not None:
            await provider.destroy(worker)

    async def exists(self, worker: Worker) -> bool:
        provider = self._of(worker)
        if provider is None:
            raise RuntimeError(f"no {worker.provider} provider in this control plane")
        return await provider.exists(worker)

    async def logs(self, worker: Worker, lines: int) -> str:
        provider = self._of(worker)
        if provider is None:
            raise RuntimeError(f"no {worker.provider} provider in this control plane")
        return await provider.logs(worker, lines)

    async def follow(self, worker: Worker, backlog: int) -> AsyncIterator[str]:
        provider = self._of(worker)
        if provider is None:
            raise RuntimeError(f"no {worker.provider} provider in this control plane")
        async for line in provider.follow(worker, backlog):
            yield line

    async def close(self) -> None:
        for provider in (self._local, self._sprites):
            if provider is not None:
                await provider.close()


def default_provider() -> WorkerProvider:
    return AutoProvider()
