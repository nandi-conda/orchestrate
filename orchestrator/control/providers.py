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
import os
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Protocol

import httpx


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
    repo_url: str = ""
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

    async def logs(self, worker: Worker, lines: int) -> str:
        """The last `lines` lines the worker's runner wrote to stdout/stderr."""


def tail(data: bytes, lines: int) -> str:
    return "\n".join(data.decode(errors="replace").splitlines()[-lines:]) + "\n"


async def wait_healthy(url: str, timeout: float = 120.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    async with httpx.AsyncClient(timeout=5.0) as c:
        while True:
            try:
                r = await c.get(url.rstrip("/") + "/health")
                if r.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(f"runner at {url} did not become healthy")
            await asyncio.sleep(0.25)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def passthrough_env() -> dict:
    """Model and git credentials a runner needs; never the Sprites token.

    Look each one up with `get_secret` rather than reading the environment
    directly: a key stored only in the keyring is copied into `os.environ` when
    it is found, so the runner subprocess/sprite service inherits it just like
    an environment variable would.
    """
    from .secrets import get_secret

    keep = ("DEEPSEEK_API_KEY", "DEEPSEEK_MODEL", "DEEPSEEK_BASE_URL", "RUNNER_MAX_STEPS", "GITHUB_TOKEN")
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
        worker = Worker(id=wid, project_id=spec.project_id, provider=self.name, handle="",
                        url=f"http://127.0.0.1:{port}", capacity=spec.capacity, index=spec.index)
        worker.token = spec.token or worker.token
        wdir = (self.workdir / wid).resolve()
        wdir.mkdir(parents=True, exist_ok=True)
        env = {**os.environ, **passthrough_env(), **spec.env,
               "RUNNER_PORT": str(port), "RUNNER_HOST": "127.0.0.1",
               "RUNNER_TOKEN": worker.token, "RUNNER_WORKDIR": str(wdir),
               "RUNNER_CAPACITY": str(spec.capacity)}
        if spec.repo_url:
            env["RUNNER_REPO"] = spec.repo_url
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

    def log_path(self, wid: str) -> Path:
        return (self.workdir / wid).resolve() / "runner.log"

    async def logs(self, worker: Worker, lines: int) -> str:
        return tail(self.log_path(worker.id).read_bytes(), lines)

    async def destroy(self, worker: Worker) -> None:
        proc = self.procs.pop(worker.id, None)
        if proc and proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), 5)
            except asyncio.TimeoutError:
                proc.kill()

    async def close(self) -> None:
        for wid in list(self.procs):
            proc = self.procs.pop(wid)
            if proc.returncode is None:
                proc.terminate()
                await proc.wait()


# Get the monorepo onto the sprite, then let runner/deploy/bootstrap.sh install pixi
# and the runner's prod environment (it is idempotent, so re-running updates).
BOOTSTRAP = r"""
set -euo pipefail
if [ -d /opt/orchestrate/.git ]; then
  git -C /opt/orchestrate fetch -q --depth 1 origin "$ORCHESTRATE_REF"
  git -C /opt/orchestrate checkout -q FETCH_HEAD
else
  git clone -q --depth 1 --branch "$ORCHESTRATE_REF" "$ORCHESTRATE_REPO" /opt/orchestrate
fi
bash /opt/orchestrate/runner/deploy/bootstrap.sh
"""


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

        await self.exec(name, BOOTSTRAP, env={"ORCHESTRATE_REPO": self.source, "ORCHESTRATE_REF": self.ref})
        env = {**passthrough_env(), **spec.env, "RUNNER_PORT": "8080", "RUNNER_HOST": "0.0.0.0",
               "RUNNER_TOKEN": worker.token, "RUNNER_WORKDIR": "/home/sprite/threads",
               "RUNNER_CAPACITY": str(spec.capacity)}
        if spec.repo_url:
            env["RUNNER_REPO"] = spec.repo_url
        service = {"cmd": "bash", "args": ["/opt/orchestrate/runner/deploy/start.sh"],
                   "env": {**env, "ORCHESTRATE_HOME": "/opt/orchestrate"}, "http_port": 8080}
        (await self.http.put(f"/sprites/{name}/services/runner", json=service)).raise_for_status()
        async with self.http.stream("POST", f"/sprites/{name}/services/runner/start") as s:
            s.raise_for_status()
            async for _ in s.aiter_lines():
                pass
        try:
            await wait_healthy(worker.url, timeout=300)
        except TimeoutError:
            # A runner that refuses to start (exits, and the service with it)
            # says why in the service log.
            raise RuntimeError(f"worker {name}'s runner did not answer in 300s; its log ends:\n"
                               f"{await self.logs(worker, 20)}") from None
        return worker

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

    async def destroy(self, worker: Worker) -> None:
        await self.http.delete(f"/sprites/{shlex.quote(worker.handle)}")

    async def close(self) -> None:
        await self.http.aclose()


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

    async def logs(self, worker: Worker, lines: int) -> str:
        provider = self._of(worker)
        if provider is None:
            raise RuntimeError(f"no {worker.provider} provider in this control plane")
        return await provider.logs(worker, lines)

    async def close(self) -> None:
        for provider in (self._local, self._sprites):
            if provider is not None:
                await provider.close()


def default_provider() -> WorkerProvider:
    return AutoProvider()
