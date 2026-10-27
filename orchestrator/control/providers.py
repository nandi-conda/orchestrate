"""Worker providers: where runner processes live.

A worker is one runner process (runner/API.md) that hosts many threads, of
every project. The control plane keeps a pool of a fixed number of workers,
shared by all its projects, each in a slot numbered from 1 (WorkerSpec.index):
a slot always names the same worker, so the pool never holds more workers
than it has slots, and a provider can tell which of the workers it has are
the pool's (`sweep` destroys the rest). The pool has a name of its own, kept
in the control plane's store (Store.pool_id): two control planes (a phone and
a desktop) on one Sprites org each have their own workers.

- `LocalProvider` starts runners from ../runner as subprocesses on this
  machine. It needs DEEPSEEK_API_KEY, as the runner does.
- `SpritesProvider` makes a Fly Sprite per slot, `orch-<pool>-w<n>`, labeled
  `orchestrator-<pool>`, installs the runner on it and runs it as a sprite service
  on port 8080.

Where a thread's workers run (threads.workers) is the router's decision, per
thread: "sprites" unless the request asks for "local" (this computer).
`Places` holds the provider of each place this control plane offers, and why
it refuses the others; the router is told both, and a thread on a refused
place fails with that reason instead of running somewhere else. Each place
has its own pool of slots.

A worker is never told where the control plane is and never connects to it:
the control plane opens every connection, including the one that collects its
peer calls. So a provider only has to hand back a URL the control plane can reach.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import AsyncIterator, Protocol

import httpx

from . import paths
from .gitio import git
from .steps import Steps

log = logging.getLogger("orchestrator.providers")


@dataclass
class Worker:
    id: str
    provider: str
    handle: str
    url: str
    token: str = field(default_factory=lambda: secrets.token_urlsafe(24))
    capacity: int = 16
    index: int = 0  # the pool slot it was made for (WorkerSpec.index)

    def record(self) -> dict:
        return asdict(self)


@dataclass
class WorkerSpec:
    index: int  # the pool slot, from 1: it names the worker
    pool: str = ""  # the pool's name (Store.pool_id): it names the worker too
    capacity: int = 16
    env: dict = field(default_factory=dict)
    # The token of the worker this slot names, when one was made before: a
    # runner still going from an earlier control plane keeps answering to it.
    token: str = ""
    # Where `create` says what it is doing, step by step, with timings: the
    # control plane's stdout and the Worker log tab of threads waiting on it.
    steps: Steps = field(default_factory=lambda: Steps("worker"))



class WorkerProvider(Protocol):
    name: str

    async def create(self, spec: WorkerSpec) -> Worker:
        """Start a runner and return once its /health answers. A spec whose
        slot was made before (and not destroyed) names the same worker: its
        disk, with the threads' worktrees, is kept, and its runner answers to
        spec.token."""

    async def destroy(self, worker: Worker) -> None: ...

    def worker_name(self, index: int, pool: str) -> str:
        """The name of the worker in slot `index` of pool `pool` (its id)."""

    async def sweep(self, slots: int, pool: str) -> list[str]:
        """Destroy every worker of pool `pool` this provider has that is not
        in one of its `slots` (left by an earlier, bigger pool), and those
        from before pools had names; the names of those destroyed. Other
        pools' workers are left alone: another control plane uses them."""

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
# How long one sprite log follow runs before it is opened again: a Go duration
# string, as the Sprites SDKs send it (sprites-js getServiceLogs). A bare
# number is not one, and the stream then ends at once.
SPRITE_FOLLOW = "1h"
# How long a sprite log follow waits before it is opened again.
FOLLOW_AGAIN = 1.0
# How long SpritesProvider.serves waits for a defined runner service to answer
# (a paused sprite wakes and starts it) before it restarts the service instead.
SERVES_WAIT = 60.0
# What a cold sprite's services API answers (a 500) until the sprite has
# started its services manager; how long that is waited out, and how often
# it is asked again meanwhile.
SERVICES_BOOTING = "services manager not started"
SERVICES_WAIT = 60.0
SERVICES_LOOK_EVERY = 1.0


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
# How long a local runner gets after SIGTERM to end its threads' turns (each
# prime-agent gets 5s to exit, its /events streams as long) before it is killed.
# A kill is safe: its prime-agents die with it (runner/src/runner/harness.py,
# owned_by) and the turns it cut resume on its next start.
RUNNER_STOP_GRACE = 15.0
# A runner holds this lock on its workdir while it lives, its pid in it
# (runner/src/runner/thread.py, RUNNER_LOCK).
RUNNER_LOCK = ".runner.lock"


def runner_holding(workdir: Path) -> int | None:
    """The pid of the runner that holds `workdir`, or None when none does."""
    import fcntl

    try:
        fd = os.open(workdir / RUNNER_LOCK, os.O_RDONLY)
    except FileNotFoundError:
        return None
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return int(os.read(fd, 32).decode().strip())
        fcntl.flock(fd, fcntl.LOCK_UN)
        return None
    finally:
        os.close(fd)


def passthrough_env() -> dict:
    """Model credentials a runner needs; never the Sprites token, nor a git
    token (workers reach no repository: the control plane brings them commits).

    Look each one up with `get_secret` rather than reading the environment
    directly: a key stored only in the keyring is copied into `os.environ` when
    it is found, so the runner subprocess/sprite service inherits it just like
    an environment variable would.
    """
    from .secrets import get_secret

    keep = ("DEEPSEEK_API_KEY", "DEEPSEEK_MODEL", "DEEPSEEK_BASE_URL", "OPENGATEWAY_API_TOKEN")
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
    detached checkout has neither, hence the public repository as fallback, as
    for the conda package (recipe/), which is no checkout at all: git is not
    asked about its directory, or it would answer for a repository around it."""
    root = root or Path(__file__).resolve().parents[2]
    if not (root / ".git").exists():
        return DEFAULT_SOURCE, DEFAULT_REF
    branch = _git(root, "symbolic-ref", "--short", "HEAD")
    return (_git(root, "remote", "get-url", "origin") or DEFAULT_SOURCE,
            branch or DEFAULT_REF)


def checkout_note(root: Path, ref: str, commit: str) -> str:
    """What the sprite runs instead of this checkout, or "" when it runs
    exactly it. `commit` is what `ref` names on origin now (remote_commit asks
    origin itself: a remote-tracking ref here may be missing or stale)."""
    head = _git(root, "rev-parse", "HEAD")
    if head and head != commit:
        return (f"sprites run origin/{ref} ({commit[:12]}), not this checkout's HEAD ({head[:12]}); "
                "local commits and edits are not in it.")
    return ""


class LocalProvider:
    name = "local"

    def __init__(self, runner_dir: str | None = None, workdir: str | Path = paths.WORKERS,
                 command: list[str] | None = None) -> None:
        """Runs the runner from `runner_dir` (../runner by default). `command`
        replaces it, for the tests' echo runner (tests/doubles.py)."""
        default = Path(__file__).resolve().parents[2] / "runner"
        self.runner_dir = Path(runner_dir) if runner_dir else default
        if command is None:
            from .secrets import get_secret, missing_message

            if not (self.runner_dir / "pixi.toml").exists():
                raise RuntimeError(f"LocalProvider needs the runner project at {self.runner_dir}")
            if not get_secret("DEEPSEEK_API_KEY"):
                raise RuntimeError(missing_message(["DEEPSEEK_API_KEY"]))
            pixi = shutil.which("pixi") or "pixi"
            command = [pixi, "run", "--manifest-path", str(self.runner_dir / "pixi.toml"), "serve"]
        self._command = command
        self.workdir = Path(workdir)
        self.procs: dict[str, asyncio.subprocess.Process] = {}

    def command(self) -> list[str]:
        return self._command

    async def create(self, spec: WorkerSpec) -> Worker:
        port = free_port()
        wid = self.worker_name(spec.index)
        # Its runner, started again as a sprite's service is: the one before
        # goes first, whether this provider started it or an earlier control
        # plane that is gone left it (one runner per worker directory).
        await self._stop(wid)
        worker = Worker(id=wid, provider=self.name, handle="",
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
        spec.steps.note(f"started its runner, pid {proc.pid}, logging to {self.log_path(wid)}")
        worker.handle = str(proc.pid)
        self.procs[wid] = proc
        # The first run installs the runner env. A runner that exits instead
        # (it refuses to start without what it needs) says why in its log.
        healthy = asyncio.create_task(wait_healthy(worker.url, timeout=600))
        exited = asyncio.create_task(proc.wait())
        async with spec.steps.step(f"waiting up to 600s for its runner to answer at {worker.url}"):
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

    @staticmethod
    def worker_name(index: int, pool: str = "") -> str:
        # The workers' directories are in the control plane's own data
        # directory, so the pool's name need not be in theirs.
        return f"w{index}"

    async def sweep(self, slots: int, pool: str) -> list[str]:
        """Every worker directory but the slots' (each holds a worker's disk
        and log) is deleted, its runner stopped if this provider started it.
        The directories are in the control plane's own data directory, so
        they are its pool's whatever its name."""
        keep = {self.worker_name(i) for i in range(1, slots + 1)}
        gone = sorted(d.name for d in self.workdir.iterdir() if d.is_dir() and d.name not in keep) \
            if self.workdir.exists() else []
        for wid in gone:
            await self.destroy(Worker(id=wid, provider=self.name, handle="", url=""))
            shutil.rmtree(self.workdir / wid, ignore_errors=True)
        return gone

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
        await self._stop(worker.id)

    async def close(self) -> None:
        """Stop every runner this provider started, as destroy() does."""
        await asyncio.gather(*(self._stop(wid) for wid in list(self.procs)))

    async def _stop(self, wid: str) -> None:
        """Stop worker `wid`'s runner: the process holding its directory's
        runner lock, then the process this provider started it with. The
        runner is signalled itself, not through what started it: `pixi run`
        does not pass a SIGTERM on to it, and a runner left serving the
        directory keeps its threads' sessions from the next."""
        wdir = (self.workdir / wid).resolve()
        holder = runner_holding(wdir)
        if holder is not None:
            with contextlib.suppress(ProcessLookupError):  # it may have just ended
                os.kill(holder, signal.SIGTERM)
                os.kill(holder, signal.SIGCONT)  # a stopped one handles it too (as systemd does)
            if not await self._released(wdir, RUNNER_STOP_GRACE):
                log.warning("worker %s's runner (pid %s) did not stop in %ss after SIGTERM: killing it",
                            wid, holder, RUNNER_STOP_GRACE)
                with contextlib.suppress(ProcessLookupError):
                    os.kill(holder, signal.SIGKILL)
                await self._released(wdir, STOP_GRACE)
        proc = self.procs.pop(wid, None)
        if proc and proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), STOP_GRACE)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()

    @staticmethod
    async def _released(wdir: Path, within: float) -> bool:
        loop = asyncio.get_running_loop()
        end = loop.time() + within
        while runner_holding(wdir) is not None:
            if loop.time() >= end:
                return False
            await asyncio.sleep(0.1)
        return True


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

# The install takes minutes (pixi install of the runner's environments), far
# longer than one HTTP request through the Sprites API should be held open: a
# held POST /exec came back as a bare 502 from the API's gateway mid-install.
# So the install runs detached on the sprite: INSTALL_START puts BOOTSTRAP in
# a run directory and starts it in its own session, which outlives the exec
# that started it, and returns at once. While it runs it holds the sprite
# active with a task on the Tasks API (docs.fly.io/sprites/keeping-sprites-
# running: a 5 minute hold refreshed every minute, released when it ends), so
# the sprite does not pause under it between the control plane's looks. It
# writes its output to `log` and, when it ends, its exit code to `exit`.
# INSTALL_LOOK, a short exec, says how the run stands and gives the log lines
# after the first $INSTALL_FROM.
INSTALL_ROOT = "/home/sprite/.orchestrate-install"
INSTALL_TASK = "orchestrate-install"

INSTALL_START = r"""
set -euo pipefail
rm -rf "$INSTALL_ROOT"
run="$INSTALL_ROOT/$INSTALL_RUN"
mkdir -p "$run"
cat /proc/sys/kernel/random/boot_id > "$run/boot"
cat > "$run/install.sh" <<'ORCHESTRATE_INSTALL_EOF'
__BOOTSTRAP__
ORCHESTRATE_INSTALL_EOF
cat > "$run/run.sh" <<'ORCHESTRATE_INSTALL_EOF'
run="$1"
echo $$ > "$run/pid.tmp" && mv "$run/pid.tmp" "$run/pid"
tasks() { curl -sS --unix-socket /.sprite/api.sock -H "Content-Type: application/json" "$@"; }
( while :; do tasks -X PUT "http://sprite/v1/tasks/$INSTALL_TASK" -d '{"expire": "5m"}'; sleep 60; done ) \
  >> "$run/hold.log" 2>&1 &
hold=$!
bash "$run/install.sh" > "$run/log" 2>&1
code=$?
kill "$hold"
tasks -X DELETE "http://sprite/v1/tasks/$INSTALL_TASK" >> "$run/hold.log" 2>&1
echo "$code" > "$run/exit.tmp" && mv "$run/exit.tmp" "$run/exit"
ORCHESTRATE_INSTALL_EOF
setsid bash "$run/run.sh" "$run" < /dev/null > "$run/run.log" 2>&1 &
for _ in $(seq 100); do [ -s "$run/pid" ] && break; sleep 0.1; done
if [ ! -s "$run/pid" ]; then
  echo "the install did not start in 10s; its launcher said:"; cat "$run/run.log"; exit 1
fi
"""

# The first line is the run's state: `exit <code>`, `running`, or `gone` (its
# process is not there and it wrote no exit code: the sprite rebooted under
# it, or it was killed). Liveness is read before the exit code, so a run that
# ends between the two reads is seen as ended, never as gone.
INSTALL_LOOK = r"""
run="$INSTALL_ROOT/$INSTALL_RUN"
alive=no
if [ "$(cat /proc/sys/kernel/random/boot_id)" = "$(cat "$run/boot" 2>/dev/null)" ] \
   && kill -0 "$(cat "$run/pid" 2>/dev/null)" 2>/dev/null; then alive=yes; fi
if [ -f "$run/exit" ]; then echo "exit $(cat "$run/exit")"
elif [ "$alive" = yes ]; then echo running
else echo gone; fi
tail -n "+$((INSTALL_FROM + 1))" "$run/log" 2>/dev/null
"""
# SpritesProvider.exec runs each script as `bash -c EXEC_WRAP exec <dir>
# <script>`: the script's output goes to <dir>/out and its exit code to
# <dir>/code, for the filesystem API to read.
EXEC_ROOT = "/tmp/orchestrate-exec"
EXEC_WRAP = 'mkdir -p "$1" && { bash -c "$2" > "$1/out" 2>&1; echo $? > "$1/code"; }'
# How often a running install is looked at, and how long it may take.
INSTALL_LOOK_EVERY = 5.0
INSTALL_TIMEOUT = 1800.0


async def remote_commit(source: str, ref: str) -> str:
    """The commit `ref` names on `source` now: what a new worker runs."""
    r = await git(Path.cwd(), "ls-remote", source, f"refs/heads/{ref}", check=False)
    sha = r.out.split("\t", 1)[0].strip()
    if r.code != 0 or not sha:
        raise RuntimeError(f"cannot resolve branch {ref!r} of {source}: {r.err or 'no such branch'}")
    return sha


class SpritesProvider:
    """Workers on Fly Sprites (https://api.sprites.dev/v1, Bearer $SPRITES_TOKEN).

    Each worker is a sprite named `<prefix><pool>-w<slot>` (`orch-3f9a1c2b-w1`,
    ...) and labeled `orchestrator-<pool>`, so a control plane finds its own
    sprites among the organization's (`sweep`) and never another's: each has
    its own runner token and projects, and would restart the other's runners
    and prune its projects off them. The sprite's URL is public at the
    proxy level; the runner itself requires RUNNER_TOKEN, so the Authorization
    header stays free for it.
    """

    name = "sprites"
    API = "https://api.sprites.dev/v1"
    LABEL = "orchestrator"

    def __init__(self, token: str | None = None, source: str | None = None, ref: str | None = None,
                 prefix: str | None = None) -> None:
        from .secrets import get_secret

        self.token = token or get_secret("SPRITES_TOKEN") or ""
        if not self.token:
            raise RuntimeError("SpritesProvider needs SPRITES_TOKEN")
        default_source, default_ref = repo_source_default()
        self.source = source or default_source
        self.ref = ref or default_ref
        # The checkout this control plane runs from, when sprites run its branch.
        self.checkout = None if source or ref else Path(__file__).resolve().parents[2]
        if not self.source.startswith(("http://", "https://")):
            print(f"SpritesProvider: {self.source} is not an http(s) URL and a sprite has no git "
                  "credentials; the clone will fail unless the URL is publicly readable.", file=sys.stderr)
        self.prefix = prefix if prefix is not None else "orch-"
        self.http = httpx.AsyncClient(base_url=self.API, timeout=httpx.Timeout(60.0, read=600.0),
                                      headers={"authorization": f"Bearer {self.token}"})

    def worker_name(self, index: int, pool: str) -> str:
        if not pool:
            raise ValueError("a sprite worker needs its pool's name (Store.pool_id)")
        return f"{self.prefix}{pool}-w{index}"

    def label(self, pool: str) -> str:
        return f"{self.LABEL}-{pool}"

    def unnamed(self, sprite: dict) -> bool:
        """A sprite of a control plane from before pools had names: labeled
        just `orchestrator` (the one pool every control plane shared,
        `<prefix>w<n>`), or named as each project's own workers were before
        that (`<prefix>p-<id>-w<n>`). No control plane uses these now."""
        name = sprite.get("name") or ""
        legacy = re.fullmatch(re.escape(self.prefix) + r"p-[0-9a-f]+-w\d+", name)
        return self.LABEL in (sprite.get("labels") or []) or legacy is not None

    async def sprites(self) -> list[dict]:
        """Every sprite whose name starts with the prefix, page by page."""
        out, token = [], None
        while True:
            params = {"prefix": self.prefix, "max_results": 100}
            if token:
                params["continuation_token"] = token
            page = (await self.http.get("/sprites", params=params)).raise_for_status().json()
            out += page.get("sprites") or []
            token = page.get("next_continuation_token")
            if not page.get("has_more") or not token:
                return out

    async def sweep(self, slots: int, pool: str) -> list[str]:
        keep = {self.worker_name(i, pool) for i in range(1, slots + 1)}
        label = self.label(pool)
        gone = sorted(s["name"] for s in await self.sprites()
                      if (label in (s.get("labels") or []) and s["name"] not in keep) or self.unnamed(s))
        for name in gone:
            await self.destroy(Worker(id=name, provider=self.name, handle=name, url=""))
        return gone

    async def create(self, spec: WorkerSpec) -> Worker:
        from .secrets import get_secret

        if not get_secret("DEEPSEEK_API_KEY"):
            raise RuntimeError("SpritesProvider needs DEEPSEEK_API_KEY (env or keyring): "
                               "the runner cannot start without it")
        name, label, steps = self.worker_name(spec.index, spec.pool), self.label(spec.pool), spec.steps
        steps.name = f"worker {name}"
        async with steps.step(f"making sprite {name}, or finding it made"):
            r = await self.http.post("/sprites", json={"name": name, "url_settings": {"auth": "public"},
                                                       "labels": [label]})
            if r.status_code == 409:  # the slot's sprite is there: it carries the label too
                (await self.http.put(f"/sprites/{name}", json={"labels": [label]})).raise_for_status()
            elif r.status_code not in (200, 201):
                r.raise_for_status()
            info = (await self.http.get(f"/sprites/{name}")).raise_for_status().json()
        steps.note(f"sprite {name} " + ("was there" if r.status_code == 409 else "made")
                   + f", status {info.get('status', '?')}, at {info['url']}")
        worker = Worker(id=name, provider=self.name, handle=name,
                        url=info["url"].rstrip("/"), capacity=spec.capacity, index=spec.index)
        worker.token = spec.token or worker.token
        async with steps.step(f"resolving branch {self.ref} of {self.source}"):
            commit = await remote_commit(self.source, self.ref)
        note = checkout_note(self.checkout, self.ref, commit) if self.checkout else ""
        if note:
            steps.note(note)
        env = {**passthrough_env(), **spec.env, "RUNNER_PORT": "8080", "RUNNER_HOST": "0.0.0.0",
               "RUNNER_TOKEN": worker.token, "RUNNER_WORKDIR": "/home/sprite/threads",
               "RUNNER_CAPACITY": str(spec.capacity), "PYTHONUNBUFFERED": "1"}
        service = {"cmd": "bash", "args": ["/opt/orchestrate/runner/deploy/start.sh"],
                   "env": {**env, "ORCHESTRATE_HOME": "/opt/orchestrate", "ORCHESTRATE_COMMIT": commit},
                   "http_port": 8080}
        want = service_spec(service)
        service["env"]["RUNNER_SPEC"] = want
        # A sprite an earlier control plane made (or this one, before the
        # worker stopped answering) still has its runner service. Restarting
        # it ends every turn under way there, so it is left running when it
        # is exactly the runner this would start: the same commit, model key,
        # token and settings, as the fingerprint it reports says. Otherwise
        # the service is removed and put again (see below), so the runner
        # serving the worker is always this checkout's, with this control
        # plane's key and token; the turns that cuts short resume when their
        # threads are posted again (runner/API.md, "Interrupted turns").
        async with steps.step(f"looking for its runner service (up to {SERVICES_WAIT:.0f}s: a cold sprite "
                              "boots its services manager first)"):
            defined = await self.runner_defined(name)
        if defined:
            async with steps.step(f"asking its runner whether it is orchestrate {commit[:12]} as this control "
                                  f"plane would start it (up to {SERVES_WAIT:.0f}s: a paused sprite wakes)"):
                serving = await self.serves(worker, want)
            if serving:
                steps.note("it is: left running")
                return worker
            steps.note("it is not (another commit, key, token or settings, or no answer): its service is "
                       "removed and put again")

        async with steps.step(f"installing orchestrate {commit[:12]} on it (git fetch, pixi install, prime-agent)"):
            await self.install(name, commit, steps)
        if defined:
            # A definition is removed, not replaced: the API answers a PUT whose
            # command is the one it already has with "Service already running
            # with that command, use .../restart" and keeps the definition it
            # has, whether that service runs or not. The env travels in the
            # definition (this control plane's token, model key, commit and
            # fingerprint), so a redefined service kept the old ones for good:
            # the runner reported no fingerprint, and every attach restarted a
            # worker a matching one would have been left running. Removing the
            # service stops the runner it ran; the PUT below makes it, on this
            # definition, and starts it.
            async with steps.step("removing its runner service (a PUT cannot replace a definition)"):
                gone = await self.http.delete(f"/sprites/{name}/services/runner")
                if gone.status_code != 404:  # already gone
                    gone.raise_for_status()
        async with steps.step("defining its runner service, which starts it"):
            (await self.http.put(f"/sprites/{name}/services/runner", json=service)).raise_for_status()
        async with steps.step("waiting up to 300s for its runner to answer"):
            health = await self.wait_ready(worker)
        if health.get("commit") != commit:
            raise RuntimeError(f"worker {name}'s runner runs orchestrate {health.get('commit')}, not "
                               f"{commit} ({self.ref}); its log ends:\n{await self.logs(worker, 20)}")
        async with steps.step("checking its runner accepts the token it was started with"):
            accepted = await self.accepts_token(worker)
        if not accepted:
            raise RuntimeError(f"worker {name}'s runner does not accept the token it was started "
                               f"with; its log ends:\n{await self.logs(worker, 20)}")
        return worker

    async def runner_defined(self, name: str) -> bool:
        """Whether sprite `name` has a runner service. A cold sprite's services
        API answers 500 "services manager not started" until the sprite has
        booted it, so that answer is waited out (up to SERVICES_WAIT); a 404
        is no service, and any other answer an error."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + SERVICES_WAIT
        while True:
            r = await self.http.get(f"/sprites/{name}/services/runner")
            if r.status_code in (200, 404):
                return r.status_code == 200
            booting = r.status_code == 500 and SERVICES_BOOTING in r.text
            if not booting or loop.time() > deadline:
                raise RuntimeError(failure(f"looking for sprite {name}'s runner service", r)
                                   + (f" (still, after {SERVICES_WAIT:.0f}s)" if booting else ""))
            await asyncio.sleep(SERVICES_LOOK_EVERY)

    async def serves(self, worker: Worker, want: str) -> bool:
        """Whether the runner serving the sprite now was started from the
        service definition fingerprinted `want` (its token among it). A
        paused sprite wakes for the look, its service with it, so the look
        waits SERVES_WAIT for an answer; one that gives none, or another
        fingerprint, is not that runner."""
        try:
            health = await wait_healthy(worker.url, timeout=SERVES_WAIT)
        except TimeoutError:
            return False
        return health.get("spec") == want

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

    async def install(self, name: str, commit: str, steps: Steps, script: str = BOOTSTRAP) -> None:
        """Run `script` (BOOTSTRAP) for `commit` on sprite `name`, detached (INSTALL_START),
        and look at it every INSTALL_LOOK_EVERY seconds until it ends, noting
        each line it writes. Raises, with the end of its log, when it fails,
        dies or takes longer than INSTALL_TIMEOUT."""
        run = f"{commit[:12]}-{secrets.token_hex(4)}"
        env = {"INSTALL_ROOT": INSTALL_ROOT, "INSTALL_RUN": run, "INSTALL_TASK": INSTALL_TASK}
        await self.exec(name, INSTALL_START.replace("__BOOTSTRAP__", script.strip()), env={**env, "ORCHESTRATE_REPO": self.source,
                                                  "ORCHESTRATE_COMMIT": commit})
        steps.note(f"  install started on {name}, in {INSTALL_ROOT}/{run}")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + INSTALL_TIMEOUT
        seen: list[str] = []  # the log's lines read so far
        while True:
            out = await self.exec(name, INSTALL_LOOK, env={**env, "INSTALL_FROM": str(len(seen))})
            state, _, rest = out.partition("\n")
            ended = state != "running"
            lines = rest.split("\n")
            # A last line without its newline may still be being written: it
            # is read again next time, unless the run has ended.
            if lines[-1] == "" or not ended:
                lines = lines[:-1]
            for line in lines:
                steps.note(f"  install | {line}")
            seen += lines
            if state == "exit 0":
                return
            end = "\n".join(seen[-30:])
            if state.startswith("exit "):
                raise RuntimeError(f"installing orchestrate {commit[:12]} on sprite {name} failed "
                                   f"({state}); its output ends:\n{end}")
            if state == "gone":
                raise RuntimeError(f"the install of orchestrate {commit[:12]} on sprite {name} stopped "
                                   f"without finishing (the sprite restarted, or it was killed); its "
                                   f"output ends:\n{end}")
            if state != "running":
                raise RuntimeError(f"looking at the install on sprite {name} gave {state!r}, not a state")
            if loop.time() > deadline:
                raise RuntimeError(f"installing orchestrate {commit[:12]} on sprite {name} took longer than "
                                   f"{INSTALL_TIMEOUT:.0f}s; it is still running there; its output "
                                   f"ends:\n{end}")
            await asyncio.sleep(INSTALL_LOOK_EVERY)

    async def exec(self, name: str, script: str, env: dict | None = None) -> str:
        """Run a short bash script on sprite `name` and return its output
        (stdout and stderr together); raise, with that output, if it exits
        non-zero. POST /exec holds the request until the script ends, so long
        work goes through `install`'s pattern.

        The exec's own response body is not read: it is stdout, stderr and the
        exit code multiplexed into frames with a stream byte and no length
        (sprites-js: "HTTP combined frames can remain ambiguous"), so it cannot
        be parsed reliably. EXEC_WRAP writes the output and the exit code to
        files on the sprite instead, which the filesystem API reads back as
        plain bytes, and the files are removed after."""
        out = f"{EXEC_ROOT}/{secrets.token_hex(8)}"
        params = [("cmd", "bash"), ("cmd", "-c"), ("cmd", EXEC_WRAP), ("cmd", "exec"), ("cmd", out),
                  ("cmd", script)]
        params += [("env", f"{k}={v}") for k, v in (env or {}).items()]
        r = await self.http.post(f"/sprites/{name}/exec", params=params)
        if r.status_code >= 400:
            raise RuntimeError(failure(f"exec on sprite {name}", r))
        try:
            code = (await self.read(name, f"{out}/code")).decode().strip()
            output = (await self.read(name, f"{out}/out")).decode(errors="replace")
        finally:
            gone = await self.http.request("DELETE", f"/sprites/{name}/fs/delete",
                                           json={"path": out, "workingDir": "/", "recursive": True,
                                                 "asRoot": False})
            if gone.status_code >= 400:
                raise RuntimeError(failure(f"removing {out} on sprite {name}", gone))
        if code != "0":
            raise RuntimeError(f"a script on sprite {name} exited {code}; its output:\n{output[-2000:]}")
        return output

    async def read(self, name: str, path: str) -> bytes:
        """The bytes of file `path` on sprite `name`."""
        r = await self.http.get(f"/sprites/{name}/fs/read", params={"path": path, "workingDir": "/"})
        if r.status_code >= 400:
            raise RuntimeError(failure(f"reading {path} on sprite {name}", r))
        return r.content

    # A sprite service's stdout and stderr, timestamped (Sprites docs, "Services").
    LOG = "/.sprite/logs/services/runner.log"

    async def logs(self, worker: Worker, lines: int) -> str:
        return tail(await self.read(worker.handle, self.LOG), lines)

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
        r = await self.http.delete(f"/sprites/{shlex.quote(worker.handle)}")
        if r.status_code != 404:  # already gone
            r.raise_for_status()

    async def close(self) -> None:
        await self.http.aclose()


def failure(what: str, r: httpx.Response) -> str:
    """A failed Sprites API call as one line that says enough to chase it: the
    status and its reason, how long the call took, the request id the API's
    proxy gave it, and the body, or that there was none (a gateway's 502)."""
    try:
        took = f" after {r.elapsed.total_seconds():.1f}s"
    except RuntimeError:  # a response built by hand (tests) has no elapsed
        took = ""
    ids = [f"{h} {r.headers[h]}" for h in ("fly-request-id", "x-request-id", "server", "via") if h in r.headers]
    body = r.text.strip()
    return (f"{what} failed: {r.status_code} {r.reason_phrase}{took} ({r.request.method} {r.request.url.path}"
            + (f"; {', '.join(ids)}" if ids else "") + "): "
            + (body[:500] if body else "the API answered with an empty body"))


def service_spec(service: dict) -> str:
    """A fingerprint of a runner service definition (its command, and its
    environment with the commit, model key and token): the runner reports it
    in /health as `spec`, from $RUNNER_SPEC, so a control plane can tell a
    runner it would start the same way. One-way, since /health is open."""
    return hashlib.sha256(json.dumps(service, sort_keys=True).encode()).hexdigest()


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


# Where a thread's workers can run, by the name its provider goes by
# (Worker.provider), with how the UI names it.
PLACES = {"local": "This computer", "sprites": "Sprites"}

# What a thread's work can reach in each place: the router places threads by
# what their work must touch (router.py), so this is what it goes by.
REACHES = {
    "local": ("the person's own computer, where this control plane runs, through the runner's sandbox: its "
              "system read-only (installed programs, /etc, system logs, crash dumps) and its local "
              "network; not its running processes, the person's home, or its devices"),
    "sprites": ("a cloud machine with the project's repository, the internet and whatever tools the thread "
                "installs; it sees nothing of the person's computer"),
}


class PlaceRefused(Exception):
    """A thread was to run where this control plane runs no workers."""


class Places:
    """The providers of the places this control plane runs workers in, and
    for each place it does not, why. The router names each thread's place;
    nothing picks another one for it."""

    def __init__(self, providers: dict[str, WorkerProvider], refused: dict[str, str] | None = None) -> None:
        unknown = (set(providers) | set(refused or {})) - set(PLACES)
        if unknown:
            raise ValueError(f"unknown worker place {', '.join(sorted(unknown))}")
        self.providers = providers
        self.refused = {p: (refused or {}).get(p, "this control plane does not run workers there")
                        for p in PLACES if p not in providers}

    def provider(self, place: str) -> WorkerProvider:
        """The provider of `place`, or PlaceRefused saying why there is none."""
        if place in self.providers:
            return self.providers[place]
        if place not in PLACES:
            raise PlaceRefused(f"workers cannot run on {place!r}: it is none of {', '.join(PLACES)}")
        raise PlaceRefused(f"workers cannot run on {PLACES[place]}: {self.refused[place]}")

    def offered(self) -> list[dict]:
        """Every place, in PLACES order, as the UI and the router see it:
        what work there `reaches`, and `why` not, empty for those this
        control plane runs workers in."""
        return [{"id": p, "label": label, "reaches": REACHES[p], "why": self.refused.get(p, "")}
                for p, label in PLACES.items()]

    async def close(self) -> None:
        for provider in self.providers.values():
            close = getattr(provider, "close", None)
            if close is not None:
                await close()


SPRITES_MISSING = ("it needs SPRITES_TOKEN, a Fly Sprites organization token (sent as "
                   "`Authorization: Bearer <token>` to https://api.sprites.dev/v1), and found none when "
                   "the app started ({looked}). Store it in Settings or with `pixi run set-key SPRITES_TOKEN`, "
                   "or set it in the environment, and start the app again")


def default_places() -> Places:
    """This computer, when it can run the runner (Linux, with ../runner and
    DEEPSEEK_API_KEY), and Sprites, when SPRITES_TOKEN is stored."""
    from .secrets import get_secret, not_found

    providers: dict[str, WorkerProvider] = {}
    refused: dict[str, str] = {}
    if not sys.platform.startswith("linux"):
        refused["local"] = ("the runner is Linux-only (it sandboxes threads with bubblewrap), "
                            f"and this computer runs {sys.platform}")
    else:
        try:
            providers["local"] = LocalProvider()
        except RuntimeError as e:
            refused["local"] = str(e)
    if get_secret("SPRITES_TOKEN"):
        providers["sprites"] = SpritesProvider()
    else:
        refused["sprites"] = SPRITES_MISSING.format(looked=not_found("SPRITES_TOKEN"))
    return Places(providers, refused)
