"""Sandbox interface: where a thread's commands and file edits actually happen."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

MAX_OUTPUT = 30_000

log = logging.getLogger(__name__)

# Which bwrap option mounts /proc: `--proc` (a fresh procfs) or `--tmpfs` (an
# empty directory). The latter is the fallback on kernels that refuse to mount
# procfs in an unprivileged user namespace (Sprites' does). `check()` picks one.
_proc_mount = "proc"


@dataclass
class ExecResult:
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool = False


class Sandbox(Protocol):
    root: str

    async def exec(self, cmd: str, timeout: float = 120.0) -> ExecResult: ...
    async def read_file(self, path: str) -> bytes: ...
    async def write_file(self, path: str, data: bytes) -> None: ...
    async def list_dir(self, path: str) -> list[str]: ...


class SandboxError(RuntimeError):
    pass


def _clip(b: bytes) -> str:
    s = b.decode("utf-8", errors="replace")
    if len(s) > MAX_OUTPUT:
        half = MAX_OUTPUT // 2
        s = s[:half] + f"\n... [{len(s) - MAX_OUTPUT} chars cut] ...\n" + s[-half:]
    return s


@dataclass(frozen=True)
class View:
    """What a thread's commands see of the worker: the project, and nothing of ours.

    The worker runs the runner from a checkout of the orchestrate monorepo and
    keeps every thread's workspace, the project clone and its secrets on the
    same machine. A thread's command sees the machine's system read-only, with
    `hidden` paths empty (the runner's checkout and environment, the worker's
    home and state), its workspace and the clone's git dir writable at their
    own paths, its own `home`, and its own `tmp` as /tmp. It has its own pid
    namespace, so the runner's processes (and their environments) are not in
    its /proc.
    """
    root: str                         # the workspace: the project's worktree, where commands start
    home: str                         # the thread's HOME
    tmp: str                          # the thread's /tmp
    writable: tuple[str, ...] = ()    # more of the worker the workspace needs (the clone's .git)
    hidden: tuple[str, ...] = ()


def runner_checkout() -> Path:
    """The orchestrate checkout this runner runs from (deploy/bootstrap.sh and
    the orchestrator's LocalProvider both run it from one): src/runner/ is two
    levels into runner/, which is one into the checkout."""
    return Path(__file__).resolve().parents[3]


def worker_private(workdir: str | os.PathLike) -> tuple[str, ...]:
    """Everything on a worker that is not the project: the runner's checkout
    and environment, the worker user's home (keyring, caches) and the runner's
    working directory (other threads' workspaces)."""
    return tuple(str(Path(p).resolve()) for p in (runner_checkout(), sys.prefix, Path.home(), workdir))


def _under(path: str, roots: tuple[str, ...]) -> bool:
    p = Path(path)
    return any(p == Path(r) or Path(r) in p.parents for r in roots)


def thread_env(view: View) -> dict[str, str]:
    """A command's environment, made afresh: none of the runner's variables
    (tokens, keys, RUNNER_*, the pixi environment's activation), and a PATH
    without the directories the view hides."""
    keep = {k: v for k, v in os.environ.items()
            if k in ("LANG", "TERM", "TZ", "USER", "LOGNAME", "SHELL") or k.startswith("LC_")}
    path = os.pathsep.join(p for p in os.environ.get("PATH", "").split(os.pathsep)
                           if p and not _under(str(Path(p).resolve()), view.hidden))
    return {**keep, "PATH": path, "HOME": view.home, "TMPDIR": "/tmp"}


def bwrap_args(view: View) -> list[str]:
    """bubblewrap's mounts for `view`, parents before children so a writable
    path inside a hidden one is put back after the hiding."""
    hidden = [p for p in view.hidden if Path(p).is_dir()]
    # A hidden path inside another is hidden already; its own mount point would show.
    mounts = [(p, 0, ["--tmpfs", p]) for p in hidden if not _under(str(Path(p).parent), tuple(hidden))]
    mounts += [("/tmp", 1, ["--bind", view.tmp, "/tmp"])]
    mounts += [(p, 1, ["--bind", p, p]) for p in (view.root, view.home, *view.writable)]
    mounts.sort(key=lambda m: (len(Path(m[0]).parts), m[1]))
    args = ["--ro-bind", "/", "/", "--dev", "/dev", "--unshare-pid",
            f"--{_proc_mount}", "/proc", "--die-with-parent"]
    for _, _, a in mounts:
        args += a
    return args + ["--chdir", view.root]


def bwrap() -> str:
    """The bubblewrap binary (a dependency of the runner's pixi environment)."""
    path = shutil.which("bwrap")
    if not path:
        raise SandboxError("bwrap (bubblewrap) is not on PATH; it ships with the runner's pixi environment")
    return path


def _no_capabilities() -> None:
    """Runs in the child before bwrap is exec'd: clear the ambient capability
    set. A Sprite's services run as a non-root user that holds capabilities in
    it, which survive exec; bwrap refuses those ("Unexpected capabilities but
    not setuid"), and a thread's commands have no use for them either. Without
    ambient capabilities, bwrap starts with none and makes its sandbox from a
    user namespace."""
    import ctypes

    PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL = 47, 4
    if ctypes.CDLL(None, use_errno=True).prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0):
        raise OSError(ctypes.get_errno(), "prctl(PR_CAP_AMBIENT_CLEAR_ALL) failed")


def _probe(proc_mount: str) -> subprocess.CompletedProcess:
    return subprocess.run([bwrap(), "--ro-bind", "/", "/", "--unshare-pid",
                           f"--{proc_mount}", "/proc", "true"],
                          capture_output=True, text=True, preexec_fn=_no_capabilities)


def check() -> None:
    """Prepare the sandbox and fail now, not at a thread's first command, when
    this machine cannot run it (no user namespaces for this user, for one).

    It also picks how /proc is mounted: a fresh procfs, or an empty tmpfs where
    the kernel refuses to mount procfs in a user namespace (Sprites' does)."""
    global _proc_mount
    procfs = _probe("proc")
    if procfs.returncode == 0:
        _proc_mount = "proc"
        return
    empty = _probe("tmpfs")
    if empty.returncode == 0:
        _proc_mount = "tmpfs"
        log.warning("cannot mount procfs in a user namespace here; threads get an empty /proc instead "
                    "(bubblewrap: %s)", procfs.stderr.strip())
        return
    raise SandboxError(f"bubblewrap cannot make a sandbox here: {procfs.stderr.strip()}; "
                       f"nor without a procfs: {empty.stderr.strip()}")


class LocalSandbox:
    """Runs on this machine, in a bubblewrap view of it (see `View`).

    File tools resolve paths inside the workspace, symlinks included; commands
    run in the view, where nothing outside it exists to reach.
    """

    def __init__(self, view: View, env: dict[str, str] | None = None):
        self.view = view
        self.root = str(Path(view.root).resolve())
        for d in (self.root, view.home, view.tmp):
            Path(d).mkdir(parents=True, exist_ok=True)
        self.env = env
        self.bwrap = bwrap()

    def _path(self, path: str) -> Path:
        p = (Path(self.root) / path).resolve()
        if p != Path(self.root) and Path(self.root) not in p.parents:
            raise SandboxError(f"path escapes the workspace: {path}")
        return p

    async def exec(self, cmd: str, timeout: float = 120.0) -> ExecResult:
        env = {**thread_env(self.view), **(self.env or {})}
        proc = await asyncio.create_subprocess_exec(
            self.bwrap, *bwrap_args(self.view), "bash", "-lc", cmd, env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            start_new_session=True, preexec_fn=_no_capabilities,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout)
        except asyncio.TimeoutError:
            try:
                os.killpg(proc.pid, 9)
            except ProcessLookupError:
                pass
            out, err = await proc.communicate()
            return ExecResult(-1, _clip(out), _clip(err), timed_out=True)
        return ExecResult(proc.returncode or 0, _clip(out), _clip(err))

    async def read_file(self, path: str) -> bytes:
        return self._path(path).read_bytes()

    async def write_file(self, path: str, data: bytes) -> None:
        p = self._path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    async def list_dir(self, path: str) -> list[str]:
        p = self._path(path)
        return sorted(f"{c.name}/" if c.is_dir() else c.name for c in p.iterdir())
