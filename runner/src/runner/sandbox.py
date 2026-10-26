"""The sandbox a thread's agent works in: what it can see of the worker.

A thread's agent runs its code (prime-agent's Python kernel, and every command
and process the kernel starts) in bubblewrap with a `View` of the worker; see
harness.py for how the kernel is put in it.
"""

from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path


class SandboxError(RuntimeError):
    pass


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
    `readonly` paths inside hidden ones are put back read-only (the kernel's
    Python environment, which lives in the runner's checkout). `tools` are
    programs of the worker's put back read-only at their own paths and on the
    command's PATH (pixi: every project is a pixi project).
    """
    root: str                         # the workspace: the project's worktree, where commands start
    home: str                         # the thread's HOME
    tmp: str                          # the thread's /tmp
    writable: tuple[str, ...] = ()    # more of the worker the workspace needs (the clone's .git)
    hidden: tuple[str, ...] = ()
    readonly: tuple[str, ...] = ()
    tools: tuple[str, ...] = ()


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


def pixi() -> str:
    """The pixi the runner runs under (`pixi run` sets PIXI_EXE), which a
    thread's commands get too: in the worker's home, which they do not see,
    when deploy/bootstrap.sh installed it."""
    exe = os.environ.get("PIXI_EXE")
    if not exe:
        raise SandboxError("PIXI_EXE is not set: run the runner with `pixi run` (serve, run, test), "
                           "so threads get the pixi it runs under")
    return str(Path(exe).resolve())


def _under(path: str, roots: tuple[str, ...]) -> bool:
    p = Path(path)
    return any(p == Path(r) or Path(r) in p.parents for r in roots)


def thread_env(view: View) -> dict[str, str]:
    """The sandbox's environment, made afresh: none of the runner's variables
    (tokens, keys, RUNNER_*, the pixi environment's activation), and a PATH
    with the view's tools and without the directories the view hides. Tools
    keep what they make in the thread's HOME (pixi its cache and global tools)."""
    keep = {k: v for k, v in os.environ.items()
            if k in ("LANG", "TERM", "TZ", "USER", "LOGNAME", "SHELL") or k.startswith("LC_")}
    tools = list(dict.fromkeys(str(Path(t).parent) for t in view.tools))
    path = os.pathsep.join(tools + [p for p in os.environ.get("PATH", "").split(os.pathsep)
                                    if p and p not in tools and not _under(str(Path(p).resolve()), view.hidden)])
    return {**keep, "PATH": path, "HOME": view.home, "TMPDIR": "/tmp"}


def bwrap_args(view: View) -> list[str]:
    """bubblewrap's mounts for `view`, parents before children so a writable
    path inside a hidden one is put back after the hiding."""
    hidden = [p for p in view.hidden if Path(p).is_dir()]
    # A hidden path inside another is hidden already; its own mount point would show.
    mounts = [(p, 0, ["--tmpfs", p]) for p in hidden if not _under(str(Path(p).parent), tuple(hidden))]
    mounts += [("/tmp", 1, ["--bind", view.tmp, "/tmp"])]
    mounts += [(p, 1, ["--bind", p, p]) for p in (view.root, view.home, *view.writable)]
    mounts += [(p, 1, ["--ro-bind", p, p]) for p in (*view.readonly, *view.tools)]
    mounts.sort(key=lambda m: (len(Path(m[0]).parts), m[1]))
    args = ["--ro-bind", "/", "/", "--dev", "/dev", "--unshare-pid", "--proc", "/proc", "--die-with-parent"]
    for _, _, a in mounts:
        args += a
    return args + ["--chdir", view.root]


def bwrap() -> str:
    """The bubblewrap binary (a dependency of the runner's pixi environment)."""
    path = shutil.which("bwrap")
    if not path:
        raise SandboxError("bwrap (bubblewrap) is not on PATH; it ships with the runner's pixi environment")
    return path


def no_capabilities() -> None:
    """Runs in a child before it execs whatever starts bwrap (prime-agent, or
    bwrap itself): clear the ambient capability set. A Sprite's services run as
    a non-root user that holds capabilities in it, which survive exec; bwrap
    refuses those ("Unexpected capabilities but not setuid"), and a thread has
    no use for them either. Without ambient capabilities, bwrap starts with
    none and makes its sandbox from a user namespace."""
    import ctypes

    PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL = 47, 4
    if ctypes.CDLL(None, use_errno=True).prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0):
        raise OSError(ctypes.get_errno(), "prctl(PR_CAP_AMBIENT_CLEAR_ALL) failed")


def check() -> None:
    """Fail now, not at a thread's first command, when this machine cannot run
    the sandbox (no user namespaces for this user, for one) or the runner has
    no pixi to give threads."""
    import subprocess

    pixi()

    r = subprocess.run([bwrap(), "--ro-bind", "/", "/", "--unshare-pid", "--proc", "/proc", "true"],
                       capture_output=True, text=True, preexec_fn=no_capabilities)
    if r.returncode:
        raise SandboxError(f"bubblewrap cannot make a sandbox here: {r.stderr.strip()} (it needs unprivileged "
                           "user namespaces and a /proc with nothing mounted over it; deploy/start.sh unmasks it)")
