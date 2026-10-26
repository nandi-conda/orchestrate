"""Git plumbing shared by the hub (control/hub.py) and the tests' echo runner's
worker repository (tests/devrunner.py): running git, and git bundles, which
carry commits between a project's hub and its workers (runner/API.md,
"Repository"). The runner has its own copy in runner/src/runner/repo.py; the
two are separate pixi projects."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

AUTHOR = ("orchestrate", "threads@orchestrate.invalid")


class GitError(RuntimeError):
    pass


@dataclass
class Git:
    code: int
    out: str
    err: str


async def git(cwd: str | os.PathLike, *args: str, check: bool = True, env: dict | None = None) -> Git:
    proc = await asyncio.create_subprocess_exec(
        "git", *args, cwd=str(cwd), env={**os.environ, "GIT_TERMINAL_PROMPT": "0", **(env or {})},
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate()
    r = Git(proc.returncode or 0, out.decode(errors="replace").strip(), err.decode(errors="replace").strip())
    if check and r.code:
        raise GitError(f"git {' '.join(args)} failed: {r.err or r.out}")
    return r


async def has_commit(repo: str | os.PathLike, sha: str) -> bool:
    return (await git(repo, "rev-parse", "-q", "--verify", f"{sha}^{{commit}}", check=False)).code == 0


async def tips(repo: str | os.PathLike, names: list[str]) -> dict[str, str]:
    """The refs among `names` that exist, with their tips."""
    out = {}
    for name in names:
        r = await git(repo, "rev-parse", "-q", "--verify", name, check=False)
        if r.code == 0:
            out[name] = r.out
    return out


async def bundle(repo: str | os.PathLike, refs: list[str], have: list[str]) -> bytes:
    """A bundle of `refs` without what `have` reaches; empty when that is
    everything (git refuses to make an empty bundle)."""
    known = [h for h in have if await has_commit(repo, h)]
    nots = ["--not", *known] if known else []
    if (await git(repo, "rev-list", "--count", *refs, *nots)).out == "0":
        return b""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "out.bundle")
        await git(repo, "bundle", "create", "-q", path, *refs, *nots)
        return Path(path).read_bytes()


async def unbundle(repo: str | os.PathLike, data: bytes) -> None:
    """Add a bundle's objects to `repo`; its refs are set apart, by name."""
    if not data:
        return
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "in.bundle")
        Path(path).write_bytes(data)
        await git(repo, "bundle", "unbundle", path)


def refs_header(refs: dict[str, str]) -> str:
    return json.dumps(refs, separators=(",", ":"))


class Checkout:
    """A worker's repository, as the echo runner keeps it (the runner's
    repo.Repo does the same): remote-tracking refs the orchestrator sets, a
    worktree per thread on `thread/<id>`, snapshots of whole worktrees, and
    each thread's session as a commit of its session directory."""

    SHIPPED = ("refs/remotes/origin/", "refs/remotes/origin-snapshots/", "refs/remotes/origin-sessions/")

    def __init__(self, path: str | os.PathLike) -> None:
        self.path = Path(path)
        self.base = ""
        self.lock = asyncio.Lock()

    async def open(self) -> None:
        if not (self.path / ".git").exists():
            self.path.mkdir(parents=True, exist_ok=True)
            await git(self.path, "init", "-q")
        for key, value in (("user.name", AUTHOR[0]), ("user.email", AUTHOR[1]), ("gc.auto", "0")):
            await git(self.path, "config", key, value)
        self.base = (await git(self.path, "config", "--get", "orchestrate.base", check=False)).out

    async def refs(self) -> dict[str, str]:
        out = (await git(self.path, "for-each-ref", "--format=%(refname) %(objectname)")).out
        return dict(line.split(" ", 1) for line in out.splitlines() if line)

    async def receive(self, data: bytes, base: str, refs: dict[str, str]) -> None:
        bad = [r for r in refs if not r.startswith(self.SHIPPED)]
        if bad:
            raise GitError(f"refs the orchestrator may not set: {', '.join(bad)}")
        async with self.lock:
            await unbundle(self.path, data)
            for ref, sha in refs.items():
                await git(self.path, "update-ref", ref, sha)
            await git(self.path, "config", "orchestrate.base", base)
            self.base = base

    async def send(self, tid: str, have: list[str]) -> tuple[bytes, dict[str, str]]:
        refs = await tips(self.path, [f"refs/heads/thread/{tid}", f"refs/snapshots/{tid}", f"refs/sessions/{tid}"])
        if not refs:
            raise GitError(f"thread {tid} has no branch on this worker")
        return await bundle(self.path, list(refs), have), refs

    async def worktree(self, path: Path, tid: str) -> None:
        """As runner/API.md, "Resuming a thread": keep the worktree while it
        holds the hub's branch, else make it from that branch and snapshot,
        else from the base."""
        if not self.base:
            raise GitError("the orchestrator has not sent the project's repository to this worker")
        theirs, snapshot = f"refs/remotes/origin/thread/{tid}", f"refs/remotes/origin-snapshots/{tid}"
        async with self.lock:
            await git(self.path, "worktree", "prune")
            known = await has_commit(self.path, theirs)
            listed = (await git(self.path, "worktree", "list", "--porcelain")).out.splitlines()
            if f"worktree {path.resolve()}" in listed:
                if not known or (await git(path, "merge-base", "--is-ancestor", theirs, "HEAD", check=False)).code == 0:
                    return
                await git(self.path, "worktree", "remove", "--force", str(path))
            if path.exists():
                shutil.rmtree(path)
            await git(self.path, "worktree", "add", "-q", "-B", f"thread/{tid}", str(path),
                      theirs if known else f"refs/remotes/origin/{self.base}")
            if known and await has_commit(self.path, snapshot):
                env = await self.index(path, "restore.index")
                await git(path, "read-tree", snapshot, env=env)
                await git(path, "checkout-index", "-a", "-f", env=env)
                os.unlink(env["GIT_INDEX_FILE"])
                await git(path, "reset", "-q", "--hard", "HEAD")

    async def index(self, path: Path, name: str) -> dict:
        p = Path((await git(path, "rev-parse", "--git-path", name)).out)
        return {"GIT_INDEX_FILE": str(p if p.is_absolute() else path / p)}

    async def save(self, path: Path, tid: str, message: str) -> str:
        """Commit what the worktree holds and snapshot all of it; the branch tip."""
        await git(path, "add", "-A")
        if (await git(path, "diff", "--cached", "--quiet", check=False)).code:
            await git(path, "commit", "-q", "-m", message)
        env = await self.index(path, "snapshot.index")
        await git(path, "add", "-A", "-f", env=env)
        tree = (await git(path, "write-tree", env=env)).out
        snap = (await git(path, "commit-tree", tree, "-p", "HEAD", "-m", f"Snapshot of thread/{tid}")).out
        await git(path, "update-ref", f"refs/snapshots/{tid}", snap)
        return (await git(path, "rev-parse", "HEAD")).out

    def _session_env(self, tid: str, directory: Path) -> dict:
        index = self.path / ".git" / "sessions" / f"{tid}.index"
        index.parent.mkdir(parents=True, exist_ok=True)
        return {"GIT_DIR": str(self.path / ".git"), "GIT_WORK_TREE": str(directory), "GIT_INDEX_FILE": str(index)}

    async def save_session(self, tid: str, directory: Path) -> None:
        """Commit the thread's session directory at `refs/sessions/<id>`."""
        env = self._session_env(tid, directory)
        await git(directory, "add", "-A", "-f", ".", env=env)
        tree = (await git(directory, "write-tree", env=env)).out
        ref = f"refs/sessions/{tid}"
        parent = ["-p", ref] if await has_commit(self.path, ref) else []
        commit = (await git(directory, "commit-tree", tree, *parent, "-m", f"Session of thread/{tid}", env=env)).out
        await git(self.path, "update-ref", ref, commit)

    async def session(self, tid: str, directory: Path) -> None:
        """As the runner's Repo.session: the worker's session while it holds
        the hub's, else the hub's."""
        theirs, ours = f"refs/remotes/origin-sessions/{tid}", f"refs/sessions/{tid}"
        async with self.lock:
            if not await has_commit(self.path, theirs):
                return
            if await has_commit(self.path, ours) and (await git(
                    self.path, "merge-base", "--is-ancestor", theirs, ours, check=False)).code == 0:
                return
            shutil.rmtree(directory, ignore_errors=True)
            directory.mkdir(parents=True)
            env = self._session_env(tid, directory)
            await git(directory, "read-tree", theirs, env=env)
            await git(directory, "checkout-index", "-a", "-f", env=env)
            await git(self.path, "update-ref", ours, theirs)

    async def follow(self, path: Path, tid: str) -> None:
        await git(path, "reset", "-q", "--keep", f"refs/remotes/origin/thread/{tid}")
