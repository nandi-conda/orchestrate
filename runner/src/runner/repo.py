"""The projects' git repositories on a worker, and how threads' work leaves them.

A worker is shared by every project: it holds one repository per project whose
threads it has run (`<workdir>/.repos/<project>`, `Repos`). The orchestrator
keeps one git repository per project, its hub, and the worker's repository is
a copy of the parts its threads need. The worker never reaches the hub, or any
remote: the orchestrator opens every connection and moves commits both ways as
git bundles (runner/API.md, "Repository"):

- `POST /projects/{project}/repo` brings the hub's refs to the worker, as
  remote-tracking refs: the project's default branch as `origin/<base>`, a
  thread's branch as `origin/thread/<id>`, its snapshot as
  `origin-snapshots/<id>` and its session as `origin-sessions/<id>`.
- `GET /projects/{project}/threads/{id}/repo` hands the orchestrator a
  thread's branch `thread/<id>`, its snapshot `refs/snapshots/<id>` and its
  session `refs/sessions/<id>`.

Every thread gets a worktree on its own branch `thread/<id>`. A new thread's
branch starts from `origin/<base>`. A thread brought back on a worker keeps
the worktree that worker still has for it when that holds all the hub has of
its branch; otherwise the worktree is made again from `origin/thread/<id>`,
with the files of its snapshot.

A snapshot is a commit of the whole worktree, untracked and ignored files
included (built with an index of its own), whose parent is the branch tip, so
a thread that moves to another worker finds its build output too.

A session is a commit of the thread's prime-agent session directory (its
conversation, compaction and local harness state; harness.py), child of the
thread's previous session commit, so the thread's agent picks up where it
was on any worker.

At the end of every turn the runner commits whatever the thread left and
makes a snapshot; the orchestrator takes both (peer call `save`) and lands
the branch, unless a model call failed in the turn: the orchestrator rebases
it onto the default branch in the hub and brings both back, and the worktree
follows the rebased branch. A rebase that conflicts goes back to the thread
to resolve against `origin/<base>`.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path

AUTHOR = ("orchestrate", "threads@orchestrate.invalid")
# The refs the orchestrator may set on a worker.
SHIPPED = re.compile(r"^refs/remotes/(origin|origin-snapshots|origin-sessions)/[\w./-]+$")
THREAD_ID = re.compile(r"^[\w-]+$")
PROJECT_ID = re.compile(r"^[\w-]+$")
# What a snapshot leaves out: pixi's environments, in the subdirectories of any
# `.pixi/` (its config.toml, beside them, stays).
PIXI_ENVIRONMENTS = ":(exclude,glob)**/.pixi/*/**"


class GitError(RuntimeError):
    pass


@dataclass
class Git:
    code: int
    out: str
    err: str
    raw: bytes = b""


def snapshot_ref(tid: str) -> str:
    return f"refs/snapshots/{tid}"


def session_ref(tid: str) -> str:
    return f"refs/sessions/{tid}"


class Repo:
    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        self.env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
        self.base = ""
        self.opened = False
        self.opening = asyncio.Lock()  # requests come in at once; one makes the repository
        # Refs are shared by every thread on the worker: changes to them take turns.
        self.lock = asyncio.Lock()

    async def git(self, *args: str, cwd: str | os.PathLike | None = None, check: bool = True,
                  env: dict | None = None, input: bytes | None = None) -> Git:
        proc = await asyncio.create_subprocess_exec(
            "git", *args, cwd=str(cwd or self.path), env={**self.env, **(env or {})},
            stdin=asyncio.subprocess.PIPE if input is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await proc.communicate(input)
        r = Git(proc.returncode or 0, out.decode(errors="replace").strip(), err.decode(errors="replace").strip(), out)
        if check and r.code:
            raise GitError(f"git {' '.join(args)} failed: {r.err or r.out}")
        return r

    async def open(self) -> None:
        """Make the repository on first use; learn the base branch the
        orchestrator named, if it sent one yet. Opening one that is there
        only reads it, so asking a worker what it has (GET /repo) writes
        nothing and answers at once even while its disk is slow to write."""
        async with self.opening:
            if self.opened:
                return
            if not (self.path / ".git").exists():
                await self.make()
            self.base = (await self.git("config", "--get", "orchestrate.base", check=False)).out
            self.opened = True

    async def make(self) -> None:
        """A new repository, set up as every worker's is."""
        self.path.mkdir(parents=True, exist_ok=True)
        await self.git("init", "-q")
        # Shared by every worktree, so threads' own commits work too and
        # `git rebase --continue` never waits on an editor.
        for key, value in (("user.name", AUTHOR[0]), ("user.email", AUTHOR[1]), ("core.editor", "true"),
                           ("gc.auto", "0")):
            await self.git("config", key, value)

    async def exists(self, ref: str) -> bool:
        return (await self.git("rev-parse", "-q", "--verify", f"{ref}^{{commit}}", check=False)).code == 0

    async def refs(self) -> dict[str, str]:
        """Every ref, by name: what the orchestrator may leave out of the next
        bundle; none while the worker has no repository for the project (the
        first bundle makes it)."""
        if not (self.path / ".git").exists():
            return {}
        await self.open()
        out = (await self.git("for-each-ref", "--format=%(refname) %(objectname)")).out
        return dict(line.split(" ", 1) for line in out.splitlines() if line)

    async def unbundle(self, data: bytes) -> None:
        with tempfile.NamedTemporaryFile(suffix=".bundle", delete=False) as f:
            f.write(data)
        try:
            await self.git("bundle", "unbundle", f.name)
        finally:
            os.unlink(f.name)

    async def receive(self, data: bytes, base: str, refs: dict[str, str]) -> None:
        """Take the hub's commits (a bundle, empty when the worker has them
        all) and point the remote-tracking refs at them."""
        bad = [r for r in refs if not SHIPPED.match(r)]
        if bad:
            raise GitError(f"refs the orchestrator may not set: {', '.join(bad)}")
        await self.open()
        async with self.lock:
            if data:
                await self.unbundle(data)
            for ref, sha in refs.items():
                await self.git("update-ref", ref, sha)
            if base != self.base:
                await self.git("config", "orchestrate.base", base)
                self.base = base

    async def send(self, tid: str, have: list[str]) -> tuple[bytes, dict[str, str]]:
        """A thread's branch, snapshot and session as a bundle without the commits in
        `have` (the hub's), and the refs' tips. The bundle is empty when the
        hub has everything; the tips travel apart from it, since a bundle
        leaves out a ref whose tip is in `have`."""
        await self.open()
        names = [f"refs/heads/thread/{tid}", snapshot_ref(tid), session_ref(tid)]
        refs = {}
        for name in names:
            r = await self.git("rev-parse", "-q", "--verify", name, check=False)
            if r.code == 0:
                refs[name] = r.out
        if not refs:
            raise GitError(f"thread {tid} has no branch on this worker")
        known = [h for h in have if await self.exists(h)]
        return await self.bundle(list(refs), known), refs

    async def bundle(self, refs: list[str], have: list[str]) -> bytes:
        nots = ["--not", *have] if have else []
        if (await self.git("rev-list", "--count", *refs, *nots)).out == "0":
            return b""
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "out.bundle")
            await self.git("bundle", "create", "-q", path, *refs, *nots)
            return Path(path).read_bytes()

    async def seed(self, source: str) -> None:
        """Start from another repository's branches, with no orchestrator (the
        CLI): its branches become `origin/*` and its HEAD the base, which is a
        pixi project as every project is."""
        await self.open()
        head = (await self.git("ls-remote", "--symref", source, "HEAD")).out.splitlines()
        base = next((line.split()[1].removeprefix("refs/heads/") for line in head if line.startswith("ref:")), "")
        if not base:
            raise GitError(f"{source} has no default branch")
        await self.git("fetch", "-q", source, "+refs/heads/*:refs/remotes/origin/*")
        if not await self.is_pixi(f"refs/remotes/origin/{base}"):
            raise GitError(f"{base} of {source} is not a pixi project (no pixi.toml, nor a pyproject.toml "
                           "with [tool.pixi]); every project is one: make it one with `pixi init`")
        await self.git("config", "orchestrate.base", base)
        self.base = base

    async def is_pixi(self, commit: str) -> bool:
        """Whether `commit` is a pixi project: pixi.toml at its root, or a
        pyproject.toml with a [tool.pixi] table (the orchestrator's hub.py asks the same)."""
        if (await self.git("cat-file", "-e", f"{commit}:pixi.toml", check=False)).code == 0:
            return True
        pyproject = await self.git("show", f"{commit}:pyproject.toml", check=False)
        if pyproject.code:
            return False
        try:
            return "pixi" in tomllib.loads(pyproject.out).get("tool", {})
        except tomllib.TOMLDecodeError:
            return False

    def _session_env(self, tid: str, directory: Path) -> dict:
        index = self.path / ".git" / "sessions" / f"{tid}.index"
        index.parent.mkdir(parents=True, exist_ok=True)
        return {"GIT_DIR": str(self.path / ".git"), "GIT_WORK_TREE": str(directory), "GIT_INDEX_FILE": str(index)}

    async def save_session(self, tid: str, directory: str | os.PathLike, parts: tuple[str, ...]) -> str:
        """Commit the `parts` of the thread's agent directory (its session) at
        `refs/sessions/<id>`, on top of its previous session commit."""
        directory = Path(directory)
        env = self._session_env(tid, directory)
        await self.git("read-tree", "--empty", cwd=directory, env=env)
        present = [p for p in parts if (directory / p).exists()]
        if present:
            await self.git("add", "-A", "-f", "--", *present, cwd=directory, env=env)
        tree = (await self.git("write-tree", cwd=directory, env=env)).out
        parent = ["-p", session_ref(tid)] if await self.exists(session_ref(tid)) else []
        commit = (await self.git("commit-tree", tree, *parent, "-m", f"Session of thread/{tid}",
                                 cwd=directory, env=env)).out
        await self.git("update-ref", session_ref(tid), commit)
        return commit

    async def session_commit(self, tid: str) -> str:
        """The thread's session commit on this worker, "" while it has none."""
        r = await self.git("rev-parse", "-q", "--verify", session_ref(tid), check=False)
        return r.out if r.code == 0 else ""

    async def session(self, tid: str, directory: str | os.PathLike, parts: tuple[str, ...]) -> None:
        """Make the thread's agent directory hold its latest session: the one
        the worker has, while that holds everything the hub has of it, else
        the hub's (the thread went on on another worker since)."""
        directory, theirs = Path(directory), f"refs/remotes/origin-sessions/{tid}"
        await self.open()
        async with self.lock:
            if not await self.exists(theirs):
                return
            if await self.exists(session_ref(tid)) and (await self.git(
                    "merge-base", "--is-ancestor", theirs, session_ref(tid), check=False)).code == 0:
                return
            for part in parts:
                shutil.rmtree(directory / part, ignore_errors=True)
            directory.mkdir(parents=True, exist_ok=True)
            env = self._session_env(tid, directory)
            await self.git("read-tree", theirs, cwd=directory, env=env)
            await self.git("checkout-index", "-a", "-f", cwd=directory, env=env)
            await self.git("update-ref", session_ref(tid), theirs)

    async def in_hub(self, tid: str) -> float | None:
        """When the hub has everything this worker has of the thread (its
        branch, snapshot and session are what the orchestrator last brought
        back), the time of its snapshot; else None: a turn whose save failed
        left work only here."""
        theirs = {f"refs/heads/thread/{tid}": f"refs/remotes/origin/thread/{tid}",
                  snapshot_ref(tid): f"refs/remotes/origin-snapshots/{tid}",
                  session_ref(tid): f"refs/remotes/origin-sessions/{tid}"}
        for ours, hub in theirs.items():
            mine = await self.git("rev-parse", "-q", "--verify", ours, check=False)
            if mine.code:
                continue
            if (await self.git("merge-base", "--is-ancestor", ours, hub, check=False)).code:
                return None
        when = await self.git("log", "-1", "--format=%ct", snapshot_ref(tid), check=False)
        return float(when.out) if when.code == 0 and when.out else 0.0

    async def drop(self, path: str | os.PathLike, tid: str) -> None:
        """Take a thread's worktree off this worker, and its session with it
        (the agent directory goes with the thread's HOME): the next time the
        thread starts here, both are made again from the hub's. The caller
        holds the lock."""
        await self.git("worktree", "remove", "--force", str(path))
        await self.git("update-ref", "-d", session_ref(tid), check=False)

    async def worktree(self, path: str | os.PathLike, tid: str) -> "Workspace":
        """The thread's worktree at `path` on `thread/<tid>`. One this worker
        already has is kept as it is, with its uncommitted and ignored files,
        while it holds everything the hub has of the branch. One that does not
        is stale (the thread went on on another worker since) and is made
        again, from the hub's branch and with its snapshot's files when the
        hub has them, else from the default branch."""
        await self.open()
        if not self.base:
            raise GitError("the orchestrator has not sent the project's repository to this worker")
        path, branch = Path(path), f"thread/{tid}"
        theirs, snapshot = f"refs/remotes/origin/thread/{tid}", f"refs/remotes/origin-snapshots/{tid}"
        async with self.lock:
            await self.git("worktree", "prune")
            known = await self.exists(theirs)
            listed = (await self.git("worktree", "list", "--porcelain")).out
            if f"worktree {path.resolve()}" in listed.splitlines():
                if not known or (await self.git("merge-base", "--is-ancestor", theirs, "HEAD",
                                                 cwd=path, check=False)).code == 0:
                    return Workspace(self, path, branch, tid)
                await self.git("worktree", "remove", "--force", str(path))
            if path.exists():  # a leftover directory git no longer knows as a worktree
                await asyncio.to_thread(shutil.rmtree, path)  # may be large: not on the event loop
            await self.git("worktree", "add", "-q", "-B", branch, str(path),
                           theirs if known else f"refs/remotes/origin/{self.base}")
            ws = Workspace(self, path, branch, tid)
            if known and await self.exists(snapshot):
                await ws.restore(snapshot)
        return ws


@dataclass
class Workspace:
    repo: Repo
    path: Path
    branch: str
    tid: str

    async def git(self, *args: str, check: bool = True, env: dict | None = None) -> Git:
        return await self.repo.git(*args, cwd=self.path, check=check, env=env)

    async def rebasing(self) -> bool:
        r = await self.git("rev-parse", "--git-path", "rebase-merge")
        apply = await self.git("rev-parse", "--git-path", "rebase-apply")
        return any((self.path / p).exists() for p in (r.out, apply.out))

    async def commit(self, message: str) -> str:
        """Commit everything left in the worktree; the branch tip."""
        if await self.rebasing():
            raise GitError(f"a rebase is in progress in {self.branch}; finish it "
                           "(git rebase --continue) or abort it before the work can be saved")
        await self.git("add", "-A")
        if (await self.git("diff", "--cached", "--quiet", check=False)).code:
            await self.git("commit", "-q", "-m", message)
        return (await self.git("rev-parse", "HEAD")).out

    async def changed(self) -> list[str]:
        """The files the branch changes from the base."""
        return (await self.git("diff", "--name-only", f"refs/remotes/origin/{self.repo.base}...HEAD")).out.splitlines()

    async def index(self, name: str) -> dict:
        p = Path((await self.git("rev-parse", "--git-path", name)).out)
        return {"GIT_INDEX_FILE": str(p if p.is_absolute() else self.path / p)}

    async def snapshot(self) -> str:
        """Commit the whole worktree, ignored files included, as a child of the
        branch tip at `refs/snapshots/<id>`, but for pixi's environments (what
        is in a `.pixi/` directory's subdirectories), which pixi.lock makes
        again. Its index lives on with the worktree, so only changed files are
        hashed again."""
        env = await self.index("snapshot.index")
        await self.git("add", "-A", "-f", "--", ".", PIXI_ENVIRONMENTS, env=env)
        tree = (await self.git("write-tree", env=env)).out
        commit = (await self.git("commit-tree", tree, "-p", "HEAD", "-m", f"Snapshot of {self.branch}")).out
        await self.git("update-ref", snapshot_ref(self.tid), commit)
        return commit

    async def restore(self, snapshot: str) -> None:
        """Write a snapshot's files into the worktree, then put the tracked
        ones back as the branch has them (the snapshot may predate a rebase)."""
        env = await self.index("restore.index")
        await self.git("read-tree", snapshot, env=env)
        await self.git("checkout-index", "-a", "-f", env=env)
        os.unlink(env["GIT_INDEX_FILE"])
        await self.git("reset", "-q", "--hard", "HEAD")

    async def follow(self) -> None:
        """Move the branch to the hub's (rebased onto the base when it landed)."""
        await self.git("reset", "-q", "--keep", f"refs/remotes/origin/{self.branch}")

    async def land_here(self) -> dict:
        """Land on the worker's own `origin/<base>`, for a runner with no
        orchestrator (the CLI, tests): the same answer the orchestrator's
        `save` gives."""
        base = self.repo.base
        async with self.repo.lock:
            rebase = await self.git("rebase", "-q", f"refs/remotes/origin/{base}", check=False)
            if rebase.code:
                files = (await self.git("diff", "--name-only", "--diff-filter=U")).out.split()
                await self.git("rebase", "--abort")
                return {"landed": False, "base": base, "conflict": files, "message": rebase.err or rebase.out}
            head = (await self.git("rev-parse", "HEAD")).out
            await self.git("update-ref", f"refs/remotes/origin/{base}", head)
            await self.git("update-ref", f"refs/remotes/origin/{self.branch}", head)
        return {"landed": True, "base": base, "commit": head}


class Repos:
    """The worker's repositories, one per project, made on first use."""

    def __init__(self, root: str | os.PathLike):
        self.root = Path(root)
        self.repos: dict[str, Repo] = {}

    def get(self, project: str) -> Repo:
        if not PROJECT_ID.match(project or ""):
            raise GitError(f"bad project id {project!r}")
        repo = self.repos.get(project)
        if repo is None:
            repo = self.repos[project] = Repo(self.root / project)
        return repo

    def projects(self) -> list[str]:
        """Every project this worker has a repository for, made by this
        runner process or an earlier one."""
        on_disk = {p.name for p in self.root.iterdir() if (p / ".git").exists()} if self.root.exists() else set()
        return sorted(on_disk | set(self.repos))

    async def worktrees(self, project: str) -> list[Path]:
        """The thread worktrees a project's repository has."""
        repo = self.get(project)
        if not (repo.path / ".git").exists():
            return []
        out = (await repo.git("worktree", "list", "--porcelain")).out
        main = repo.path.resolve()
        return [Path(line.removeprefix("worktree ")) for line in out.splitlines()
                if line.startswith("worktree ") and Path(line.removeprefix("worktree ")) != main]

    def remove(self, project: str) -> None:
        """Delete a project's repository from this worker."""
        repo = self.repos.pop(project, None)
        shutil.rmtree(repo.path if repo else self.root / project, ignore_errors=True)


def refs_header(refs: dict[str, str]) -> str:
    return json.dumps(refs, separators=(",", ":"))
