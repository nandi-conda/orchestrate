"""The project's git repository on a worker, and how threads' commits reach it.

One clone per worker (`<workdir>/repo`); every thread gets a worktree of it on
its own branch `thread/<id>`. A new thread's branch starts from the remote's
default branch; a thread brought back on a worker (after its old worker went
away, or after it stopped) keeps the worktree that worker still has for it
when that holds all it pushed, and otherwise continues from its pushed
`thread/<id>`. The
clone's object store and refs are shared by all threads on the worker, while
each worktree has its own files, index and HEAD.

Commits leave the worker on one path:

- At the end of every turn the runner commits whatever the thread left
  uncommitted and pushes `thread/<id>` to origin, so a deleted worker loses
  nothing.
- When a turn ends with `finish`, the thread lands: fetch, rebase onto
  `origin/<default>`, fast-forward push to the default branch. A push rejected
  because another thread landed first goes round again. A rebase that
  conflicts is aborted and handed back to the thread to resolve.

Authentication: with a token, git sends `Authorization: Basic
base64("x-access-token:<token>")` to the repo URL through
`http.<url>.extraHeader`, set only on the runner's own git commands (never in
the clone's config or the threads' shell environment).
"""

from __future__ import annotations

import asyncio
import base64
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

AUTHOR = ("orchestrate", "threads@orchestrate.invalid")
LAND_ATTEMPTS = 5  # pushes rejected by a concurrent landing before giving up


class GitError(RuntimeError):
    pass


@dataclass
class Git:
    code: int
    out: str
    err: str


@dataclass
class Landing:
    """The outcome of `Workspace.land`. `conflict` lists the files a rebase onto
    the default branch could not merge; `commit` is the default branch's new tip."""
    ok: bool
    base: str
    commit: str = ""
    conflict: list[str] | None = None
    message: str = ""


class Repo:
    def __init__(self, url: str, path: str | os.PathLike, token: str | None = None):
        self.url, self.path = url, Path(path)
        self.env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
        if token:
            basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
            self.env.update({"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": f"http.{url}.extraHeader",
                             "GIT_CONFIG_VALUE_0": f"Authorization: Basic {basic}"})
        self.base = ""
        # Fetches and pushes from threads on this worker share the clone's refs.
        self.lock = asyncio.Lock()

    async def git(self, *args: str, cwd: str | os.PathLike | None = None, check: bool = True) -> Git:
        proc = await asyncio.create_subprocess_exec(
            "git", *args, cwd=str(cwd or self.path), env=self.env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await proc.communicate()
        r = Git(proc.returncode or 0, out.decode().strip(), err.decode().strip())
        if check and r.code:
            raise GitError(f"git {' '.join(args)} failed: {r.err or r.out}")
        return r

    async def open(self) -> None:
        """Clone on first use, fetch after; learn the remote's default branch."""
        async with self.lock:
            if (self.path / ".git").exists():
                await self.git("fetch", "-q", "--prune", "origin")
            else:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                await self.git("clone", "-q", self.url, str(self.path), cwd=self.path.parent)
            # Shared by every worktree, so threads' own commits work too and
            # `git rebase --continue` never waits on an editor.
            for key, value in (("user.name", AUTHOR[0]), ("user.email", AUTHOR[1]), ("core.editor", "true")):
                await self.git("config", key, value)
            head = await self.git("symbolic-ref", "--short", "refs/remotes/origin/HEAD")
            self.base = head.out.removeprefix("origin/")

    async def worktree(self, path: str | os.PathLike, branch: str) -> "Workspace":
        """The thread's worktree at `path` on `branch`. One this worker already
        has is kept as it is, with its uncommitted and ignored files, while it
        holds everything the thread pushed. One that does not is stale (the
        thread went on on another worker since) and is made again. A worktree
        is made from origin's `branch` when the thread pushed one, else from
        origin's default branch."""
        if not self.base:
            await self.open()
        path = Path(path)
        async with self.lock:
            await self.git("worktree", "prune")
            pushed = bool((await self.git("ls-remote", "--heads", "origin", branch)).out)
            start = branch if pushed else self.base
            await self.git("fetch", "-q", "origin", f"+refs/heads/{start}:refs/remotes/origin/{start}")
            listed = (await self.git("worktree", "list", "--porcelain")).out
            if f"worktree {path.resolve()}" in listed.splitlines():
                if not pushed or (await self.git("merge-base", "--is-ancestor", f"origin/{branch}", "HEAD",
                                                 cwd=path, check=False)).code == 0:
                    return Workspace(self, path, branch)
                await self.git("worktree", "remove", "--force", str(path))
            if path.exists():  # a leftover directory git no longer knows as a worktree
                shutil.rmtree(path)
            await self.git("worktree", "add", "-q", "-B", branch, str(path), f"origin/{start}")
        return Workspace(self, path, branch)


@dataclass
class Workspace:
    repo: Repo
    path: Path
    branch: str

    async def git(self, *args: str, check: bool = True) -> Git:
        return await self.repo.git(*args, cwd=self.path, check=check)

    async def rebasing(self) -> bool:
        r = await self.git("rev-parse", "--git-path", "rebase-merge")
        apply = await self.git("rev-parse", "--git-path", "rebase-apply")
        return any((self.path / p).exists() for p in (r.out, apply.out))

    async def save(self, message: str) -> str:
        """Commit everything left in the worktree and push the thread's branch.
        Returns the branch tip."""
        if await self.rebasing():
            raise GitError(f"a rebase is in progress in {self.branch}; finish it "
                           "(git rebase --continue) or abort it before the work can be saved")
        await self.git("add", "-A")
        if (await self.git("diff", "--cached", "--quiet", check=False)).code:
            await self.git("commit", "-q", "-m", message)
        async with self.repo.lock:
            # The branch is the thread's own; a rebase while landing rewrites it.
            await self.git("push", "-q", "--force", "origin", f"HEAD:refs/heads/{self.branch}")
        return (await self.git("rev-parse", "HEAD")).out

    async def land(self, message: str) -> Landing:
        """Put this thread's commits on the default branch: rebase, fast-forward push."""
        base = self.repo.base
        await self.save(message)
        for _ in range(LAND_ATTEMPTS):
            async with self.repo.lock:
                await self.git("fetch", "-q", "origin", base)
            rebase = await self.git("rebase", "-q", f"origin/{base}", check=False)
            if rebase.code:
                files = (await self.git("diff", "--name-only", "--diff-filter=U")).out.split()
                await self.git("rebase", "--abort")
                return Landing(False, base, conflict=files, message=rebase.err or rebase.out)
            async with self.repo.lock:
                push = await self.git("push", "-q", "origin", f"HEAD:refs/heads/{base}", check=False)
                if push.code == 0:
                    await self.git("push", "-q", "--force", "origin", f"HEAD:refs/heads/{self.branch}")
                    return Landing(True, base, commit=(await self.git("rev-parse", "HEAD")).out)
            if not any(s in push.err for s in ("fetch first", "non-fast-forward")):
                raise GitError(f"pushing to {base} failed: {push.err or push.out}")
            # Another thread landed between our fetch and push. Go again on its tip.
        return Landing(False, base, message=f"{base} kept moving; {LAND_ATTEMPTS} pushes were rejected")
