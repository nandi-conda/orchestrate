"""One local git repository per project: the hub its workers' commits go to.

A project's hub is a bare repository, `<repos>/<project id>.git`, on the
machine the control plane runs on. It holds:

- `refs/heads/<base>`: the project's main branch. With a GitHub repository
  (the project's `repo_url`) it mirrors that repository's default branch;
  without one it starts at an empty commit, "Start the project".
- `refs/heads/thread/<id>`: each thread's branch, as of its last turn.
- `refs/snapshots/<id>`: each thread's whole worktree as of its last turn,
  untracked and ignored files included, as a child of its branch tip. A
  thread brought back on another worker continues with these files.

Workers never reach the hub, nor GitHub. The control plane moves commits as
git bundles over the runner API, each way leaving out what the other side has
(runner/API.md, "Repository"): it sends the base before a thread starts and
the thread's branch and snapshot before one comes back; it takes both at the
end of every turn. A finished thread lands here: its branch is rebased onto
the base in a worktree of the hub, pushed to GitHub first when the project has
a repository there, and only then becomes the hub's base. Thread branches and
snapshots never leave the machine.
"""

from __future__ import annotations

import asyncio
import base64
import os
import shutil
import tempfile
from pathlib import Path

from . import gitio
from .gitio import AUTHOR, GitError, git

LAND_ATTEMPTS = 5  # pushes rejected because the remote moved before giving up


def worker_ref(ref: str, base: str) -> str:
    """Where a hub ref goes on a worker: under its remote-tracking refs."""
    if ref == f"refs/heads/{base}" or ref.startswith("refs/heads/thread/"):
        return "refs/remotes/origin/" + ref.removeprefix("refs/heads/")
    if ref.startswith("refs/snapshots/"):
        return "refs/remotes/origin-snapshots/" + ref.removeprefix("refs/snapshots/")
    raise ValueError(f"{ref} does not go to workers")


class Hub:
    def __init__(self, path: str | os.PathLike, repo_url: str = "", token: str | None = None) -> None:
        self.path = Path(path)
        self.url = repo_url
        self.env: dict = {}
        if token and repo_url.startswith(("http://", "https://")):
            basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
            self.env = {"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": f"http.{repo_url}.extraHeader",
                        "GIT_CONFIG_VALUE_0": f"Authorization: Basic {basic}"}
        self.base = ""
        # Ref changes take turns: a landing, a sync, a turn's work coming in.
        self.lock = asyncio.Lock()

    async def git(self, *args: str, check: bool = True) -> gitio.Git:
        return await git(self.path, *args, check=check, env=self.env)

    async def open(self) -> None:
        """Make the hub on first use: from the project's repository when it has
        one (its default branch, and the `thread/*` branches workers pushed
        there before hubs existed), else from an empty first commit."""
        if (self.path / "HEAD").exists():
            self.base = (await self.git("config", "--get", "orchestrate.base")).out
            return
        new = self.path.with_name(self.path.name + ".new")
        if new.exists():
            shutil.rmtree(new)
        new.parent.mkdir(parents=True, exist_ok=True)
        await git(new.parent, "init", "-q", "--bare", str(new))
        for key, value in (("user.name", AUTHOR[0]), ("user.email", AUTHOR[1]), ("gc.auto", "0")):
            await git(new, "config", key, value)
        if self.url:
            base = await self.remote_base(new)
            await git(new, "fetch", "-q", self.url, f"+refs/heads/{base}:refs/heads/{base}",
                      "+refs/heads/thread/*:refs/heads/thread/*", env=self.env)
        else:
            base = "main"
            with tempfile.TemporaryDirectory() as d:
                empty = Path(d) / "empty"
                empty.write_bytes(b"")
                tree = (await git(new, "hash-object", "-t", "tree", "-w", str(empty))).out
            commit = (await git(new, "commit-tree", tree, "-m", "Start the project")).out
            await git(new, "update-ref", f"refs/heads/{base}", commit)
        await git(new, "symbolic-ref", "HEAD", f"refs/heads/{base}")
        await git(new, "config", "orchestrate.base", base)
        new.rename(self.path)
        self.base = base

    async def remote_base(self, cwd: Path) -> str:
        head = (await git(cwd, "ls-remote", "--symref", self.url, "HEAD", env=self.env)).out.splitlines()
        for line in head:
            if line.startswith("ref: refs/heads/"):
                return line.split()[1].removeprefix("refs/heads/")
        raise GitError(f"{self.url} has no default branch yet; push a first commit to it")

    async def sync(self) -> None:
        """Bring the base up to the project's repository, which may have moved
        on without the hub (someone pushed)."""
        if not self.url:
            return
        async with self.lock:
            await self.git("fetch", "-q", self.url, f"+refs/heads/{self.base}:refs/heads/{self.base}")

    async def have(self, tid: str) -> list[str]:
        """The hub's tips a worker can leave out of a thread's bundle."""
        refs = await gitio.tips(self.path, [f"refs/heads/{self.base}", f"refs/heads/thread/{tid}",
                                            f"refs/snapshots/{tid}"])
        return list(refs.values())

    async def receive(self, tid: str, data: bytes, refs: dict[str, str]) -> None:
        """A turn's work from a worker: the thread's branch and snapshot."""
        allowed = {f"refs/heads/thread/{tid}", f"refs/snapshots/{tid}"}
        bad = set(refs) - allowed
        if bad:
            raise GitError(f"a worker may not set {', '.join(sorted(bad))}")
        async with self.lock:
            await gitio.unbundle(self.path, data)
            for ref, sha in refs.items():
                await self.git("update-ref", ref, sha)

    async def pack(self, tid: str | None, have: list[str]) -> tuple[bytes, dict[str, str]]:
        """The base, and a thread's branch and snapshot when `tid` is given, as a
        bundle without what the worker has (`have`), and the tips as the
        worker names them."""
        names = [f"refs/heads/{self.base}"]
        if tid:
            names += [f"refs/heads/thread/{tid}", f"refs/snapshots/{tid}"]
        async with self.lock:
            refs = await gitio.tips(self.path, names)
            data = await gitio.bundle(self.path, list(refs), have)
        return data, {worker_ref(r, self.base): sha for r, sha in refs.items()}

    async def land(self, tid: str) -> dict:
        """Rebase the thread's branch onto the base and make it the base: on
        the project's repository first when there is one, then here. Answers
        as the runner's `save` peer call does."""
        base, branch = self.base, f"refs/heads/thread/{tid}"
        wt = self.path.with_name(self.path.stem + ".landing") / tid
        async with self.lock:
            for _ in range(LAND_ATTEMPTS):
                if self.url:
                    await self.git("fetch", "-q", self.url, f"+refs/heads/{base}:refs/heads/{base}")
                await self.git("worktree", "prune")
                if wt.exists():
                    shutil.rmtree(wt)
                wt.parent.mkdir(parents=True, exist_ok=True)
                await self.git("worktree", "add", "-q", "--detach", str(wt), branch)
                try:
                    rebase = await git(wt, "rebase", "-q", f"refs/heads/{base}", check=False)
                    if rebase.code:
                        files = (await git(wt, "diff", "--name-only", "--diff-filter=U")).out.split()
                        await git(wt, "rebase", "--abort")
                        return {"landed": False, "base": base, "conflict": files,
                                "message": rebase.err or rebase.out}
                    new = (await git(wt, "rev-parse", "HEAD")).out
                finally:
                    await self.git("worktree", "remove", "--force", str(wt))
                if self.url:
                    push = await self.git("push", "-q", self.url, f"{new}:refs/heads/{base}", check=False)
                    if push.code:
                        if any(s in push.err for s in ("fetch first", "non-fast-forward")):
                            continue  # the repository moved since the fetch; rebase onto its new tip
                        raise GitError(f"pushing to {base} of {self.url} failed: {push.err or push.out}")
                await self.git("update-ref", f"refs/heads/{base}", new)
                await self.git("update-ref", branch, new)
                return {"landed": True, "base": base, "commit": new}
        return {"landed": False, "base": base,
                "message": f"{base} kept moving; {LAND_ATTEMPTS} pushes were rejected"}
