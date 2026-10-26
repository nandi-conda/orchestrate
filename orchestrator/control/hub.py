"""One local git repository per project: the hub its workers' commits go to.

A project's hub is a bare repository, `<repos>/<project id>.git`, on the
machine the control plane runs on. Every project starts with one, from an
empty commit ("Start the project"). It holds:

- `refs/heads/<base>`: the project's main branch.
- `refs/heads/thread/<id>`: each thread's branch, as of its last turn.
- `refs/snapshots/<id>`: each thread's whole worktree as of its last turn,
  untracked and ignored files included, as a child of its branch tip. A
  thread brought back on another worker continues with these files.
- `refs/sessions/<id>`: each thread's agent session as of its last turn
  (prime-agent's conversation and state; runner/src/runner/harness.py). A
  thread brought back on another worker continues its conversation from it.

A project may have a remote (its `repo_url`, e.g. a GitHub repository),
attached, changed or removed at any time from the project's settings
(`Hub.connect`). With one, the base mirrors the remote's default branch: it is
fetched before workers get it and before every landing, and a landing is
pushed there first. Without one, the hub is the only copy.

Workers never reach the hub, nor the remote. The control plane moves commits
as git bundles over the runner API, each way leaving out what the other side
has (runner/API.md, "Repository"): it sends the base before a thread starts
and the thread's branch, snapshot and session before one comes back; it takes
all three at the end of every turn. A finished thread lands here: its branch
is rebased onto the base in a worktree of the hub, pushed to the remote first
when there is one, and only then becomes the hub's base. Thread branches,
snapshots and sessions never leave the machine.
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
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def worker_ref(ref: str, base: str) -> str:
    """Where a hub ref goes on a worker: under its remote-tracking refs."""
    if ref == f"refs/heads/{base}" or ref.startswith("refs/heads/thread/"):
        return "refs/remotes/origin/" + ref.removeprefix("refs/heads/")
    if ref.startswith("refs/snapshots/"):
        return "refs/remotes/origin-snapshots/" + ref.removeprefix("refs/snapshots/")
    if ref.startswith("refs/sessions/"):
        return "refs/remotes/origin-sessions/" + ref.removeprefix("refs/sessions/")
    raise ValueError(f"{ref} does not go to workers")


def thread_refs(tid: str) -> list[str]:
    """A thread's refs in the hub; a worker sends the ones it has."""
    return [f"refs/heads/thread/{tid}", f"refs/snapshots/{tid}", f"refs/sessions/{tid}"]


class Hub:
    def __init__(self, path: str | os.PathLike, token: str | None = None) -> None:
        self.path = Path(path).resolve()  # git runs in other directories
        self.token = token  # GITHUB_TOKEN, sent to http(s) remotes
        self.url = ""
        self.env: dict = {}
        self.base = ""
        # Ref changes take turns: a landing, a sync, a turn's work coming in.
        self.lock = asyncio.Lock()

    async def git(self, *args: str, check: bool = True) -> gitio.Git:
        return await git(self.path, *args, check=check, env=self.env)

    def auth(self, url: str) -> dict:
        """git's environment for talking to `url`: the token as a header, for http(s)."""
        if not (self.token and url.startswith(("http://", "https://"))):
            return {}
        basic = base64.b64encode(f"x-access-token:{self.token}".encode()).decode()
        return {"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": f"http.{url}.extraHeader",
                "GIT_CONFIG_VALUE_0": f"Authorization: Basic {basic}"}

    async def open(self) -> None:
        """Make the hub on first use, from an empty first commit; else learn
        its base and the remote it is connected to."""
        if (self.path / "HEAD").exists():
            self.base = (await self.git("config", "--get", "orchestrate.base")).out
            self.url = (await self.git("config", "--get", "orchestrate.remote", check=False)).out
            self.env = self.auth(self.url)
            return
        new = self.path.with_name(self.path.name + ".new")
        if new.exists():
            shutil.rmtree(new)
        new.parent.mkdir(parents=True, exist_ok=True)
        await git(new.parent, "init", "-q", "--bare", str(new))
        for key, value in (("user.name", AUTHOR[0]), ("user.email", AUTHOR[1]), ("gc.auto", "0")):
            await git(new, "config", key, value)
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

    async def connect(self, url: str) -> None:
        """Make `url` the project's remote, or leave it without one ("").

        The base and the remote's default branch become one line of history:
        - a remote without commits gets the base, under the base's name;
        - a remote ahead of the base (or a base that is still only "Start the
          project") gives the base its commits;
        - a base ahead of the remote is pushed there, as a landing would be;
        - histories that have each gone their own way are refused, and the
          project keeps the remote it had.
        The base takes the name of the remote's default branch. Removing the
        remote keeps everything here; landings stay on this machine from then on."""
        async with self.lock:
            if url:
                env = self.auth(url)
                await self._join(url, env)
                await self.git("config", "orchestrate.remote", url)
            else:
                env = {}
                await self.git("config", "--unset-all", "orchestrate.remote", check=False)
            self.url, self.env = url, env

    async def _join(self, url: str, env: dict) -> None:
        ours = (await self.git("rev-parse", f"refs/heads/{self.base}")).out
        name = await self.remote_base(url, env)
        if name is None:
            await git(self.path, "push", "-q", url, f"{ours}:refs/heads/{self.base}", env=env)
            return
        incoming = "refs/orchestrate/connecting"
        await git(self.path, "fetch", "-q", url, f"+refs/heads/{name}:{incoming}", env=env)
        try:
            theirs = (await self.git("rev-parse", incoming)).out
            if ours == theirs or await self._ancestor(theirs, ours):
                tip = ours
                if ours != theirs:
                    await git(self.path, "push", "-q", url, f"{ours}:refs/heads/{name}", env=env)
            elif await self._ancestor(ours, theirs) or await self._fresh(ours):
                tip = theirs
            else:
                raise GitError(f"{name} of {url} and this project's {self.base} have each gone their "
                               "own way, so neither can follow the other; connect a repository that "
                               f"is empty or shares {self.base}'s history")
        finally:
            await self.git("update-ref", "-d", incoming)
        await self.git("update-ref", f"refs/heads/{name}", tip)
        if name != self.base:
            await self.git("symbolic-ref", "HEAD", f"refs/heads/{name}")
            await self.git("config", "orchestrate.base", name)
            await self.git("update-ref", "-d", f"refs/heads/{self.base}")
            self.base = name

    async def _ancestor(self, a: str, b: str) -> bool:
        return (await self.git("merge-base", "--is-ancestor", a, b, check=False)).code == 0

    async def _fresh(self, commit: str) -> bool:
        """Whether `commit` is the hub's first, empty commit: nothing landed yet."""
        info = (await self.git("log", "-1", "--format=%T %P", commit)).out.split()
        return info == [EMPTY_TREE]

    async def remote_base(self, url: str, env: dict) -> str | None:
        """The default branch of `url`, or None when it has no commits yet."""
        head = (await git(self.path, "ls-remote", "--symref", url, "HEAD", env=env)).out.splitlines()
        for line in head:
            if line.startswith("ref: refs/heads/"):
                return line.split()[1].removeprefix("refs/heads/")
        if (await git(self.path, "ls-remote", "--heads", url, env=env)).out:
            raise GitError(f"{url} has branches but no default branch")
        return None

    async def sync(self) -> None:
        """Bring the base up to the project's repository, which may have moved
        on without the hub (someone pushed)."""
        if not self.url:
            return
        async with self.lock:
            await self.git("fetch", "-q", self.url, f"+refs/heads/{self.base}:refs/heads/{self.base}")

    async def have(self, tid: str) -> list[str]:
        """The hub's tips a worker can leave out of a thread's bundle."""
        refs = await gitio.tips(self.path, [f"refs/heads/{self.base}", *thread_refs(tid)])
        return list(refs.values())

    async def saved(self, tid: str) -> bool:
        """Whether a turn of the thread ever ended and was saved here (its session)."""
        return bool(await gitio.tips(self.path, [f"refs/sessions/{tid}"]))

    async def receive(self, tid: str, data: bytes, refs: dict[str, str]) -> None:
        """A turn's work from a worker: the thread's branch, snapshot and session."""
        allowed = set(thread_refs(tid))
        bad = set(refs) - allowed
        if bad:
            raise GitError(f"a worker may not set {', '.join(sorted(bad))}")
        async with self.lock:
            await gitio.unbundle(self.path, data)
            for ref, sha in refs.items():
                await self.git("update-ref", ref, sha)

    async def pack(self, tid: str | None, have: list[str]) -> tuple[bytes, dict[str, str]]:
        """The base, and a thread's branch, snapshot and session when `tid` is
        given, as a bundle without what the worker has (`have`), and the tips
        as the worker names them."""
        names = [f"refs/heads/{self.base}"]
        if tid:
            names += thread_refs(tid)
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
                    # --no-keep-empty: a branch begun before a remote was connected starts
                    # at "Start the project", which has no place on the remote's history.
                    rebase = await git(wt, "rebase", "-q", "--no-keep-empty", f"refs/heads/{base}", check=False)
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
