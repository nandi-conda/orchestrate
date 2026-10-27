"""One local git repository per project: the hub its workers' commits go to.

A project's hub is a bare repository, `<repos>/<project id>.git`, on the
machine the control plane runs on. Every project starts with one, from a
commit that makes it a pixi project ("Start the project": pixi.toml, and
pixi's lines in .gitignore and .gitattributes). Every project is a pixi
project: a base that is not one (a hub from before this was so, or a remote's
history the base adopts) gets a commit that makes it one, "Make the project a
pixi project", landed like a thread's work. It holds:

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

A remote can be a folder on the control plane's machine (a project opened from
disk): a git repository's own folder, or any folder, which becomes one
(`folder`). Landings are pushed into its checked-out branch and update its
files (receive.denyCurrentBranch=updateInstead), so they show up where its
owner works. Opening it commits what it had not, so it starts clean; git
refuses a landing while that checkout has changes made since and not
committed, and the thread's work lands again at the end of its next turn.

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
import json
import os
import shutil
import tempfile
import tomllib
from pathlib import Path

from . import gitio
from .gitio import AUTHOR, GitError, git

LAND_ATTEMPTS = 5  # pushes rejected because the remote moved before giving up
START, MAKE_PIXI = "Start the project", "Make the project a pixi project"  # the hub's own commits
IMPORT = "Add the folder's files"  # what a folder opened as a project had not committed
# A new project's pixi workspace: on conda-forge, for the platforms workers run on.
PLATFORMS = ("linux-64", "linux-aarch64")
# What `pixi init` puts in .gitignore and .gitattributes: environments stay out
# of git, and pixi.lock is never merged line by line.
GITIGNORE = ("# pixi environments", ".pixi/*", "!.pixi/config.toml")
GITATTRIBUTES = ("# SCM syntax highlighting & preventing 3-way merges",
                 "pixi.lock merge=binary linguist-language=YAML linguist-generated=true -diff")
PIXI_FILES = ("pixi.toml", ".gitignore", ".gitattributes")  # what pixi_tree writes


def manifest(name: str) -> str:
    """A new project's pixi.toml."""
    return (f"[workspace]\nname = {json.dumps(name)}\nchannels = [\"conda-forge\"]\n"
            f"platforms = {json.dumps(list(PLATFORMS))}\n\n[tasks]\n\n[dependencies]\n")


async def is_pixi(repo: str | os.PathLike, commit: str) -> bool:
    """Whether `commit` is a pixi project: pixi.toml at its root, or a
    pyproject.toml with a [tool.pixi] table."""
    if (await git(repo, "cat-file", "-e", f"{commit}:pixi.toml", check=False)).code == 0:
        return True
    pyproject = await git(repo, "show", f"{commit}:pyproject.toml", check=False)
    if pyproject.code:
        return False
    try:
        return "pixi" in tomllib.loads(pyproject.out).get("tool", {})
    except tomllib.TOMLDecodeError:
        return False


async def pixi_tree(repo: str | os.PathLike, commit: str | None, name: str) -> str:
    """The tree of `commit` (None: an empty one) made a pixi project, as
    `pixi init` makes one: a pixi.toml named `name`, and pixi's lines added to
    .gitignore and .gitattributes where they are missing."""
    with tempfile.TemporaryDirectory() as d:
        env = {"GIT_INDEX_FILE": os.path.join(d, "index")}
        await git(repo, "read-tree", *([commit] if commit else ["--empty"]), env=env)
        files = {PIXI_FILES[0]: manifest(name)}
        for path, lines in zip(PIXI_FILES[1:], (GITIGNORE, GITATTRIBUTES)):
            old = (await git(repo, "show", f"{commit}:{path}", check=False)).out if commit else ""
            have = old.splitlines()
            files[path] = "\n".join(have + [line for line in lines if line not in have]) + "\n"
        for path, text in files.items():
            src = Path(d) / "blob"
            src.write_text(text)
            blob = (await git(repo, "hash-object", "-w", str(src))).out
            await git(repo, "update-index", "--add", "--cacheinfo", f"100644,{blob},{path}", env=env)
        return (await git(repo, "write-tree", env=env)).out


def is_folder(url: str) -> bool:
    """Whether a remote is a folder on this machine: an absolute path, or one
    from the home folder (~), not a URL or an scp-like host:path."""
    return Path(url).expanduser().is_absolute()


def remote_url(url: str) -> str:
    """A remote as a project keeps it: a folder as its absolute path, without
    ~, .. or symlinks, so the same folder is always written the same way."""
    url = url.strip()
    return str(Path(url).expanduser().resolve()) if url and is_folder(url) else url


async def folder(path: str) -> str | None:
    """Make the folder at `path` (as remote_url writes it) a remote the hub can
    land in, and say which branch it has checked out (None for a bare
    repository, which is one as it is). A git repository's own folder lets
    pushes update its checked-out branch and files; a folder that is not one
    becomes one (on `main`). The project starts from what is there: what the
    folder has not committed (what its .gitignore leaves out stays out) gets a
    commit, so threads see every file in it and no landing has to write over
    one. A folder with nothing in it gets no commit: it stays an empty
    repository, which gets the project's history (Hub.connect). The files that make it a pixi project are its own in that
    commit even when ignored, so making it one edits them, never replaces
    them. Refused: no such folder, a folder inside another repository (open
    that one), and a checkout on no branch."""
    where = Path(path).resolve()
    if not where.is_dir():
        raise GitError(f"there is no folder {path}")
    repo = await git(where, "rev-parse", "--is-bare-repository", "--is-inside-git-dir", check=False)
    if repo.code:
        await git(where, "init", "-q", "-b", "main")
    else:
        bare, in_git_dir = repo.out.split()
        if bare == "true":
            return None
        if in_git_dir == "true":
            raise GitError(f"{path} is inside a repository's .git folder; open the folder it belongs to")
        top = (await git(where, "rev-parse", "--show-toplevel")).out
        if Path(top).resolve() != where:
            raise GitError(f"{path} is inside the git repository {top}; open that folder instead")
    head = await git(where, "symbolic-ref", "-q", "--short", "HEAD", check=False)
    if head.code:
        raise GitError(f"{path} has no branch checked out; check out the branch the project should follow")
    await git(where, "config", "receive.denyCurrentBranch", "updateInstead")
    await git(where, "add", "-A")
    if own := [f for f in PIXI_FILES if (where / f).is_file()]:
        await git(where, "add", "-f", "--", *own)
    if (await git(where, "diff", "--cached", "--quiet", check=False)).code:
        who = ("-c", f"user.name={AUTHOR[0]}", "-c", f"user.email={AUTHOR[1]}")
        await git(where, *who, "commit", "-q", "-m", IMPORT)
    return head.out


def git_folder(where: Path) -> bool:
    """Whether where is a git repository's folder. One that cannot be looked
    into — a system folder only root may read, a mount that has gone — is not
    one, and the picker lists it like any other: opening it is what says it
    cannot be read."""
    try:
        return (where / ".git").exists()
    except OSError:
        return False


def folders(path: str = "") -> dict:
    """The folders in a folder on this machine, for picking one to open as a
    project (`path` as remote_url takes it; "" is the home folder): its path,
    its parent ("" at the top), and the folders in it, by name, each with its
    path and whether it is a git repository's folder (git_folder). Hidden
    folders are left out; a folder that is not one, or cannot be read, is
    refused with the reason."""
    where = Path(remote_url(path) if path.strip() else Path.home().resolve())
    if not is_folder(str(where)):
        raise GitError(f"{path} is not a folder path; give its full path")
    if not where.is_dir():
        raise GitError(f"there is no folder {where}")
    try:
        names = sorted((e.name for e in os.scandir(where)
                        if not e.name.startswith(".") and e.is_dir()), key=str.lower)
    except OSError as e:
        raise GitError(f"{where} cannot be read: {e.strerror}") from None
    return {"path": str(where), "parent": "" if where.parent == where else str(where.parent),
            "entries": [{"name": n, "path": str(where / n), "git": git_folder(where / n)} for n in names]}


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
    def __init__(self, path: str | os.PathLike, name: str, token: str | None = None) -> None:
        self.path = Path(path).resolve()  # git runs in other directories
        self.name = name  # the project's, for the pixi.toml the hub writes
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
        """Make the hub on first use, from a first commit that makes the
        project a pixi project; else learn its base and the remote it is
        connected to, and make the base a pixi project if it is not one."""
        if (self.path / "HEAD").exists():
            self.base = (await self.git("config", "--get", "orchestrate.base")).out
            self.url = (await self.git("config", "--get", "orchestrate.remote", check=False)).out
            self.env = self.auth(self.url)
            async with self.lock:
                await self._make_pixi()
            return
        new = self.path.with_name(self.path.name + ".new")
        if new.exists():
            shutil.rmtree(new)
        new.parent.mkdir(parents=True, exist_ok=True)
        await git(new.parent, "init", "-q", "--bare", str(new))
        for key, value in (("user.name", AUTHOR[0]), ("user.email", AUTHOR[1]), ("gc.auto", "0")):
            await git(new, "config", key, value)
        base = "main"
        tree = await pixi_tree(new, None, self.name)
        commit = (await git(new, "commit-tree", tree, "-m", START)).out
        await git(new, "update-ref", f"refs/heads/{base}", commit)
        await git(new, "symbolic-ref", "HEAD", f"refs/heads/{base}")
        await git(new, "config", "orchestrate.base", base)
        new.rename(self.path)
        self.base = base

    async def connect(self, url: str) -> None:
        """Make `url` the project's remote, or leave it without one ("").

        The base and the remote's default branch become one line of history:
        - a remote without commits gets the base, under the base's name (a
          folder's, under the name of the branch it has checked out);
        - a remote ahead of the base (or a base with nothing landed on it yet)
          gives the base its commits;
        - a base ahead of the remote is pushed there, as a landing would be;
        - histories that have each gone their own way are refused, and the
          project keeps the remote it had.
        The base takes the name of the remote's default branch, and is made a
        pixi project if it is not one (pushed there, as a landing would be).
        A folder (is_folder; as remote_url writes it) is made a remote the hub
        can land in first (`folder`). Removing the remote keeps everything
        here; landings stay on this machine from then on."""
        async with self.lock:
            if url:
                branch = await folder(url) if is_folder(url) else None
                env = self.auth(url)
                await self._join(url, env, branch)
                await self.git("config", "orchestrate.remote", url)
            else:
                env = {}
                await self.git("config", "--unset-all", "orchestrate.remote", check=False)
            self.url, self.env = url, env
            await self._make_pixi()

    async def _make_pixi(self) -> None:
        """Make the base a pixi project, if it is not one, with a commit landed
        as a thread's work is: on the remote first when there is one."""
        base = f"refs/heads/{self.base}"
        for _ in range(LAND_ATTEMPTS):
            if self.url:
                await self.git("fetch", "-q", self.url, f"+{base}:{base}")
            tip = (await self.git("rev-parse", base)).out
            if await is_pixi(self.path, tip):
                return
            tree = await pixi_tree(self.path, tip, self.name)
            new = (await self.git("commit-tree", tree, "-p", tip, "-m", MAKE_PIXI)).out
            if self.url:
                push = await self.git("push", "-q", self.url, f"{new}:{base}", check=False)
                if push.code:
                    if any(s in push.err for s in ("fetch first", "non-fast-forward")):
                        continue  # the repository moved since the fetch
                    raise GitError(f"pushing to {self.base} of {self.url} failed: {push.err or push.out}")
            await self.git("update-ref", base, new)
            return
        raise GitError(f"{self.base} of {self.url} kept moving; {LAND_ATTEMPTS} pushes were rejected")

    async def _join(self, url: str, env: dict, branch: str | None) -> None:
        ours = (await self.git("rev-parse", f"refs/heads/{self.base}")).out
        name = await self.remote_base(url, env)
        if name is None:
            name = branch or self.base
            await git(self.path, "push", "-q", url, f"{ours}:refs/heads/{name}", env=env)
            await self._rename(name, ours)
            return
        incoming = "refs/orchestrate/connecting"
        await git(self.path, "fetch", "-q", url, f"+refs/heads/{name}:{incoming}", env=env)
        try:
            theirs = (await self.git("rev-parse", incoming)).out
            if ours == theirs or await self._ancestor(theirs, ours):
                tip = ours
                if ours != theirs:
                    await git(self.path, "push", "-q", url, f"{ours}:refs/heads/{name}", env=env)
            elif await self._ancestor(ours, theirs) or not await self._landed(ours):
                tip = theirs
            else:
                raise GitError(f"{name} of {url} and this project's {self.base} have each gone their "
                               "own way, so neither can follow the other; connect a repository that "
                               f"is empty or shares {self.base}'s history")
        finally:
            await self.git("update-ref", "-d", incoming)
        await self._rename(name, tip)

    async def _rename(self, name: str, tip: str) -> None:
        """Make the base `name`, at `tip`: the remote's default branch."""
        await self.git("update-ref", f"refs/heads/{name}", tip)
        if name != self.base:
            await self.git("symbolic-ref", "HEAD", f"refs/heads/{name}")
            await self.git("config", "orchestrate.base", name)
            await self.git("update-ref", "-d", f"refs/heads/{self.base}")
            self.base = name

    async def _ancestor(self, a: str, b: str) -> bool:
        return (await self.git("merge-base", "--is-ancestor", a, b, check=False)).code == 0

    async def _own(self, commit: str) -> list[str]:
        """The hub's own commits `commit`'s history starts with ("Start the
        project", then "Make the project a pixi project" in a hub from before
        every project began as one), oldest first."""
        own = []
        for line in (await self.git("log", "--reverse", "--format=%H %s", commit)).out.splitlines():
            sha, subject = line.split(" ", 1)
            if subject != (START, MAKE_PIXI)[len(own)]:
                break
            own.append(sha)
            if len(own) == 2:
                break
        return own

    async def _landed(self, commit: str) -> bool:
        """Whether anything landed on the base `commit`: it is more than the hub's own commits."""
        own = await self._own(commit)
        return not own or own[-1] != commit

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
                # A branch begun before a remote was connected shares no history with
                # the base: it goes on top without the hub's own commits it starts from,
                # which have no place on the remote's history.
                onto = [f"refs/heads/{base}"]
                if (await self.git("merge-base", f"refs/heads/{base}", branch, check=False)).code:
                    onto = ["--onto", onto[0], (await self._own(branch))[-1]]
                try:
                    rebase = await git(wt, "rebase", "-q", *onto, check=False)
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
