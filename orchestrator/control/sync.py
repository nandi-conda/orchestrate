"""Sharing projects between control planes on several computers, through the
sync server (sync/ at the top of the repository: a Cloudflare Worker).

The sync server is a public relay (DEFAULT_URL, or ORCHESTRATE_SYNC_URL): it
has no secret, and keeps each *space* apart, named by a random key. Each
computer makes one the first time (`space` in its data directory), so its
projects are synced from the start; another computer given the same key as
ORCHESTRATE_SYNC_SPACE is in the same space, and shares every project: each
computer's control plane shows every project, live, and any of them can
drive it. The key is the only way into a space, so it is shared like a secret
link. The control plane opens every connection to the sync server, as it
does to workers; nothing ever connects to it.

What is shared is a few documents, each a set of rows (JSON text) by key, of
which each computer keeps a copy (`sync.db` in its data directory). A row's
newest write wins: only a project's driver writes its rows (others add
inbox rows of their own), so there is nothing to merge, and the server keeps
only each row's latest value. Each computer sends the server its writes
(POST /docs/<doc>) and reads the rows written since it last looked
(GET /docs/<doc>?after=<version>):

- "projects": every project, keyed by its id.
- "project:<id>": its threads ("thread:<id>"), the messages between them
  ("message:<id>"), and its inbox ("inbox:<time><id>"): what the person did
  on a computer that does not drive the project, for the one that does.

and the logs: "log:<id>:main", "log:<id>:<thread id>", the project's own
events and each thread's. Events are never changed and only a project's
driver logs them, so a log needs no merging: on the server it is a list of
entries (uid and row), each added once, which can grow to any size. Each
computer sends the server every event its store has that the server has not
(a mark per project), and fetches the entries of a log it has not got, a
page at a time. A model call's streaming `delta` events stay on the computer
that has them; the call's own events, once it ends, are shared.

The store stays what every part of the control plane reads (SyncedStore wraps
it): a change of a document from another computer is applied to it, and
what this computer writes goes into the documents.

Driving. Each project is driven by one computer at a time: it routes the
project's requests, runs its threads on its own workers, keeps its hub, and
is the only one whose writes to the project are shared. Which one holds the
project's lease on the sync server; the driver renews it every RENEW_EVERY
seconds. A computer that does not drive a project shows it as the documents
have it and forwards what its person does to the driver, through the inbox.
A computer that needs to drive a project (its person opens it, or acts on
it) takes the lease when nobody holds it or its holder has not renewed it
for half a minute (the server's LEASE_STALE); Take over takes it at once.
The computer that loses a lease lets go of the project's threads (they stop
on its workers); the new driver brings them back on its own, from the hub.

The hub travels as git bundles (Hubs below): after every change of its refs
the driver adds a bundle of what is new, with the table of every ref after
it, and a computer that becomes the driver fetches the bundles it lacks.
Pasted images go to the server as they are pasted, so the driver can send
them to a worker whichever computer they were pasted on.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import secrets
import socket
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

import httpx

from . import gitio, paths
from .gitio import git
from .hub import bare
from .secrets import get_secret
from .store import Store

log = logging.getLogger("orchestrator.sync")

URL_KEY, SPACE_KEY = "ORCHESTRATE_SYNC_URL", "ORCHESTRATE_SYNC_SPACE"
DEFAULT_URL = "https://orchestrate-sync.nandithebull.workers.dev"  # ORCHESTRATE_SYNC_URL names another
SPACE_HEX = 64  # a space's key: 32 random bytes, in hex
INDEX = "projects"
RENEW_EVERY = 10.0
WAIT = 25.0  # how long a look at the change feed waits for a change
PUSH_DELAY = 0.2  # writes made within this of each other are synced together
RETRY = 2.0  # after the sync server could not be reached
DRIVER_FRESH = 5.0  # a lease another computer holds is asked about again after this
LOG_BATCH = 2_000_000  # bytes of events per POST /logs
FORMAT = 3  # of the replica: 3 since documents are rows on the server, no longer Automerge
LOCAL_EVENTS = {"delta"}  # a model call's streaming; its own events are shared once it ends
LOCAL_THREAD_FIELDS = {"token"}
HUB_REFS = ("refs/heads/", "refs/snapshots/", "refs/sessions/")


def project_doc(project_id: str) -> str:
    return f"project:{project_id}"


def log_doc(project_id: str, thread_id: str | None) -> str:
    return f"log:{project_id}:{thread_id or 'main'}"


def is_space(key: str) -> bool:
    return len(key) == SPACE_HEX and all(c in "0123456789abcdef" for c in key)


def space_key(data: str | Path) -> str:
    """This computer's own space: its key, made the first time and kept in
    the data directory (`space`, readable by its owner only)."""
    path = Path(data) / "space"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(mode=0o600)
        path.write_text(secrets.token_hex(SPACE_HEX // 2) + "\n")
    key = path.read_text().strip()
    if not is_space(key):
        raise RuntimeError(f"{path} does not hold a space's key ({SPACE_HEX} hex digits); remove it to make a new one")
    return key


def dumps(row: dict) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


class SyncError(RuntimeError):
    """The sync server refused a request, or could not be reached."""


class NotDriving(RuntimeError):
    """Another computer drives the project; the text says which."""


def seq_refs(typ: str, payload: dict) -> list[tuple[dict, str]]:
    """Where an event's payload names another event by its seq: (the dict, its key).
    A seq is the store's own, so a shared event names it by uid instead."""
    out = []
    if typ in ("edited", "route") and "of" in payload:
        out.append((payload, "of"))
    if typ == "message_in" and "edit" in payload:
        out.append((payload, "edit"))
    quote = payload.get("quote")
    if typ in ("request", "message_in") and isinstance(quote, dict) and "of" in quote:
        out.append((quote, "of"))
    return out


def refusal(r: httpx.Response) -> str:
    """Why the server refused, in a line: its `error`, or the title of an
    error page Cloudflare answered with instead (an exception in the Worker)."""
    try:
        return str(r.json().get("error", r.text))
    except ValueError:
        pass
    title = re.search(r"<title>(.*?)</title>", r.text, re.S)
    if title:
        return " ".join(title.group(1).split())
    return " ".join(r.text.split())[:200]


class SyncClient:
    """The sync server's HTTP API (sync/src/index.js); every request names
    the space with `X-Sync-Space: <key>`."""

    def __init__(self, url: str, space: str, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.url = url.rstrip("/")
        self.http = httpx.AsyncClient(base_url=self.url, headers={"x-sync-space": space},
                                      timeout=httpx.Timeout(60.0, connect=10.0), transport=transport)

    async def close(self) -> None:
        await self.http.aclose()

    async def _call(self, method: str, path: str, **kw) -> httpx.Response:
        try:
            r = await self.http.request(method, path, **kw)
        except httpx.HTTPError as e:
            raise SyncError(f"the sync server at {self.url} did not answer {method} {path}: "
                            f"{type(e).__name__}: {e}") from e
        if r.status_code >= 400:
            raise SyncError(f"the sync server refused {method} {path} ({r.status_code}): {refusal(r)}")
        return r

    async def write(self, doc: str, values: dict[str, str | None]) -> int:
        return (await self._call("POST", f"/docs/{doc}", json={"values": values})).json()["version"]

    async def read(self, doc: str, after: int) -> dict:
        return (await self._call("GET", f"/docs/{doc}", params={"after": after})).json()

    async def changes(self, since: int, wait: float) -> dict:
        return (await self._call("GET", "/changes", params={"since": since, "wait": wait},
                                 timeout=wait + 30)).json()

    async def append(self, log: str, entries: list[dict]) -> int:
        return (await self._call("POST", f"/logs/{log}", json={"entries": entries})).json()["n"]

    async def entries(self, log: str, after: int) -> dict:
        return (await self._call("GET", f"/logs/{log}", params={"after": after})).json()

    async def lease(self, project: str, holder: str, name: str, take: bool) -> dict:
        return (await self._call("POST", "/lease", json={"project": project, "holder": holder, "name": name,
                                                         "take": take})).json()

    async def renew(self, holder: str, projects: list[str]) -> list[str]:
        return (await self._call("POST", "/leases/renew", json={"holder": holder, "projects": projects})).json()["lost"]

    async def release(self, holder: str, projects: list[str]) -> None:
        await self._call("POST", "/leases/release", json={"holder": holder, "projects": projects})

    async def leases(self) -> list[dict]:
        return (await self._call("GET", "/leases")).json()

    async def bundles(self, project: str) -> list[dict]:
        return (await self._call("GET", f"/hubs/{project}")).json()

    async def add_bundle(self, project: str, holder: str, base: str, refs: dict, data: bytes) -> int:
        meta = json.dumps({"base": base, "refs": refs}).encode()
        r = await self._call("PUT", f"/hubs/{project}", params={"holder": holder}, content=meta + data,
                             headers={"x-meta-length": str(len(meta))},
                             timeout=httpx.Timeout(600.0, connect=10.0))  # a whole hub, the first time
        return r.json()["n"]

    async def bundle(self, project: str, n: int) -> bytes:
        return (await self._call("GET", f"/hubs/{project}/{n}")).content

    async def put_blob(self, key: str, data: bytes, content_type: str, meta: dict) -> None:
        await self._call("PUT", f"/blobs/{key}", content=data,
                         headers={"content-type": content_type, "x-meta": json.dumps(meta)})

    async def get_blob(self, key: str) -> tuple[bytes, dict] | None:
        try:
            r = await self.http.get(f"/blobs/{key}")
        except httpx.HTTPError as e:
            raise SyncError(f"the sync server at {self.url} did not answer GET /blobs/{key}: {e}") from e
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            raise SyncError(f"the sync server refused GET /blobs/{key} ({r.status_code}): {refusal(r)}")
        return r.content, json.loads(r.headers.get("x-meta") or "{}")


class Replica:
    """This computer's copy of the synced documents, in a SQLite file: each
    row as the server last had it or this computer last wrote it, and the
    writes the server has not taken yet (the outbox)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.db = None

    async def open(self) -> None:
        import aiosqlite

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = await aiosqlite.connect(self.path)
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("CREATE TABLE IF NOT EXISTS rows (doc TEXT NOT NULL, key TEXT NOT NULL, "
                              "value TEXT NOT NULL, PRIMARY KEY (doc, key))")
        await self.db.execute("CREATE TABLE IF NOT EXISTS outbox (doc TEXT NOT NULL, key TEXT NOT NULL, "
                              "value TEXT, PRIMARY KEY (doc, key))")
        await self.db.execute("CREATE TABLE IF NOT EXISTS state (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        await self.db.commit()

    async def close(self) -> None:
        if self.db is not None:
            await self.db.close()

    async def names(self) -> list[str]:
        """The documents with writes the server has not taken."""
        async with self.db.execute("SELECT DISTINCT doc FROM outbox") as cur:
            return [r[0] for r in await cur.fetchall()]

    async def items(self, name: str) -> dict[str, str]:
        async with self.db.execute("SELECT key, value FROM rows WHERE doc = ?", (name,)) as cur:
            return {k: v for k, v in await cur.fetchall()}

    async def put(self, name: str, values: dict[str, str | None]) -> bool:
        """Set (or, for None, delete) rows of a document, for the server too;
        whether anything changed."""
        have = await self.items(name)
        todo = {k: v for k, v in values.items() if have.get(k) != v}
        if not todo:
            return False
        for key, value in todo.items():
            if value is None:
                await self.db.execute("DELETE FROM rows WHERE doc = ? AND key = ?", (name, key))
            else:
                await self.db.execute("INSERT INTO rows (doc, key, value) VALUES (?, ?, ?) ON CONFLICT (doc, key) "
                                      "DO UPDATE SET value = excluded.value", (name, key, value))
            await self.db.execute("INSERT INTO outbox (doc, key, value) VALUES (?, ?, ?) ON CONFLICT (doc, key) "
                                  "DO UPDATE SET value = excluded.value", (name, key, value))
        await self.db.commit()
        return True

    async def outbox(self, name: str) -> dict[str, str | None]:
        async with self.db.execute("SELECT key, value FROM outbox WHERE doc = ?", (name,)) as cur:
            return {k: v for k, v in await cur.fetchall()}

    async def sent(self, name: str, values: dict[str, str | None]) -> None:
        """The server took these writes: they leave the outbox, unless written again since."""
        for key, value in values.items():
            await self.db.execute("DELETE FROM outbox WHERE doc = ? AND key = ? AND value IS ?", (name, key, value))
        await self.db.commit()

    async def received(self, name: str, rows: list[dict]) -> None:
        """Keep rows as the server has them; a row this computer has written
        since, and the server has not taken yet, stays as written."""
        pending = await self.outbox(name)
        for r in rows:
            if r["key"] in pending:
                continue
            if r["value"] is None:
                await self.db.execute("DELETE FROM rows WHERE doc = ? AND key = ?", (name, r["key"]))
            else:
                await self.db.execute("INSERT INTO rows (doc, key, value) VALUES (?, ?, ?) ON CONFLICT (doc, key) "
                                      "DO UPDATE SET value = excluded.value", (name, r["key"], r["value"]))
        await self.db.commit()

    async def state(self, key: str, default: Any = None) -> Any:
        async with self.db.execute("SELECT v FROM state WHERE k = ?", (key,)) as cur:
            row = await cur.fetchone()
        return json.loads(row[0]) if row else default

    async def set_state(self, key: str, value: Any) -> None:
        await self.db.execute("INSERT INTO state (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v",
                              (key, json.dumps(value)))
        await self.db.commit()

    async def forget_automerge(self) -> None:
        """Drop the documents kept as Automerge (FORMAT < 3): each computer
        writes its rows again, and the server's are read from the start."""
        await self.db.execute("DROP TABLE IF EXISTS saves")
        await self.db.execute("DROP TABLE IF EXISTS changes")
        await self.db.execute("DELETE FROM state WHERE k = 'version' OR k LIKE 'doc%'")
        await self.db.commit()

    async def forget_space(self) -> None:
        """Forget where this replica was in its space's change feed and hubs."""
        await self.db.execute("DELETE FROM state WHERE k = 'version' OR k LIKE 'hub:%' OR k LIKE 'log%' "
                              "OR k LIKE 'doc%'")
        await self.db.execute("DELETE FROM rows")
        await self.db.execute("DELETE FROM outbox")
        await self.db.commit()


class Sync:
    """This computer's side of the sync server: the replica and its syncing,
    the leases it holds, and its projects' hubs on the server."""

    def __init__(self, url: str, space: str, data: str | Path = paths.DATA, name: str | None = None,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.space = space
        self.client = SyncClient(url, space, transport)
        self.data = Path(data)
        self.name = name or socket.gethostname()
        self.holder = ""
        self.replica: Replica | None = None
        self.store: Store | None = None  # the store SyncedStore wraps
        self.orch = None  # the Orchestrator, once it is made (bind)
        self.held: set[str] = set()  # the projects this computer drives
        self.drivers: dict[str, tuple[float, dict]] = {}  # lease of a project another computer drives, when looked at
        self._claims: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.dirty: set[str] = set()
        self.kick = asyncio.Event()
        self.version = 0
        # Per document, each value as it was last applied to the store or written
        # from it: a value that is the same again is not looked at.
        self.seen: dict[str, dict[str, str]] = defaultdict(dict)
        self.known: dict[str, set[str]] = {}  # per log document, the uids the store has
        # Events whose payload names an event the store does not have yet (it
        # is in a document still on its way): applied once it is in.
        self.deferred: dict[str, dict] = {}
        self.applying = asyncio.Lock()
        self.hub_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.hub_seen: dict[str, dict] = {}  # per project, its hub's last bundle on the server
        self.inbox_running: set[str] = set()
        self.logs_dirty: set[str] = set()  # projects with events the server may not have
        self.log_kick = asyncio.Event()
        self.log_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        # Per project, the uids of the events the server has (sent or fetched) this run.
        self.on_server: dict[str, set[str]] = defaultdict(set)
        self.tasks: list[asyncio.Task] = []
        self.caught_up = asyncio.Event()
        self.problem = ""  # why the last try to catch up with the server failed, "" once it has

    @classmethod
    def from_env(cls, data: str | Path = paths.DATA) -> "Sync":
        """The sync server at ORCHESTRATE_SYNC_URL (environment, then keyring;
        DEFAULT_URL when neither has it), in the space ORCHESTRATE_SYNC_SPACE
        names, or this computer's own (space_key)."""
        url = get_secret(URL_KEY) or DEFAULT_URL
        space = get_secret(SPACE_KEY)
        if space is not None:
            space = space.strip().lower()  # as pasted: a phone keyboard may capitalize it
        if not space:
            space = space_key(data)
        elif not is_space(space):
            raise RuntimeError(f"{SPACE_KEY} must be a space's key, {SPACE_HEX} hex digits "
                               f"(Settings shows it on the computer that has it)")
        return cls(url, space, data)

    # ------------------------------------------------------------ lifecycle

    async def open(self, store: Store) -> None:
        """Open the replica; called by SyncedStore.open once its store is open."""
        self.store = store
        self.data.mkdir(parents=True, exist_ok=True)
        ident = self.data / "computer"
        if not ident.exists():
            ident.write_text(f"c_{secrets.token_hex(8)}\n")
        self.holder = ident.read_text().strip()
        self.replica = Replica(self.data / "sync.db")
        await self.replica.open()
        if await self.replica.state("space", self.space) != self.space:
            # Paired into another space: what this replica has goes to it as
            # new, and the new space's change feed is followed from its start.
            await self.replica.forget_space()
        await self.replica.set_state("space", self.space)
        if await self.replica.state("format", 1) < FORMAT:
            await self.replica.forget_automerge()
            await self.replica.set_state("format", FORMAT)
        self.version = await self.replica.state("version", 0)

    def bind(self, orch) -> None:
        self.orch = orch

    async def start(self) -> None:
        """Catch up with the server and start syncing: every document this
        replica has is synced (it may hold changes the server never got), then
        every one changed since the last look; each is applied to the store.
        Projects only this computer has are then shared, driven by it.

        All of that runs in the background: starting never waits on the
        server, so a computer that cannot reach it (or a server that refuses
        it) starts all the same, and catches up once it can. Until then it
        drives no project (claim raises SyncError)."""
        self.tasks = [asyncio.create_task(self._catch_up())]

    async def _catch_up(self) -> None:
        while True:
            try:
                got = await self.client.changes(self.version, 0)
                await self._push_docs(await self.replica.names())
                names = {n for n in got["docs"] if not n.startswith("log:")}
                await self._apply(await self._pull_docs(names))
                await self._pull_logs([n for n in got["docs"] if n.startswith("log:")])
                self.version = got["version"]
                await self.replica.set_state("version", self.version)
                await self._share_local_projects()
                # Every project's events the server has not got yet go to it.
                self.logs_dirty |= {p["id"] for p in await self.store.list_projects()
                                    + await self.store.list_projects(archived=True)}
                self.log_kick.set()
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.problem = str(e) or type(e).__name__
                log.warning("could not catch up with the sync server at %s: %s", self.client.url, self.problem)
                await asyncio.sleep(RETRY)
        self.problem = ""
        projects = len(await self.store.list_projects())
        log.info("syncing with %s in space %s…: %d document(s), %d project(s) here",
                 self.client.url, self.space[:8], len(names), projects)
        self.caught_up.set()
        self.tasks += [asyncio.create_task(self._push_loop()), asyncio.create_task(self._pull_loop()),
                       asyncio.create_task(self._renew_loop()), asyncio.create_task(self._log_push_loop())]

    async def close(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.dirty:
            with contextlib.suppress(Exception):
                await self._push_docs(set(self.dirty))
        for pid in sorted(self.logs_dirty):
            with contextlib.suppress(Exception):
                await self._push_logs(pid)
        if self.held:
            with contextlib.suppress(Exception):
                await self.client.release(self.holder, sorted(self.held))
        self.held.clear()
        await self.client.close()
        if self.replica is not None:
            await self.replica.close()

    # ------------------------------------------------------------ syncing

    async def _push_docs(self, names) -> None:
        """Send the server this computer's writes it has not taken yet."""
        for name in sorted(set(names)):
            values = await self.replica.outbox(name)
            if values:
                await self.client.write(name, values)
                await self.replica.sent(name, values)

    async def _pull_docs(self, names) -> set[str]:
        """Read the rows written on the server since the last look, a page at
        a time; the documents that had any."""
        changed = set()
        for name in sorted(set(names)):
            after = await self.replica.state(f"docpull:{name}", 0)
            while True:
                page = await self.client.read(name, after)
                if page["rows"]:
                    await self.replica.received(name, page["rows"])
                    after = page["rows"][-1]["version"]
                    await self.replica.set_state(f"docpull:{name}", after)
                    changed.add(name)
                if not page["more"]:
                    break
        return changed

    async def _push_loop(self) -> None:
        while True:
            await self.kick.wait()
            await asyncio.sleep(PUSH_DELAY)
            self.kick.clear()
            names, self.dirty = self.dirty, set()
            try:
                await self._push_docs(names)
            except asyncio.CancelledError:
                self.dirty |= names
                raise
            except Exception as e:
                log.warning("could not sync %d document(s) to the sync server: %s", len(names), e)
                self.dirty |= names
                await asyncio.sleep(RETRY)
                self.kick.set()

    async def _pull_loop(self) -> None:
        while True:
            try:
                got = await self.client.changes(self.version, WAIT)
                docs = [n for n in got["docs"] if not n.startswith("log:")]
                if docs:
                    changed = await self._pull_docs(docs)
                    if changed:
                        await self._apply(changed)
                await self._pull_logs([n for n in got["docs"] if n.startswith("log:")])
                if got["version"] != self.version:
                    self.version = got["version"]
                    await self.replica.set_state("version", self.version)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("could not follow the sync server's changes: %s", e)
                await asyncio.sleep(RETRY)

    async def _put(self, name: str, values: dict[str, str | None]) -> None:
        if await self.replica.put(name, values):
            self.seen[name].update({k: v for k, v in values.items() if v is not None})
            self.dirty.add(name)
            self.kick.set()

    # ------------------------------------------------------------ applying

    async def _apply(self, names) -> None:
        """Bring the store up to documents that changed on the server."""
        async with self.applying:
            names = set(names)
            order = sorted(names, key=lambda n: (n != INDEX, not n.startswith("project:"), n))
            for name in order:
                fresh = {k: v for k, v in (await self.replica.items(name)).items() if self.seen[name].get(k) != v}
                if name == INDEX:
                    await self._apply_projects(fresh)
                elif name.startswith("project:"):
                    await self._apply_project(name.removeprefix("project:"), fresh)
            await self._apply_deferred()

    async def _apply_projects(self, fresh: dict[str, str]) -> None:
        for pid, value in fresh.items():
            if pid not in self.held:
                row = json.loads(value)
                if await self.store.get_project(pid) != row:
                    await self.store.import_project(row)
            self.seen[INDEX][pid] = value

    async def _apply_project(self, pid: str, fresh: dict[str, str]) -> None:
        name = project_doc(pid)
        inbox = False
        for key, value in fresh.items():
            kind, _, rid = key.partition(":")
            if kind == "inbox":
                inbox = True
                continue
            if pid not in self.held:
                row = json.loads(value)
                if kind == "thread":
                    have = await self.store.get_thread(rid)
                    if have is None or row.get("updated_at", 0) >= have.get("updated_at", 0):
                        await self.store.import_thread({**row, "token": (have or {}).get("token", "")})
                elif kind == "message":
                    if await self.store.get_message(rid) != row:
                        await self.store.import_message(row)
            self.seen[name][key] = value
        if inbox and pid in self.held:
            self._run_inbox(pid)

    # ------------------------------------------------------------ logs

    async def _pull_logs(self, names) -> None:
        """Fetch the entries of logs that changed on the server, and log the
        events this store has not got."""
        if not names:
            return
        async with self.applying:
            touched = set()
            for name in sorted(set(names)):
                _, pid, tid = name.split(":", 2)
                if name not in self.known:
                    self.known[name] = await self.store.event_uids(pid, None if tid == "main" else tid)
                after = await self.replica.state(f"logpull:{name}", 0)
                while True:
                    page = await self.client.entries(name, after)
                    for e in page["entries"]:
                        self.on_server[pid].add(e["uid"])
                        if e["uid"] not in self.known[name]:
                            self.deferred[e["uid"]] = json.loads(e["row"])
                        after = e["n"]
                    await self.replica.set_state(f"logpull:{name}", after)
                    if not page["more"]:
                        break
                touched.add(pid)
            await self._apply_deferred()
            for pid in touched:
                await self._skip_pushed(pid)

    async def _skip_pushed(self, project_id: str) -> int:
        """Move the project's LOG_PUSHED mark past the events the server has
        (or that are never sent); the seq it is at."""
        key = f"logpush:{project_id}"
        at = start = await self.replica.state(key, 0)
        done = False
        while not done:
            page = await self.store.list_events(project_id, None, at, limit=1000)
            for ev in page:
                if ev["type"] not in LOCAL_EVENTS and ev["uid"] not in self.on_server[project_id]:
                    done = True
                    break
                at = ev["seq"]
            done = done or len(page) < 1000
        if at != start:
            await self.replica.set_state(key, at)
        return at

    async def _push_logs(self, project_id: str) -> None:
        """Send the server the project's events it has not got, in order,
        LOG_BATCH bytes at a time, each to its log."""
        async with self.log_locks[project_id]:
            at = await self._skip_pushed(project_id)
            while page := await self.store.list_events(project_id, None, at, limit=1000):
                batch: dict[str, list[dict]] = defaultdict(list)
                size = 0
                for ev in page:
                    if ev["type"] not in LOCAL_EVENTS and ev["uid"] not in self.on_server[project_id]:
                        row = await self._event_row(ev)
                        batch[log_doc(project_id, ev["thread_id"])].append({"uid": ev["uid"], "row": row})
                        size += len(row)
                    if size >= LOG_BATCH or ev is page[-1]:
                        for name, entries in batch.items():
                            await self.client.append(name, entries)
                            self.on_server[project_id].update(e["uid"] for e in entries)
                            self.known.setdefault(name, set()).update(e["uid"] for e in entries)
                        batch.clear()
                        size = 0
                        at = ev["seq"]
                        await self.replica.set_state(f"logpush:{project_id}", at)

    async def _event_row(self, ev: dict) -> str:
        """An event as its log's entry has it: seqs it names, as uids."""
        payload = json.loads(json.dumps(ev["payload"]))
        refs = seq_refs(ev["type"], payload)
        uids = await self.store.uids_of([d[k] for d, k in refs if isinstance(d[k], int)])
        for d, k in refs:
            if d[k] in uids:
                d[k] = uids[d[k]]
        row = {k: ev[k] for k in ("uid", "project_id", "thread_id", "type", "created_at", "peer_id")}
        return dumps({**row, "payload": payload})

    async def _log_push_loop(self) -> None:
        while True:
            await self.log_kick.wait()
            await asyncio.sleep(PUSH_DELAY)
            self.log_kick.clear()
            todo, self.logs_dirty = self.logs_dirty, set()
            for pid in sorted(todo):
                try:
                    await self._push_logs(pid)
                except asyncio.CancelledError:
                    self.logs_dirty |= todo
                    raise
                except Exception as e:
                    log.warning("could not send project %s's events to the sync server: %s", pid, e)
                    self.logs_dirty.add(pid)
            if self.logs_dirty and not self.log_kick.is_set():
                await asyncio.sleep(RETRY)
                self.log_kick.set()

    async def _apply_deferred(self) -> None:
        """Log the events waiting to be, oldest first; one that names an event
        the store does not have yet waits on."""
        while True:
            progress = False
            for uid, ev in sorted(self.deferred.items(), key=lambda kv: (kv[1]["created_at"], kv[0])):
                payload = json.loads(json.dumps(ev["payload"]))
                refs = seq_refs(ev["type"], payload)
                seqs = await self.store.seqs_of([d[k] for d, k in refs if isinstance(d[k], str)])
                if any(isinstance(d[k], str) and d[k] not in seqs for d, k in refs):
                    continue
                for d, k in refs:
                    if isinstance(d[k], str):
                        d[k] = seqs[d[k]]
                await self.store.import_event({**ev, "payload": payload})
                self.known.setdefault(log_doc(ev["project_id"], ev["thread_id"]), set()).add(uid)
                del self.deferred[uid]
                progress = True
            if not progress:
                return

    # ------------------------------------------------------------ writing

    def driving(self, project_id: str) -> bool:
        return project_id in self.held

    async def project_written(self, project: dict) -> None:
        if self.driving(project["id"]):
            await self._put(INDEX, {project["id"]: dumps(project)})

    async def thread_written(self, thread: dict) -> None:
        if self.driving(thread["project_id"]):
            row = {k: v for k, v in thread.items() if k not in LOCAL_THREAD_FIELDS}
            await self._put(project_doc(thread["project_id"]), {f"thread:{thread['id']}": dumps(row)})

    async def message_written(self, message: dict) -> None:
        if self.driving(message["project_id"]):
            await self._put(project_doc(message["project_id"]), {f"message:{message['id']}": dumps(message)})

    async def event_written(self, ev: dict) -> None:
        if ev["type"] in LOCAL_EVENTS:
            return
        self.known.setdefault(log_doc(ev["project_id"], ev["thread_id"]), set()).add(ev["uid"])
        self.logs_dirty.add(ev["project_id"])
        self.log_kick.set()

    async def image_written(self, project_id: str, image: dict, data: bytes) -> None:
        """Every image goes to the server as it is stored: the driver may be another computer."""
        await self.client.put_blob(image["id"], data, image["media_type"], {
            "project_id": project_id, "media_type": image["media_type"], "name": image["name"],
            "size": image["size"]})

    async def fetch_image(self, image_id: str) -> dict | None:
        """An image stored on another computer, from the server."""
        got = await self.client.get_blob(image_id)
        if got is None:
            return None
        data, meta = got
        return {"id": image_id, "project_id": meta.get("project_id", ""), "media_type": meta.get("media_type", ""),
                "name": meta.get("name", ""), "size": len(data), "data": data, "created_at": 0.0}

    async def _share_local_projects(self) -> None:
        """Share the projects only this computer has (made before it synced):
        it drives each, and writes all of it into the documents."""
        index = await self.replica.items(INDEX)
        for p in await self.store.list_projects() + await self.store.list_projects(archived=True):
            if p["id"] in index or not await self.claim(p["id"]):
                continue
            await self._share(p)

    async def _share(self, p: dict) -> None:
        pid = p["id"]
        await self.project_written(p)
        for t in await self.store.list_threads(pid) + await self.store.list_threads(pid, archived=True):
            await self.thread_written(t)
            for m in await self.store.list_messages(t["id"]):
                await self.message_written(m)
        self.logs_dirty.add(pid)
        self.log_kick.set()
        log.info("shared project %s (%s) through the sync server", p["name"], pid)

    # ------------------------------------------------------------ driving

    async def claim(self, project_id: str, take: bool = False) -> bool:
        """Whether this computer drives the project, taking its lease when it
        can (nobody holds it, or its holder stopped renewing it) or, with
        `take`, at once. A computer that becomes the driver first brings the
        project's hub up to the server's (Orchestrator.became_driver)."""
        if project_id in self.held and not take:
            return True
        async with self._claims[project_id]:
            if project_id in self.held:
                return True
            seen = self.drivers.get(project_id)
            loop = asyncio.get_running_loop()
            if (not take and seen is not None and loop.time() - seen[0] < DRIVER_FRESH
                    and not seen[1]["stale"] and seen[1]["holder"] != self.holder):
                return False
            r = await self.client.lease(project_id, self.holder, self.name, take)
            if not r["granted"]:
                self.drivers[project_id] = (loop.time(), r["lease"])
                return False
            self.drivers.pop(project_id, None)
            self.held.add(project_id)
            try:
                if self.orch is not None:
                    await self.orch.became_driver(project_id)
                self._run_inbox(project_id)
            except BaseException:
                self.held.discard(project_id)
                with contextlib.suppress(Exception):
                    await self.client.release(self.holder, [project_id])
                raise
            log.info("this computer drives project %s now", project_id)
            return True

    def driver(self, project_id: str) -> dict | None:
        """Who drives the project, as far as this computer knows: {name, me}
        (None when nobody does that it knows of)."""
        if project_id in self.held:
            return {"name": self.name, "me": True}
        seen = self.drivers.get(project_id)
        if seen is None or seen[1]["stale"]:
            return None
        return {"name": seen[1]["name"], "me": False}

    async def _renew_loop(self) -> None:
        while True:
            await asyncio.sleep(RENEW_EVERY)
            try:
                if self.held:
                    lost = await self.client.renew(self.holder, sorted(self.held))
                    for pid in lost:
                        await self._lost(pid)
                loop = asyncio.get_running_loop()
                for lease in await self.client.leases():
                    if lease["holder"] != self.holder:
                        self.drivers[lease["project"]] = (loop.time(), lease)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("could not renew this computer's leases: %s", e)

    async def _lost(self, project_id: str) -> None:
        """Another computer took the project's lease: let go of its threads here."""
        if project_id not in self.held:
            return
        self.held.discard(project_id)
        log.warning("another computer drives project %s now; letting go of its threads here", project_id)
        if self.orch is not None:
            await self.orch.let_go(project_id)
        # What the new driver wrote before this computer knew it had lost the
        # lease was passed over (only a driver's own writes count): the store
        # is brought up to the documents now.
        name = project_doc(project_id)
        async with self.applying:
            self.seen[INDEX].pop(project_id, None)
            self.seen.pop(name, None)
        await self._apply([INDEX, name])

    def not_driving(self, project_id: str) -> NotDriving:
        who = self.driver(project_id)
        return NotDriving(f"{who['name']} drives this project" if who else "another computer drives this project")

    # ------------------------------------------------------------ the inbox

    async def forward(self, project_id: str, op: str, args: dict) -> None:
        """Hand an action of this computer's person to the project's driver."""
        iid = f"inbox:{time.time_ns():020d}{uuid.uuid4().hex[:8]}"  # oldest first
        await self._put(project_doc(project_id), {iid: dumps({"op": op, "args": args, "from": self.name})})

    def _run_inbox(self, project_id: str) -> None:
        if project_id in self.inbox_running or self.orch is None:
            return
        self.inbox_running.add(project_id)
        task = asyncio.create_task(self._inbox(project_id))
        self.tasks.append(task)
        task.add_done_callback(lambda t: self.tasks.remove(t) if t in self.tasks else None)

    async def _inbox(self, project_id: str) -> None:
        """Do what was forwarded to this computer, the project's driver, oldest
        first; each entry leaves the inbox as it is taken."""
        name = project_doc(project_id)
        try:
            while project_id in self.held:
                entries = sorted((k, v) for k, v in (await self.replica.items(name)).items() if k.startswith("inbox:"))
                if not entries:
                    return
                key, value = entries[0]
                await self._put(name, {key: None})
                entry = json.loads(value)
                await self.orch.forwarded(project_id, entry["op"], entry["args"], entry.get("from", ""))
        finally:
            self.inbox_running.discard(project_id)

    # ------------------------------------------------------------ hubs

    async def push_hub(self, project_id: str, path: Path) -> None:
        """Add a bundle of what the project's hub has that the server's lacks,
        with the table of every ref after it. Only the driver does."""
        async with self.hub_locks[project_id]:
            if project_id not in self.held or not (path / "HEAD").exists():
                return
            refs = await hub_refs(path)
            base = (await git(path, "config", "--get", "orchestrate.base")).out
            last = await self._last_bundle(project_id)
            if last is not None and last["refs"] == refs and last["base"] == base:
                return
            had = last["refs"] if last else {}
            moved = [r for r, sha in refs.items() if had.get(r) != sha]
            data = await gitio.bundle(path, moved, list(had.values())) if moved else b""
            n = await self.client.add_bundle(project_id, self.holder, base, refs, data)
            self.hub_seen[project_id] = {"n": n, "base": base, "refs": refs}
            await self.replica.set_state(f"hub:{project_id}", n)

    async def _last_bundle(self, project_id: str) -> dict | None:
        if project_id not in self.hub_seen:
            bundles = await self.client.bundles(project_id)
            if bundles:
                self.hub_seen[project_id] = bundles[-1]
        return self.hub_seen.get(project_id)

    async def pull_hub(self, project_id: str, path: Path) -> bool:
        """Bring the project's hub at `path` up to the server's: the bundles it
        has not had, then every ref as the last one's table has it (refs the
        table lacks are deleted). Made from them when there is no hub here.
        Whether the server had the hub at all."""
        async with self.hub_locks[project_id]:
            bundles = await self.client.bundles(project_id)
            if not bundles:
                return False
            had = await self.replica.state(f"hub:{project_id}", 0)
            if not (path / "HEAD").exists():
                await bare(path)
                had = 0
            for b in bundles:
                if b["n"] > had:
                    await gitio.unbundle(path, await self.client.bundle(project_id, b["n"]))
            last = bundles[-1]
            for ref in await hub_refs(path):
                if ref not in last["refs"]:
                    await git(path, "update-ref", "-d", ref)
            for ref, sha in last["refs"].items():
                await git(path, "update-ref", ref, sha)
            await git(path, "config", "orchestrate.base", last["base"])
            await git(path, "symbolic-ref", "HEAD", f"refs/heads/{last['base']}")
            self.hub_seen[project_id] = last
            await self.replica.set_state(f"hub:{project_id}", last["n"])
            return True


async def hub_refs(path: Path) -> dict[str, str]:
    """A hub's refs that are shared: its branches, snapshots and sessions."""
    out = (await git(path, "for-each-ref", "--format=%(refname) %(objectname)", *HUB_REFS)).out
    return dict(line.split(" ", 1) for line in out.splitlines() if line)


class Solo:
    """No sync server: this computer has its projects to itself, and drives each."""

    async def pull_hub(self, project_id: str, path: Path) -> bool:
        return False

    def bind(self, orch) -> None:
        pass

    async def start(self) -> None:
        pass

    def driving(self, project_id: str) -> bool:
        return True

    async def claim(self, project_id: str, take: bool = False) -> bool:
        return True

    def driver(self, project_id: str) -> dict | None:
        return None

    async def push_hub(self, project_id: str, path: Path) -> None:
        pass


class SyncedStore:
    """A store whose projects are shared through the sync server: it is the
    store it wraps for everything, and what it writes goes to the documents
    too (Sync.*_written; only the driver's writes do)."""

    def __init__(self, inner: Store, sync: Sync) -> None:
        self.inner = inner
        self.sync = sync

    def __getattr__(self, name: str):
        return getattr(self.inner, name)

    async def open(self) -> None:
        await self.inner.open()
        await self.sync.open(self.inner)

    async def close(self) -> None:
        await self.sync.close()
        await self.inner.close()

    async def create_project(self, name: str, model: str = "", repo_url: str = "",
                             project_id: str | None = None) -> dict:
        p = await self.inner.create_project(name, model, repo_url, project_id)
        if await self.sync.claim(p["id"]):  # nobody holds a new project's lease
            await self.sync.project_written(p)
        return p

    async def update_project(self, project_id: str, **fields: Any) -> dict | None:
        p = await self.inner.update_project(project_id, **fields)
        if p is not None:
            await self.sync.project_written(p)
        return p

    async def create_thread(self, *args, **kw) -> dict:
        t = await self.inner.create_thread(*args, **kw)
        await self.sync.thread_written(t)
        return t

    async def update_thread(self, thread_id: str, **fields: Any) -> dict | None:
        t = await self.inner.update_thread(thread_id, **fields)
        if t is not None:
            await self.sync.thread_written(t)
        return t

    async def append_event(self, *args, **kw) -> dict:
        ev = await self.inner.append_event(*args, **kw)
        await self.sync.event_written(ev)
        return ev

    async def add_message(self, *args, **kw) -> dict:
        m = await self.inner.add_message(*args, **kw)
        await self.sync.message_written(m)
        return m

    async def mark_read(self, message_ids: list[str]) -> None:
        await self.inner.mark_read(message_ids)
        for mid in message_ids:
            m = await self.inner.get_message(mid)
            if m is not None:
                await self.sync.message_written(m)

    async def add_image(self, project_id: str, media_type: str, data: bytes, name: str = "") -> dict:
        image = await self.inner.add_image(project_id, media_type, data, name)
        await self.sync.image_written(project_id, image, data)
        return image

    async def get_image(self, image_id: str) -> dict | None:
        row = await self.inner.get_image(image_id)
        if row is None:
            row = await self.sync.fetch_image(image_id)
            if row is not None:
                await self.inner.import_image(row)
        return row
