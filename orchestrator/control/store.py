"""Storage for the control plane.

`Store` is the interface the rest of the control plane uses. Two backends:

- `SqliteStore`: the default, for local dev and tests. One process only.
- `PostgresStore`: chosen when `--db` is a postgres:// URL. Several
  control plane instances can share it; appends are announced with
  LISTEN/NOTIFY so every instance wakes its SSE streams and inboxes.

A project is archived when its `archived_at` is set: `list_projects` lists
the live projects (the default) or only the archived ones, oldest first.
A thread is archived when `archived_at` is set. `list_threads` lists either
the live threads (the default: the main list, the router's directory, peers)
or only the archived ones. Working threads (queued or working) come first,
then the rest; each group newest first, so a thread moves up when its turn
starts and back when it ends, and nothing else reshuffles.

A thread's `busy_since` is when the turn it is in began (its state queued,
working or waiting), NULL between turns; `update_thread` keeps it as the state
changes. With `backlog` (the messages to a thread it has not got to) it is
what load.py makes a thread's load from.

Events carry a project-wide sequence number (`seq`), so a client can resume a
project or thread stream from `?after=seq`.

Pasted images are stored here too, as rows in `images`, so every control plane
instance serving the same database can show them. Events and messages carry
only their ids (`{"id", "media_type", "name", "size"}`); the bytes never go
through the event log or an SSE stream. The bytes are sent to a runner inside
the message it delivers, because a worker never reaches back here.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Protocol

from . import paths
from .states import BINDING, RUNNING, WORKER

THREAD_STATES = ("queued", "working", "waiting", "idle", "done", "failed", "stopped")
RUNNING_SQL = ", ".join(f"'{s}'" for s in sorted(RUNNING))


def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(4)}"


def now() -> float:
    return time.time()


class Store(Protocol):
    async def open(self) -> None: ...
    async def close(self) -> None: ...

    async def create_project(self, name: str, model: str = "", repo_url: str = "",
                             project_id: str | None = None) -> dict:
        """A new project; `repo_url` and `project_id` are given when its hub
        was made and joined to its remote first (Orchestrator.create_project)."""
    async def list_projects(self, archived: bool = False) -> list[dict]:
        """The projects that are not archived, or with archived=True only the
        archived ones; oldest first."""
    async def get_project(self, project_id: str) -> dict | None: ...
    async def update_project(self, project_id: str, **fields: Any) -> dict | None:
        """Change a project's name, model or repo_url (its remote; the
        control plane connects the hub to it first, Orchestrator.update_project),
        or archived_at (Orchestrator.archive_project)."""

    async def create_thread(self, project_id: str, title: str, request: str, workers: str,
                            parent_id: str | None = None, workers_why: str = "") -> dict:
        """A new thread whose workers run on `workers` (providers.PLACES),
        for the reason `workers_why` (the router's)."""
    async def list_threads(self, project_id: str, archived: bool = False) -> list[dict]:
        """The project's threads that are not archived, or with archived=True
        only the archived ones; working (queued or working) threads first,
        then the rest, each group newest first."""
    async def get_thread(self, thread_id: str) -> dict | None: ...
    async def update_thread(self, thread_id: str, **fields: Any) -> dict | None: ...

    async def append_event(self, project_id: str, thread_id: str | None, type: str,
                           payload: dict, peer_id: str | None = None) -> dict:
        """Log an event on a thread. `peer_id` is a second thread the event
        belongs to: a message between threads is one event, on the thread it
        went to, that the thread it came from lists too."""
    async def list_events(self, project_id: str, thread_id: str | None = None,
                          after: int = 0, limit: int = 500) -> list[dict]:
        """The project's events, or a thread's (its own and those it is the peer of)."""
    async def recent_events(self, project_id: str, thread_id: str | None = None, *,
                            limit: int) -> list[dict]:
        """The newest `limit` events, oldest first: the window a pane shows of a
        log that has outgrown one page. list_events pages from the front, which
        is what a stream resuming from its last event needs; a pane needs the end."""
    async def get_event(self, seq: int) -> dict | None: ...
    async def wait_events(self, project_id: str, after: int, timeout: float) -> None:
        """Return when an event with seq > after may exist, or on timeout."""

    async def add_message(self, project_id: str, sender: str, to_thread: str, body: str,
                          reply_to: str | None = None, hops: int = 0) -> dict: ...
    async def list_messages(self, thread_id: str, unread_only: bool = False) -> list[dict]: ...
    async def mark_read(self, message_ids: list[str]) -> None: ...
    async def backlog(self, project_id: str) -> dict[str, int]:
        """Per live thread of the project, the messages to it it has not got
        to: those not yet delivered to its runner, and those that came in
        during the turn it is in (they steer that turn, so they are still on
        its plate). Threads with none are left out."""

    async def add_image(self, project_id: str, media_type: str, data: bytes, name: str = "") -> dict:
        """Store an image's bytes; returns its reference for an event or message."""
    async def get_image(self, image_id: str) -> dict | None:
        """An image row, its `data` bytes included."""

    async def upsert_worker(self, worker: dict) -> None: ...
    async def lose_worker(self, worker_id: str, reason: str, log: str) -> None:
        """Mark a worker lost (destroyed); it is never attached to again.
        `reason` says why it was dropped, `log` is the end of its runner log
        then (the worker, and its log with it, is gone afterwards)."""
    async def set_worker_state(self, worker_id: str, state: str, reason: str) -> None:
        """A worker's state (states.WORKER) and why it is in it."""
    async def projects_on_worker(self, worker_id: str) -> list[str]:
        """The projects with a thread on the worker, or last on it (threads.worker_id)."""
    async def list_workers(self) -> list[dict]:
        """Every worker ever made; `idx` is the pool slot it was made for,
        `lost_at` is set once it was lost. `project_id` is "" for the pool's
        workers, and names the project of a worker from before workers were
        a pool shared by every project (each project had its own)."""


class Notifier:
    """Wakes waiters in this process when a project gets a new event."""

    def __init__(self) -> None:
        self._conds: dict[str, asyncio.Event] = defaultdict(asyncio.Event)

    def poke(self, project_id: str) -> None:
        ev = self._conds.pop(project_id, None)
        if ev is not None:
            ev.set()

    async def wait(self, project_id: str, timeout: float) -> None:
        ev = self._conds[project_id]
        try:
            await asyncio.wait_for(ev.wait(), timeout)
        except asyncio.TimeoutError:
            pass


SCHEMA = [
    """CREATE TABLE IF NOT EXISTS projects (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, repo_url TEXT NOT NULL DEFAULT '',
        model TEXT NOT NULL DEFAULT '', settings TEXT NOT NULL DEFAULT '{}',
        archived_at DOUBLE PRECISION, created_at DOUBLE PRECISION NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS threads (
        id TEXT PRIMARY KEY, project_id TEXT NOT NULL, parent_id TEXT,
        title TEXT NOT NULL, request TEXT NOT NULL DEFAULT '', summary TEXT NOT NULL DEFAULT '',
        activity TEXT NOT NULL DEFAULT '', state TEXT NOT NULL DEFAULT 'queued', worker_id TEXT, files TEXT NOT NULL DEFAULT '[]',
        token TEXT NOT NULL DEFAULT '', archived_at DOUBLE PRECISION, created_at DOUBLE PRECISION NOT NULL,
        updated_at DOUBLE PRECISION NOT NULL, binding TEXT NOT NULL DEFAULT 'unbound',
        binding_reason TEXT NOT NULL DEFAULT '', busy_since DOUBLE PRECISION)""",
    "CREATE INDEX IF NOT EXISTS threads_project ON threads(project_id)",
    """CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY, project_id TEXT NOT NULL, sender TEXT NOT NULL,
        to_thread TEXT NOT NULL, body TEXT NOT NULL, reply_to TEXT, hops INTEGER NOT NULL DEFAULT 0,
        created_at DOUBLE PRECISION NOT NULL, read_at DOUBLE PRECISION)""",
    "CREATE INDEX IF NOT EXISTS messages_to ON messages(to_thread)",
    """CREATE TABLE IF NOT EXISTS workers (
        id TEXT PRIMARY KEY, project_id TEXT NOT NULL, provider TEXT NOT NULL,
        handle TEXT NOT NULL, url TEXT NOT NULL, token TEXT NOT NULL DEFAULT '',
        capacity INTEGER NOT NULL DEFAULT 16, idx INTEGER NOT NULL DEFAULT 0,
        created_at DOUBLE PRECISION NOT NULL, lost_at DOUBLE PRECISION,
        lost_reason TEXT NOT NULL DEFAULT '', lost_log TEXT NOT NULL DEFAULT '',
        state TEXT NOT NULL DEFAULT 'ready', state_reason TEXT NOT NULL DEFAULT '')""",
]

SQLITE_EVENTS = """CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, project_id TEXT NOT NULL, thread_id TEXT,
    type TEXT NOT NULL, payload TEXT NOT NULL, created_at DOUBLE PRECISION NOT NULL)"""
PG_EVENTS = """CREATE TABLE IF NOT EXISTS events (
    seq BIGSERIAL PRIMARY KEY, project_id TEXT NOT NULL, thread_id TEXT,
    type TEXT NOT NULL, payload TEXT NOT NULL, created_at DOUBLE PRECISION NOT NULL)"""
EVENTS_INDEX = "CREATE INDEX IF NOT EXISTS events_project ON events(project_id, seq)"

PROJECT_FIELDS = {"name", "model", "repo_url", "archived_at"}

# The two databases spell binary differently.
SQLITE_IMAGES = """CREATE TABLE IF NOT EXISTS images (
    id TEXT PRIMARY KEY, project_id TEXT NOT NULL, media_type TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0, data BLOB NOT NULL,
    created_at DOUBLE PRECISION NOT NULL)"""
PG_IMAGES = """CREATE TABLE IF NOT EXISTS images (
    id TEXT PRIMARY KEY, project_id TEXT NOT NULL, media_type TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0, data BYTEA NOT NULL,
    created_at DOUBLE PRECISION NOT NULL)"""
THREAD_FIELDS = ("parent_id", "title", "request", "summary", "activity", "state", "worker_id", "files", "token",
                 "archived_at", "binding", "binding_reason", "workers", "workers_why")

# Columns added to tables after their first release: (table, column, definition).
# open() adds any a database made before them is missing.
# A thread's `workers` is where its workers run (providers.PLACES), as the
# router decided; "" only in a database from before threads had a place,
# until Orchestrator.start fills it in. `workers_why` is the router's reason.
ADDED_COLUMNS = [("projects", "archived_at", "DOUBLE PRECISION"),
                 ("threads", "workers", "TEXT NOT NULL DEFAULT ''"),
                 ("threads", "activity", "TEXT NOT NULL DEFAULT ''"),
                 ("threads", "archived_at", "DOUBLE PRECISION"),
                 ("workers", "idx", "INTEGER NOT NULL DEFAULT 0"),
                 ("workers", "lost_at", "DOUBLE PRECISION"),
                 ("workers", "lost_reason", "TEXT NOT NULL DEFAULT ''"),
                 ("workers", "lost_log", "TEXT NOT NULL DEFAULT ''"),
                 ("threads", "binding", "TEXT NOT NULL DEFAULT 'unbound'"),
                 ("threads", "binding_reason", "TEXT NOT NULL DEFAULT ''"),
                 ("workers", "state", "TEXT NOT NULL DEFAULT 'ready'"),
                 ("workers", "state_reason", "TEXT NOT NULL DEFAULT ''"),
                 ("events", "peer_id", "TEXT"),
                 ("threads", "busy_since", "DOUBLE PRECISION"),
                 ("threads", "workers_why", "TEXT NOT NULL DEFAULT ''")]


def _thread(row: dict) -> dict:
    row = dict(row)
    row["files"] = json.loads(row.get("files") or "[]")
    return row


def _event(row: dict) -> dict:
    row = dict(row)
    row["payload"] = json.loads(row["payload"])
    return row


def _project(row: dict) -> dict:
    row = dict(row)
    row["settings"] = json.loads(row.get("settings") or "{}")
    return row


class _SqlStore:
    """Shared logic; subclasses supply _exec/_fetch/_fetchone/_insert_event and
    the placeholder style (`?` or `$n`)."""

    notifier: Notifier

    def _q(self, sql: str) -> str:
        return sql

    async def _exec(self, sql: str, *args: Any) -> None: ...
    async def _fetch(self, sql: str, *args: Any) -> list[dict]: ...
    async def _insert_event(self, row: tuple) -> int: ...

    async def _fetchone(self, sql: str, *args: Any) -> dict | None:
        rows = await self._fetch(sql, *args)
        return rows[0] if rows else None

    # projects
    async def create_project(self, name: str, model: str = "", repo_url: str = "",
                             project_id: str | None = None) -> dict:
        p = {"id": project_id or new_id("p"), "name": name, "repo_url": repo_url, "model": model,
             "settings": "{}", "archived_at": None, "created_at": now()}
        cols = ", ".join(p)
        marks = ", ".join("?" for _ in p)
        await self._exec(self._q(f"INSERT INTO projects ({cols}) VALUES ({marks})"), *p.values())
        return _project(p)

    async def list_projects(self, archived: bool = False) -> list[dict]:
        which = "IS NOT NULL" if archived else "IS NULL"
        return [_project(r) for r in
                await self._fetch(f"SELECT * FROM projects WHERE archived_at {which} ORDER BY created_at")]

    async def get_project(self, project_id: str) -> dict | None:
        r = await self._fetchone(self._q("SELECT * FROM projects WHERE id = ?"), project_id)
        return _project(r) if r else None

    async def update_project(self, project_id: str, **fields: Any) -> dict | None:
        bad = set(fields) - PROJECT_FIELDS
        if bad:
            raise ValueError(f"unknown project field {', '.join(sorted(bad))}")
        if fields:
            sets = ", ".join(f"{k} = ?" for k in fields)
            await self._exec(self._q(f"UPDATE projects SET {sets} WHERE id = ?"), *fields.values(), project_id)
        return await self.get_project(project_id)

    # threads
    async def create_thread(self, project_id: str, title: str, request: str, workers: str,
                            parent_id: str | None = None, workers_why: str = "") -> dict:
        t = now()
        row = {"id": new_id("t"), "project_id": project_id, "parent_id": parent_id, "title": title,
               "workers": workers, "workers_why": workers_why,
               "request": request, "summary": "", "activity": "", "state": "queued", "worker_id": None,
               "binding": "unbound", "binding_reason": "", "files": "[]", "token": secrets.token_urlsafe(24), "archived_at": None, "created_at": t, "updated_at": t,
               "busy_since": t}
        cols = ", ".join(row)
        marks = ", ".join("?" for _ in row)
        await self._exec(self._q(f"INSERT INTO threads ({cols}) VALUES ({marks})"), *row.values())
        return _thread(row)

    async def list_threads(self, project_id: str, archived: bool = False) -> list[dict]:
        which = "IS NOT NULL" if archived else "IS NULL"
        rows = await self._fetch(self._q(f"SELECT * FROM threads WHERE project_id = ? AND archived_at {which} "
                                         "ORDER BY CASE WHEN state IN ('queued', 'working') THEN 0 ELSE 1 END, "
                                         "created_at DESC"), project_id)
        return [_thread(r) for r in rows]

    async def get_thread(self, thread_id: str) -> dict | None:
        r = await self._fetchone(self._q("SELECT * FROM threads WHERE id = ?"), thread_id)
        return _thread(r) if r else None

    async def update_thread(self, thread_id: str, **fields: Any) -> dict | None:
        sets, args = [], []
        for k, v in fields.items():
            if k not in THREAD_FIELDS:
                raise ValueError(f"unknown thread field {k}")
            if k == "state" and v not in THREAD_STATES:
                raise ValueError(f"unknown thread state {v}")
            if k == "binding" and v not in BINDING.states:
                raise ValueError(f"unknown thread binding {v}")
            sets.append(f"{k} = ?")
            args.append(json.dumps(v) if k == "files" else v)
        if "state" in fields:
            # busy_since: when its current turn began, kept while it stays in
            # one (queued -> working), NULL when it is not in one. Read from
            # the row as it was, so two updates cannot both start the turn.
            if fields["state"] in RUNNING:
                sets.append(f"busy_since = CASE WHEN state IN ({RUNNING_SQL}) AND busy_since IS NOT NULL "
                            "THEN busy_since ELSE ? END")
                args.append(now())
            else:
                sets.append("busy_since = NULL")
        if sets:
            sets.append("updated_at = ?")
            args += [now(), thread_id]
            await self._exec(self._q(f"UPDATE threads SET {', '.join(sets)} WHERE id = ?"), *args)
        return await self.get_thread(thread_id)

    # events
    async def append_event(self, project_id: str, thread_id: str | None, type: str,
                           payload: dict, peer_id: str | None = None) -> dict:
        t = now()
        seq = await self._insert_event((project_id, thread_id, type, json.dumps(payload), t, peer_id))
        self.notifier.poke(project_id)
        return {"seq": seq, "project_id": project_id, "thread_id": thread_id, "type": type,
                "payload": payload, "created_at": t, "peer_id": peer_id}

    async def _merge_peer_messages(self) -> None:
        """Once, when events gain `peer_id`: a message between threads used to
        be two events, `message_out` on the sender and `message_in` on the
        recipient. The `message_in` becomes the one event, listed by both."""
        threads = {r["id"] for r in await self._fetch("SELECT id FROM threads")}
        for r in await self._fetch("SELECT seq, payload FROM events WHERE type = 'message_in'"):
            frm = json.loads(r["payload"]).get("from")
            if frm in threads:
                await self._exec(self._q("UPDATE events SET peer_id = ? WHERE seq = ?"), frm, r["seq"])
        await self._exec("DELETE FROM events WHERE type = 'message_out'")

    async def list_events(self, project_id: str, thread_id: str | None = None,
                          after: int = 0, limit: int = 500) -> list[dict]:
        if thread_id:
            rows = await self._fetch(self._q("SELECT * FROM events WHERE project_id = ? AND (thread_id = ? OR peer_id = ?) "
                                             "AND seq > ? ORDER BY seq LIMIT ?"),
                                     project_id, thread_id, thread_id, after, limit)
        else:
            rows = await self._fetch(self._q("SELECT * FROM events WHERE project_id = ? AND seq > ? "
                                             "ORDER BY seq LIMIT ?"), project_id, after, limit)
        return [_event(r) for r in rows]

    async def recent_events(self, project_id: str, thread_id: str | None = None, *,
                            limit: int) -> list[dict]:
        if thread_id:
            rows = await self._fetch(self._q("SELECT * FROM events WHERE project_id = ? AND (thread_id = ? OR peer_id = ?) "
                                             "ORDER BY seq DESC LIMIT ?"), project_id, thread_id, thread_id, limit)
        else:
            rows = await self._fetch(self._q("SELECT * FROM events WHERE project_id = ? "
                                             "ORDER BY seq DESC LIMIT ?"), project_id, limit)
        return [_event(r) for r in reversed(rows)]

    async def get_event(self, seq: int) -> dict | None:
        r = await self._fetchone(self._q("SELECT * FROM events WHERE seq = ?"), seq)
        return _event(r) if r else None

    async def delete_events(self, project_id: str, thread_id: str, type: str) -> None:
        await self._exec(self._q("DELETE FROM events WHERE project_id = ? AND thread_id = ? AND type = ?"),
                         project_id, thread_id, type)

    async def wait_events(self, project_id: str, after: int, timeout: float) -> None:
        await self.notifier.wait(project_id, timeout)

    # messages
    async def add_message(self, project_id: str, sender: str, to_thread: str, body: str,
                          reply_to: str | None = None, hops: int = 0) -> dict:
        m = {"id": new_id("m"), "project_id": project_id, "sender": sender, "to_thread": to_thread,
             "body": body, "reply_to": reply_to, "hops": hops, "created_at": now(), "read_at": None}
        cols = ", ".join(m)
        marks = ", ".join("?" for _ in m)
        await self._exec(self._q(f"INSERT INTO messages ({cols}) VALUES ({marks})"), *m.values())
        return m

    async def list_messages(self, thread_id: str, unread_only: bool = False) -> list[dict]:
        sql = "SELECT * FROM messages WHERE to_thread = ?"
        if unread_only:
            sql += " AND read_at IS NULL"
        return await self._fetch(self._q(sql + " ORDER BY created_at"), thread_id)

    async def backlog(self, project_id: str) -> dict[str, int]:
        rows = await self._fetch(self._q(
            "SELECT m.to_thread AS id, COUNT(*) AS n FROM messages m JOIN threads t ON t.id = m.to_thread "
            "WHERE t.project_id = ? AND t.archived_at IS NULL "
            "AND (m.read_at IS NULL OR (t.busy_since IS NOT NULL AND m.created_at >= t.busy_since)) "
            "GROUP BY m.to_thread"), project_id)
        return {r["id"]: int(r["n"]) for r in rows}

    async def mark_read(self, message_ids: list[str]) -> None:
        t = now()
        for mid in message_ids:
            await self._exec(self._q("UPDATE messages SET read_at = ? WHERE id = ?"), t, mid)

    # images
    async def add_image(self, project_id: str, media_type: str, data: bytes, name: str = "") -> dict:
        i = {"id": new_id("i"), "media_type": media_type, "name": name, "size": len(data)}
        await self._exec(self._q("INSERT INTO images (id, project_id, media_type, name, size, data, created_at) "
                                 "VALUES (?, ?, ?, ?, ?, ?, ?)"),
                         i["id"], project_id, media_type, name, i["size"], data, now())
        return i

    async def get_image(self, image_id: str) -> dict | None:
        return await self._fetchone(self._q("SELECT * FROM images WHERE id = ?"), image_id)

    # workers
    async def upsert_worker(self, worker: dict) -> None:
        w = {"id": worker["id"], "project_id": "", "provider": worker["provider"],
             "handle": worker["handle"], "url": worker["url"], "token": worker.get("token", ""),
             "capacity": worker.get("capacity", 16), "idx": worker.get("index", 0),
             "created_at": worker.get("created_at", now())}
        await self._exec(self._q("DELETE FROM workers WHERE id = ?"), w["id"])
        cols = ", ".join(w)
        marks = ", ".join("?" for _ in w)
        await self._exec(self._q(f"INSERT INTO workers ({cols}) VALUES ({marks})"), *w.values())

    async def lose_worker(self, worker_id: str, reason: str, log: str) -> None:
        await self._exec(self._q("UPDATE workers SET lost_at = ?, lost_reason = ?, lost_log = ?, state = 'lost', "
                                 "state_reason = ? WHERE id = ?"), now(), reason, log, reason, worker_id)

    async def set_worker_state(self, worker_id: str, state: str, reason: str) -> None:
        if state not in WORKER.states:
            raise ValueError(f"unknown worker state {state}")
        await self._exec(self._q("UPDATE workers SET state = ?, state_reason = ? WHERE id = ?"),
                         state, reason, worker_id)

    async def list_workers(self) -> list[dict]:
        return await self._fetch(self._q("SELECT * FROM workers ORDER BY created_at"))

    async def projects_on_worker(self, worker_id: str) -> list[str]:
        rows = await self._fetch(self._q("SELECT DISTINCT project_id FROM threads WHERE worker_id = ? "
                                         "ORDER BY project_id"), worker_id)
        return [r["project_id"] for r in rows]


class SqliteStore(_SqlStore):
    def __init__(self, path: str | Path = paths.DB) -> None:
        self.path = path
        self.notifier = Notifier()
        self.db = None

    async def open(self) -> None:
        import aiosqlite

        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.db = await aiosqlite.connect(self.path)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA journal_mode=WAL")
        for stmt in SCHEMA + [SQLITE_EVENTS, EVENTS_INDEX, SQLITE_IMAGES]:
            await self.db.execute(stmt)
        added = set()
        for table, column, definition in ADDED_COLUMNS:
            async with self.db.execute(f"PRAGMA table_info({table})") as cur:
                have = {r["name"] for r in await cur.fetchall()}
            if column not in have:
                await self.db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                added.add((table, column))
        await self.db.commit()
        if ("events", "peer_id") in added:
            await self._merge_peer_messages()

    async def close(self) -> None:
        if self.db is not None:
            await self.db.close()

    async def _exec(self, sql: str, *args: Any) -> None:
        await self.db.execute(sql, args)
        await self.db.commit()

    async def _fetch(self, sql: str, *args: Any) -> list[dict]:
        async with self.db.execute(sql, args) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def _insert_event(self, row: tuple) -> int:
        cur = await self.db.execute("INSERT INTO events (project_id, thread_id, type, payload, created_at, "
                                    "peer_id) VALUES (?, ?, ?, ?, ?, ?)", row)
        await self.db.commit()
        return cur.lastrowid


class PostgresStore(_SqlStore):
    CHANNEL = "orchestrator_events"

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.notifier = Notifier()
        self.pool = None
        self._listener = None

    def _q(self, sql: str) -> str:
        out, n = [], 0
        for ch in sql:
            if ch == "?":
                n += 1
                out.append(f"${n}")
            else:
                out.append(ch)
        return "".join(out)

    async def open(self) -> None:
        import asyncpg

        self.pool = await asyncpg.create_pool(self.dsn)
        async with self.pool.acquire() as c:
            for stmt in SCHEMA + [PG_EVENTS, EVENTS_INDEX, PG_IMAGES]:
                await c.execute(stmt)
            had_peer_id = await c.fetchval("SELECT 1 FROM information_schema.columns "
                                           "WHERE table_name = 'events' AND column_name = 'peer_id'")
            for table, column, definition in ADDED_COLUMNS:
                await c.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}")
        if not had_peer_id:
            await self._merge_peer_messages()
        # A dedicated connection hears appends made by any instance.
        self._listener = await asyncpg.connect(self.dsn)
        await self._listener.add_listener(self.CHANNEL, lambda *a: self.notifier.poke(a[3]))

    async def close(self) -> None:
        if self._listener is not None:
            await self._listener.close()
        if self.pool is not None:
            await self.pool.close()

    async def _exec(self, sql: str, *args: Any) -> None:
        async with self.pool.acquire() as c:
            await c.execute(sql, *args)

    async def _fetch(self, sql: str, *args: Any) -> list[dict]:
        async with self.pool.acquire() as c:
            return [dict(r) for r in await c.fetch(sql, *args)]

    async def _insert_event(self, row: tuple) -> int:
        async with self.pool.acquire() as c:
            seq = await c.fetchval("INSERT INTO events (project_id, thread_id, type, payload, created_at, peer_id) "
                                   "VALUES ($1, $2, $3, $4, $5, $6) RETURNING seq", *row)
            await c.execute("SELECT pg_notify($1, $2)", self.CHANNEL, row[0])
            return seq


def store_from_url(url: str | None) -> Store:
    if url and url.startswith(("postgres://", "postgresql://")):
        return PostgresStore(url)
    path = url.removeprefix("sqlite:///") if url else paths.DB
    return SqliteStore(path)
