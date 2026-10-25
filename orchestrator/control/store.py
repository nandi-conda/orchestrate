"""Storage for the control plane.

`Store` is the interface the rest of the control plane uses. Two backends:

- `SqliteStore`: the default, for local dev and tests. One process only.
- `PostgresStore`: chosen when `--db` is a postgres:// URL. Several
  control plane instances can share it; appends are announced with
  LISTEN/NOTIFY so every instance wakes its SSE streams and inboxes.

A thread is archived when `archived_at` is set. `list_threads` lists either
the live threads (the default: the main list, the router's directory, peers)
or only the archived ones. Working threads (queued or working) come first,
then the rest; each group newest first, so a thread moves up when its turn
starts and back when it ends, and nothing else reshuffles.

Events carry a project-wide sequence number (`seq`), so a client can resume a
project or thread stream from `?after=seq`.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import time
from collections import defaultdict
from typing import Any, Protocol

THREAD_STATES = ("queued", "working", "waiting", "idle", "done", "failed", "stopped")


def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(4)}"


def now() -> float:
    return time.time()


class Store(Protocol):
    async def open(self) -> None: ...
    async def close(self) -> None: ...

    async def create_project(self, name: str, repo_url: str = "", model: str = "") -> dict: ...
    async def list_projects(self) -> list[dict]: ...
    async def get_project(self, project_id: str) -> dict | None: ...

    async def create_thread(self, project_id: str, title: str, request: str,
                            parent_id: str | None = None) -> dict: ...
    async def list_threads(self, project_id: str, archived: bool = False) -> list[dict]:
        """The project's threads that are not archived, or with archived=True
        only the archived ones; working (queued or working) threads first,
        then the rest, each group newest first."""
    async def get_thread(self, thread_id: str) -> dict | None: ...
    async def update_thread(self, thread_id: str, **fields: Any) -> dict | None: ...

    async def append_event(self, project_id: str, thread_id: str | None, type: str,
                           payload: dict) -> dict: ...
    async def list_events(self, project_id: str, thread_id: str | None = None,
                          after: int = 0, limit: int = 500) -> list[dict]: ...
    async def get_event(self, seq: int) -> dict | None: ...
    async def wait_events(self, project_id: str, after: int, timeout: float) -> None:
        """Return when an event with seq > after may exist, or on timeout."""

    async def add_message(self, project_id: str, sender: str, to_thread: str, body: str,
                          reply_to: str | None = None, hops: int = 0) -> dict: ...
    async def list_messages(self, thread_id: str, unread_only: bool = False) -> list[dict]: ...
    async def mark_read(self, message_ids: list[str]) -> None: ...

    async def upsert_worker(self, worker: dict) -> None: ...
    async def lose_worker(self, worker_id: str) -> None:
        """Mark a worker lost (destroyed); it is never attached to again."""
    async def list_workers(self, project_id: str) -> list[dict]:
        """Every worker made for the project; `idx` is the number its worker
        spec was made with, `lost_at` is set once it was lost."""


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
        created_at DOUBLE PRECISION NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS threads (
        id TEXT PRIMARY KEY, project_id TEXT NOT NULL, parent_id TEXT,
        title TEXT NOT NULL, request TEXT NOT NULL DEFAULT '', summary TEXT NOT NULL DEFAULT '',
        activity TEXT NOT NULL DEFAULT '', state TEXT NOT NULL DEFAULT 'queued', worker_id TEXT, files TEXT NOT NULL DEFAULT '[]',
        token TEXT NOT NULL DEFAULT '', archived_at DOUBLE PRECISION, created_at DOUBLE PRECISION NOT NULL,
        updated_at DOUBLE PRECISION NOT NULL)""",
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
        created_at DOUBLE PRECISION NOT NULL, lost_at DOUBLE PRECISION)""",
]

SQLITE_EVENTS = """CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, project_id TEXT NOT NULL, thread_id TEXT,
    type TEXT NOT NULL, payload TEXT NOT NULL, created_at DOUBLE PRECISION NOT NULL)"""
PG_EVENTS = """CREATE TABLE IF NOT EXISTS events (
    seq BIGSERIAL PRIMARY KEY, project_id TEXT NOT NULL, thread_id TEXT,
    type TEXT NOT NULL, payload TEXT NOT NULL, created_at DOUBLE PRECISION NOT NULL)"""
EVENTS_INDEX = "CREATE INDEX IF NOT EXISTS events_project ON events(project_id, seq)"

THREAD_FIELDS = ("parent_id", "title", "request", "summary", "activity", "state", "worker_id", "files", "token",
                 "archived_at")

# Columns added to tables after their first release: (table, column, definition).
# open() adds any a database made before them is missing.
ADDED_COLUMNS = [("threads", "activity", "TEXT NOT NULL DEFAULT ''"),
                 ("threads", "archived_at", "DOUBLE PRECISION"),
                 ("workers", "idx", "INTEGER NOT NULL DEFAULT 0"),
                 ("workers", "lost_at", "DOUBLE PRECISION")]


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
    async def create_project(self, name: str, repo_url: str = "", model: str = "") -> dict:
        p = {"id": new_id("p"), "name": name, "repo_url": repo_url, "model": model,
             "settings": "{}", "created_at": now()}
        await self._exec(self._q("INSERT INTO projects (id, name, repo_url, model, settings, created_at) "
                                 "VALUES (?, ?, ?, ?, ?, ?)"), *p.values())
        return _project(p)

    async def list_projects(self) -> list[dict]:
        return [_project(r) for r in await self._fetch("SELECT * FROM projects ORDER BY created_at")]

    async def get_project(self, project_id: str) -> dict | None:
        r = await self._fetchone(self._q("SELECT * FROM projects WHERE id = ?"), project_id)
        return _project(r) if r else None

    # threads
    async def create_thread(self, project_id: str, title: str, request: str,
                            parent_id: str | None = None) -> dict:
        t = now()
        row = {"id": new_id("t"), "project_id": project_id, "parent_id": parent_id, "title": title,
               "request": request, "summary": "", "activity": "", "state": "queued", "worker_id": None,
               "files": "[]", "token": secrets.token_urlsafe(24), "archived_at": None, "created_at": t, "updated_at": t}
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
            sets.append(f"{k} = ?")
            args.append(json.dumps(v) if k == "files" else v)
        if sets:
            sets.append("updated_at = ?")
            args += [now(), thread_id]
            await self._exec(self._q(f"UPDATE threads SET {', '.join(sets)} WHERE id = ?"), *args)
        return await self.get_thread(thread_id)

    # events
    async def append_event(self, project_id: str, thread_id: str | None, type: str,
                           payload: dict) -> dict:
        t = now()
        seq = await self._insert_event((project_id, thread_id, type, json.dumps(payload), t))
        self.notifier.poke(project_id)
        return {"seq": seq, "project_id": project_id, "thread_id": thread_id, "type": type,
                "payload": payload, "created_at": t}

    async def list_events(self, project_id: str, thread_id: str | None = None,
                          after: int = 0, limit: int = 500) -> list[dict]:
        if thread_id:
            rows = await self._fetch(self._q("SELECT * FROM events WHERE project_id = ? AND thread_id = ? "
                                             "AND seq > ? ORDER BY seq LIMIT ?"),
                                     project_id, thread_id, after, limit)
        else:
            rows = await self._fetch(self._q("SELECT * FROM events WHERE project_id = ? AND seq > ? "
                                             "ORDER BY seq LIMIT ?"), project_id, after, limit)
        return [_event(r) for r in rows]

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

    async def mark_read(self, message_ids: list[str]) -> None:
        t = now()
        for mid in message_ids:
            await self._exec(self._q("UPDATE messages SET read_at = ? WHERE id = ?"), t, mid)

    # workers
    async def upsert_worker(self, worker: dict) -> None:
        w = {"id": worker["id"], "project_id": worker["project_id"], "provider": worker["provider"],
             "handle": worker["handle"], "url": worker["url"], "token": worker.get("token", ""),
             "capacity": worker.get("capacity", 16), "idx": worker.get("index", 0),
             "created_at": worker.get("created_at", now())}
        await self._exec(self._q("DELETE FROM workers WHERE id = ?"), w["id"])
        cols = ", ".join(w)
        marks = ", ".join("?" for _ in w)
        await self._exec(self._q(f"INSERT INTO workers ({cols}) VALUES ({marks})"), *w.values())

    async def lose_worker(self, worker_id: str) -> None:
        await self._exec(self._q("UPDATE workers SET lost_at = ? WHERE id = ?"), now(), worker_id)

    async def list_workers(self, project_id: str) -> list[dict]:
        return await self._fetch(self._q("SELECT * FROM workers WHERE project_id = ? ORDER BY created_at"),
                                 project_id)


class SqliteStore(_SqlStore):
    def __init__(self, path: str = "orchestrator.db") -> None:
        self.path = path
        self.notifier = Notifier()
        self.db = None

    async def open(self) -> None:
        import aiosqlite

        self.db = await aiosqlite.connect(self.path)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA journal_mode=WAL")
        for stmt in SCHEMA + [SQLITE_EVENTS, EVENTS_INDEX]:
            await self.db.execute(stmt)
        for table, column, definition in ADDED_COLUMNS:
            async with self.db.execute(f"PRAGMA table_info({table})") as cur:
                have = {r["name"] for r in await cur.fetchall()}
            if column not in have:
                await self.db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        await self.db.commit()

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
        cur = await self.db.execute("INSERT INTO events (project_id, thread_id, type, payload, created_at) "
                                    "VALUES (?, ?, ?, ?, ?)", row)
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
            for stmt in SCHEMA + [PG_EVENTS, EVENTS_INDEX]:
                await c.execute(stmt)
            for table, column, definition in ADDED_COLUMNS:
                await c.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}")
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
            seq = await c.fetchval("INSERT INTO events (project_id, thread_id, type, payload, created_at) "
                                   "VALUES ($1, $2, $3, $4, $5) RETURNING seq", *row)
            await c.execute("SELECT pg_notify($1, $2)", self.CHANNEL, row[0])
            return seq


def store_from_url(url: str | None) -> Store:
    if url and url.startswith(("postgres://", "postgresql://")):
        return PostgresStore(url)
    path = url.removeprefix("sqlite:///") if url else "orchestrator.db"
    return SqliteStore(path)
