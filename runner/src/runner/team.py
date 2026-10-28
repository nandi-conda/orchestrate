"""The runner's side of the team skill (runner/skills/team): a thread's calls
to its peers and the web, from its kernel in the sandbox.

Each thread has a unix socket of its own, bound into its sandbox; the kernel
connects once per call, writes `{"op", "args"}` as a JSON line and reads
`{"ok", "result"}` or `{"ok": false, "error"}` back. Whoever connects is that
thread: the socket is its identity, and nothing else of the runner is
reachable from the sandbox.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .thread import AgentThread

log = logging.getLogger("runner.team")

LIMIT = 16 * 1024 * 1024


class TeamError(RuntimeError):
    pass


async def _list_threads(t: AgentThread, query: str | None = None) -> list[dict]:
    rows = await t.peers.list_threads(t.id, t.project, query)
    return [r for r in rows if r.get("id") != t.id]


async def _read_thread(t: AgentThread, id: str, last_n: int = 20) -> dict:
    return await t.peers.read_thread(t.id, id, int(last_n))


async def _send_message(t: AgentThread, to: str, body: str, reply_to: str | None = None) -> dict:
    return await t.peers.send_message(t.id, to, body, reply_to)


async def _spawn_thread(t: AgentThread, title: str, request: str) -> dict:
    if t.spawned >= t.limits.max_spawn:
        raise TeamError(f"spawn limit ({t.limits.max_spawn}) reached for this thread")
    t.spawned += 1
    return await t.peers.spawn_thread(t.id, t.project, title, request)


async def _set_summary(t: AgentThread, summary: str) -> None:
    t.summary = summary
    await t.peers.set_summary(t.id, t.summary, sorted(t.files))


async def _objectives(t: AgentThread) -> list[dict]:
    return t.objectives


async def _update_objective(t: AgentThread, id: str, note: str) -> list[dict]:
    return t.move_objective(id, note=note)


async def _web_search(t: AgentThread, query: str, count: int = 10, question: str | None = None) -> list[dict]:
    return await t.web.search(query, int(count), question)


async def _web_fetch(t: AgentThread, url: str) -> dict:
    return await t.web.fetch(url)


OPS = {"list_threads": _list_threads, "read_thread": _read_thread, "send_message": _send_message,
       "spawn_thread": _spawn_thread, "set_summary": _set_summary, "objectives": _objectives,
       "update_objective": _update_objective, "web_search": _web_search,
       "web_fetch": _web_fetch}


async def answer(t: AgentThread, line: bytes) -> dict:
    try:
        call = json.loads(line)
        op = OPS[call["op"]]
        return {"ok": True, "result": await op(t, **(call.get("args") or {}))}
    except KeyError as e:
        return {"ok": False, "error": f"unknown call or missing field {e}"}
    except TypeError as e:  # arguments the op does not take
        return {"ok": False, "error": str(e)}
    except Exception as e:  # noqa: BLE001 - the call's failure goes back to the model, not up the stack
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


class TeamServer:
    """A thread's team socket."""

    def __init__(self, t: AgentThread, path: str):
        self.t, self.path = t, path
        self.server: asyncio.Server | None = None

    async def start(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        if os.path.exists(self.path):
            os.unlink(self.path)  # the socket of an earlier run of this thread
        self.server = await asyncio.start_unix_server(self._serve, self.path, limit=LIMIT)

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if line:
                writer.write(json.dumps(await answer(self.t, line), default=str).encode() + b"\n")
                await writer.drain()
        except Exception:
            log.exception("team call from thread %s failed", self.t.id)
        finally:
            writer.close()

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        if os.path.exists(self.path):
            os.unlink(self.path)
