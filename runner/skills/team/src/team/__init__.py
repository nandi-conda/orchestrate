"""A thread's calls to the rest of the system, answered by its runner.

The kernel runs in the thread's sandbox; the runner, outside it, listens on a
unix socket bound into the sandbox at $ORCHESTRATE_TEAM_SOCKET. Each call is
one connection: a JSON line `{"op", "args"}` out, a JSON line `{"ok",
"result"}` or `{"ok": false, "error"}` back. The socket is the thread's own,
so the runner knows who is calling.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

LIMIT = 64 * 1024 * 1024  # a fetched page or a thread's events fit in one line


class TeamError(RuntimeError):
    pass


async def _call(op: str, **args: Any) -> Any:
    reader, writer = await asyncio.open_unix_connection(os.environ["ORCHESTRATE_TEAM_SOCKET"], limit=LIMIT)
    try:
        writer.write(json.dumps({"op": op, "args": args}).encode() + b"\n")
        await writer.drain()
        line = await reader.readline()
    finally:
        writer.close()
    if not line:
        raise TeamError(f"{op}: the runner closed the connection")
    reply = json.loads(line)
    if not reply.get("ok"):
        raise TeamError(f"{op}: {reply.get('error') or 'failed'}")
    return reply.get("result")


async def list_threads(query: str | None = None) -> list[dict]:
    return await _call("list_threads", query=query)


async def read_thread(id: str, last_n: int = 20) -> dict:
    return await _call("read_thread", id=id, last_n=last_n)


async def send_message(to: str, body: str, reply_to: str | None = None) -> dict:
    return await _call("send_message", to=to, body=body, reply_to=reply_to)


async def spawn_thread(title: str, request: str) -> dict:
    return await _call("spawn_thread", title=title, request=request)


async def set_summary(summary: str) -> None:
    await _call("set_summary", summary=summary)


async def web_search(query: str, count: int = 10, question: str | None = None) -> list[dict]:
    return await _call("web_search", query=query, count=count, question=question)


async def web_fetch(url: str) -> dict:
    return await _call("web_fetch", url=url)
