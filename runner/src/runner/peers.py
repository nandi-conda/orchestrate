"""How a thread reaches the rest of the system: other threads and the orchestrator.

The orchestrator owns the thread directory and the message bus. A runner talks
to it through `HttpPeers` when it can dial the orchestrator, or through
`PulledPeers` when it cannot (a control plane on a laptop): then the calls wait
in a `PeerOutbox` until the orchestrator collects them. `LocalPeers` keeps
everything in-process, for a runner used on its own (CLI, tests).
"""

from __future__ import annotations

import asyncio
import secrets
from typing import Any, Callable, Awaitable, Protocol

import httpx


class Peers(Protocol):
    async def list_threads(self, project: str, query: str | None = None) -> list[dict]: ...
    async def read_thread(self, thread_id: str, last_n: int = 20) -> dict: ...
    async def send_message(self, sender: str, to: str, body: str, reply_to: str | None = None) -> dict: ...
    async def spawn_thread(self, parent: str, project: str, title: str, request: str) -> dict: ...
    async def set_summary(self, thread_id: str, summary: str, files: list[str] | None = None) -> None: ...


class HttpPeers:
    """Calls the orchestrator's peer API (see README: "Orchestrator peer API")."""

    def __init__(self, base_url: str, token: str, client: httpx.AsyncClient | None = None):
        self.base = base_url.rstrip("/")
        self.client = client or httpx.AsyncClient(timeout=30.0)
        self.headers = {"Authorization": f"Bearer {token}"}

    async def _req(self, method: str, path: str, **kw) -> Any:
        r = await self.client.request(method, f"{self.base}{path}", headers=self.headers, **kw)
        r.raise_for_status()
        return r.json() if r.content else None

    async def list_threads(self, project, query=None):
        return await self._req("GET", f"/api/peer/projects/{project}/threads", params={"q": query} if query else None)

    async def read_thread(self, thread_id, last_n=20):
        return await self._req("GET", f"/api/peer/threads/{thread_id}", params={"last_n": last_n})

    async def send_message(self, sender, to, body, reply_to=None):
        return await self._req("POST", "/api/peer/messages", json={"from": sender, "to": to, "body": body, "reply_to": reply_to})

    async def spawn_thread(self, parent, project, title, request):
        return await self._req("POST", f"/api/peer/projects/{project}/threads", json={"parent": parent, "title": title, "request": request})

    async def set_summary(self, thread_id, summary, files=None):
        await self._req("PUT", f"/api/peer/threads/{thread_id}/summary", json={"summary": summary, "files": files or []})


class PeerOutbox:
    """Peer API calls waiting for the orchestrator to collect them.

    The orchestrator long-polls `GET /peer/calls` (runner/API.md), runs each
    call against its own peer API and posts the answer to `/peer/calls/{id}`.
    Every connection is opened by the orchestrator, so the worker never needs
    to reach it. A call handed out but not answered within `lease` seconds
    (a dropped poll response) is handed out again.
    """

    def __init__(self, lease: float = 60.0) -> None:
        self.lease = lease
        self.calls: dict[str, dict] = {}
        self.leased: dict[str, float] = {}
        self.futures: dict[str, asyncio.Future] = {}
        self.cond = asyncio.Condition()

    async def call(self, method: str, path: str, params: dict | None, json: Any, timeout: float) -> tuple[int, Any]:
        cid = f"pc_{secrets.token_hex(6)}"
        fut = asyncio.get_running_loop().create_future()
        self.futures[cid] = fut
        async with self.cond:
            self.calls[cid] = {"id": cid, "method": method, "path": path, "params": params, "json": json}
            self.cond.notify_all()
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self.calls.pop(cid, None)
            self.leased.pop(cid, None)
            self.futures.pop(cid, None)

    async def take(self, wait: float = 25.0) -> list[dict]:
        """Calls due for handing out, waiting up to `wait` seconds for one."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait
        async with self.cond:
            while True:
                now = loop.time()
                due = [c for cid, c in self.calls.items() if self.leased.get(cid, 0.0) <= now]
                if due or now >= deadline:
                    break
                try:
                    await asyncio.wait_for(self.cond.wait(), deadline - now)
                except asyncio.TimeoutError:
                    pass
            for c in due:
                self.leased[c["id"]] = now + self.lease
            return due

    def answer(self, cid: str, status: int, body: Any) -> bool:
        fut = self.futures.get(cid)
        if fut is None or fut.done():
            return False
        fut.set_result((status, body))
        return True


class PulledPeers(HttpPeers):
    """The peer API, carried over connections the orchestrator opens."""

    def __init__(self, outbox: PeerOutbox | None = None, timeout: float = 900.0):
        # spawn_thread can wait on a new worker being created, hence the long timeout
        self.outbox = outbox or PeerOutbox()
        self.timeout = timeout

    async def _req(self, method: str, path: str, params: dict | None = None, json: Any = None, **kw) -> Any:
        try:
            status, body = await self.outbox.call(method, path, params, json, self.timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(f"orchestrator did not collect peer call {method} {path} "
                               f"within {self.timeout:.0f}s") from None
        if status >= 400:
            raise RuntimeError(f"peer API {method} {path} failed ({status}): {str(body)[:300]}")
        return body


class LocalPeers:
    """In-process directory and bus over the threads of a single runner."""

    def __init__(self):
        self.host: Any = None  # set by ThreadHost
        self.spawn_hook: Callable[..., Awaitable[dict]] | None = None

    def _threads(self):
        return self.host.threads if self.host else {}

    async def list_threads(self, project, query=None):
        out = []
        for t in self._threads().values():
            if t.project != project:
                continue
            d = t.directory_entry()
            if query and query.lower() not in (d["title"] + " " + d["summary"]).lower():
                continue
            out.append(d)
        return out

    async def read_thread(self, thread_id, last_n=20):
        t = self._threads().get(thread_id)
        if not t:
            return {"error": f"no thread {thread_id}"}
        return {**t.directory_entry(), "events": t.events[-last_n:]}

    async def send_message(self, sender, to, body, reply_to=None):
        t = self._threads().get(to)
        if not t:
            return {"ok": False, "error": f"no thread {to}"}
        t.deliver(body, sender=sender)
        return {"ok": True}

    async def spawn_thread(self, parent, project, title, request):
        t = await self.host.start(project=project, title=title, request=request, parent=parent)
        return {"id": t.id, "title": t.title}

    async def set_summary(self, thread_id, summary, files=None):
        t = self._threads().get(thread_id)
        if t:
            t.summary = summary
            if files:
                t.files.update(files)
