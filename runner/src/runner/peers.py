"""How a thread reaches the rest of the system: other threads and the orchestrator.

The orchestrator owns the thread directory and the message bus. A runner talks
to it through `HttpPeers`. `LocalPeers` keeps everything in-process, for a
runner used on its own (CLI, tests).
"""

from __future__ import annotations

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
