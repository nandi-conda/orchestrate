"""How a thread reaches the rest of the system: other threads and the orchestrator.

The orchestrator owns the thread directory and the message bus, and it is never
reachable from a worker: it opens every connection. So a runner served under an
orchestrator uses `OrchestratorPeers`, which queues each peer call in a
`PeerOutbox`; the orchestrator long-polls `GET /peer/calls`, runs the calls and
posts the answers back (runner/API.md, "Peer calls"). `LocalPeers` keeps
everything in-process, for a runner used on its own (CLI, tests).
"""

from __future__ import annotations

import asyncio
import secrets
from typing import Any, Callable, Awaitable, Protocol


class Peers(Protocol):
    """Every call names the thread making it (`caller`, `sender`, `parent` or
    `thread_id`): a worker is shared by every project, and the orchestrator
    answers each call within the caller's project."""

    async def list_threads(self, caller: str, project: str, query: str | None = None) -> list[dict]: ...
    async def read_thread(self, caller: str, thread_id: str, last_n: int = 20) -> dict: ...
    async def send_message(self, sender: str, to: str, body: str, reply_to: str | None = None) -> dict: ...
    async def report(self, thread_id: str, summary: str) -> dict:
        """Tell the thread's parent it finished: `{"ok": true, "delivered"}`,
        `delivered` false when the parent is archived and takes no messages."""
    async def spawn_thread(self, parent: str, project: str, title: str, request: str) -> dict: ...
    async def set_summary(self, thread_id: str, summary: str, files: list[str] | None = None) -> None: ...
    async def save(self, thread_id: str, land: bool = False) -> dict:
        """Take the thread's branch and snapshot (runner/API.md, "Repository");
        with `land`, land the branch: `{"landed", "base", "commit"?,
        "conflict"?, "message"?}`."""


class PeerOutbox:
    """Peer calls waiting for the orchestrator to collect them.

    A long poll returns as soon as a call is queued, so a call costs one round
    trip from the orchestrator's side. A call handed out but not answered within
    `lease` seconds (a poll response lost on the way) is handed out again.
    """

    def __init__(self, lease: float = 60.0) -> None:
        self.lease = lease
        self.calls: dict[str, dict] = {}
        self.leased: dict[str, float] = {}
        self.futures: dict[str, asyncio.Future] = {}
        self.cond = asyncio.Condition()

    async def call(self, op: str, thread: str, args: dict, timeout: float) -> dict:
        """Queue thread `thread`'s call and wait for its answer."""
        cid = f"pc_{secrets.token_hex(6)}"
        fut = asyncio.get_running_loop().create_future()
        self.futures[cid] = fut
        async with self.cond:
            self.calls[cid] = {"id": cid, "thread": thread, "op": op, "args": args}
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

    def answer(self, cid: str, reply: dict) -> bool:
        fut = self.futures.get(cid)
        if fut is None or fut.done():
            return False
        fut.set_result(reply)
        return True


class PeerError(RuntimeError):
    pass


class OrchestratorPeers:
    """The peer API, carried over connections the orchestrator opens."""

    def __init__(self, outbox: PeerOutbox | None = None, timeout: float = 900.0):
        # spawn_thread can wait on a new worker being created, hence the long timeout
        self.outbox = outbox or PeerOutbox()
        self.timeout = timeout

    async def _call(self, op: str, thread: str, **args: Any) -> Any:
        try:
            reply = await self.outbox.call(op, thread, args, self.timeout)
        except asyncio.TimeoutError:
            raise PeerError(f"the orchestrator did not answer {op} within {self.timeout:.0f}s") from None
        if not reply.get("ok"):
            raise PeerError(f"{op}: {reply.get('error') or 'failed'}")
        return reply.get("result")

    async def list_threads(self, caller, project, query=None):
        return await self._call("list_threads", caller, project=project, query=query)

    async def read_thread(self, caller, thread_id, last_n=20):
        return await self._call("read_thread", caller, thread_id=thread_id, last_n=last_n)

    async def send_message(self, sender, to, body, reply_to=None):
        return await self._call("send_message", sender, sender=sender, to=to, body=body, reply_to=reply_to)

    async def report(self, thread_id, summary):
        return await self._call("report", thread_id, thread_id=thread_id, summary=summary)

    async def spawn_thread(self, parent, project, title, request):
        return await self._call("spawn_thread", parent, parent=parent, project=project, title=title,
                                request=request)

    async def set_summary(self, thread_id, summary, files=None):
        await self._call("set_summary", thread_id, thread_id=thread_id, summary=summary, files=files or [])

    async def save(self, thread_id, land=False):
        return await self._call("save", thread_id, thread_id=thread_id, land=land)


class LocalPeers:
    """In-process directory and bus over the threads of a single runner."""

    def __init__(self):
        self.host: Any = None  # set by ThreadHost
        self.spawn_hook: Callable[..., Awaitable[dict]] | None = None

    def _threads(self):
        return self.host.threads if self.host else {}

    async def list_threads(self, caller, project, query=None):
        out = []
        for t in self._threads().values():
            if t.project != project:
                continue
            d = t.directory_entry()
            if query and query.lower() not in (d["title"] + " " + d["summary"]).lower():
                continue
            out.append(d)
        return out

    async def read_thread(self, caller, thread_id, last_n=20):
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

    async def report(self, thread_id, summary):
        t = self._threads().get(thread_id)
        parent = self._threads().get(t.parent) if t and t.parent else None
        if parent is None:
            return {"ok": False, "error": f"no parent of thread {thread_id}"}
        parent.deliver(f"Finished: {summary}", sender=thread_id)
        return {"ok": True, "delivered": True}

    async def spawn_thread(self, parent, project, title, request):
        t = await self.host.start(project=project, title=title, request=request, parent=parent)
        return {"id": t.id, "title": t.title}

    async def set_summary(self, thread_id, summary, files=None):
        t = self._threads().get(thread_id)
        if t:
            t.summary = summary
            if files:
                t.files.update(files)

    async def save(self, thread_id, land=False):
        """No hub here: the worker's repository is the project's, and landing
        moves its `origin/<base>`."""
        ws = self._threads()[thread_id].workspace
        if land:
            return await ws.land_here()
        return {"landed": False, "base": ws.repo.base}
