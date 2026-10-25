"""Agent threads and the host that runs many of them in one process."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from . import tools as T
from .model import Model
from .peers import LocalPeers, Peers
from .repo import GitError, Repo, Workspace
from .sandbox import LocalSandbox, Sandbox

SYSTEM_PROMPT = """You are one thread in a team of agent threads working on the project "{project}".
Your thread id is {id}. Your title: {title}.
Your workspace is a git worktree of the project at {root}; your branch is {branch}.

How the team works:
- Many threads run at once. Spawn a new thread for independent work instead of doing it yourself.
- Use list_threads before touching shared areas; if another thread owns it, send_message instead of editing.
- Messages from other threads and the user arrive as user messages that start with [from ...].
- Keep your directory entry current with set_summary when your focus changes.
- When the work for this request is done, call finish with a summary. You may get follow-ups later.

Commits: commit your work on {branch} with clear messages; do not push. At the end of every
turn the runner commits anything left over and pushes your branch. When you call finish it
lands your branch on the project's main branch (rebase, fast-forward push). If that rebase
conflicts you get a message from "runner" listing the files: rebase onto the named branch
yourself, resolve, `git rebase --continue`, and call finish again.
"""


@dataclass
class Limits:
    max_steps: int = 60
    max_spawn: int = 8
    max_hops: int = 6
    tool_timeout: float = 600.0


class DeltaBuffer:
    """Gathers a streaming reply into `delta` events: one per kind (and tool) at
    a time, sent every `every` seconds or `size` characters, so the UI follows
    the model without an event per token."""

    def __init__(self, emit: Callable[[str, Any], None], every: float = 0.25, size: int = 400):
        self._emit, self.every, self.size = emit, every, size
        self.kind, self.name, self.parts = "", "", []
        self.sent = time.monotonic()

    def __call__(self, kind: str, text: str, name: str = "") -> None:
        if (kind, name) != (self.kind, self.name):
            self.flush()
            self.kind, self.name = kind, name
            if kind == "tool":  # a tool call starts: say which, before its arguments arrive
                self._emit("delta", {"kind": kind, "name": name, "text": ""})
        if text:
            self.parts.append(text)
        if sum(map(len, self.parts)) >= self.size or time.monotonic() - self.sent >= self.every:
            self.flush()

    def flush(self) -> None:
        if self.parts:
            data = {"kind": self.kind, "text": "".join(self.parts)}
            if self.kind == "tool":
                data["name"] = self.name
            self._emit("delta", data)
            self.parts = []
        self.sent = time.monotonic()


log = logging.getLogger("runner")


def _id() -> str:
    return "t_" + uuid.uuid4().hex[:10]


class AgentThread:
    def __init__(self, *, id: str, project: str, title: str, model: Model, sandbox: Sandbox, peers: Peers,
                 tools: list[T.Tool], limits: Limits, parent: str | None, workspace: Workspace | None,
                 emit: Callable[["AgentThread", str, Any], None]):
        self.id, self.project, self.title, self.parent = id, project, title, parent
        self.workspace = workspace
        self.branch = workspace.branch if workspace else None
        self.conflicts = 0  # landings in a row that hit a conflict
        self.model, self.sandbox, self.peers, self.limits = model, sandbox, peers, limits
        self.tools = T.by_name(tools)
        self.tool_schemas = T.schemas(tools)
        self._emit = emit
        self.state = "queued"
        self.summary = ""
        self.files: set[str] = set()
        self.spawned = 0
        self.finish_requested = False
        self.events: list[dict] = []
        self.inbox: asyncio.Queue[dict] = asyncio.Queue()
        self.messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT.format(
            project=project, id=id, title=title, root=sandbox.root, branch=self.branch or "(none)")}]
        self.task: asyncio.Task | None = None

    def emit(self, type: str, data: Any = None):
        self._emit(self, type, data)

    def set_state(self, state: str):
        self.state = state
        self.emit("state", {"state": state})

    def directory_entry(self) -> dict:
        return {"id": self.id, "project": self.project, "title": self.title, "state": self.state,
                "summary": self.summary, "files": sorted(self.files), "parent": self.parent}

    def deliver(self, body: str, sender: str = "user", hops: int = 0):
        self.inbox.put_nowait({"from": sender, "body": body, "hops": hops})

    def _take(self, item: dict) -> bool:
        if item["hops"] > self.limits.max_hops:
            self.emit("dropped", {"from": item["from"], "reason": "hop limit"})
            return False
        content = item["body"] if item["from"] == "user" else f"[from {item['from']}] {item['body']}"
        self.messages.append({"role": "user", "content": content})
        self.emit("message_in", {"from": item["from"], "body": item["body"]})
        return True

    async def run(self):
        try:
            while True:
                if not self._take(await self.inbox.get()):
                    continue
                await self._turn()
        except asyncio.CancelledError:
            self.set_state("stopped")
            raise
        except Exception as e:
            log.exception("thread %s failed", self.id)
            self.emit("error", {"error": f"{type(e).__name__}: {e or repr(e)}"})
            self.set_state("failed")

    async def _turn(self):
        self.set_state("working")
        self.finish_requested = False
        final = ""
        for step in range(self.limits.max_steps):
            while not self.inbox.empty():  # mid-turn messages land before the next model call
                self._take(self.inbox.get_nowait())
            deltas = DeltaBuffer(self.emit)
            try:
                resp = await self.model.complete(self.messages, self.tool_schemas, deltas)
            finally:
                deltas.flush()
            self.messages.append(resp.message)
            if resp.reasoning:
                self.emit("reasoning", {"content": resp.reasoning})
            if resp.content:
                final = resp.content
                self.emit("assistant", {"content": resp.content})
            if resp.usage:
                self.emit("usage", resp.usage)
            if not resp.tool_calls:
                break
            for tc in resp.tool_calls:
                self.emit("tool_call", {"id": tc.id, "name": tc.name, "arguments": tc.arguments})
                try:
                    result = await asyncio.wait_for(T.call(self.tools, self, tc.name, tc.arguments), self.limits.tool_timeout)
                except asyncio.TimeoutError:
                    result = "error: tool timed out"
                self.emit("tool_result", {"id": tc.id, "name": tc.name, "result": result[:4000]})
                self.messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})
            if self.finish_requested:
                break
        else:
            self.emit("error", {"error": f"step limit {self.limits.max_steps} reached"})
        if not self.summary:  # the model ended without finish/set_summary: its last words stand in
            self.summary = (final or self.title)[:500]
        if self.workspace:
            await self._commit_work()
        self.set_state("idle")
        self.emit("turn_done", {"summary": self.summary, "final": final})
        try:
            await self.peers.set_summary(self.id, self.summary, sorted(self.files))
            if self.parent:
                await self.peers.send_message(self.id, self.parent, f"Finished: {self.summary or final}")
        except Exception as e:
            self.emit("error", {"error": f"peer update failed: {e}"})

    async def _commit_work(self):
        """Save the turn's work to origin; land it on the default branch after finish."""
        message = f"{self.title}\n\n{self.summary}\n\nThread: {self.id}"
        try:
            if not self.finish_requested:
                self.emit("saved", {"branch": self.branch, "commit": await self.workspace.save(message)})
                return
            landing = await self.workspace.land(message)
        except GitError as e:
            self.emit("error", {"error": str(e)})
            return
        if landing.ok:
            self.conflicts = 0
            self.emit("landed", {"branch": self.branch, "base": landing.base, "commit": landing.commit})
        elif landing.conflict is not None:
            self.conflicts += 1
            self.emit("land_conflict", {"branch": self.branch, "base": landing.base, "files": landing.conflict})
            # Another turn for the thread; the hop limit ends a run of conflicts it cannot resolve.
            self.deliver(f"Landing {self.branch} on {landing.base} failed: rebasing onto origin/{landing.base} "
                         f"conflicts in {', '.join(landing.conflict) or 'some files'}. Run "
                         f"`git rebase origin/{landing.base}`, resolve the conflicts, `git rebase --continue`, "
                         "then call finish again.", sender="runner", hops=self.conflicts)
        else:
            self.emit("error", {"error": f"landing {self.branch} failed: {landing.message}"})


class ThreadHost:
    """Runs many AgentThreads in one process. One host per worker."""

    def __init__(self, *, model_factory: Callable[[dict | None], Model], peers: Peers | None = None,
                 repo: Repo | None = None, workdir: str = "/work/threads", limits: Limits | None = None,
                 sandbox_factory: Callable[[str], Sandbox] = LocalSandbox, capacity: int = 16,
                 tools: list[T.Tool] | None = None):
        self.model_factory = model_factory
        self.peers = peers or LocalPeers()
        if isinstance(self.peers, LocalPeers):
            self.peers.host = self
        self.repo = repo
        self.workdir = Path(workdir)
        self.limits = limits or Limits()
        self.sandbox_factory = sandbox_factory
        self.capacity = capacity
        self.tools = tools or T.ALL_TOOLS
        self.threads: dict[str, AgentThread] = {}
        self.subscribers: dict[str | None, set[asyncio.Queue]] = {}
        self._seq = 0

    # events -----------------------------------------------------------------
    def _emit(self, t: AgentThread, type: str, data: Any):
        self._seq += 1
        ev = {"seq": self._seq, "thread": t.id, "type": type, "data": data, "ts": time.time()}
        t.events.append(ev)
        for key in (t.id, None):
            for q in self.subscribers.get(key, ()):
                q.put_nowait(ev)

    def subscribe(self, thread_id: str | None = None) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self.subscribers.setdefault(thread_id, set()).add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue, thread_id: str | None = None):
        self.subscribers.get(thread_id, set()).discard(q)

    # lifecycle --------------------------------------------------------------
    def running(self) -> int:
        return sum(1 for t in self.threads.values() if t.state in ("queued", "working"))

    async def _workspace(self, tid: str) -> tuple[str, Workspace | None]:
        path = self.workdir / tid
        if not self.repo:
            path.mkdir(parents=True, exist_ok=True)
            return str(path), None
        path.parent.mkdir(parents=True, exist_ok=True)
        return str(path), await self.repo.worktree(path, f"thread/{tid}")

    async def start(self, *, project: str, title: str, request: str, parent: str | None = None,
                    model: dict | None = None, id: str | None = None) -> AgentThread:
        tid = id or _id()
        if tid in self.threads:
            raise ValueError(f"thread {tid} exists")
        root, workspace = await self._workspace(tid)
        t = AgentThread(id=tid, project=project, title=title, model=self.model_factory(model),
                        sandbox=self.sandbox_factory(root), peers=self.peers, tools=self.tools,
                        limits=self.limits, parent=parent, workspace=workspace, emit=self._emit)
        self.threads[tid] = t
        t.emit("created", t.directory_entry())
        t.deliver(request, sender="user" if parent is None else parent)
        t.task = asyncio.create_task(t.run(), name=tid)
        return t

    def deliver(self, thread_id: str, body: str, sender: str = "user", hops: int = 0):
        self.threads[thread_id].deliver(body, sender=sender, hops=hops)

    async def stop(self, thread_id: str):
        t = self.threads[thread_id]
        if t.task and not t.task.done():
            t.task.cancel()
            try:
                await t.task
            except asyncio.CancelledError:
                pass

    async def wait_idle(self, thread_id: str, timeout: float = 60.0):
        """Wait until a thread has finished at least one turn and its inbox is empty (tests, CLI)."""
        t = self.threads[thread_id]
        async def _w():
            while not (t.state in ("idle", "failed", "stopped") and t.inbox.empty()
                       and any(e["type"] in ("turn_done", "error") for e in t.events)):
                await asyncio.sleep(0.02)
        await asyncio.wait_for(_w(), timeout)

    def health(self) -> dict:
        return {"ok": True, "threads": len(self.threads), "running": self.running(), "capacity": self.capacity}


def dumps(ev: dict) -> str:
    return json.dumps(ev, default=str)
