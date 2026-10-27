"""Agent threads and the host that runs many of them in one process.

A thread's agent is prime-agent (harness.py); the thread feeds it its
messages, turns its events into the runner's (runner/API.md, "SSE"), answers
its team calls (team.py), and at the end of each turn saves its work and
session to the orchestrator (repo.py).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .harness import HARNESS, KERNEL, SESSION, Harness, HarnessError, Place, PrimeAgent
from .peers import LocalPeers, PeerError, Peers
from .repo import GitError, Repos, Workspace
from .sandbox import SandboxError, View, pixi, worker_private
from .team import TeamServer
from .web import DeepSeekWeb

PROMPT = """# This thread

You are one thread in a team of agent threads working on the project "{project}".
Your thread id is {id}. Your title: {title}.
Your workspace is a git worktree of the project at {root}; your branch is {branch}.
The project is all there is to work on: your kernel and every command it runs are in a sandbox
that holds this workspace, your own home and /tmp, and the system's tools read-only.

How the team works, through the `team` skill (read its SKILL.md):
- Many threads run at once. Spawn a new thread (`team.spawn_thread`) for independent work instead
  of doing it yourself.
- Use `team.list_threads()` before touching shared areas; if another thread owns one, use
  `team.send_message` instead of editing it.
- Messages from other threads, and user requests the project's router passes on, arrive as user
  messages that start with [from <thread id>] or [from router]; the user's own come as they are.
- Keep your directory entry current with `team.set_summary` when your focus changes.
- `team.web_search` and `team.web_fetch` search and read the web.

The project is a pixi project (https://pixi.sh): pixi.toml at the root of your workspace says
its dependencies, environments and tasks, and pixi.lock pins them. Use pixi for all of it:
- `pixi add <package>` (conda-forge) or `pixi add --pypi <package>` for a dependency the project
  needs, never pip, uv or another installer; `pixi run <task or command>` to run in the project's
  environment; `pixi task add <name> <command>` for how the project is built, tested and run.
- Commit pixi.toml and pixi.lock with the work that changes them. Environments live in .pixi/,
  which git ignores and the runner does not save: `pixi run` and `pixi install` make them again.
- Your kernel's own venv (`uv pip install`) is for your scratch work only, never for the project.

Commits: commit your work on {branch} with clear messages; there is no remote to push to.
At the end of every turn the runner commits anything left over and your branch lands on the
project's main branch (rebase, fast-forward), and every file in your workspace is saved to the
project's repository. So end a turn with the work in a state fit for main: when you stop to ask
something, leave what you have working. If landing conflicts you get a message from "runner"
listing the files: rebase onto the named branch yourself, resolve, `git rebase --continue`, and
end the turn; it lands again.
"""

# Interrupted turns. A turn under way when the runner stops (the control plane
# restarts its service, a sprite is stopped, the process crashes) is not lost
# with it: the thread's session file holds the conversation up to the cut and
# its worktree everything it changed, both on the worker's disk. While a turn
# is under way its thread's `Place.turn` file exists, naming the session commit
# the turn started from; it goes when the turn ends, or the thread fails or is
# stopped, but not when the runner itself is stopping. A thread started on a
# worker that still has the file, with that session, resumes the cut turn
# (ThreadHost.start): the agent gets RESUME instead of the request it already
# has.
RESUME = ("Your last turn was cut short: the runner restarted while it ran. Your conversation and your "
          "workspace (uncommitted files included) are as they were when it stopped; a command that was "
          "running then did not finish. Pick up where you left off and finish that turn.")

# A thread's team socket is a file under the worker's workdir, so the workdir's
# depth is bounded by the socket's address: sun_path is 108 bytes, NUL included.
SOCKET_ADDRESS_MAX = 108


@dataclass
class Limits:
    max_spawn: int = 8
    max_hops: int = 6


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


def user_text(sender: str, body: str) -> str:
    return body if sender == "user" else f"[from {sender}] {body}"


def result_text(result: dict | None) -> str:
    """A tool result's text parts, as the model read them."""
    return "\n".join(c.get("text", "") for c in (result or {}).get("content") or [] if c.get("type") == "text")


class AgentThread:
    def __init__(self, *, id: str, project: str, title: str, agent: PrimeAgent, model: dict | None, place: Place,
                 peers: Peers, web: DeepSeekWeb, limits: Limits, parent: str | None, workspace: Workspace | None,
                 emit: Callable[["AgentThread", str, Any], None], summary: str = ""):
        self.id, self.project, self.title, self.parent = id, project, title, parent
        self.agent, self.model, self.place = agent, model, place
        self.root = place.view.root
        self.workspace = workspace
        self.branch = workspace.branch if workspace else None
        self.conflicts = 0  # landings in a row that hit a conflict
        self.peers, self.web, self.limits = peers, web, limits
        self._emit = emit
        self.state = "queued"
        self.summary = summary
        self.files: set[str] = set()
        self.spawned = 0
        self.events: list[dict] = []
        self.inbox: asyncio.Queue[dict] = asyncio.Queue()
        self.preparing = True  # its worktree and session are being made (ThreadHost._prepare)
        self.first: dict | None = None  # the message its first turn takes, before its inbox's (ThreadHost._prepare)
        self.harness: Harness | None = None
        self.team = TeamServer(self, place.socket)
        self.task: asyncio.Task | None = None
        # The runner is stopping (ThreadHost.shut_down): a turn its end cuts
        # short is interrupted, to resume, not failed.
        self.stopping = ""
        self.session = ""  # the thread's session commit on this worker, as its last turn left it
        self.resumed = False  # started to resume a turn cut short here

    def emit(self, type: str, data: Any = None):
        if type == "error":  # every failure a thread reports is in the worker log too
            log.error("thread %s: %s", self.id, data.get("error"))
        self._emit(self, type, data)

    def set_state(self, state: str):
        self.state = state
        self.emit("state", {"state": state})

    def directory_entry(self) -> dict:
        return {"id": self.id, "project": self.project, "title": self.title, "state": self.state,
                "summary": self.summary, "files": sorted(self.files), "parent": self.parent}

    def deliver(self, body: str, sender: str = "user", hops: int = 0, images: list[dict] | None = None):
        self.inbox.put_nowait({"from": sender, "body": body, "hops": hops, "images": images or []})

    def prompt(self) -> str:
        return PROMPT.format(project=self.project, id=self.id, title=self.title, root=self.root,
                             branch=self.branch or "(none)")

    async def _send(self, item: dict) -> bool:
        """Hand a message to the agent, unless it has gone round too often."""
        if item["hops"] > self.limits.max_hops:
            self.emit("dropped", {"from": item["from"], "reason": "hop limit"})
            return False
        self.emit("message_in", {"from": item["from"], "body": item["body"]})
        await self.harness.prompt(user_text(item["from"], item["body"]), item.get("images"))
        return True

    async def run(self, prepare: Callable[[], Any] | None = None):
        """The thread's life: `prepare` (its worktree and session, which can
        take minutes: ThreadHost.start returns before it), then its agent,
        fed its messages one turn at a time."""
        try:
            if prepare is not None:
                await prepare()
            await self.team.start()
            self.harness = await self.agent.start(self.place, model=self.model, append_prompt=self.prompt())
            while True:
                item, self.first = self.first or await self.inbox.get(), None
                if await self._send(item):
                    await self._turn()
        except asyncio.CancelledError:
            if not self.stopping:
                self._turn_over()
                self.set_state("stopped")
            raise
        except Exception as e:
            if self.stopping:
                log.warning("thread %s: its turn was cut short by the runner's shutdown (%s); it resumes when "
                            "the thread is started on this worker again (%s: %s)",
                            self.id, self.stopping, type(e).__name__, e)
                return
            log.exception("thread %s failed", self.id)
            self._turn_over()
            self.emit("error", {"error": f"{type(e).__name__}: {e or repr(e)}"})
            self.set_state("failed")
        finally:
            if self.harness is not None:
                await self.harness.close()
            await self.team.close()

    async def _turn(self):
        """One turn: the agent's run from the message just sent until it is idle
        again. Messages that arrive meanwhile steer it."""
        self.set_state("working")
        Path(self.place.turn).write_text(json.dumps({"session": self.session, "since": time.time()}))
        turn = Turn(self)
        events = self.harness.events
        ev_get = asyncio.ensure_future(events.get())
        in_get = asyncio.ensure_future(self.inbox.get())
        try:
            while True:
                done, _ = await asyncio.wait({ev_get, in_get}, return_when=asyncio.FIRST_COMPLETED)
                if in_get in done:
                    await self._send(in_get.result())
                    in_get = asyncio.ensure_future(self.inbox.get())
                if ev_get in done:
                    ev = ev_get.result()
                    if ev is None:
                        raise HarnessError(f"prime-agent {self.harness.ended()}")
                    ended = await turn.on(ev)
                    ev_get = asyncio.ensure_future(events.get())
                    if ended and not await self.harness.busy() and not in_get.done():
                        break
        finally:
            in_get.cancel()
            ev_get.cancel()
        if not self.summary:  # the model never called set_summary: its last words stand in
            self.summary = (turn.final or self.title)[:500]
        if self.workspace:
            await self._commit_work(land=not turn.failed)
        self._turn_over()
        self.set_state("idle")
        self.emit("turn_done", {"summary": self.summary, "final": turn.final})
        try:
            await self.peers.set_summary(self.id, self.summary, sorted(self.files))
            if self.parent:
                await self.peers.send_message(self.id, self.parent, f"Finished: {self.summary or turn.final}")
        except Exception as e:
            log.warning("thread %s: peer update failed", self.id, exc_info=True)
            self.emit("error", {"error": f"peer update failed: {type(e).__name__}: {e}"})

    def _turn_over(self):
        """No turn of the thread's is under way any more: none to resume."""
        Path(self.place.turn).unlink(missing_ok=True)

    async def _commit_work(self, land: bool):
        """Commit the turn's work, snapshot the worktree and save the agent's
        session; the orchestrator saves all three and lands the branch. A turn
        a model call failed in is cut short, so its work is saved, not landed."""
        message = f"{self.title}\n\n{self.summary}\n\nThread: {self.id}"
        repo = self.workspace.repo
        try:
            commit = await self.workspace.commit(message)
            self.files = set(await self.workspace.changed())
            await self.workspace.snapshot()
            self.session = await repo.save_session(self.id, self.place.agent_dir, SESSION)
            result = await self.peers.save(self.id, land=land)
            if result.get("landed"):
                await self.workspace.follow()
        except (GitError, PeerError) as e:
            log.warning("thread %s: saving the turn's work failed", self.id, exc_info=True)
            self.emit("error", {"error": str(e)})
            return
        if not land:
            self.emit("saved", {"branch": self.branch, "commit": commit})
        elif result.get("landed"):
            self.conflicts = 0
            self.emit("landed", {"branch": self.branch, "base": result["base"], "commit": result["commit"]})
        elif result.get("conflict") is not None:
            base, files = result["base"], result["conflict"]
            self.conflicts += 1
            self.emit("land_conflict", {"branch": self.branch, "base": base, "files": files})
            # Another turn for the thread; the hop limit ends a run of conflicts it cannot resolve.
            self.deliver(f"Landing {self.branch} on {base} failed: rebasing onto origin/{base} "
                         f"conflicts in {', '.join(files) or 'some files'}. Run "
                         f"`git rebase origin/{base}`, resolve the conflicts, `git rebase --continue`, "
                         "then end the turn; it lands again.", sender="runner", hops=self.conflicts)
        else:
            self.emit("error", {"error": f"landing {self.branch} failed: {result.get('message') or 'no reason given'}"})


class Turn:
    """One turn's prime-agent events, as the runner's: `delta`s while the model
    streams, then per model call its `reasoning`, `assistant`, `tool_call`s and
    `usage`, and a `tool_result` per call it ran."""

    def __init__(self, t: AgentThread):
        self.t = t
        self.deltas = DeltaBuffer(t.emit)
        self.steps = 0
        self.final = ""
        self.failed = False  # a model call failed: the turn ended early

    async def on(self, ev: dict) -> bool:
        """Take one event; True when the agent's run has ended."""
        typ = ev.get("type")
        if typ == "message_update":
            self._delta(ev)
        elif typ == "message_end" and (ev.get("message") or {}).get("role") == "assistant":
            self.deltas.flush()
            self._step(ev["message"])
        elif typ == "tool_execution_end":
            if ev.get("isError"):
                log.warning("thread %s: tool %s failed:\n%s", self.t.id, ev.get("toolName"),
                            result_text(ev.get("result")))
            self.t.emit("tool_result", {"id": ev.get("toolCallId"), "name": ev.get("toolName"),
                                        "result": result_text(ev.get("result"))[:4000]})
        elif typ == "agent_end":
            self.deltas.flush()
            return True
        return False

    def _delta(self, ev: dict):
        a = ev.get("assistantMessageEvent") or {}
        kind = a.get("type")
        if kind == "text_delta":
            self.deltas("content", a.get("delta", ""))
        elif kind == "thinking_delta":
            self.deltas("reasoning", a.get("delta", ""))
        elif kind in ("toolcall_start", "toolcall_delta"):
            content = (ev.get("message") or {}).get("content") or []
            i = a.get("contentIndex", -1)
            name = content[i].get("name", "") if 0 <= i < len(content) else ""
            self.deltas("tool", a.get("delta", ""), name)

    def _step(self, m: dict):
        blocks = m.get("content") or []
        reasoning = "".join(b.get("thinking", "") for b in blocks if b.get("type") == "thinking")
        text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
        if reasoning:
            self.t.emit("reasoning", {"content": reasoning})
        if text:
            self.final = text
            self.t.emit("assistant", {"content": text})
        for b in blocks:
            if b.get("type") == "toolCall":
                self.t.emit("tool_call", {"id": b.get("id"), "name": b.get("name"),
                                          "arguments": b.get("arguments") or {}, "step": self.steps})
        if m.get("usage"):
            self.t.emit("usage", m["usage"])
        if m.get("stopReason") == "error":
            log.error("thread %s: model call failed: %s", self.t.id,
                      json.dumps({k: v for k, v in m.items() if k != "content"}, default=str))
            self.failed = True
            self.t.emit("error", {"error": m.get("errorMessage") or "the model call failed"})
        self.steps += 1


class ThreadHost:
    """Runs many AgentThreads in one process. One host per worker."""

    def __init__(self, *, agent: PrimeAgent, web: DeepSeekWeb, peers: Peers | None = None,
                 repos: Repos | None = None, workdir: str = "/work/threads", limits: Limits | None = None,
                 capacity: int = 16):
        self.agent, self.web = agent, web
        self.peers = peers or LocalPeers()
        if isinstance(self.peers, LocalPeers):
            self.peers.host = self
        # One repository per project: the worker is shared by every project.
        self.repos = repos
        self.workdir = Path(workdir)
        self.limits = limits or Limits()
        self.capacity = capacity
        self.threads: dict[str, AgentThread] = {}
        self.subscribers: dict[str | None, set[asyncio.Queue]] = {}
        self._seq = 0
        self.stopping = ""  # why the runner is stopping, once it is (shut_down)

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

    def _place(self, tid: str, project: str) -> Place:
        """Where the thread lives on this worker: its worktree, and the view
        its kernel gets of the worker: that, its project's repository's git dir, the
        thread's own home and /tmp under `.home/` and `.tmp/`, its team socket,
        the kernel's Python environment and prime-agent's skills read-only, and pixi."""
        work = self.workdir.resolve()
        socket = str(work / ".team" / f"{tid}.sock")
        if len(os.fsencode(socket)) >= SOCKET_ADDRESS_MAX:
            raise SandboxError(f"a thread's team socket would be {socket!r}, longer than a unix socket "
                               f"address ({SOCKET_ADDRESS_MAX - 1} bytes): use a shorter workdir than "
                               f"{str(work)!r}")
        view = View(root=str(work / tid), home=str(work / ".home" / tid), tmp=str(work / ".tmp" / tid),
                    writable=((str(self.repos.get(project).path.resolve() / ".git"),) if self.repos else ())
                    + (socket,),
                    hidden=worker_private(self.workdir), readonly=(str(KERNEL), str(HARNESS / "skills")),
                    tools=(pixi(),))
        return Place(view=view, socket=socket, kernel=str(work / ".team" / f"{tid}.kernel"),
                     turn=str(work / ".team" / f"{tid}.turn"))

    def shut_down(self, why: str) -> None:
        """The runner is stopping: the turns its end cuts short are interrupted,
        to resume when their threads are started here again, not failed.
        Called from the signal handler, before anything else hears of it."""
        self.stopping = why
        for t in self.threads.values():
            t.stopping = why

    async def start(self, *, project: str, title: str, request: str | None = None, parent: str | None = None,
                    model: dict | None = None, id: str | None = None, summary: str = "",
                    images: list[dict] | None = None) -> AgentThread:
        """Start a thread on this worker. A thread the orchestrator brings back
        continues its branch and its agent's session; with no `request` it
        waits idle for its next message. `images` belong to the request. An
        id whose thread here has ended (stopped or failed) is started afresh.
        It returns at once, the thread `queued`: making its worktree and
        session (_prepare) is the start of its task, since that can take
        minutes (a snapshot with many files to write).

        A thread whose turn this worker's runner was cut off in resumes that
        turn ("Interrupted turns" above): its session has the turn's messages,
        the request among them when it was the first turn, so it gets RESUME
        and not the request again."""
        tid = id or _id()
        old = self.threads.get(tid)
        if old is not None and old.task is not None and not old.task.done():
            raise ValueError(f"thread {tid} exists")
        place = self._place(tid, project)
        t = AgentThread(id=tid, project=project, title=title, agent=self.agent, model=model, place=place,
                        peers=self.peers, web=self.web, limits=self.limits, parent=parent, workspace=None,
                        emit=self._emit, summary=summary)
        t.stopping = self.stopping
        if old is not None:
            t.events = old.events  # the worker's event backlog keeps the thread's earlier events
        self.threads[tid] = t
        t.emit("created", t.directory_entry())
        t.task = asyncio.create_task(t.run(lambda: self._prepare(t, request, images)), name=tid)
        return t

    async def _prepare(self, t: AgentThread, request: str | None, images: list[dict] | None) -> None:
        """Make the thread's worktree and session on this worker, then give it
        its first message: RESUME for a turn cut short here, else its request,
        else nothing (it goes idle). Messages posted meanwhile wait in its
        inbox and come after it. Each step is a `preparing` event, and the end
        a `prepared` one saying whether it resumes."""
        if self.repos:
            repo = self.repos.get(t.project)
            Path(t.root).parent.mkdir(parents=True, exist_ok=True)
            t.emit("preparing", {"step": "making its worktree"})
            t.workspace = await repo.worktree(t.root, t.id)
            t.branch = t.workspace.branch
            t.emit("preparing", {"step": "taking its session"})
            await repo.session(t.id, t.place.agent_dir, SESSION)
            t.session = await repo.session_commit(t.id)
        t.resumed = self._interrupted(t.place, t.session)
        t.preparing = False
        idle = not t.resumed and request is None
        t.emit("prepared", {"resumed": t.resumed, "idle": idle})
        if t.resumed:
            log.info("thread %s: resuming the turn the runner's restart cut short", t.id)
            t.first = {"from": "runner", "body": RESUME, "hops": 0, "images": []}
        elif request is not None:
            t.first = {"from": "user" if t.parent is None else t.parent, "body": request, "hops": 0,
                       "images": images or []}
        else:
            t.set_state("idle")

    async def prune(self, keep: set[str]) -> list[str]:
        """Take every project not in `keep` off this worker: its threads here
        are stopped and forgotten, and its repository and their worktrees,
        homes, /tmp and team sockets deleted. The orchestrator keeps all of
        it in its hub, so the project comes back from there if it is ever
        used again. Returns the projects removed."""
        gone = [p for p in self.repos.projects() if p not in keep] if self.repos else []
        for project in gone:
            for tid in [t.id for t in self.threads.values() if t.project == project]:
                await self.stop(tid)
                del self.threads[tid]
            for tree in await self.repos.worktrees(project):
                tid = tree.name
                for path in (tree, self.workdir / ".home" / tid, self.workdir / ".tmp" / tid):
                    await asyncio.to_thread(shutil.rmtree, path, ignore_errors=True)  # off the event loop
                for leftover in (self.workdir / ".team").glob(f"{tid}.*"):
                    leftover.unlink(missing_ok=True)
            await asyncio.to_thread(self.repos.remove, project)
            log.info("project %s is no longer on this worker", project)
        return gone

    @staticmethod
    def _interrupted(place: Place, session: str) -> bool:
        """Whether the thread has a turn on this worker to resume: one was under
        way when this worker's runner stopped, and the thread's session is
        still the one that turn went on from. A thread that went on on
        another worker since has a newer one (Repo.session took it from the
        hub), and the cut turn is history there."""
        path = Path(place.turn)
        try:
            was = json.loads(path.read_text())
        except FileNotFoundError:
            return False
        except ValueError:  # cut off while it was written: the turn had only just started
            was = {"session": session}
        if was.get("session", "") == session:
            return True
        path.unlink()
        return False

    def deliver(self, thread_id: str, body: str, sender: str = "user", hops: int = 0,
                images: list[dict] | None = None):
        self.threads[thread_id].deliver(body, sender=sender, hops=hops, images=images)

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
            while not (t.state in ("idle", "failed", "stopped") and t.first is None and t.inbox.empty()
                       and any(e["type"] in ("turn_done", "error") for e in t.events)):
                await asyncio.sleep(0.02)
        await asyncio.wait_for(_w(), timeout)

    def health(self) -> dict:
        return {"ok": True, "threads": len(self.threads), "running": self.running(), "capacity": self.capacity}


def dumps(ev: dict) -> str:
    return json.dumps(ev, default=str)
