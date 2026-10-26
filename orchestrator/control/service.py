"""The control plane's core: workers, threads, routing and the message bus.

Every runner event is relayed into the store, so the UI reads one ordered
event log per project no matter how many workers the project has. Every
project's commits gather in its hub, a git repository on this machine
(hub.py), which the control plane syncs workers with.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from .hub import Hub
from .providers import Worker, WorkerProvider, WorkerSpec
from .router import Router
from .runner_client import RunnerClient
from .secrets import get_secret
from .store import Store, now

log = logging.getLogger("orchestrator")

MAX_HOPS = 6
# A worker is watched every HEALTH_EVERY seconds; one whose /health fails
# HEALTH_TRIES times in a row (HEALTH_GAP seconds apart) is lost.
HEALTH_EVERY = 10.0
HEALTH_TRIES = 3
HEALTH_GAP = 1.0
HEALTH_TIMEOUT = 5.0
CAPACITY = 16  # running threads per worker before another one starts
# A worker's runner log goes to this process's stdout as it is written, each
# line tagged with the worker; following starts LOG_BACKLOG lines back, and the
# last LOG_KEPT lines are kept with the worker when it is lost.
LOG_BACKLOG = 50
LOG_KEPT = 300
RUNNER_STATES = {"queued", "working", "idle", "failed", "stopped", "waiting", "done"}
# States in which a thread is done with its runner; archiving any other thread stops it first.
ENDED = {"done", "failed", "stopped"}
# The runner events a thread's conversation is rebuilt from when it is
# brought back on a worker (runner/API.md, "Resuming a thread").
# Runner events that end a model call: once one is in, that call's `delta`
# events (its reply while it streamed) are folded into it and dropped.
ENDS_STREAM = {"reasoning", "assistant", "tool_call", "turn_done", "state", "error"}


def worker_spec(project: dict, index: int) -> WorkerSpec:
    return WorkerSpec(project_id=project["id"], index=index, capacity=CAPACITY)


def attachable(w: dict) -> bool:
    """A stored worker that a control plane can attach to again by its index.
    Workers start at index 1; 0 marks a row stored before workers kept their
    index, whose spec cannot be made again."""
    return w["idx"] >= 1


def stored_worker(w: dict) -> Worker:
    """A worker as the store keeps it, for the provider."""
    return Worker(id=w["id"], project_id=w["project_id"], provider=w["provider"], handle=w["handle"],
                  url=w["url"], token=w["token"], capacity=w["capacity"], index=w["idx"])


def peer_entry(t: dict) -> dict:
    """A thread as the runner's directory entry shape (runner/API.md)."""
    return {"id": t["id"], "project": t["project_id"], "title": t["title"], "state": t["state"],
            "summary": t["summary"], "files": t["files"], "parent": t["parent_id"]}


def tool_arg(args) -> str:
    """The part of a tool call's arguments worth showing: a command or path, else all of it."""
    if isinstance(args, dict):
        return str(args.get("command") or args.get("path") or json.dumps(args))
    return str(args)


def one_line(s: str, n: int) -> str:
    s = " ".join(s.split())
    return s if len(s) <= n else s[:n] + " …"


def image_title(images: list[dict] | None) -> str:
    """A thread title for a request that is only images."""
    n = len(images or [])
    return "Pasted image" if n == 1 else f"{n} pasted images"


def activity_of(typ: str, data: dict) -> str | None:
    """What a thread is doing now, given its latest event; None leaves it as it was.

    It names the running tool while one runs; while the model has the turn it
    says what the model is streaming (reasoning, a reply, a tool call). Any
    state but working clears it."""
    if typ == "delta":
        kind = data.get("kind")
        if kind == "content":
            return "writing a reply"
        if kind == "tool":
            return f"calling {data.get('name') or 'a tool'}"
        return "thinking"
    if typ == "tool_call":
        return one_line(f"running {data.get('name', 'tool')}: {tool_arg(data.get('arguments'))}", 160)
    if typ in ("reasoning", "assistant", "tool_result"):
        return "thinking"
    if typ == "state":
        return "thinking" if data.get("state") == "working" else ""
    return None


def describe(e: Exception) -> str:
    """An exception as one line that says what failed: httpx's timeouts carry
    no message, so name the request they were for."""
    text = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
    if isinstance(e, httpx.RequestError):
        try:
            text += f" on {e.request.method} {e.request.url}"
        except RuntimeError:  # no request attached
            pass
    return text


class PeerCallError(Exception):
    """A peer call the orchestrator refuses; its text goes back to the tool."""


class ArchivedError(Exception):
    """The thread, or its project, is archived: it takes no work until it is unarchived."""


class EditError(Exception):
    """An edit the control plane refuses: not one of the person's own messages."""


def quote(text: str) -> str:
    return "\n".join(f"> {line}" if line else ">" for line in text.splitlines())


def correction(kind: str, previous: str, text: str) -> str:
    """What a thread is told when the person edits a message that reached it:
    both versions, and that the new one is what counts."""
    what = "my message to you" if kind == "message_in" else "a request you're working from"
    return (f"I edited {what}. It now reads:\n\n{quote(text)}\n\nIt read:\n\n{quote(previous)}\n\n"
            "Work from the new version: keep what it still asks for, and drop or undo what it no longer does.")


def editable(ev: dict) -> bool:
    """The person's own messages: requests typed in the project, and messages
    sent straight to a thread (not the corrections an edit sends)."""
    d = ev["payload"]
    if ev["type"] == "request":
        return True
    return ev["type"] == "message_in" and d.get("from") == "user" and "edit" not in d


@dataclass
class LiveWorker:
    worker: Worker
    client: RunnerClient
    last_seq: int = 0
    relay: asyncio.Task | None = None
    pump: asyncio.Task | None = None
    watch: asyncio.Task | None = None
    logs: asyncio.Task | None = None
    threads: set[str] = field(default_factory=set)
    starting: int = 0  # threads picked for it and not yet posted: they count as running
    log_tail: deque = field(default_factory=lambda: deque(maxlen=LOG_KEPT))
    health_error: str = ""  # why its last /health failed, "" while it answers

    def tasks(self) -> list[asyncio.Task]:
        return [task for task in (self.relay, self.pump, self.watch, self.logs) if task]


def print_worker_line(worker_id: str, line: str) -> None:
    sys.stdout.write(f"{worker_id} | {line}\n")
    sys.stdout.flush()


def when(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(ts))


class Orchestrator:
    def __init__(self, store: Store, provider: WorkerProvider, router: Router, repos: str | Path,
                 max_running: int = 64) -> None:
        self.store = store
        # One git repository per project, the hub its threads' work goes to (hub.py).
        self.repos = Path(repos).resolve()
        self.hubs: dict[str, Hub] = {}
        self._hubs_lock = asyncio.Lock()
        self.provider = provider
        self.router = router
        self.max_running = max_running
        self.workers: dict[str, list[LiveWorker]] = {}
        # Per project, the indexes of the workers it ever made (from the
        # store, on first use) and of those lost. A worker outlives the
        # control plane: its disk holds the threads' worktrees, so a later
        # control plane attaches to it again by its index. A new worker takes
        # an index never used, so it never takes a lost worker's name.
        self.made: dict[str, set[int]] = {}
        self.lost: dict[str, set[int]] = {}
        self.by_thread: dict[str, LiveWorker] = {}
        # Per thread, the messages the control plane logged when it took them
        # (a spawned thread's request as "created", the rest as "message_in")
        # and the runner has yet to echo back as message_in; the echo is dropped.
        self.unechoed: dict[str, list[str]] = {}
        # Work runs in the background, in order per key: a project's requests
        # (routing them may ask a model) and a thread's start and deliveries
        # (starting may create a worker). The API only logs and queues.
        self.request_chain: dict[str, asyncio.Task] = {}
        self.thread_chain: dict[str, asyncio.Task] = {}
        self.starting: dict[str, asyncio.Task] = {}
        self.reviving: dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        # Every chained task, not only each chain's last: close() ends them all.
        self.background: set[asyncio.Task] = set()

    def _chain(self, chains: dict[str, asyncio.Task], key: str, work) -> asyncio.Task:
        """Run `work()` once the work queued before it under `key` has ended,
        however that ended. `work` reports its own failures as events."""
        prev = chains.get(key)

        async def run() -> None:
            if prev is not None:
                await asyncio.wait({prev})
            await work()

        task = asyncio.create_task(run())
        chains[key] = task
        self.background.add(task)
        task.add_done_callback(self.background.discard)
        task.add_done_callback(lambda t: chains.pop(key) if chains.get(key) is t else None)
        return task

    async def close(self) -> None:
        """Let go of the workers. They are not destroyed: what the threads
        left on them (files never committed, ignored build output) is there
        for the next control plane, which attaches to them again."""
        tasks = list(self.background)
        for lws in self.workers.values():
            for lw in lws:
                tasks += lw.tasks()
        for task in tasks:
            task.cancel()
        # Wait for them to end, so none touches the store after it closes.
        await asyncio.gather(*tasks, return_exceptions=True)
        for lws in self.workers.values():
            for lw in lws:
                await lw.client.close()

    # ---------------------------------------------------------------- workers

    async def _pick_worker(self, project: dict, start: bool = False) -> LiveWorker:
        """The least loaded worker with room, made when none has. With `start`
        the caller is about to start a thread there and holds a place for it
        (`starting`) until it has, so concurrent starts spread out."""
        async with self._lock:
            lw = await self._least_loaded(project)
            if start:
                lw.starting += 1
            return lw

    async def _least_loaded(self, project: dict) -> LiveWorker:
        """Callers hold self._lock."""
        lws = self.workers.setdefault(project["id"], [])
        best, best_load = None, None
        for lw in list(lws):
            h = await self._health(lw)
            if h is None:
                await self._lose(lw, lw.health_error)
                continue
            running = h.get("running", 0) + lw.starting
            if running < h.get("capacity", lw.worker.capacity) and (best_load is None or running < best_load):
                best, best_load = lw, running
        if best is not None:
            return best
        made = await self._made(project["id"])
        live = {lw.worker.index for lw in lws}
        idle = sorted(made - live - self.lost[project["id"]])  # left by an earlier control plane
        return await self._attach(project, idle[0] if idle else max(made, default=0) + 1)

    async def _made(self, project_id: str) -> set[int]:
        if project_id not in self.made:
            workers = [w for w in await self.store.list_workers(project_id) if attachable(w)]
            self.made[project_id] = {w["idx"] for w in workers}
            self.lost[project_id] = {w["idx"] for w in workers if w["lost_at"] is not None}
        return self.made[project_id]

    async def _attach(self, project: dict, index: int) -> LiveWorker:
        """The project's worker `index`, started: made new, or the one an
        earlier control plane made, with its disk. Callers hold self._lock."""
        for lw in self.workers.get(project["id"], []):
            if lw.worker.index == index:
                return lw
        spec = worker_spec(project, index)
        spec.token = next((w["token"] for w in await self.store.list_workers(project["id"])
                           if w["idx"] == index and attachable(w)), "")
        worker = await self.provider.create(spec)
        await self.store.upsert_worker(worker.record())
        (await self._made(project["id"])).add(index)
        lw = LiveWorker(worker, RunnerClient(worker.url, worker.token))
        lw.logs = asyncio.create_task(self._follow_logs(lw))
        lw.relay = asyncio.create_task(self._relay(project["id"], lw))
        lw.pump = asyncio.create_task(self._pump_peer_calls(lw))
        lw.watch = asyncio.create_task(self._watch(lw))
        self.workers.setdefault(project["id"], []).append(lw)
        await self.store.append_event(project["id"], None, "worker",
                                      {"id": worker.id, "provider": worker.provider, "url": worker.url})
        return lw

    async def _home(self, project: dict, worker_id: str | None) -> LiveWorker | None:
        """The worker a thread ran on, attached again if an earlier control
        plane left it; None when it has none or that worker was lost."""
        if worker_id is None:
            return None
        async with self._lock:
            for lw in self.workers.get(project["id"], []):
                if lw.worker.id == worker_id:
                    return lw
            for w in await self.store.list_workers(project["id"]):
                if w["id"] == worker_id and w["lost_at"] is None and attachable(w):
                    return await self._attach(project, w["idx"])
        return None

    async def _health(self, lw: LiveWorker) -> dict | None:
        """The worker's /health, or None when it failed HEALTH_TRIES times in a row."""
        for attempt in range(HEALTH_TRIES):
            try:
                h = await lw.client.health(timeout=HEALTH_TIMEOUT)
                lw.health_error = ""
                return h
            except Exception as e:
                last = e
            if attempt + 1 < HEALTH_TRIES:
                await asyncio.sleep(HEALTH_GAP)
        lw.health_error = f"its /health failed {HEALTH_TRIES} times in a row, last with {describe(last)}"
        log.warning("worker %s is lost: %s", lw.worker.id, lw.health_error)
        return None

    async def _watch(self, lw: LiveWorker) -> None:
        """Notice a worker that died while nothing was sent to it, so its
        threads stop showing as running."""
        while True:
            await asyncio.sleep(HEALTH_EVERY)
            if await self._health(lw) is None:
                await self._lose(lw, lw.health_error)
                return

    async def _lose(self, lw: LiveWorker, why: str) -> None:
        """Drop a worker that stopped answering: no thread runs on it any more,
        so each comes back on another worker with its next message (live_worker).
        It is destroyed too, so it can never run a thread a second time. Why,
        and the end of its log as it was followed, stay with it in the store:
        destroying it takes its log too."""
        lws = self.workers.get(lw.worker.project_id, [])
        if lw not in lws:
            return
        lws.remove(lw)
        me = asyncio.current_task()
        for task in lw.tasks():
            if task is not me:
                task.cancel()
        project_id, wid = lw.worker.project_id, lw.worker.id
        log.warning("dropping worker %s: %s", wid, why)
        reason = (f"worker {wid} stopped answering ({why}); "
                  "the thread comes back on another worker with its next message")
        for tid in sorted(lw.threads):
            if self.by_thread.get(tid) is not lw:
                continue
            del self.by_thread[tid]
            t = await self.store.get_thread(tid)
            fields = {"worker_id": None, "activity": ""}
            if t["state"] in ("queued", "working"):  # its turn was cut short
                fields["state"] = "failed"
            await self.store.update_thread(tid, **fields)
            await self.store.append_event(project_id, tid, "worker_lost", {"worker": wid, "error": reason})
        if not lw.threads:
            await self.store.append_event(project_id, None, "worker_lost",
                                          {"worker": wid, "error": f"worker {wid} stopped answering ({why})"})
        await self._made(project_id)
        self.lost[project_id].add(lw.worker.index)
        await self.store.lose_worker(wid, why, "\n".join(lw.log_tail))
        try:
            await self.provider.destroy(lw.worker)
        except Exception as e:
            log.warning("destroying lost worker %s: %s", wid, describe(e))
        await lw.client.close()

    async def _follow_logs(self, lw: LiveWorker) -> None:
        """Print the worker's runner log to stdout as it is written, and keep
        its end for when the worker is lost (see _lose)."""
        wid = lw.worker.id
        try:
            async for line in self.provider.follow(lw.worker, LOG_BACKLOG):
                lw.log_tail.append(line)
                print_worker_line(wid, line)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("cannot follow the log of worker %s: %s", wid, describe(e))

    async def _relay(self, project_id: str, lw: LiveWorker) -> None:
        """Copy a worker's event stream into the store; reconnect on drops."""
        while True:
            try:
                async for ev in lw.client.events(after=lw.last_seq):
                    lw.last_seq = max(lw.last_seq, ev.get("seq", 0))
                    await self._ingest(project_id, ev)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("relay from %s dropped: %s", lw.worker.id, describe(e))
            await asyncio.sleep(1.0)

    # ---------------------------------------------------------------- peer calls
    #
    # The control plane is never reachable from a worker: it opens every
    # connection. Each worker queues its threads' peer calls (list, read,
    # message, spawn, summary) and this pump long-polls them off it, runs each
    # one concurrently and posts the answer back (runner/API.md, "Peer calls").

    async def _pump_peer_calls(self, lw: LiveWorker) -> None:
        running: set[asyncio.Task] = set()
        try:
            while True:
                try:
                    calls = await lw.client.peer_calls(wait=25.0)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("peer calls from %s: %s", lw.worker.id, describe(e))
                    await asyncio.sleep(1.0)
                    continue
                for call in calls:
                    task = asyncio.create_task(self._answer_peer_call(lw, call))
                    running.add(task)
                    task.add_done_callback(running.discard)
        finally:
            for task in running:
                task.cancel()

    async def _answer_peer_call(self, lw: LiveWorker, call: dict) -> None:
        op, args = call.get("op"), call.get("args") or {}
        handler = self.PEER_OPS.get(op) if isinstance(op, str) else None
        try:
            if handler is None:
                raise PeerCallError(f"unknown op {op!r}")
            reply = {"ok": True, "result": await handler(self, lw, **args)}
        except PeerCallError as e:
            reply = {"ok": False, "error": str(e)}
        except TypeError as e:  # wrong or missing args
            reply = {"ok": False, "error": f"bad arguments for {op}: {e}"}
        except Exception as e:
            log.exception("peer call %s from %s", op, lw.worker.id)
            reply = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        try:
            await lw.client.answer_peer_call(call["id"], reply)
        except Exception as e:  # the worker hands the call out again after its lease
            log.warning("answering peer call %s on %s: %s", call.get("id"), lw.worker.id, e)

    async def _own_thread(self, project_id: str, thread_id: str | None) -> dict:
        """A thread of the calling worker's project; nothing else is visible to it."""
        t = await self.store.get_thread(thread_id) if thread_id else None
        if t is None or t["project_id"] != project_id:
            raise PeerCallError(f"no thread {thread_id}")
        return t

    async def peer_list_threads(self, lw: LiveWorker, project: str, query: str | None = None) -> list[dict]:
        project_id = lw.worker.project_id
        if project != project_id:
            raise PeerCallError("threads of another project are not visible")
        threads = [peer_entry(t) for t in await self.store.list_threads(project_id)]
        q = (query or "").lower()
        return [t for t in threads if q in (t["title"] + " " + t["summary"]).lower()] if q else threads

    async def peer_read_thread(self, lw: LiveWorker, thread_id: str, last_n: int = 20) -> dict:
        project_id = lw.worker.project_id
        t = await self._own_thread(project_id, thread_id)
        events = await self.store.list_events(project_id, t["id"], 0, limit=5000)
        recent = [{"seq": e["seq"], "thread": e["thread_id"], "type": e["type"], "ts": e["created_at"],
                   "data": e["payload"]} for e in events[-int(last_n):]]
        return {**peer_entry(t), "events": recent}

    async def peer_send_message(self, lw: LiveWorker, sender: str, to: str, body: str,
                                reply_to: str | None = None, hops: int = 0) -> dict:
        project_id = lw.worker.project_id
        t = await self._own_thread(project_id, to)
        src = await self.store.get_thread(sender) if sender else None
        try:
            await self.send(t["id"], str(body), sender or "unknown", int(hops or 0) + 1, reply_to)
        except ArchivedError as e:
            raise PeerCallError(str(e)) from None
        if src and src["project_id"] == project_id:
            await self.store.append_event(project_id, sender, "message_out", {"to": to, "body": body})
        return {"ok": True}

    async def peer_spawn_thread(self, lw: LiveWorker, parent: str | None, project: str, title: str,
                                request: str) -> dict:
        project_id = lw.worker.project_id
        if project != project_id:
            raise PeerCallError("cannot spawn threads in another project")
        request = str(request or "").strip()
        if not request:
            raise PeerCallError("request is required")
        from .router import title_for
        try:
            t = await self.spawn(project_id, title or title_for(request), request, parent)
        except (RuntimeError, ArchivedError) as e:
            raise PeerCallError(str(e)) from None
        return {"id": t["id"], "title": t["title"]}

    async def peer_set_summary(self, lw: LiveWorker, thread_id: str, summary: str,
                               files: list | None = None) -> dict:
        t = await self._own_thread(lw.worker.project_id, thread_id)
        fields = {"summary": str(summary or "")}
        if isinstance(files, list):
            fields["files"] = [str(f) for f in files]
        await self.store.update_thread(t["id"], **fields)
        return {"ok": True}

    async def peer_save(self, lw: LiveWorker, thread_id: str, land: bool = False) -> dict:
        """A thread's turn ended: take its branch, snapshot and session into the hub;
        with `land`, land the branch there and send the worker the new base
        and branch (runner/API.md, "Repository")."""
        t = await self._own_thread(lw.worker.project_id, thread_id)
        project = await self.store.get_project(t["project_id"])
        hub = await self.hub(project)
        data, refs = await lw.client.pull_thread(t["id"], await hub.have(t["id"]))
        await hub.receive(t["id"], data, refs)
        if not land:
            return {"landed": False, "base": hub.base}
        result = await hub.land(t["id"])
        await self._ship(lw, project, t["id"])
        return result

    PEER_OPS = {"list_threads": peer_list_threads, "read_thread": peer_read_thread,
                "send_message": peer_send_message, "spawn_thread": peer_spawn_thread,
                "set_summary": peer_set_summary, "save": peer_save}

    # ---------------------------------------------------------------- hubs

    def hub_path(self, project_id: str) -> Path:
        return self.repos / f"{project_id}.git"

    async def hub(self, project: dict) -> Hub:
        """The project's hub, made on first use and connected to the
        project's remote (a project from before hubs had one may not be yet)."""
        async with self._hubs_lock:
            hub = self.hubs.get(project["id"])
            if hub is None:
                hub = Hub(self.hub_path(project["id"]), get_secret("GITHUB_TOKEN"))
                await hub.open()
                if hub.url != project["repo_url"]:
                    await hub.connect(project["repo_url"])
                self.hubs[project["id"]] = hub
        return hub

    async def update_project(self, project_id: str, **fields) -> dict:
        """Rename a project, change its model, or attach, change or remove its
        remote (`repo_url`, "" for none). A new remote is connected to the hub
        first (Hub.connect); one that cannot be leaves the project as it was."""
        project = await self.store.get_project(project_id)
        if project is None:
            raise KeyError(project_id)
        if "repo_url" in fields and fields["repo_url"] != project["repo_url"]:
            hub = await self.hub(project)
            await hub.connect(fields["repo_url"])
        return await self.store.update_project(project_id, **fields)

    async def _ship(self, lw: LiveWorker, project: dict, tid: str | None = None) -> None:
        """Bring a worker up to the hub: the base (as it is on the project's
        repository, when there is one), and a thread's branch, snapshot and session."""
        hub = await self.hub(project)
        await hub.sync()
        data, refs = await hub.pack(tid, list((await lw.client.repo_refs()).values()))
        await lw.client.push_repo(data, hub.base, refs)

    async def _ingest(self, project_id: str, ev: dict) -> None:
        tid, typ, data = ev.get("thread"), ev.get("type", "event"), ev.get("data") or {}
        if not tid or await self.store.get_thread(tid) is None:
            return
        if typ == "created":
            return  # the control plane already logged the thread
        if typ == "message_in" and self._echoed(tid, data.get("body")):
            return  # already logged when the control plane took it
        fields = {}
        activity = activity_of(typ, data)
        if activity is not None:
            fields["activity"] = activity
        if typ == "state" and data.get("state") in RUNNER_STATES:
            fields["state"] = data["state"]
        elif typ == "turn_done" and data.get("summary"):
            fields["summary"] = data["summary"]
        if fields:
            await self.store.update_thread(tid, **fields)
        await self.store.append_event(project_id, tid, typ, data)
        if typ in ENDS_STREAM:
            await self.store.delete_events(project_id, tid, "delta")

    # ---------------------------------------------------------------- threads

    async def spawn(self, project_id: str, title: str, request: str, parent: str | None = None,
                    images: list[dict] | None = None) -> dict:
        project = await self.open_project(project_id)
        running = [t for t in await self.store.list_threads(project_id)
                   if t["state"] in ("queued", "working")]
        if len(running) >= self.max_running:
            raise RuntimeError(f"project has {len(running)} running threads (limit {self.max_running})")
        thread = await self.store.create_thread(project_id, title, request, parent)
        tid = thread["id"]
        created = {"title": title, "request": request, "parent": parent}
        if images:
            created["images"] = images
        await self.store.append_event(project_id, tid, "created", created)
        self._expect_echo(tid, request)
        thread = await self.store.update_thread(tid, activity="starting on a worker")
        self.starting[tid] = self._chain(self.thread_chain, tid,
                                         lambda: self._start(project, thread, parent, images))
        return thread

    async def _start(self, project: dict, thread: dict, parent: str | None,
                     images: list[dict] | None = None) -> None:
        """Put a spawned thread on a worker, creating one when every worker is full."""
        tid = thread["id"]
        try:
            lw = await self._pick_worker(project, start=True)
            try:
                await self._ship(lw, project)
                model = {"provider": "deepseek", "model": project["model"]} if project.get("model") else None
                await lw.client.create_thread(id=tid, request=thread["request"], title=thread["title"],
                                              project=project["id"], parent=parent, model=model,
                                              images=await self._inline_images(images) or None)
            finally:
                lw.starting -= 1
        except asyncio.CancelledError:  # stopped before it started
            self.unechoed.pop(tid, None)
            raise
        except Exception as e:
            self.unechoed.pop(tid, None)
            await self.store.update_thread(tid, state="failed", activity="")
            await self.store.append_event(project["id"], tid, "error",
                                          {"error": f"could not start thread: {describe(e)}"})
            return
        finally:
            self.starting.pop(tid, None)
        lw.threads.add(tid)
        self.by_thread[tid] = lw
        await self.store.update_thread(tid, worker_id=lw.worker.id)

    def _expect_echo(self, tid: str, body: str) -> None:
        self.unechoed.setdefault(tid, []).append(body)

    def _echoed(self, tid: str, body) -> bool:
        """Whether a runner's message_in is the echo of a message already logged;
        if so it is no longer expected."""
        pending = self.unechoed.get(tid)
        if not pending or body not in pending:
            return False
        pending.remove(body)
        if not pending:
            del self.unechoed[tid]
        return True

    async def send(self, to: str, body: str, sender: str = "user", hops: int = 0,
                   reply_to: str | None = None, edit: int | None = None,
                   images: list[dict] | None = None) -> dict:
        """Log a message to a thread and queue its delivery. `edit` is the seq
        of the person's message this one corrects, when it is an edit's correction."""
        thread = await self.store.get_thread(to)
        if thread is None:
            raise KeyError(to)
        if thread["archived_at"] is not None:
            raise ArchivedError(f"thread {to} is archived; unarchive it to message it")
        await self.open_project(thread["project_id"])
        msg = await self.store.add_message(thread["project_id"], sender, to, body, reply_to, hops)
        if hops > MAX_HOPS:
            await self.store.append_event(thread["project_id"], to, "dropped",
                                          {"from": sender, "reason": f"hop limit {MAX_HOPS}"})
            return msg
        payload = {"from": sender, "body": body, "message": msg["id"]}
        if edit is not None:
            payload["edit"] = edit
        if images:
            payload["images"] = images
        await self.store.append_event(thread["project_id"], to, "message_in", payload)
        self._expect_echo(to, body)
        self._chain(self.thread_chain, to,
                    lambda: self._deliver(thread["project_id"], msg, hops, images))
        return msg

    async def _deliver(self, project_id: str, msg: dict, hops: int,
                       images: list[dict] | None = None) -> None:
        """Hand a logged message to the thread's runner, after the thread's start
        and the messages before it. A thread on no live worker is brought back
        on one first."""
        to, body = msg["to_thread"], msg["body"]
        try:
            lw = await self.live_worker(to)
            await lw.client.post_message(to, body, msg["sender"], hops,
                                         images=await self._inline_images(images) or None)
        except Exception as e:
            self._echoed(to, body)
            await self.store.append_event(project_id, to, "error",
                                          {"error": f"message not delivered: {describe(e)}"})
            return
        await self.store.mark_read([msg["id"]])

    async def live_worker(self, thread_id: str) -> LiveWorker:
        """The worker running the thread. A thread that runs nowhere (its worker
        went away with an earlier control plane, it stopped or it failed) is
        brought back first, on the worker it ran on while that one is alive:
        its worktree and its conversation, idle until its next message.
        Concurrent callers share one revival.

        Raises KeyError for an unknown thread, ArchivedError for an archived
        one or one in an archived project."""
        t = await self.store.get_thread(thread_id)
        if t is None:
            raise KeyError(thread_id)
        if t["archived_at"] is not None:
            raise ArchivedError(f"thread {thread_id} is archived; unarchive it to use it")
        await self.open_project(t["project_id"])
        starting = self.starting.get(thread_id)
        if starting is not None:
            await asyncio.wait({starting})
            t = await self.store.get_thread(thread_id)
        lw = self.by_thread.get(thread_id)
        if lw is not None:
            if await self._health(lw) is None:
                await self._lose(lw, lw.health_error)
                lw = None
            elif t["state"] not in ENDED:
                return lw
        task = self.reviving.get(thread_id)
        if task is None:
            task = asyncio.create_task(self._revive(t, lw))
            self.reviving[thread_id] = task
            self.background.add(task)
            task.add_done_callback(self.background.discard)
            task.add_done_callback(lambda _: self.reviving.pop(thread_id, None))
        return await asyncio.shield(task)

    async def _revive(self, t: dict, lw: LiveWorker | None) -> LiveWorker:
        """Start the thread again on the worker that ran it (`lw` when it is
        live here, else the one an earlier control plane left), so its
        worktree there is the one it continues in; on a picked worker when
        that was lost, from its branch, snapshot and session in the hub."""
        tid, project = t["id"], await self.store.get_project(t["project_id"])
        if lw is None:
            lw = await self._home(project, t["worker_id"]) or await self._pick_worker(project)
        await self._ship(lw, project, tid)
        model = {"provider": "deepseek", "model": project["model"]} if project.get("model") else None
        await lw.client.create_thread(id=tid, title=t["title"], project=project["id"], parent=t["parent_id"],
                                      model=model, summary=t["summary"])
        old = self.by_thread.get(tid)
        if old is not None:
            old.threads.discard(tid)
        lw.threads.add(tid)
        self.by_thread[tid] = lw
        await self.store.update_thread(tid, worker_id=lw.worker.id, state="idle", activity="")
        await self.store.append_event(project["id"], tid, "revived", {"worker": lw.worker.id})
        return lw

    async def _inline_images(self, refs: list[dict] | None) -> list[dict]:
        """Image references as a runner needs them: the bytes, base64-encoded.
        A worker cannot reach this control plane, so they travel in the message."""
        out = []
        for ref in refs or []:
            row = await self.store.get_image(ref.get("id")) if isinstance(ref, dict) else None
            if row is not None:
                out.append({"media_type": row["media_type"],
                            "data": base64.b64encode(row["data"]).decode("ascii")})
        return out

    async def worker_logs(self, thread_id: str, lines: int = 300) -> str:
        """The runner log of the worker a thread runs on, or last ran on when
        that one is not attached (the control plane reads it from the
        provider; the worker never sends it). A thread on no worker gets why:
        the worker it lost, why and when, and that worker's log as it ended;
        or why it never started on one. KeyError for an unknown thread."""
        lw = self.by_thread.get(thread_id)
        if lw is not None:
            return await self.provider.logs(lw.worker, lines)
        t = await self.store.get_thread(thread_id)
        if t is None:
            raise KeyError(thread_id)
        workers = {w["id"]: w for w in await self.store.list_workers(t["project_id"])}
        w = workers.get(t["worker_id"])
        if w is not None and w["lost_at"] is None:  # down now, or left by an earlier control plane
            return await self.provider.logs(stored_worker(w), lines)
        return await self._no_worker(t, workers, lines)

    async def _no_worker(self, t: dict, workers: dict[str, dict], lines: int) -> str:
        tid = t["id"]
        events = await self.store.recent_events(t["project_id"], tid, limit=500)
        last = next((e for e in reversed(events) if e["type"] in ("worker_lost", "revived")
                     or (e["type"] == "error" and e["payload"].get("error", "").startswith("could not start"))),
                    None)
        if last is not None and last["type"] == "worker_lost":
            wid = last["payload"].get("worker", "")
            w = workers.get(wid) or {}
            kept = (w.get("lost_log") or "").splitlines()[-lines:]
            text = (f"thread {tid} is on no worker: its worker {wid} was dropped at {when(last['created_at'])}, "
                    f"because {w.get('lost_reason') or 'no reason was recorded'}.\n"
                    "It comes back on a worker with its next message.\n\n")
            if kept:
                return text + f"The end of {wid}'s runner log when it was dropped:\n" + "\n".join(kept) + "\n"
            return text + f"No log was read from {wid} before it was dropped.\n"
        if last is not None and last["type"] == "error":
            return (f"thread {tid} never started on a worker ({when(last['created_at'])}): "
                    f"{last['payload']['error']}\n")
        if tid in self.starting:
            return (f"thread {tid} is waiting for a worker: one is being started or picked for it. "
                    "The control plane's output shows the new worker's log as it starts.\n")
        return f"thread {tid} has no worker and has no record of one (state {t['state']}).\n"

    async def list_files(self, thread_id: str, path: str) -> dict:
        """A directory of the thread's worktree, read from the worker that holds
        it (the control plane asks; the worker never sends files on its own).
        A thread with no live worker is revived first, from the hub."""
        lw = await self.live_worker(thread_id)
        return await lw.client.list_files(thread_id, path)

    async def open_file(self, thread_id: str, path: str) -> httpx.Response:
        """A file of the thread's worktree, streaming from its worker; the
        caller closes the response."""
        lw = await self.live_worker(thread_id)
        return await lw.client.open_file(thread_id, path)

    async def stop(self, thread_id: str) -> None:
        starting = self.starting.get(thread_id)
        if starting:
            starting.cancel()
        lw = self.by_thread.get(thread_id)
        if lw:
            await lw.client.stop(thread_id)
        await self.store.update_thread(thread_id, state="stopped", activity="")

    async def archive(self, thread_id: str) -> dict:
        """Hide a thread from the main list, the router and its peers. A thread
        still on its runner is stopped first."""
        t = await self.store.get_thread(thread_id)
        if t is None:
            raise KeyError(thread_id)
        if t["archived_at"] is not None:
            return t
        if t["state"] not in ENDED:
            await self.stop(thread_id)
        t = await self.store.update_thread(thread_id, archived_at=now())
        await self.store.append_event(t["project_id"], thread_id, "archived", {})
        return t

    async def unarchive(self, thread_id: str) -> dict:
        t = await self.store.get_thread(thread_id)
        if t is None:
            raise KeyError(thread_id)
        if t["archived_at"] is None:
            return t
        t = await self.store.update_thread(thread_id, archived_at=None)
        await self.store.append_event(t["project_id"], thread_id, "unarchived", {})
        return t

    # ---------------------------------------------------------------- projects
    #
    # An archived project leaves the project list and takes no work: no
    # requests, messages, edits, spawns or file reads, from the person or from
    # a peer. Archiving it stops every thread still on a runner and destroys
    # the project's workers, so it holds no compute. Its threads keep their
    # own state and archived flags, its log and its hub (every turn's work is
    # in the hub already). Unarchived, it is as it was: each thread comes back
    # on a new worker, from its branch, snapshot and session in the hub, with its next
    # message.

    async def open_project(self, project_id: str) -> dict:
        """The project, if it can take work: KeyError for an unknown one,
        ArchivedError for an archived one."""
        p = await self.store.get_project(project_id)
        if p is None:
            raise KeyError(project_id)
        if p["archived_at"] is not None:
            raise ArchivedError(f"project {p['name']} is archived; unarchive it to use it")
        return p

    async def archive_project(self, project_id: str) -> dict:
        """Archive a project: it is marked first, so nothing new starts (requests
        still queued for routing fail, and say so in its log), then its running
        threads are stopped and its workers destroyed. Archiving an
        archived project finishes that (a worker whose destroy failed is
        destroyed again) without logging it twice."""
        p = await self.store.get_project(project_id)
        if p is None:
            raise KeyError(project_id)
        if p["archived_at"] is None:
            p = await self.store.update_project(project_id, archived_at=now())
            await self.store.append_event(project_id, None, "project_archived", {})
        for t in await self.store.list_threads(project_id) + await self.store.list_threads(project_id, True):
            if t["state"] not in ENDED:
                await self.stop(t["id"])
        await self._destroy_workers(project_id)
        return p

    async def _destroy_workers(self, project_id: str) -> None:
        """Let go of every worker the project has and destroy it. Each is marked
        lost, so its index (and its sprite's name) is never used again."""
        async with self._lock:
            for lw in self.workers.pop(project_id, []):
                for task in lw.tasks():
                    task.cancel()
                for tid in lw.threads:
                    if self.by_thread.get(tid) is lw:
                        del self.by_thread[tid]
                await lw.client.close()
            await self._made(project_id)
            for w in await self.store.list_workers(project_id):
                if w["lost_at"] is None and attachable(w):
                    await self.provider.destroy(stored_worker(w))
                    self.lost[project_id].add(w["idx"])
                    await self.store.lose_worker(w["id"], "its project was archived", "")

    async def unarchive_project(self, project_id: str) -> dict:
        p = await self.store.get_project(project_id)
        if p is None:
            raise KeyError(project_id)
        if p["archived_at"] is None:
            return p
        p = await self.store.update_project(project_id, archived_at=None)
        await self.store.append_event(project_id, None, "project_unarchived", {})
        return p

    # ---------------------------------------------------------------- requests

    async def submit(self, project_id: str, text: str, mode: str = "auto",
                     target: str | None = None, images: list[dict] | None = None) -> dict:
        """Take a request typed in a project's main window; it returns at once.

        Sending to a thread or forcing a new one only logs and queues (a
        thread's start and deliveries run in the background). Letting the
        router decide may ask a model, so the request is logged and routed in
        the background, in the order requests came in; its plan arrives as the
        "route" event. A request that is only images needs no router: there is
        no text to route, so it starts one thread."""
        await self.open_project(project_id)
        if mode == "thread":
            t = await self.store.get_thread(target) if target else None
            if t is None or t["project_id"] != project_id:
                raise KeyError(f"no thread {target} in this project")
        if mode != "auto":
            return await self.handle_request(project_id, text, mode, target, images)
        payload = {"text": text}
        if images:
            payload["images"] = images
        ev = await self.store.append_event(project_id, None, "request", payload)
        if images and not text:
            return await self.handle_request(project_id, text, "new", None, images, of=ev["seq"])
        self._chain(self.request_chain, project_id,
                    lambda: self._route(project_id, text, ev["seq"], images))
        return {"request": text, "mode": mode, "queued": True, "seq": ev["seq"]}

    async def _route(self, project_id: str, text: str, of: int,
                     images: list[dict] | None = None) -> None:
        try:
            await self.handle_request(project_id, text, "auto", None, images, of=of)
        except Exception as e:
            log.exception("request in %s", project_id)
            await self.store.append_event(project_id, None, "error",
                                          {"error": f"could not route the request: {describe(e)}"})

    async def handle_request(self, project_id: str, text: str, mode: str = "auto",
                             target: str | None = None, images: list[dict] | None = None,
                             of: int | None = None) -> dict:
        """mode: auto (router decides), thread (send to target), new (force a new thread).
        `of` is the seq of the request event it routes; the route event names it,
        so an edit of the request knows which threads it reached."""
        directory = await self.store.list_threads(project_id)
        if mode == "thread" and target:
            plan = {"spawn": [], "send": [{"thread": target, "message": text}], "why": "sent directly"}
        elif mode == "new":
            from .router import title_for
            title = title_for(text) if text else image_title(images)
            plan = {"spawn": [{"title": title, "request": text}], "send": [], "why": "new thread requested"}
        else:
            plan = await self.router.plan(text, directory)
        spawned, sent = [], []
        for s in plan["spawn"]:
            t = await self.spawn(project_id, s["title"], s["request"], images=images)
            spawned.append({"id": t["id"], "title": t["title"]})
        # A message the router wrote is from the router; one sent directly is the user's own.
        sender = "user" if mode == "thread" else "router"
        for s in plan["send"]:
            await self.send(s["thread"], s["message"], sender, images=images)
            sent.append(s["thread"])
        # The summary names threads by title; the ids stay in spawned and sent.
        titles = {t["id"]: t["title"] for t in directory}
        parts = []
        if spawned:
            parts.append("started " + ", ".join(f"“{t['title']}”" for t in spawned))
        if sent:
            parts.append("told " + ", ".join(f"“{titles.get(tid, tid)}”" for tid in sent))
        result = {"request": text, "mode": mode, "spawned": spawned, "sent": sent, "why": plan.get("why", ""),
                  "summary": ", ".join(parts) or "nothing to do"}
        if of is not None:
            result["of"] = of
        await self.store.append_event(project_id, None, "route", result)
        return result

    # ---------------------------------------------------------------- edits
    #
    # The person can edit a message they sent: a request typed in the project
    # or a message sent straight to a thread. The edit is logged at once (the
    # UI shows the new text on the message, marked edited) and re-assessed in
    # the background, after any routing queued before it, so the original's
    # own routing has always run first:
    #
    # - every live thread the message reached (the threads its routes, and
    #   earlier edits' routes, spawned or told; the thread a direct message
    #   went to) gets a correction from the person with both versions, and
    #   decides itself what to keep, drop or undo. Nothing is stopped: a
    #   thread mid-turn reads it before its next model call, an ended one is
    #   brought back and acts on it;
    # - the router revises the plan for the new text knowing those threads,
    #   spawning threads only for parts none of them covers.

    async def edit(self, project_id: str, seq: int, text: str) -> dict:
        """Take an edit of the person's message `seq`; it returns at once."""
        ev = await self.store.get_event(seq)
        if ev is None or ev["project_id"] != project_id:
            raise KeyError(f"no message {seq} in this project")
        if not editable(ev):
            raise EditError("only your own messages can be edited")
        await self.open_project(project_id)
        if ev["thread_id"] is not None:
            t = await self.store.get_thread(ev["thread_id"])
            if t["archived_at"] is not None:
                raise ArchivedError(f"thread {t['id']} is archived; unarchive it to edit messages to it")
        previous = await self.current_text(ev)
        if text == previous:
            return {"seq": seq, "text": text, "changed": False}
        await self.store.append_event(project_id, ev["thread_id"], "edited",
                                      {"of": seq, "text": text, "previous": previous})
        self._chain(self.request_chain, project_id, lambda: self._reassess(ev, previous, text))
        return {"seq": seq, "text": text, "changed": True, "queued": True}

    async def current_text(self, ev: dict) -> str:
        """The message's text after its edits so far."""
        text = ev["payload"].get("text" if ev["type"] == "request" else "body", "")
        for e in await self._all_events(ev["project_id"], ev["seq"]):
            if e["type"] == "edited" and e["payload"].get("of") == ev["seq"]:
                text = e["payload"].get("text", text)
        return text

    async def reached(self, ev: dict) -> list[str]:
        """The threads a message reached so far, in the order it reached them."""
        out = [ev["thread_id"]] if ev["thread_id"] else []
        for e in await self._all_events(ev["project_id"], ev["seq"]):
            if e["type"] == "route" and e["payload"].get("of") == ev["seq"]:
                d = e["payload"]
                out += [s["id"] for s in d.get("spawned") or []] + list(d.get("sent") or [])
        return list(dict.fromkeys(out))

    async def _all_events(self, project_id: str, after: int) -> list[dict]:
        events = []
        while page := await self.store.list_events(project_id, None, after, limit=1000):
            events += page
            after = page[-1]["seq"]
        return events

    async def _reassess(self, ev: dict, previous: str, text: str) -> None:
        project_id, seq = ev["project_id"], ev["seq"]
        try:
            await self._revise(ev, previous, text)
        except Exception as e:
            log.exception("edit of %s in %s", seq, project_id)
            await self.store.append_event(project_id, None, "error",
                                          {"error": f"could not re-route the edited message: {describe(e)}"})

    async def _revise(self, ev: dict, previous: str, text: str) -> None:
        project_id, seq = ev["project_id"], ev["seq"]
        directory = await self.store.list_threads(project_id)
        live = {t["id"] for t in directory}
        reached = [tid for tid in await self.reached(ev) if tid in live]
        plan = await self.router.revise(text, previous, reached, directory)
        body = correction(ev["type"], previous, text)
        for tid in reached:
            await self.send(tid, body, "user", edit=seq)
        spawned, told = [], []
        for s in plan["spawn"]:
            t = await self.spawn(project_id, s["title"], s["request"])
            spawned.append({"id": t["id"], "title": t["title"]})
        for s in plan["send"]:
            await self.send(s["thread"], s["message"], "router")
            told.append(s["thread"])
        titles = {t["id"]: t["title"] for t in directory}
        parts = []
        if reached:
            parts.append("corrected " + ", ".join(f"“{titles[tid]}”" for tid in reached))
        if spawned:
            parts.append("started " + ", ".join(f"“{t['title']}”" for t in spawned))
        if told:
            parts.append("told " + ", ".join(f"“{titles.get(tid, tid)}”" for tid in told))
        await self.store.append_event(project_id, None, "route", {
            "request": text, "mode": "edit", "of": seq, "corrected": reached, "spawned": spawned,
            "sent": list(dict.fromkeys(reached + told)), "why": plan.get("why", ""),
            "summary": "edit: " + (", ".join(parts) or "nothing to do")})
