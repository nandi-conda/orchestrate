"""The control plane's core: workers, threads, routing and the message bus.

Every runner event is relayed into the store, so the UI reads one ordered
event log per project no matter how many workers the project has. Every
project's commits gather in its hub, a git repository on this machine
(hub.py), which the control plane syncs workers with.
"""

from __future__ import annotations

import asyncio
import base64
import calendar
import contextlib
import json
import logging
import re
import shutil
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from .hub import Hub, remote_url
from .load import load_of
from .providers import PLACES, Places, Worker, WorkerProvider, WorkerSpec, wait_healthy
from .router import Router
from .runner_client import RunnerClient
from .secrets import get_secret
from .states import BINDING, RUNNING, WORKER
from .steps import Steps, describe
from .store import Store, new_id, now

log = logging.getLogger("orchestrator")

MAX_HOPS = 6
# A worker is watched every HEALTH_EVERY seconds. A /health that does not
# answer does not lose it: a sprite pauses while nothing talks to it and its
# runner starts afresh on a cold wake, and this control plane's own network
# can be down. A worker is lost only when its provider says it is gone; one
# that is still there is unreachable, and is reattached (its runner started
# again) once it has not answered for READY_TIMEOUT seconds (states.py).
HEALTH_EVERY = 10.0
HEALTH_TIMEOUT = 5.0
READY_TIMEOUT = 300.0
CAPACITY = 16  # running threads per worker before another one starts
# Each place workers run in (providers.PLACES) has a pool of at most POOL
# workers, shared by every project that runs there. A slot (a place and
# 1..POOL) always names the same worker, so the pool never holds more, and
# what a provider has beyond the slots is destroyed when the control plane
# starts (Orchestrator.start). A thread that finds every worker of its
# project's place full, or not ready, waits for room; no worker is made for it.
POOL = 3
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


def worker_spec(index: int) -> WorkerSpec:
    return WorkerSpec(index=index, capacity=CAPACITY)


def pooled(w: dict) -> bool:
    """A stored worker of a pool, which a control plane attaches to again
    by its slot (its provider, the place, and its index). The rest are from
    before workers were a pool shared by every project (each project had its
    own, named for it)."""
    return w["project_id"] == "" and w["idx"] >= 1


Slot = tuple[str, int]  # a place (providers.PLACES) and an index in its pool, from 1


def slot_of(worker: Worker) -> Slot:
    return worker.provider, worker.index


def stored_worker(w: dict) -> Worker:
    """A worker as the store keeps it, for the provider."""
    return Worker(id=w["id"], provider=w["provider"], handle=w["handle"],
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


class PeerCallError(Exception):
    """A peer call the orchestrator refuses; its text goes back to the tool."""


class ArchivedError(Exception):
    """The thread, or its project, is archived: it takes no work until it is unarchived."""


class NotOnWorker(Exception):
    """The thread is on no ready worker now, so its worktree cannot be read;
    the text says where it stands."""


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
    # Picks of it that ended (posted or not). A /health looked at before one
    # ended may not count it, so a pick counts those that ended since its look.
    picked: int = 0
    log_tail: deque = field(default_factory=lambda: deque(maxlen=LOG_KEPT))
    boot: str | None = None  # the runner process serving it, as /health names it
    # Its state in states.WORKER and why it is in it; only Orchestrator._worker_to changes them.
    state: str = "ready"
    reason: str = ""
    unreachable_since: float = 0.0
    failures: int = 0  # reattaches that failed, so a waiter knows one did
    reattach: asyncio.Task | None = None
    changed: asyncio.Condition = field(default_factory=asyncio.Condition)

    def tasks(self) -> list[asyncio.Task]:
        return [task for task in (self.relay, self.pump, self.watch, self.logs, self.reattach) if task]


# The time a runner log line starts with: UTC, to the millisecond, marked Z
# (configure_logging in runner/src/runner/server.py).
RUNNER_STAMP = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d{3})Z ")


def local_line(line: str) -> str:
    """A runner log line with its UTC time in the control plane's local time."""
    m = RUNNER_STAMP.match(line)
    if m is None:
        return line
    ts = calendar.timegm(time.strptime(m[1], "%Y-%m-%dT%H:%M:%S"))
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) + m[2] + " " + line[m.end():]


def local_log(text: str) -> str:
    return "\n".join(local_line(line) for line in text.split("\n"))


def print_worker_line(worker_id: str, line: str) -> None:
    sys.stdout.write(f"{worker_id} | {local_line(line)}\n")
    sys.stdout.flush()


def when(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(ts))


class Orchestrator:
    def __init__(self, store: Store, places: Places, router: Router, repos: str | Path,
                 max_running: int = 64, pool: int = POOL) -> None:
        self.store = store
        # One git repository per project, the hub its threads' work goes to (hub.py).
        self.repos = Path(repos).resolve()
        self.hubs: dict[str, Hub] = {}
        self._hubs_lock = asyncio.Lock()
        # Where workers run: each project's `workers` names a place of these.
        self.places = places
        self.router = router
        self.max_running = max_running
        # The pools' workers this control plane has attached, each in its slot
        # (slot_of: its place and index, 1..pool), shared by every project
        # that runs there. A worker
        # outlives the control plane: its disk holds the threads' worktrees,
        # so a later control plane attaches to it again by its slot. A lost
        # worker's slot is filled again with a new worker of the same name.
        self.pool = pool
        self.workers: list[LiveWorker] = []
        # Notified whenever a worker may have room again (a thread's state
        # changed, a worker's did, a start was posted); _pick_worker waits on it.
        self.room = asyncio.Condition()
        self.by_thread: dict[str, LiveWorker] = {}
        # Per thread, the messages the control plane logged when it took them
        # (a spawned thread's request as "created", the rest as "message_in")
        # and the runner has yet to echo back as message_in; the echo is dropped.
        self.unechoed: dict[str, list[str]] = {}
        # Work runs in the background, in order per key: a project's requests
        # (routing them may ask a model) and a thread's deliveries. The API
        # only logs and queues.
        self.request_chain: dict[str, asyncio.Task] = {}
        self.thread_chain: dict[str, asyncio.Task] = {}
        # Per thread, the task putting it on a worker while its binding is
        # "binding" (_begin_binding); deliveries wait for it.
        self.binding: dict[str, asyncio.Task] = {}
        # Projects whose threads' bindings this control plane has taken over
        # from the store (open_project), and the lock each takeover holds.
        self.recovered: set[str] = set()
        self._recover_locks: dict[str, asyncio.Lock] = {}
        # Per pool slot, the attach under way (_attached) and the steps of
        # its latest attach or reattach, for the Worker log tab.
        self.attaching: dict[Slot, asyncio.Task] = {}
        self.attach_steps: dict[Slot, Steps] = {}
        # Per thread, the steps of its latest binding (_bind), for the Worker log tab.
        self.bind_steps: dict[str, Steps] = {}
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
        for lw in self.workers:
            tasks += lw.tasks()
        for task in tasks:
            task.cancel()
        # Wait for them to end, so none touches the store after it closes.
        await asyncio.gather(*tasks, return_exceptions=True)
        for lw in self.workers:
            await lw.client.close()

    async def start(self) -> None:
        """Make each place's provider hold its pool's workers and nothing
        else: every worker of ours it has outside the slots is destroyed
        (workers each project had before the pools, a pool that was bigger,
        workers of a control plane whose store is gone), and the store says
        so of those it knew. A thread from before threads had a place, that
        was on a worker, gets the place its worker was in; one that never
        was is left for the router to place when it is put on a worker.
        Called once, before the control plane takes work."""
        swept = set()
        for place, provider in self.places.providers.items():
            destroyed = await provider.sweep(self.pool)
            swept.add(place)
            if destroyed:
                log.warning("destroyed %d %s worker(s) outside the pool's %d slots: %s",
                            len(destroyed), place, self.pool, ", ".join(destroyed))
        for w in await self.store.list_workers():
            if w["lost_at"] is None and w["provider"] in swept and not (pooled(w) and w["idx"] <= self.pool):
                await self.store.lose_worker(w["id"], f"workers are a pool of {self.pool} per place, shared by "
                                                      "every project; this one was outside it and was destroyed", "")
        ran_on = {w["id"]: w["provider"] for w in await self.store.list_workers()}
        for p in await self.store.list_projects() + await self.store.list_projects(archived=True):
            for t in await self.store.list_threads(p["id"]) + await self.store.list_threads(p["id"], True):
                if not t["workers"] and t["worker_id"] in ran_on:
                    await self.store.update_thread(t["id"], workers=ran_on[t["worker_id"]])

    def provider(self, worker: Worker) -> WorkerProvider:
        """The provider of the place a worker is in."""
        return self.places.provider(worker.provider)

    # ---------------------------------------------------------------- workers

    async def _pick_worker(self, project: dict, place: str, steps: Steps) -> LiveWorker:
        """The least loaded ready worker of the pool with room, with a place
        held for the thread the caller is about to start there (`starting`,
        given back by the caller), so concurrent starts spread out. With no
        worker with room, the pool's first empty slot is filled (_attached);
        with every slot filled, it waits for room. Workers are looked at
        concurrently and never under a lock, and one being made holds no
        lock: a worker that is slow to come up only keeps the threads
        waiting for it waiting. Only the pool of `place`, where the thread's
        workers run, is looked at: PlaceRefused when this control plane runs
        no workers there."""
        self.places.provider(place)
        async with contextlib.AsyncExitStack() as waiting:
            said = False
            while True:
                seen = await self._look(place)
                lw = self._roomiest(seen)
                if lw is not None:
                    lw.starting += 1
                    return lw
                filled = {lw.worker.index for lw in self.workers if lw.worker.provider == place}
                empty = [i for i in range(1, self.pool + 1) if i not in filled]
                if empty:
                    await self._attached(project, (place, empty[0]), steps)
                    continue
                if not said:
                    said = True
                    await waiting.enter_async_context(steps.step(
                        f"waiting for room: all {self.pool} {place} workers are full or not ready "
                        f"({self._pool_state(place, seen)})"))
                # Room is looked for again when a worker may have some, or
                # after a health check's while, since a worker's load is its
                # runner's to say.
                async with self.room:
                    try:
                        await asyncio.wait_for(self.room.wait(), HEALTH_EVERY)
                    except asyncio.TimeoutError:
                        pass

    async def _made_room(self) -> None:
        async with self.room:
            self.room.notify_all()

    async def _look(self, place: str) -> dict[str, tuple[LiveWorker, dict]]:
        """Each ready worker's of the place's pool /health, looked at
        concurrently, by worker id; one that does not answer is left out (and
        moved on by _probe)."""
        ready = [lw for lw in self.workers if lw.state == "ready" and lw.worker.provider == place]
        before = [lw.picked for lw in ready]
        healths = await asyncio.gather(*(self._probe(lw) for lw in ready))
        return {lw.worker.id: (lw, {**h, "picked": n}) for lw, h, n in zip(ready, healths, before) if h is not None}

    @staticmethod
    def _running(lw: LiveWorker, h: dict) -> int:
        """The threads a worker runs, as its /health said, with those picked
        for it since: being started there, or started after the look."""
        return h.get("running", 0) + lw.starting + lw.picked - h["picked"]

    def _roomiest(self, seen: dict[str, tuple[LiveWorker, dict]]) -> LiveWorker | None:
        """Of the workers looked at, the one still ready with the most room;
        None when none has any. Nothing is awaited, so no other start can
        take the room it counts between the look and the place held."""
        best, best_load = None, None
        for lw, h in seen.values():
            if lw.state != "ready" or lw not in self.workers:
                continue
            running = self._running(lw, h)
            if running < h.get("capacity", lw.worker.capacity) and (best_load is None or running < best_load):
                best, best_load = lw, running
        return best

    def _pool_state(self, place: str, seen: dict[str, tuple[LiveWorker, dict]]) -> str:
        parts = []
        for lw in sorted((lw for lw in self.workers if lw.worker.provider == place), key=lambda lw: lw.worker.index):
            if lw.worker.id in seen:
                h = seen[lw.worker.id][1]
                parts.append(f"{lw.worker.id} runs {self._running(lw, h)} of "
                             f"{h.get('capacity', lw.worker.capacity)}")
            else:
                parts.append(f"{lw.worker.id} is {lw.state}" + (f": {lw.reason}" if lw.reason else ""))
        return "; ".join(parts)

    async def _attached(self, project: dict, slot: Slot, steps: Steps) -> LiveWorker:
        """The worker in `slot`, attached: at once when it is,
        else once the one attach of that slot under way (or started now, for
        a thread of `project`) has ended. The caller's `steps` wait on the
        attach's own, which the thread's Worker log tab shows. Calling the
        caller off does not call the attach off: other threads may wait on
        it. Only Respawn of a thread waiting on it does (respawn), and then
        every thread still waiting starts the slot's attach again."""
        place, index = slot
        while True:
            for lw in self.workers:
                if slot_of(lw.worker) == slot:
                    return lw
            task = self.attaching.get(slot)
            if task is None or task.done():  # one that ended goes from attaching as its callbacks run
                made = Steps(f"{place} worker slot {index}")
                task = asyncio.create_task(self._attach(project, slot, made))
                self.attaching[slot], self.attach_steps[slot] = task, made
                self.background.add(task)
                task.add_done_callback(self.background.discard)
                task.add_done_callback(lambda t: self.attaching.pop(slot) if self.attaching.get(slot) is t
                                       else None)
                task.add_done_callback(lambda t: t.cancelled() or t.exception())  # its waiters report it
            made = self.attach_steps[slot]
            try:
                async with steps.step(f"waiting for {place} worker slot {index} to come up"):
                    steps.waits_on = made
                    try:
                        return await asyncio.shield(task)
                    finally:
                        steps.waits_on = None
            except asyncio.CancelledError:
                if task.cancelled() and not asyncio.current_task().cancelling():
                    continue  # Respawn called the attach off (respawn): attach the slot afresh
                raise

    async def _attach(self, project: dict, slot: Slot, steps: Steps) -> LiveWorker:
        """Start the worker in `slot`: made new, or the one an earlier control
        plane made, with its disk. It is attached for a thread of `project`,
        whose log says so. Only _attached calls it, so each slot has one
        attach at a time."""
        place, index = slot
        provider = self.places.provider(place)
        spec = worker_spec(index)
        spec.steps = steps
        spec.token = next((w["token"] for w in await self.store.list_workers()
                           if (w["provider"], w["idx"]) == slot and pooled(w) and w["lost_at"] is None), "")
        async with steps.step(f"making or finding its worker ({provider.name} provider)"):
            worker = await provider.create(spec)
        steps.name = f"worker {worker.id}"
        await self.store.upsert_worker(worker.record())
        lw = LiveWorker(worker, RunnerClient(worker.url, worker.token))
        async with steps.step("reading which runner process serves it (/health)"):
            lw.boot = (await lw.client.health(timeout=HEALTH_TIMEOUT)).get("boot")
        async with steps.step("taking archived projects off it"):
            await self._prune(lw)
        lw.logs = asyncio.create_task(self._follow_logs(lw))
        lw.relay = asyncio.create_task(self._relay(lw))
        lw.pump = asyncio.create_task(self._pump_peer_calls(lw))
        lw.watch = asyncio.create_task(self._watch(lw))
        self.workers.append(lw)
        steps.note(f"attached at {worker.url}")
        await self.store.append_event(project["id"], None, "worker",
                                      {"id": worker.id, "provider": worker.provider, "url": worker.url})
        await self._made_room()
        return lw

    async def _prune(self, lw: LiveWorker) -> None:
        """Take every project that is gone from this control plane's list
        (archived) off the worker: its threads there and their worktrees.
        Everything of theirs is in the projects' hubs."""
        keep = [p["id"] for p in await self.store.list_projects()]
        removed = await lw.client.prune(keep)
        if removed:
            log.info("worker %s: took archived project(s) %s off it", lw.worker.id, ", ".join(removed))

    async def _home(self, project: dict, place: str, worker_id: str | None, steps: Steps) -> LiveWorker | None:
        """The worker a thread ran on, attached again if an earlier control
        plane left it; None when it has none, that worker was lost (or was
        from before the pools), or it is not in `place`, where the thread's
        workers run now."""
        if worker_id is None:
            return None
        for lw in self.workers:
            if lw.worker.id == worker_id:
                return lw if lw.worker.provider == place else None
        for w in await self.store.list_workers():
            if (w["id"] == worker_id and w["lost_at"] is None and pooled(w) and w["idx"] <= self.pool
                    and w["provider"] == place):
                return await self._attached(project, (w["provider"], w["idx"]), steps)
        return None

    # Every change of a worker's state goes through _worker_to, and every change
    # of a thread's binding through _bind_to: each checks the move against its
    # machine in states.py, keeps the reason and logs it as an event.

    async def _worker_to(self, lw: LiveWorker, to: str, reason: str) -> None:
        """Move a worker to state `to` (states.WORKER), or raise IllegalTransition.
        The move is made before anything is awaited, so two callers that
        looked at the same state cannot both make it."""
        frm, wid = lw.state, lw.worker.id
        WORKER.check(f"worker {wid}", frm, to)
        lw.state, lw.reason = to, reason
        if to == "unreachable" and frm != "unreachable":
            lw.unreachable_since = asyncio.get_running_loop().time()
        (log.info if to == "ready" else log.warning)("worker %s: %s -> %s: %s", wid, frm, to, reason)
        await self.store.set_worker_state(wid, to, reason)
        # The worker is every project's: each project with a thread on it, or
        # last on it, logs it.
        for project_id in await self.store.projects_on_worker(wid):
            await self.store.append_event(project_id, None, "worker_state",
                                          {"worker": wid, "from": frm, "to": to, "reason": reason})
        async with lw.changed:
            lw.changed.notify_all()
        await self._made_room()


    async def _bind_to(self, tid: str, to: str, reason: str, worker: str | None = None, **fields) -> dict:
        """Move a thread's binding to `to` (states.BINDING), or raise
        IllegalTransition. Unbinding cuts a running turn short: the thread fails."""
        t = await self.store.get_thread(tid)
        frm = t["binding"]
        BINDING.check(f"thread {tid}", frm, to)
        fields.update(binding=to, binding_reason=reason)
        fields.setdefault("activity", "starting on a worker" if to == "binding" else "")
        if to == "unbound" and t["state"] in RUNNING:
            fields["state"] = "failed"
        (log.info if to != "unbound" else log.warning)("thread %s: %s -> %s: %s", tid, frm, to, reason)
        t = await self.store.update_thread(tid, **fields)
        payload = {"from": frm, "to": to, "reason": reason}
        if worker is not None:
            payload["worker"] = worker
        await self.store.append_event(t["project_id"], tid, "binding", payload)
        return t

    async def _unbind_all(self, lw: LiveWorker, reason: str) -> None:
        """Unbind every thread bound to the worker, but those being put back
        on a worker already (_bind has them). Each keeps it as its worker_id,
        the worker it was last on: it comes back there, in its worktree,
        unless that worker is lost (_bind)."""
        for tid in sorted(lw.threads):
            if self.by_thread.get(tid) is not lw or tid in self.binding:
                continue
            del self.by_thread[tid]
            await self._bind_to(tid, "unbound", reason, worker=lw.worker.id)
        lw.threads.clear()

    async def _probe(self, lw: LiveWorker) -> dict | None:
        """Look at a ready or unreachable worker once and move it to the state
        what was seen says: its /health when it answers, else None. One that
        does not answer is lost when its provider says it is gone, unreachable
        while the provider still has it (or cannot say), and reattached once
        it has been unreachable for READY_TIMEOUT seconds."""
        if lw.state not in ("ready", "unreachable"):
            return None
        try:
            h = await lw.client.health(timeout=HEALTH_TIMEOUT)
        except Exception as e:
            failed = describe(e)
            try:
                there = await self.provider(lw.worker).exists(lw.worker)
            except Exception as x:
                there, failed = True, f"{failed}; its provider could not say whether it still has it ({describe(x)})"
            if not there:
                await self._lose(lw, f"its /health failed with {failed}, and its {lw.worker.provider} "
                                     "provider no longer has it")
            elif lw.state == "ready":
                await self._worker_to(lw, "unreachable", f"its /health failed with {failed}; "
                                                         "its provider still has it")
            elif (lw.state == "unreachable"
                  and asyncio.get_running_loop().time() - lw.unreachable_since >= READY_TIMEOUT):
                await self._reattach(lw, f"it has not answered for {READY_TIMEOUT:.0f}s ({failed})")
            return None
        await self._restarted(lw, h)
        if lw.state == "unreachable":
            await self._prune(lw)  # a project may have been archived while it was away
            await self._worker_to(lw, "ready", "its runner answers again")
        return h

    async def _reattach(self, lw: LiveWorker, why: str, waiter: Steps | None = None) -> None:
        """Have the provider start an unreachable worker's runner again, on the
        same disk (provider.create with its index and token). Concurrent
        callers share one attempt; it ends with the worker ready, unreachable
        again (the attempt failed) or lost. A `waiter`'s steps wait on the
        attempt's own."""
        if lw.reattach is None or lw.reattach.done():
            made = Steps(f"worker {lw.worker.id} reattach")
            self.attach_steps[slot_of(lw.worker)] = made
            lw.reattach = asyncio.create_task(self._do_reattach(lw, why, made))
        if waiter is None:
            await asyncio.shield(lw.reattach)
            return
        async with waiter.step(f"waiting for worker {lw.worker.id} to be reattached ({lw.reason or why})"):
            waiter.waits_on = self.attach_steps[slot_of(lw.worker)]
            try:
                await asyncio.shield(lw.reattach)
            finally:
                waiter.waits_on = None

    async def _do_reattach(self, lw: LiveWorker, why: str, steps: Steps) -> None:
        if lw.state != "unreachable":
            return
        await self._worker_to(lw, "reattaching", why)
        spec = worker_spec(lw.worker.index)
        spec.token, spec.steps = lw.worker.token, steps
        provider = self.provider(lw.worker)
        try:
            async with steps.step(f"starting its runner again ({provider.name} provider)"):
                worker = await provider.create(spec)
            client = RunnerClient(worker.url, worker.token)
            async with steps.step("reading which runner process serves it (/health)"):
                h = await client.health(timeout=HEALTH_TIMEOUT)
            async with steps.step("taking archived projects off it"):
                await client.prune([p["id"] for p in await self.store.list_projects()])
        except Exception as e:
            lw.failures += 1
            failed = describe(e)
            try:
                there = await self.provider(lw.worker).exists(lw.worker)
            except Exception as x:
                there, failed = True, f"{failed}; its provider could not say whether it still has it ({describe(x)})"
            if there:
                await self._worker_to(lw, "unreachable", f"reattaching it failed: {failed}")
            else:
                await self._lose(lw, f"reattaching it failed ({failed}), and its {lw.worker.provider} "
                                     "provider no longer has it")
            return
        old = lw.client
        lw.worker, lw.client = worker, client
        await old.close()
        await self.store.upsert_worker(worker.record())
        await self._restarted(lw, h)
        steps.note(f"ready again at {worker.url}")
        await self._worker_to(lw, "ready", f"reattached: its {worker.provider} provider started its runner again")

    async def _settled(self, lw: LiveWorker) -> None:
        """Wait for a thread's worker to be ready, looking at it first: returns
        once it is ready or lost (and its threads unbound), raises when it
        is unreachable and a reattach fails meanwhile."""
        if lw.state == "ready":
            await self._probe(lw)
        failures = lw.failures
        if lw.state not in ("ready", "lost"):
            left = READY_TIMEOUT - (asyncio.get_running_loop().time() - lw.unreachable_since)
            log.warning("worker %s is %s (%s); work for its threads waits for it to answer again, or to be "
                        "reattached in %.0fs", lw.worker.id, lw.state, lw.reason, max(left, 0))
        async with lw.changed:
            await lw.changed.wait_for(lambda: lw.state in ("ready", "lost") or lw.failures > failures)
        if lw.state not in ("ready", "lost"):
            raise RuntimeError(f"worker {lw.worker.id} is {lw.state}: {lw.reason}")

    async def _restarted(self, lw: LiveWorker, h: dict) -> None:
        """Take note of the runner process serving the worker. One started
        since the last look (it crashed, or a sprite woke cold, or it was
        reattached) runs none of the worker's threads and numbers its events
        afresh. A thread whose turn was under way is put back on this worker
        at once, where its runner resumes the turn the restart cut short
        (runner/API.md, "Interrupted turns"); the rest are unbound, to come
        back on this worker, in their worktrees here, with their next message
        (live_worker)."""
        boot = h.get("boot")
        if boot == lw.boot:
            return
        lw.boot = boot
        lw.last_seq = 0
        wid = lw.worker.id
        for tid in sorted(lw.threads):
            t = await self.store.get_thread(tid)
            if self.by_thread.get(tid) is lw and t["state"] in RUNNING and tid not in self.binding:
                await self._bind_to(tid, "binding", f"worker {wid}'s runner restarted mid-turn; putting it "
                                                    "back there to resume the turn")
                self._begin_binding(tid, "its runner restarted mid-turn", entered=True)
        await self._unbind_all(lw, f"worker {wid}'s runner restarted; it comes back on that worker with its "
                                   "next message, or with Respawn")
        if lw.state == "ready":
            await self._worker_to(lw, "ready", "its runner restarted")

    async def _watch(self, lw: LiveWorker) -> None:
        """Look at the worker every HEALTH_EVERY seconds, so a worker that
        died, stopped answering or came back is seen while nothing is sent
        to it, and an unreachable one is reattached in time."""
        while lw.state != "lost":
            await asyncio.sleep(HEALTH_EVERY)
            try:
                await self._probe(lw)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("watching worker %s", lw.worker.id)

    async def _lose(self, lw: LiveWorker, why: str) -> None:
        """Retire a worker its provider no longer has: it is lost, each of its
        threads is unbound and comes back on another worker with its next
        message (live_worker). It is destroyed too, so it can never run a
        thread a second time. Why, and the end of its log as it was followed,
        stay with it in the store: destroying it takes its log too."""
        if lw not in self.workers:
            return
        self.workers.remove(lw)  # its slot is empty: a new worker of the same name fills it
        me = asyncio.current_task()
        for task in lw.tasks():
            if task is not me:
                task.cancel()
        wid = lw.worker.id
        await self._worker_to(lw, "lost", why)
        await self._unbind_all(lw, f"worker {wid} is lost ({why}); it comes back on a worker of the pool "
                                   "with its next message, or with Respawn")
        await self.store.lose_worker(wid, why, "\n".join(lw.log_tail))
        try:
            await self.provider(lw.worker).destroy(lw.worker)
        except Exception as e:
            log.warning("destroying lost worker %s: %s", wid, describe(e))
        await lw.client.close()

    async def _follow_logs(self, lw: LiveWorker) -> None:
        """Print the worker's runner log to stdout as it is written, and keep
        its end for when the worker is lost (see _lose)."""
        wid = lw.worker.id
        try:
            async for line in self.provider(lw.worker).follow(lw.worker, LOG_BACKLOG):
                lw.log_tail.append(line)
                print_worker_line(wid, line)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("cannot follow the log of worker %s: %s", wid, describe(e))

    async def _relay(self, lw: LiveWorker) -> None:
        """Copy a worker's event stream into the store; reconnect on drops.
        Each connection starts with a look at which runner serves the worker,
        so one that restarted is streamed from its first event."""
        while True:
            try:
                await self._restarted(lw, await lw.client.health(timeout=HEALTH_TIMEOUT))
                async for ev in lw.client.events(after=lw.last_seq):
                    lw.last_seq = max(lw.last_seq, ev.get("seq", 0))
                    await self._ingest(ev)
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
    # A worker holds threads of every project: each call is answered as the
    # thread that made it, within that thread's project.

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
            caller = await self._caller(lw, call.get("thread"))
            reply = {"ok": True, "result": await handler(self, lw, caller, **args)}
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

    async def _caller(self, lw: LiveWorker, thread_id) -> dict:
        """The thread making a peer call: one that is on this worker."""
        t = await self.store.get_thread(thread_id) if isinstance(thread_id, str) else None
        if t is None or t["worker_id"] != lw.worker.id:
            raise PeerCallError(f"thread {thread_id} is not on worker {lw.worker.id}")
        return t

    async def _own_thread(self, project_id: str, thread_id: str | None) -> dict:
        """A thread of the caller's project; nothing else is visible to it."""
        t = await self.store.get_thread(thread_id) if thread_id else None
        if t is None or t["project_id"] != project_id:
            raise PeerCallError(f"no thread {thread_id}")
        return t

    @staticmethod
    def _as_caller(caller: dict, thread_id) -> None:
        """A call made for a thread is made by that thread only."""
        if thread_id != caller["id"]:
            raise PeerCallError(f"thread {caller['id']} cannot act as {thread_id}")

    async def peer_list_threads(self, lw: LiveWorker, caller: dict, project: str,
                                query: str | None = None) -> list[dict]:
        project_id = caller["project_id"]
        if project != project_id:
            raise PeerCallError("threads of another project are not visible")
        threads = [peer_entry(t) for t in await self.store.list_threads(project_id)]
        q = (query or "").lower()
        return [t for t in threads if q in (t["title"] + " " + t["summary"]).lower()] if q else threads

    async def peer_read_thread(self, lw: LiveWorker, caller: dict, thread_id: str, last_n: int = 20) -> dict:
        project_id = caller["project_id"]
        t = await self._own_thread(project_id, thread_id)
        events = await self.store.list_events(project_id, t["id"], 0, limit=5000)
        recent = [{"seq": e["seq"], "thread": e["thread_id"], "type": e["type"], "ts": e["created_at"],
                   "data": e["payload"]} for e in events[-int(last_n):]]
        return {**peer_entry(t), "events": recent}

    async def peer_send_message(self, lw: LiveWorker, caller: dict, sender: str, to: str, body: str,
                                reply_to: str | None = None, hops: int = 0) -> dict:
        self._as_caller(caller, sender)
        t = await self._own_thread(caller["project_id"], to)
        try:
            await self.send(t["id"], str(body), sender, int(hops or 0) + 1, reply_to)
        except ArchivedError as e:
            raise PeerCallError(str(e)) from None
        return {"ok": True}

    async def peer_spawn_thread(self, lw: LiveWorker, caller: dict, parent: str | None, project: str, title: str,
                                request: str) -> dict:
        self._as_caller(caller, parent)
        project_id = caller["project_id"]
        if project != project_id:
            raise PeerCallError("cannot spawn threads in another project")
        request = str(request or "").strip()
        if not request:
            raise PeerCallError("request is required")
        from .router import title_for
        try:
            # A thread it spawns runs where it runs.
            t = await self.spawn(project_id, title or title_for(request), request, parent,
                                 workers=caller["workers"])
        except (RuntimeError, ArchivedError) as e:
            raise PeerCallError(str(e)) from None
        return {"id": t["id"], "title": t["title"]}

    async def peer_set_summary(self, lw: LiveWorker, caller: dict, thread_id: str, summary: str,
                               files: list | None = None) -> dict:
        self._as_caller(caller, thread_id)
        t = caller
        fields = {"summary": str(summary or "")}
        if isinstance(files, list):
            fields["files"] = [str(f) for f in files]
        await self.store.update_thread(t["id"], **fields)
        return {"ok": True}

    async def peer_save(self, lw: LiveWorker, caller: dict, thread_id: str, land: bool = False) -> dict:
        """A thread's turn ended: take its branch, snapshot and session into the hub;
        with `land`, land the branch there and send the worker the new base
        and branch (runner/API.md, "Repository")."""
        self._as_caller(caller, thread_id)
        t = caller
        project = await self.store.get_project(t["project_id"])
        hub = await self.hub(project)
        data, refs = await lw.client.pull_thread(project["id"], t["id"], await hub.have(t["id"]))
        await hub.receive(t["id"], data, refs)
        if not land:
            return {"landed": False, "base": hub.base}
        result = await hub.land(t["id"])
        # Every turn that saves lands: its steps go to stdout only when it is slow or fails.
        await self._ship(lw, project, t["id"], Steps(f"thread {t['id']} landing", level=logging.DEBUG))
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
                hub = Hub(self.hub_path(project["id"]), project["name"], get_secret("GITHUB_TOKEN"))
                await hub.open()
                if hub.url != project["repo_url"]:
                    await hub.connect(project["repo_url"])
                self.hubs[project["id"]] = hub
        return hub

    async def create_project(self, name: str, model: str = "", repo_url: str = "") -> dict:
        """A new project; with `repo_url` (a project opened from a folder on
        disk), its hub is made and joined to that remote before the project
        exists (Hub.connect), so one that cannot be leaves nothing behind."""
        if not repo_url:
            return await self.store.create_project(name, model)
        repo_url, pid = remote_url(repo_url), new_id("p")
        hub = Hub(self.hub_path(pid), name, get_secret("GITHUB_TOKEN"))
        try:
            await hub.open()
            await hub.connect(repo_url)
        except BaseException:
            shutil.rmtree(hub.path, ignore_errors=True)
            raise
        project = await self.store.create_project(name, model, repo_url, pid)
        async with self._hubs_lock:
            self.hubs[pid] = hub
        return project

    async def update_project(self, project_id: str, **fields) -> dict:
        """Rename a project, change its model, or attach, change or remove its
        remote (`repo_url`, "" for none; a folder is kept as remote_url writes
        it). A new remote is connected to the hub first (Hub.connect); one that
        cannot be leaves the project as it was."""
        project = await self.store.get_project(project_id)
        if project is None:
            raise KeyError(project_id)
        if "repo_url" in fields:
            fields["repo_url"] = remote_url(fields["repo_url"])
        if "repo_url" in fields and fields["repo_url"] != project["repo_url"]:
            hub = await self.hub(project)
            await hub.connect(fields["repo_url"])
        return await self.store.update_project(project_id, **fields)

    async def move(self, thread_id: str, place: str) -> None:
        """Have a thread's workers run on `place` from now on (the router
        read it in a request, "run it locally"): PlaceRefused, and nothing
        changed, for a place this control plane runs none in. A thread on a
        worker elsewhere, or being put on one, is stopped there and unbound
        (a turn under way fails); it comes back on a worker of `place`, from
        its branch, snapshot and session in the hub, with its next message."""
        self.places.provider(place)
        t = await self.store.get_thread(thread_id)
        if t["workers"] == place:
            return
        await self.store.update_thread(thread_id, workers=place)
        await self.store.append_event(t["project_id"], thread_id, "moved", {"from": t["workers"], "to": place})
        why = f"its workers run on {PLACES[place]} now; it comes back there with its next message, or with Respawn"
        task = self.binding.get(thread_id)
        if task is not None:
            task.cancel(f"called off: {why}")
            await asyncio.wait({task})
        lw = self.by_thread.pop(thread_id, None)
        if lw is None:
            return
        lw.threads.discard(thread_id)
        if lw.state == "ready":
            await lw.client.stop_if_there(thread_id)
        await self._bind_to(thread_id, "unbound", why, worker=lw.worker.id)

    async def _ship(self, lw: LiveWorker, project: dict, tid: str | None, steps: Steps) -> None:
        """Bring a worker up to the hub: the base (as it is on the project's
        repository, when there is one), and a thread's branch, snapshot and session."""
        wid, pid = lw.worker.id, project["id"]
        async with steps.step(f"opening project {pid}'s hub"):
            hub = await self.hub(project)
        if hub.url:
            async with steps.step(f"fetching {hub.base} of {hub.url} into the hub"):
                await hub.sync()
        async with steps.step(f"asking worker {wid} which commits of project {pid} it has"):
            have = list((await lw.client.repo_refs(pid)).values())
        async with steps.step("bundling what it lacks"):
            data, refs = await hub.pack(tid, have)
        async with steps.step(f"sending worker {wid} the bundle ({len(data) / 1024:.0f} KiB)"):
            await lw.client.push_repo(pid, data, hub.base, refs)

    async def _ingest(self, ev: dict) -> None:
        tid, typ, data = ev.get("thread"), ev.get("type", "event"), ev.get("data") or {}
        t = await self.store.get_thread(tid) if tid else None
        if t is None:
            return
        project_id = t["project_id"]
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
        if "state" in fields:  # a thread that ends a turn makes room on its worker
            await self._made_room()
        if typ in ENDS_STREAM:
            await self.store.delete_events(project_id, tid, "delta")

    # ---------------------------------------------------------------- threads

    async def spawn(self, project_id: str, title: str, request: str, parent: str | None = None,
                    images: list[dict] | None = None, workers: str = "") -> dict:
        """A new thread, put on a worker in the background. `workers` is where
        its workers run, as the router planned it (PlaceRefused for a place
        this control plane runs none in); "" has the router decide from its
        request when it is first put on a worker (_bind)."""
        project = await self.open_project(project_id)
        if workers:
            self.places.provider(workers)
        running = [t for t in await self.store.list_threads(project_id)
                   if t["state"] in ("queued", "working")]
        if len(running) >= self.max_running:
            raise RuntimeError(f"project has {len(running)} running threads (limit {self.max_running})")
        thread = await self.store.create_thread(project["id"], title, request, workers, parent)
        tid = thread["id"]
        created = {"title": title, "request": request, "parent": parent}
        if images:
            created["images"] = images
        await self.store.append_event(project_id, tid, "created", created)
        # Binding before it is returned: a new thread is never shown queued on no worker.
        thread = await self._bind_to(tid, "binding", "a new thread, being put on a worker")
        self._begin_binding(tid, thread["binding_reason"], entered=True)
        return thread

    def _begin_binding(self, tid: str, why: str, entered: bool = False) -> asyncio.Task:
        """Start putting a thread on a worker (_bind), in the background;
        `entered` when the caller moved it to binding already. Its task is
        registered at once, so callers that come after it wait for it
        (live_worker) instead of starting another."""
        task = asyncio.create_task(self._bind(tid, why, entered))
        self.binding[tid] = task
        self.background.add(task)
        task.add_done_callback(self.background.discard)
        task.add_done_callback(lambda t: self.binding.pop(tid) if self.binding.get(tid) is t else None)
        # A failure is the thread's binding event; a waiter, if any, gets it too.
        task.add_done_callback(lambda t: t.cancelled() or t.exception())
        return task

    async def _bind(self, tid: str, why: str, entered: bool = False) -> LiveWorker:
        """Put a thread on a worker: unbound or bound -> binding -> bound, or
        back to unbound with why it failed. It goes on the worker it was on
        while that one is not lost (reattached first when it is unreachable),
        so the worktree there is the one it continues in; else on a picked
        worker, from its branch, snapshot and session in the hub. A thread
        none of whose turns was saved (new, or its worker went before its
        first turn ended) is given its request; the rest wait idle for their
        next message. One the worker's runner still runs (only this control
        plane lost track of it) is bound as it is: posting it would be refused."""
        if not entered:
            await self._bind_to(tid, "binding", why)
        t = await self.store.get_thread(tid)
        project = await self.store.get_project(t["project_id"])

        async def show(what: str) -> None:
            await self.store.update_thread(tid, activity=what)

        steps = Steps(f"thread {tid}", show)
        self.bind_steps[tid] = steps
        steps.note(f"putting it on a worker ({why})")
        old = self.by_thread.pop(tid, None)
        if old is not None:
            old.threads.discard(tid)
        lw, picked, posted, found = None, False, None, None
        try:
            if not t["workers"]:
                async with steps.step("asking the router where its workers run"):
                    place = await self.router.place(t["request"], self.places.offered())
                t = await self.store.update_thread(tid, workers=place)
                steps.note(f"its workers run on {PLACES[place]}")
            lw = old
            if lw is not None and lw.worker.provider != t["workers"]:
                steps.note(f"worker {lw.worker.id} is on {PLACES[lw.worker.provider]}, and its workers run "
                           f"on {PLACES[t['workers']]} now; picking one there")
                lw = None
            if lw is None and t["worker_id"] is not None:
                async with steps.step(f"finding worker {t['worker_id']}, which it was on"):
                    lw = await self._home(project, t["workers"], t["worker_id"], steps)
            if lw is not None and lw.state in ("unreachable", "reattaching"):
                await self._reattach(lw, f"thread {tid} is being put back on it", steps)
            if lw is not None and lw.state != "ready":
                steps.note(f"worker {lw.worker.id} is {lw.state} ({lw.reason}); picking another")
                lw = None
            if lw is None:
                async with steps.step("picking a worker of the pool"):
                    lw, picked = await self._pick_worker(project, t["workers"], steps), True
                steps.note(f"picked worker {lw.worker.id}")
            if lw is not old:
                async with steps.step(f"asking worker {lw.worker.id} whether its runner still runs it"):
                    there = await lw.client.get_thread(tid)
                if there is not None and there.get("state") not in ENDED:
                    found = there["state"]  # its runner still runs it; only this control plane lost track
            if found is None:
                posted = await self._post(lw, t, project, again=lw is old, steps=steps)
        except asyncio.CancelledError as e:
            await self._bind_to(tid, "unbound", e.args[0] if e.args else "putting it on a worker was called off")
            raise
        except Exception as e:
            await self._bind_to(tid, "unbound", f"could not put it on a worker: {describe(e)}")
            raise
        finally:
            if picked:
                lw.starting -= 1
                lw.picked += 1
                await self._made_room()
        lw.threads.add(tid)
        self.by_thread[tid] = lw
        wid = lw.worker.id
        if found is not None:
            reason, state = f"found again on worker {wid}, which still runs it", found
        elif posted.get("resumed"):
            reason, state = f"on worker {wid}, resuming the turn its runner's restart cut short", None
        elif posted.get("state") == "idle":
            reason, state = f"on worker {wid}, idle until its next message", "idle"
        else:
            reason, state = f"on worker {wid}, working on its request", None
        steps.note(f"bound: {reason}")
        await self._bind_to(tid, "bound", reason, worker=wid, worker_id=wid, **({"state": state} if state else {}))
        return lw

    async def _post(self, lw: LiveWorker, t: dict, project: dict, again: bool, steps: Steps) -> dict:
        """Ship the hub to a worker and post the thread to its runner (stopped
        there first when `again`). It is given its request when none of its
        turns was saved. Returns what the runner answered: the thread's
        directory entry, `idle` when it waits for its next message, and
        whether it resumes a turn cut short on that worker (runner/API.md,
        "Interrupted turns"), which it does instead of taking the request."""
        tid, wid = t["id"], lw.worker.id
        await self._ship(lw, project, tid, steps)
        if again:  # its runner may still run it: a respawn of a bound thread
            async with steps.step(f"stopping it on worker {wid} first"):
                await lw.client.stop_if_there(tid)
        request, images = None, None
        if not await (await self.hub(project)).saved(tid):
            request = t["request"]
            created = (await self.store.list_events(project["id"], tid, 0, limit=1) or [{}])[0]
            images = (created.get("payload") or {}).get("images") if created.get("type") == "created" else None
            self._expect_echo(tid, request)
        model = {"provider": "deepseek", "model": project["model"]} if project.get("model") else None
        try:
            async with steps.step(f"posting it to worker {wid}'s runner"
                                  + (" with its request" if request is not None else "")):
                entry = await lw.client.create_thread(id=tid, title=t["title"], project=project["id"],
                                                      parent=t["parent_id"], model=model, summary=t["summary"],
                                                      request=request,
                                                      images=await self._inline_images(images) or None)
        except BaseException:
            if request is not None:
                self._echoed(tid, request)
            raise
        if request is not None and entry.get("resumed"):
            self._echoed(tid, request)  # it resumes the turn that took it: no echo comes
        return entry

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
        # A message from another thread is one event, which the sender lists too.
        src = await self.store.get_thread(sender)
        peer = sender if src and src["project_id"] == thread["project_id"] else None
        await self.store.append_event(thread["project_id"], to, "message_in", payload, peer)
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
        """The worker running the thread, once it is ready. A thread being put
        on a worker is waited for; one on no worker, or whose turn ended
        (stopped, failed, done), is put on one first (_bind). Concurrent
        callers share one binding.

        Raises KeyError for an unknown thread, ArchivedError for an archived
        one or one in an archived project."""
        t = await self.store.get_thread(thread_id)
        if t is None:
            raise KeyError(thread_id)
        if t["archived_at"] is not None:
            raise ArchivedError(f"thread {thread_id} is archived; unarchive it to use it")
        await self.open_project(t["project_id"])
        while True:
            task = self.binding.get(thread_id)
            if task is not None:
                try:
                    return await asyncio.shield(task)
                except asyncio.CancelledError:
                    if task.cancelled() and not asyncio.current_task().cancelling():
                        continue  # called off by Respawn or Stop: wait for what comes after it
                    raise
            lw = self.by_thread.get(thread_id)
            if lw is not None:
                await self._settled(lw)
                t = await self.store.get_thread(thread_id)
                if self.by_thread.get(thread_id) is lw and t["state"] not in ENDED:
                    return lw
            if thread_id not in self.binding:
                self._begin_binding(thread_id, "brought back on a worker for a message to it or its files")

    async def respawn(self, thread_id: str) -> None:
        """Put a thread on a worker afresh, from whatever binding it is in
        (states.py): one being put on a worker is called off and started
        over; one on an unreachable worker has that worker reattached; one
        bound is stopped on its runner and posted again. Returns at once; the
        thread's binding events say how it went."""
        t = await self.store.get_thread(thread_id)
        if t is None:
            raise KeyError(thread_id)
        if t["archived_at"] is not None:
            raise ArchivedError(f"thread {thread_id} is archived; unarchive it to use it")
        await self.open_project(t["project_id"])
        task = self.binding.get(thread_id)
        if task is not None:
            # A worker being made for it is called off too: Respawn is for a
            # start that hangs, and that is most often where it hangs.
            steps = self.bind_steps.get(thread_id)
            for slot, made in self.attach_steps.items():
                if steps is not None and steps.waits_on is made and slot in self.attaching:
                    made.note(f"called off by Respawn of thread {thread_id}", logging.WARNING)
                    self.attaching[slot].cancel()
            task.cancel("Respawn called off putting it on a worker, to start over")
            await asyncio.wait({task})
        if thread_id not in self.binding:
            self._begin_binding(thread_id, "Respawn")

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

    async def directory(self, project_id: str, archived: bool = False) -> list[dict]:
        """The project's live threads (or archived ones), each with its "load"
        (load.py): what the router, the UI and peers see."""
        threads = await self.store.list_threads(project_id, archived)
        backlog, at = await self.store.backlog(project_id), time.time()
        return [{**t, "load": load_of(t, backlog.get(t["id"], 0), self.worker_state(t)[0], at)} for t in threads]

    async def with_load(self, t: dict) -> dict:
        """One thread with its "load" (load.py)."""
        backlog = await self.store.backlog(t["project_id"])
        return {**t, "load": load_of(t, backlog.get(t["id"], 0), self.worker_state(t)[0], time.time())}

    def worker_state(self, t: dict) -> tuple[str, str]:
        """The state of the worker a thread is bound to and why, ("", "") when
        it is bound to none."""
        lw = self.by_thread.get(t["id"])
        return (lw.state, lw.reason) if lw is not None else ("", "")

    async def worker_logs(self, thread_id: str, lines: int = 300) -> str:
        """Where a thread stands (its binding, and its worker's state, each
        with why), then the runner log of the worker it is on, or was last on
        (the control plane reads it from the provider; the worker never sends
        it). A worker that was lost left the end of its log as it was
        followed. KeyError for an unknown thread."""
        t = await self.store.get_thread(thread_id)
        if t is None:
            raise KeyError(thread_id)
        head = f"thread {thread_id} is {t['binding']}: {t['binding_reason'] or 'nothing recorded yet'}\n"
        steps = self.bind_steps.get(thread_id)
        if steps is not None:
            head += (f"\nPutting it on a worker, from {when(steps.started_at)}"
                     + (f" (now: {steps.running[-1]})" if steps.running else "") + ":\n" + steps.text() + "\n\n")
        lw = self.by_thread.get(thread_id)
        if lw is not None:
            head += f"worker {lw.worker.id} is {lw.state}: {lw.reason or 'its runner answers'}\n"
            worker = lw.worker
        else:
            w = {w["id"]: w for w in await self.store.list_workers()}.get(t["worker_id"])
            if w is None:
                return head + "It has not been on a worker, so there is no worker log.\n"
            head += f"worker {w['id']} is {w['state']}: {w['state_reason'] or 'nothing recorded'}\n"
            if w["lost_at"] is not None:
                kept = (w.get("lost_log") or "").splitlines()[-lines:]
                if not kept:
                    return head + f"No log was read from {w['id']} before it was lost.\n"
                return (head + f"\nThe end of {w['id']}'s runner log when it was lost "
                        f"({when(w['lost_at'])}):\n" + local_log("\n".join(kept)) + "\n")
            worker = stored_worker(w)
        try:
            text = await self.provider(worker).logs(worker, lines)
        except Exception as e:
            return head + f"\nCould not read {worker.id}'s runner log: {describe(e)}\n"
        return head + "\n" + local_log(text)

    async def list_files(self, thread_id: str, path: str) -> dict:
        """A directory of the thread's worktree, read from the worker that holds
        it (the control plane asks; the worker never sends files on its own)."""
        lw = await self._holding(thread_id)
        return await lw.client.list_files(thread_id, path)

    async def open_file(self, thread_id: str, path: str) -> httpx.Response:
        """A file of the thread's worktree, streaming from its worker; the
        caller closes the response."""
        lw = await self._holding(thread_id)
        return await lw.client.open_file(thread_id, path)

    async def _holding(self, thread_id: str) -> LiveWorker:
        """The ready worker a thread is bound to, whose disk holds its
        worktree. Nothing waits for one, and reading files puts no thread on
        a worker: a thread on none raises NotOnWorker with where it stands.
        KeyError for an unknown thread, ArchivedError for an archived one."""
        t = await self.store.get_thread(thread_id)
        if t is None:
            raise KeyError(thread_id)
        if t["archived_at"] is not None:
            raise ArchivedError(f"thread {thread_id} is archived; unarchive it to use it")
        await self.open_project(t["project_id"])
        t = await self.store.get_thread(thread_id)
        lw = self.by_thread.get(thread_id)
        if lw is not None and t["binding"] == "bound":
            if lw.state == "ready":
                return lw
            raise NotOnWorker(f"its worker {lw.worker.id} is {lw.state}: {lw.reason}")
        if thread_id not in self.binding:
            raise NotOnWorker(f"it is on no worker ({t['binding_reason'] or 'it has not been on one'}); "
                              "its next message, or Respawn, puts it on one")
        steps = self.bind_steps.get(thread_id)
        now = f" (now: {steps.running[-1]})" if steps is not None and steps.running else ""
        raise NotOnWorker(f"it is being put on a worker{now}; its files show once it is bound, "
                          "and the Worker log tab says how that goes")

    async def stop(self, thread_id: str) -> None:
        task = self.binding.get(thread_id)
        if task is not None:
            task.cancel("stopped before it was on a worker")
            await asyncio.wait({task})
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
    # a peer. Archiving it stops every thread still on a runner and takes the
    # project off the pool's workers (its repository and worktrees there), so
    # it holds no compute or disk. Its threads keep their own state and
    # archived flags, its log and its hub (every turn's work is in the hub
    # already). Unarchived, it is as it was: each thread comes back on a
    # worker, from its branch, snapshot and session in the hub, with its next
    # message.

    async def open_project(self, project_id: str) -> dict:
        """The project, if it can take work: KeyError for an unknown one,
        ArchivedError for an archived one."""
        p = await self.store.get_project(project_id)
        if p is None:
            raise KeyError(project_id)
        if p["archived_at"] is not None:
            raise ArchivedError(f"project {p['name']} is archived; unarchive it to use it")
        await self.recover(project_id)
        return p

    async def recover(self, project_id: str) -> None:
        """Take over the project's threads from the store, once per control
        plane. A thread an earlier control plane had bound is on no worker of
        this one. One whose turn was under way is put back on its worker at
        once: attaching to a worker leaves its runner running when it is the
        runner this control plane would start (providers.py), so the turn
        goes on there and the thread is found again; else the runner is
        started afresh and resumes the turn it cut short. The rest, and any
        thread an earlier control plane was still binding, are unbound, to
        come back with their next message or Respawn. Anything that uses a
        project's threads, or shows them, calls this first."""
        if project_id in self.recovered:
            return
        async with self._recover_locks.setdefault(project_id, asyncio.Lock()):
            if project_id not in self.recovered:
                await self._take_over(project_id)
                self.recovered.add(project_id)

    async def _take_over(self, project_id: str) -> None:
        for t in await self.store.list_threads(project_id) + await self.store.list_threads(project_id, True):
            if t["binding"] == "unbound" or t["id"] in self.by_thread or t["id"] in self.binding:
                continue
            on = f" on worker {t['worker_id']}" if t["worker_id"] else ""
            if t["binding"] == "bound" and t["state"] in RUNNING and t["archived_at"] is None:
                await self._bind_to(t["id"], "binding", f"the control plane that had it{on} stopped mid-turn; "
                                                        "putting it back there to go on with the turn")
                self._begin_binding(t["id"], "its control plane stopped mid-turn", entered=True)
            else:
                await self._bind_to(t["id"], "unbound", f"the control plane that had it{on} stopped; it comes "
                                                        "back with its next message, or with Respawn")

    async def archive_project(self, project_id: str) -> dict:
        """Archive a project: it is marked first, so nothing new starts (requests
        still queued for routing fail, and say so in its log), then its running
        threads are stopped, unbound, and taken off the pool's workers. A
        worker not ready now has it taken off when it is ready again.
        Archiving an archived project finishes that without logging it twice."""
        p = await self.store.get_project(project_id)
        if p is None:
            raise KeyError(project_id)
        if p["archived_at"] is None:
            p = await self.store.update_project(project_id, archived_at=now())
            await self.store.append_event(project_id, None, "project_archived", {})
        for t in await self.store.list_threads(project_id) + await self.store.list_threads(project_id, True):
            if t["state"] not in ENDED:
                await self.stop(t["id"])
        for t in await self.store.list_threads(project_id) + await self.store.list_threads(project_id, True):
            lw = self.by_thread.pop(t["id"], None)
            if lw is not None:
                lw.threads.discard(t["id"])
            if t["binding"] != "unbound":
                await self._bind_to(t["id"], "unbound", "its project was archived")
        for lw in list(self.workers):
            if lw.state == "ready":
                await self._prune(lw)
        return p

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
        directory = await self.directory(project_id)
        if mode == "thread" and target:
            plan = {"spawn": [], "send": [{"thread": target, "message": text}], "why": "sent directly"}
        elif mode == "new":
            from .router import title_for
            title = title_for(text) if text else image_title(images)
            plan = {"spawn": [{"title": title, "request": text}], "send": [], "why": "new thread requested"}
        else:
            plan = await self.router.plan(text, directory, self.places.offered())
        spawned, sent = [], []
        for s in plan["spawn"]:
            t = await self.spawn(project_id, s["title"], s["request"], images=images, workers=s.get("workers", ""))
            spawned.append({"id": t["id"], "title": t["title"]})
        # A message the router wrote is from the router; one sent directly is the user's own.
        sender = "user" if mode == "thread" else "router"
        for s in plan["send"]:
            if s.get("workers"):
                await self.move(s["thread"], s["workers"])
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
        directory = await self.directory(project_id)
        live = {t["id"] for t in directory}
        reached = [tid for tid in await self.reached(ev) if tid in live]
        plan = await self.router.revise(text, previous, reached, directory, self.places.offered())
        body = correction(ev["type"], previous, text)
        for tid in reached:
            await self.send(tid, body, "user", edit=seq)
        spawned, told = [], []
        for s in plan["spawn"]:
            t = await self.spawn(project_id, s["title"], s["request"], workers=s.get("workers", ""))
            spawned.append({"id": t["id"], "title": t["title"]})
        for s in plan["send"]:
            if s.get("workers"):
                await self.move(s["thread"], s["workers"])
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
