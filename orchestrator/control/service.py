"""The control plane's core: workers, threads, routing and the message bus.

Every runner event is relayed into the store, so the UI reads one ordered
event log per project no matter how many workers the project has.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field

import httpx

from .providers import Worker, WorkerProvider, WorkerSpec
from .router import Router
from .runner_client import RunnerClient
from .store import Store

log = logging.getLogger("orchestrator")

MAX_HOPS = 6
CAPACITY = 16  # running threads per worker before another one starts
RUNNER_STATES = {"queued", "working", "idle", "failed", "stopped", "waiting", "done"}
# Runner events that end a model call: once one is in, that call's `delta`
# events (its reply while it streamed) are folded into it and dropped.
ENDS_STREAM = {"reasoning", "assistant", "tool_call", "turn_done", "state", "error"}


def worker_spec(project: dict, index: int) -> WorkerSpec:
    return WorkerSpec(project_id=project["id"], index=index, repo_url=project.get("repo_url", ""),
                      capacity=CAPACITY)


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


@dataclass
class LiveWorker:
    worker: Worker
    client: RunnerClient
    last_seq: int = 0
    relay: asyncio.Task | None = None
    pump: asyncio.Task | None = None
    threads: set[str] = field(default_factory=set)


class Orchestrator:
    def __init__(self, store: Store, provider: WorkerProvider, router: Router,
                 max_running: int = 64) -> None:
        self.store = store
        self.provider = provider
        self.router = router
        self.max_running = max_running
        self.workers: dict[str, list[LiveWorker]] = {}
        self.by_thread: dict[str, LiveWorker] = {}
        # Spawned threads whose request the runner has yet to echo back as their
        # first message_in; the "created" event already shows it.
        self.unechoed: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        for lws in self.workers.values():
            for lw in lws:
                for task in (lw.relay, lw.pump):
                    if task:
                        task.cancel()
                await lw.client.close()
                await self.provider.destroy(lw.worker)

    # ---------------------------------------------------------------- workers

    async def _pick_worker(self, project: dict) -> LiveWorker:
        async with self._lock:
            lws = self.workers.setdefault(project["id"], [])
            best, best_load = None, None
            for lw in lws:
                try:
                    h = await lw.client.health()
                except Exception:  # an unreachable worker takes no new threads
                    continue
                running = h.get("running", 0)
                if running < h.get("capacity", lw.worker.capacity) and (best_load is None or running < best_load):
                    best, best_load = lw, running
            if best is not None:
                return best
            spec = worker_spec(project, len(lws) + 1)
            worker = await self.provider.create(spec)
            await self.store.upsert_worker(worker.record())
            lw = LiveWorker(worker, RunnerClient(worker.url, worker.token))
            lw.relay = asyncio.create_task(self._relay(project["id"], lw))
            lw.pump = asyncio.create_task(self._pump_peer_calls(lw))
            lws.append(lw)
            await self.store.append_event(project["id"], None, "worker",
                                          {"id": worker.id, "provider": worker.provider, "url": worker.url})
            return lw

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
            reply = {"ok": True, "result": await handler(self, lw.worker.project_id, **args)}
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

    async def peer_list_threads(self, project_id: str, project: str, query: str | None = None) -> list[dict]:
        if project != project_id:
            raise PeerCallError("threads of another project are not visible")
        threads = [peer_entry(t) for t in await self.store.list_threads(project_id)]
        q = (query or "").lower()
        return [t for t in threads if q in (t["title"] + " " + t["summary"]).lower()] if q else threads

    async def peer_read_thread(self, project_id: str, thread_id: str, last_n: int = 20) -> dict:
        t = await self._own_thread(project_id, thread_id)
        events = await self.store.list_events(project_id, t["id"], 0, limit=5000)
        recent = [{"seq": e["seq"], "thread": e["thread_id"], "type": e["type"], "ts": e["created_at"],
                   "data": e["payload"]} for e in events[-int(last_n):]]
        return {**peer_entry(t), "events": recent}

    async def peer_send_message(self, project_id: str, sender: str, to: str, body: str,
                                reply_to: str | None = None, hops: int = 0) -> dict:
        t = await self._own_thread(project_id, to)
        src = await self.store.get_thread(sender) if sender else None
        await self.send(t["id"], str(body), sender or "unknown", int(hops or 0) + 1, reply_to)
        if src and src["project_id"] == project_id:
            await self.store.append_event(project_id, sender, "message_out", {"to": to, "body": body})
        return {"ok": True}

    async def peer_spawn_thread(self, project_id: str, parent: str | None, project: str, title: str,
                                request: str) -> dict:
        if project != project_id:
            raise PeerCallError("cannot spawn threads in another project")
        request = str(request or "").strip()
        if not request:
            raise PeerCallError("request is required")
        from .router import title_for
        try:
            t = await self.spawn(project_id, title or title_for(request), request, parent)
        except RuntimeError as e:
            raise PeerCallError(str(e)) from None
        return {"id": t["id"], "title": t["title"]}

    async def peer_set_summary(self, project_id: str, thread_id: str, summary: str,
                               files: list | None = None) -> dict:
        t = await self._own_thread(project_id, thread_id)
        fields = {"summary": str(summary or "")}
        if isinstance(files, list):
            fields["files"] = [str(f) for f in files]
        await self.store.update_thread(t["id"], **fields)
        return {"ok": True}

    PEER_OPS = {"list_threads": peer_list_threads, "read_thread": peer_read_thread,
                "send_message": peer_send_message, "spawn_thread": peer_spawn_thread,
                "set_summary": peer_set_summary}

    async def _ingest(self, project_id: str, ev: dict) -> None:
        tid, typ, data = ev.get("thread"), ev.get("type", "event"), ev.get("data") or {}
        if not tid or await self.store.get_thread(tid) is None:
            return
        if typ == "created":
            return  # the control plane already logged the thread
        if typ == "message_in" and tid in self.unechoed:
            if data.get("body") == self.unechoed.pop(tid):
                return  # the request itself, already shown by "created"
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

    async def spawn(self, project_id: str, title: str, request: str, parent: str | None = None) -> dict:
        project = await self.store.get_project(project_id)
        if project is None:
            raise KeyError(project_id)
        running = [t for t in await self.store.list_threads(project_id)
                   if t["state"] in ("queued", "working")]
        if len(running) >= self.max_running:
            raise RuntimeError(f"project has {len(running)} running threads (limit {self.max_running})")
        thread = await self.store.create_thread(project_id, title, request, parent)
        await self.store.append_event(project_id, thread["id"], "created",
                                      {"title": title, "request": request, "parent": parent})
        self.unechoed[thread["id"]] = request
        try:
            lw = await self._pick_worker(project)
            model = {"provider": "deepseek", "model": project["model"]} if project.get("model") else None
            await lw.client.create_thread(id=thread["id"], request=request, title=title,
                                          project=project_id, parent=parent, model=model)
        except Exception as e:
            self.unechoed.pop(thread["id"], None)
            await self.store.update_thread(thread["id"], state="failed")
            await self.store.append_event(project_id, thread["id"], "error",
                                          {"error": f"could not start thread: {describe(e)}"})
            return await self.store.get_thread(thread["id"])
        lw.threads.add(thread["id"])
        self.by_thread[thread["id"]] = lw
        return await self.store.update_thread(thread["id"], worker_id=lw.worker.id)

    async def send(self, to: str, body: str, sender: str = "user", hops: int = 0,
                   reply_to: str | None = None) -> dict:
        thread = await self.store.get_thread(to)
        if thread is None:
            raise KeyError(to)
        msg = await self.store.add_message(thread["project_id"], sender, to, body, reply_to, hops)
        if hops > MAX_HOPS:
            await self.store.append_event(thread["project_id"], to, "dropped",
                                          {"from": sender, "reason": f"hop limit {MAX_HOPS}"})
            return msg
        lw = self.by_thread.get(to)
        if lw is None:
            await self.store.append_event(thread["project_id"], to, "error",
                                          {"error": "thread has no live worker"})
            return msg
        await lw.client.post_message(to, body, sender, hops)
        await self.store.mark_read([msg["id"]])
        return msg

    async def stop(self, thread_id: str) -> None:
        lw = self.by_thread.get(thread_id)
        if lw:
            await lw.client.stop(thread_id)
        await self.store.update_thread(thread_id, state="stopped", activity="")

    # ---------------------------------------------------------------- requests

    async def handle_request(self, project_id: str, text: str, mode: str = "auto",
                             target: str | None = None) -> dict:
        """mode: auto (router decides), thread (send to target), new (force a new thread)."""
        directory = await self.store.list_threads(project_id)
        if mode == "thread" and target:
            plan = {"spawn": [], "send": [{"thread": target, "message": text}], "why": "sent directly"}
        elif mode == "new":
            from .router import title_for
            plan = {"spawn": [{"title": title_for(text), "request": text}], "send": [], "why": "new thread requested"}
        else:
            plan = await self.router.plan(text, directory)
        spawned, sent = [], []
        for s in plan["spawn"]:
            t = await self.spawn(project_id, s["title"], s["request"])
            spawned.append({"id": t["id"], "title": t["title"]})
        for s in plan["send"]:
            await self.send(s["thread"], s["message"], "user")
            sent.append(s["thread"])
        parts = []
        if spawned:
            parts.append(f"spawned {len(spawned)}")
        if sent:
            parts.append("told " + ", ".join(sent))
        result = {"request": text, "mode": mode, "spawned": spawned, "sent": sent, "why": plan.get("why", ""),
                  "summary": ", ".join(parts) or "nothing to do"}
        await self.store.append_event(project_id, None, "route", result)
        return result
