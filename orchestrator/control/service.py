"""The control plane's core: workers, threads, routing and the message bus.

Every runner event is relayed into the store, so the UI reads one ordered
event log per project no matter how many workers the project has.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from .context import request_base_url
from .providers import Worker, WorkerProvider, WorkerSpec
from .router import Router
from .runner_client import RunnerClient
from .store import Store

log = logging.getLogger("orchestrator")

MAX_HOPS = 6
CAPACITY = 16  # running threads per worker before another one starts
RUNNER_STATES = {"queued", "working", "idle", "failed", "stopped", "waiting", "done"}


def worker_spec(project: dict, index: int, public_url: str = "") -> WorkerSpec:
    """Where the next worker should call back. `public_url` is an explicit
    override; otherwise the address the current request came in on, which is
    what decides local versus sprite too."""
    return WorkerSpec(project_id=project["id"], index=index, repo_url=project.get("repo_url", ""),
                      orchestrator_url=public_url or request_base_url.get(), capacity=CAPACITY)


@dataclass
class LiveWorker:
    worker: Worker
    client: RunnerClient
    last_seq: int = 0
    relay: asyncio.Task | None = None
    threads: set[str] = field(default_factory=set)


class Orchestrator:
    def __init__(self, store: Store, provider: WorkerProvider, router: Router,
                 public_url: str = "", max_running: int = 64) -> None:
        self.store = store
        self.provider = provider
        self.router = router
        self.public_url = public_url
        self.max_running = max_running
        self.workers: dict[str, list[LiveWorker]] = {}
        self.by_thread: dict[str, LiveWorker] = {}
        self.by_token: dict[str, LiveWorker] = {}
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        for lws in self.workers.values():
            for lw in lws:
                if lw.relay:
                    lw.relay.cancel()
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
            spec = worker_spec(project, len(lws) + 1, self.public_url)
            worker = await self.provider.create(spec)
            await self.store.upsert_worker(worker.record())
            lw = LiveWorker(worker, RunnerClient(worker.url, worker.token))
            lw.relay = asyncio.create_task(self._relay(project["id"], lw))
            lws.append(lw)
            self.by_token[worker.token] = lw
            await self.store.append_event(project["id"], None, "worker",
                                          {"id": worker.id, "provider": worker.provider, "url": worker.url})
            return lw

    def worker_for_token(self, token: str) -> LiveWorker | None:
        return self.by_token.get(token)

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
                log.warning("relay from %s dropped: %s", lw.worker.id, e)
            await asyncio.sleep(1.0)

    async def _ingest(self, project_id: str, ev: dict) -> None:
        tid, typ, data = ev.get("thread"), ev.get("type", "event"), ev.get("data") or {}
        if not tid or await self.store.get_thread(tid) is None:
            return
        if typ == "created":
            return  # the control plane already logged the thread
        if typ == "state" and data.get("state") in RUNNER_STATES:
            await self.store.update_thread(tid, state=data["state"])
        elif typ == "turn_done" and data.get("summary"):
            await self.store.update_thread(tid, summary=data["summary"])
        await self.store.append_event(project_id, tid, typ, data)

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
        try:
            lw = await self._pick_worker(project)
            model = {"provider": "deepseek", "model": project["model"]} if project.get("model") else None
            await lw.client.create_thread(id=thread["id"], request=request, title=title,
                                          project=project_id, parent=parent, model=model)
        except Exception as e:
            await self.store.update_thread(thread["id"], state="failed")
            await self.store.append_event(project_id, thread["id"], "error",
                                          {"error": f"could not start thread: {e}"})
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
        await self.store.update_thread(thread_id, state="stopped")

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
