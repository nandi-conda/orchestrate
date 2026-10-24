"""An echo runner that speaks runner/API.md, for tests and offline UI work.

It has no model. Each turn it says what it got, and:
- a line `spawn: <task>` in a request spawns a child thread through the peer API,
- a line `tell t_xxx: <text>` sends that thread a message,
- child threads report `Finished: ...` to their parent, like the real runner.

Peer calls go to ORCHESTRATOR_URL, or with RUNNER_PEERS=pull wait on
/peer/calls for the control plane to collect, as in runner/API.md.

The control plane uses it when ../runner is missing or DEEPSEEK_API_KEY is not
stored.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import secrets
import time

import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

SPAWN = re.compile(r"^\s*spawn:\s*(.+)$", re.M)
TELL = re.compile(r"^\s*tell\s+(t_[0-9a-zA-Z]+):\s*(.+)$", re.M)


class Host:
    def __init__(self, capacity: int, orch_url: str, orch_token: str, delay: float, pull: bool = False) -> None:
        self.capacity = capacity
        self.delay = delay
        self.threads: dict[str, dict] = {}
        self.events: list[dict] = []
        self.cond = asyncio.Condition()
        self.client = httpx.AsyncClient(base_url=orch_url, headers={"authorization": f"Bearer {orch_token}"},
                                        timeout=10.0) if orch_url and not pull else None
        self.pull = pull
        self.calls: dict[str, dict] = {}  # pull mode: queued, not yet handed out
        self.waiting: dict[str, asyncio.Future] = {}
        self.calls_cond = asyncio.Condition()

    @property
    def peer(self) -> bool:
        return self.pull or self.client is not None

    async def peer_call(self, method: str, path: str, json_body: dict) -> str:
        if self.client is not None:
            return (await self.client.request(method, path, json=json_body)).text
        cid = f"pc_{secrets.token_hex(5)}"
        fut = asyncio.get_running_loop().create_future()
        self.waiting[cid] = fut
        async with self.calls_cond:
            self.calls[cid] = {"id": cid, "method": method, "path": path, "params": None, "json": json_body}
            self.calls_cond.notify_all()
        try:
            _, body = await asyncio.wait_for(fut, 60)
        finally:
            self.waiting.pop(cid, None)
        return json.dumps(body)

    async def take_calls(self, wait: float) -> list[dict]:
        async with self.calls_cond:
            if not self.calls:
                try:
                    await asyncio.wait_for(self.calls_cond.wait(), wait)
                except asyncio.TimeoutError:
                    pass
            out, self.calls = list(self.calls.values()), {}
            return out

    async def emit(self, tid: str, typ: str, data: dict) -> None:
        async with self.cond:
            self.events.append({"seq": len(self.events) + 1, "thread": tid, "type": typ, "ts": time.time(),
                                "data": data})
            self.cond.notify_all()

    async def set_state(self, t: dict, state: str) -> None:
        t["state"] = state
        await self.emit(t["id"], "state", {"state": state})

    async def turn(self, t: dict, text: str) -> None:
        await self.set_state(t, "working")
        await asyncio.sleep(self.delay)
        await self.emit(t["id"], "assistant", {"content": f"Echo: {text}"})
        if self.peer:
            for task in SPAWN.findall(text):
                await self.emit(t["id"], "tool_call", {"id": "c1", "name": "spawn_thread",
                                                       "arguments": {"request": task}})
                r = await self.peer_call("POST", f"/api/peer/projects/{t['project']}/threads",
                                         {"parent": t["id"], "title": task[:40], "request": task})
                await self.emit(t["id"], "tool_result", {"id": "c1", "name": "spawn_thread", "result": r})
            for to, body in TELL.findall(text):
                await self.peer_call("POST", "/api/peer/messages", {"from": t["id"], "to": to, "body": body})
        summary = f"echoed {len(text)} chars"
        t["summary"] = summary
        await self.emit(t["id"], "turn_done", {"summary": summary, "final": f"Echo: {text}"})
        if self.peer:
            await self.peer_call("PUT", f"/api/peer/threads/{t['id']}/summary", {"summary": summary, "files": []})
            if t.get("parent") and not text.startswith("Finished:"):
                await self.peer_call("POST", "/api/peer/messages",
                                     {"from": t["id"], "to": t["parent"], "body": f"Finished: {summary}"})
        if t["state"] != "stopped":
            await self.set_state(t, "idle")

    def entry(self, t: dict) -> dict:
        return {k: t[k] for k in ("id", "project", "title", "state", "summary", "files", "parent")}


def create_app(host: Host, token: str) -> Starlette:
    def authed(req: Request) -> bool:
        return not token or req.headers.get("authorization") == f"Bearer {token}"

    async def health(req: Request):
        running = sum(1 for t in host.threads.values() if t["state"] in ("queued", "working"))
        return JSONResponse({"ok": True, "threads": len(host.threads), "running": running,
                             "capacity": host.capacity})

    async def list_threads(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        return JSONResponse([host.entry(t) for t in host.threads.values()])

    async def create_thread(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        b = await req.json()
        tid = b.get("id") or f"t_{secrets.token_hex(5)}"
        if tid in host.threads:
            return JSONResponse({"error": "exists"}, 409)
        t = {"id": tid, "project": b.get("project", ""), "title": b.get("title") or b["request"][:40],
             "state": "queued", "summary": "", "files": [], "parent": b.get("parent")}
        host.threads[tid] = t
        await host.emit(tid, "created", host.entry(t))
        await host.emit(tid, "message_in", {"from": "user", "body": b["request"]})
        asyncio.create_task(host.turn(t, b["request"]))
        return JSONResponse(host.entry(t), 201)

    async def message(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        t = host.threads.get(req.path_params["tid"])
        if t is None:
            return JSONResponse({"error": "no such thread"}, 404)
        b = await req.json()
        await host.emit(t["id"], "message_in", {"from": b.get("from", "user"), "body": b["body"]})
        asyncio.create_task(host.turn(t, b["body"]))
        return JSONResponse({"ok": True}, 202)

    async def stop(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        t = host.threads.get(req.path_params["tid"])
        if t is None:
            return JSONResponse({"error": "no such thread"}, 404)
        await host.set_state(t, "stopped")
        return JSONResponse({"ok": True})

    def stream(tid: str | None, after: int):
        async def gen():
            i = after
            while True:
                async with host.cond:
                    while len(host.events) <= i:
                        try:
                            await asyncio.wait_for(host.cond.wait(), 15)
                        except asyncio.TimeoutError:
                            break
                batch = host.events[i:]
                if not batch:
                    yield ": ping\n\n"
                for e in batch:
                    i = e["seq"]
                    if tid is None or e["thread"] == tid:
                        yield f"id: {e['seq']}\nevent: {e['type']}\ndata: {json.dumps(e)}\n\n"
        return StreamingResponse(gen(), media_type="text/event-stream")

    async def all_events(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        return stream(None, int(req.query_params.get("after", "0") or 0))

    async def thread_events(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        return stream(req.path_params["tid"], int(req.query_params.get("after", "0") or 0))

    async def peer_calls(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        if not host.pull:
            return JSONResponse([])
        return JSONResponse(await host.take_calls(min(float(req.query_params.get("wait") or 25), 55.0)))

    async def peer_answer(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        fut = host.waiting.get(req.path_params["cid"])
        if fut is None or fut.done():
            return JSONResponse({"error": "no such call"}, 404)
        b = await req.json()
        fut.set_result((b.get("status", 200), b.get("body")))
        return JSONResponse({"ok": True})

    return Starlette(routes=[
        Route("/health", health),
        Route("/peer/calls", peer_calls, methods=["GET"]),
        Route("/peer/calls/{cid}", peer_answer, methods=["POST"]),
        Route("/threads", list_threads, methods=["GET"]),
        Route("/threads", create_thread, methods=["POST"]),
        Route("/threads/{tid}/messages", message, methods=["POST"]),
        Route("/threads/{tid}/stop", stop, methods=["POST"]),
        Route("/threads/{tid}/events", thread_events),
        Route("/events", all_events),
    ])


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=int(os.environ.get("RUNNER_PORT", "8080")))
    a = p.parse_args()
    host = Host(int(os.environ.get("RUNNER_CAPACITY", "16")), os.environ.get("ORCHESTRATOR_URL", ""),
                os.environ.get("ORCHESTRATOR_TOKEN", ""), float(os.environ.get("DEVRUNNER_DELAY", "0.3")),
                pull=os.environ.get("RUNNER_PEERS") == "pull")
    uvicorn.run(create_app(host, os.environ.get("RUNNER_TOKEN", "")),
                host=os.environ.get("RUNNER_HOST", "127.0.0.1"), port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
