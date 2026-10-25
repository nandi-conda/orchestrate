"""HTTP service for one worker. Contract: runner/API.md."""

from __future__ import annotations

import asyncio
import hmac
import os

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

from .model import from_spec
from .peers import OrchestratorPeers
from .secrets import get_secret, require
from .thread import Limits, ThreadHost, dumps


def create_app(host: ThreadHost, token: str | None) -> Starlette:
    def authed(req: Request) -> bool:
        if not token:
            return True
        got = req.headers.get("authorization", "")
        return hmac.compare_digest(got, f"Bearer {token}")

    def guard(fn):
        async def inner(req: Request):
            if not authed(req):
                return JSONResponse({"error": "unauthorized"}, status_code=401)
            return await fn(req)
        return inner

    def thread_or_404(req: Request):
        return host.threads.get(req.path_params["id"])

    async def health(req: Request):
        return JSONResponse(host.health())

    async def list_threads(req: Request):
        return JSONResponse([t.directory_entry() for t in host.threads.values()])

    async def create_thread(req: Request):
        b = await req.json()
        if not b.get("request"):
            return JSONResponse({"error": "request is required"}, status_code=400)
        try:
            t = await host.start(project=b.get("project", "default"), title=b.get("title") or b["request"][:60],
                                 request=b["request"], parent=b.get("parent"), model=b.get("model"), id=b.get("id"))
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=409)
        except Exception as e:
            return JSONResponse({"error": f"{type(e).__name__}: {e}"}, status_code=500)
        return JSONResponse(t.directory_entry(), status_code=201)

    async def get_thread(req: Request):
        t = thread_or_404(req)
        if not t:
            return JSONResponse({"error": "not found"}, status_code=404)
        return JSONResponse({**t.directory_entry(), "events": len(t.events)})

    async def post_message(req: Request):
        t = thread_or_404(req)
        if not t:
            return JSONResponse({"error": "not found"}, status_code=404)
        b = await req.json()
        t.deliver(b["body"], sender=b.get("from", "user"), hops=int(b.get("hops", 0)))
        return JSONResponse({"ok": True}, status_code=202)

    async def stop(req: Request):
        if not thread_or_404(req):
            return JSONResponse({"error": "not found"}, status_code=404)
        await host.stop(req.path_params["id"])
        return JSONResponse({"ok": True})

    def sse(thread_id: str | None, after: int, backlog: list[dict]):
        q = host.subscribe(thread_id)

        async def gen():
            try:
                for ev in backlog:
                    if ev["seq"] > after:
                        yield f"id: {ev['seq']}\nevent: {ev['type']}\ndata: {dumps(ev)}\n\n"
                while True:
                    try:
                        ev = await asyncio.wait_for(q.get(), 15)
                    except asyncio.TimeoutError:
                        yield ": ping\n\n"
                        continue
                    if ev["seq"] > after:
                        yield f"id: {ev['seq']}\nevent: {ev['type']}\ndata: {dumps(ev)}\n\n"
            finally:
                host.unsubscribe(q, thread_id)

        return StreamingResponse(gen(), media_type="text/event-stream", headers={"cache-control": "no-cache"})

    def after_of(req: Request) -> int:
        return int(req.query_params.get("after") or req.headers.get("last-event-id") or 0)

    async def thread_events(req: Request):
        t = thread_or_404(req)
        if not t:
            return JSONResponse({"error": "not found"}, status_code=404)
        return sse(t.id, after_of(req), list(t.events))

    async def all_events(req: Request):
        backlog = sorted((e for t in host.threads.values() for e in t.events), key=lambda e: e["seq"])
        return sse(None, after_of(req), backlog)

    outbox = host.peers.outbox if isinstance(host.peers, OrchestratorPeers) else None

    async def peer_calls(req: Request):
        if outbox is None:
            return JSONResponse([])
        wait = min(max(float(req.query_params.get("wait") or 25), 0.0), 55.0)
        return JSONResponse(await outbox.take(wait))

    async def peer_answer(req: Request):
        b = await req.json()
        if outbox is None or not isinstance(b, dict) or not outbox.answer(req.path_params["id"], b):
            return JSONResponse({"error": "no such call"}, status_code=404)
        return JSONResponse({"ok": True})

    return Starlette(routes=[
        Route("/health", health),
        Route("/threads", guard(list_threads), methods=["GET"]),
        Route("/threads", guard(create_thread), methods=["POST"]),
        Route("/events", guard(all_events)),
        Route("/threads/{id}", guard(get_thread)),
        Route("/threads/{id}/messages", guard(post_message), methods=["POST"]),
        Route("/threads/{id}/events", guard(thread_events)),
        Route("/threads/{id}/stop", guard(stop), methods=["POST"]),
        Route("/peer/calls", guard(peer_calls), methods=["GET"]),
        Route("/peer/calls/{id}", guard(peer_answer), methods=["POST"]),
    ])


def host_from_env() -> ThreadHost:
    return ThreadHost(
        model_factory=from_spec,
        peers=OrchestratorPeers(),  # the orchestrator collects these from /peer/calls
        repo=os.environ.get("RUNNER_REPO") or None,
        workdir=os.environ.get("RUNNER_WORKDIR", "/work/threads"),
        capacity=int(os.environ.get("RUNNER_CAPACITY", "16")),
        limits=Limits(max_steps=int(os.environ.get("RUNNER_MAX_STEPS", "60"))),
    )


def main():
    import sys

    import uvicorn
    require(["DEEPSEEK_API_KEY"])
    token = get_secret("RUNNER_TOKEN")
    if not token:
        print("warning: RUNNER_TOKEN is not set (env or keyring); the API is open to anyone who can reach it", file=sys.stderr)
    app = create_app(host_from_env(), token)
    # /events streams stay open for subscribers (the control plane's relay), so
    # bound the wait for them; otherwise Ctrl+C never completes.
    uvicorn.run(app, host=os.environ.get("RUNNER_HOST", "0.0.0.0"), port=int(os.environ.get("RUNNER_PORT", "8080")),
                timeout_graceful_shutdown=int(os.environ.get("RUNNER_GRACEFUL_SHUTDOWN", "5")))


if __name__ == "__main__":
    main()
