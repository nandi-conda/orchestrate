"""HTTP service for one worker. Contract: runner/API.md."""

from __future__ import annotations

import asyncio
import base64
import hmac
import json
import os

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from . import files, sandbox
from .harness import PrimeAgent
from .peers import OrchestratorPeers
from .repo import THREAD_ID, GitError, Repo, refs_header
from .secrets import get_secret, require
from .thread import ThreadHost, dumps
from .web import deepseek_web

# The same limits the control plane enforces; a worker is reachable on its own,
# so it checks what it accepts.
IMAGE_TYPES = {"image/png", "image/jpeg", "image/webp", "image/gif"}
MAX_IMAGES = 4
MAX_IMAGE_BYTES = 8 * 1024 * 1024


def images_of(b: dict) -> list[dict]:
    """A message's `images`, each `{"media_type", "data"}` with base64 data.
    Raises ValueError for anything a model call could not take."""
    raw = b.get("images")
    if raw in (None, []):
        return []
    if not isinstance(raw, list) or len(raw) > MAX_IMAGES:
        raise ValueError(f"images must be a list of at most {MAX_IMAGES}")
    out = []
    for im in raw:
        if (not isinstance(im, dict) or im.get("media_type") not in IMAGE_TYPES
                or not isinstance(im.get("data"), str)):
            raise ValueError("each image needs a media_type (png, jpeg, webp or gif) and base64 data")
        try:
            size = len(base64.b64decode(im["data"], validate=True))
        except ValueError:
            raise ValueError("an image is not valid base64") from None
        if not size:
            raise ValueError("an image is empty")
        if size > MAX_IMAGE_BYTES:
            raise ValueError(f"each image must be under {MAX_IMAGE_BYTES // (1024 * 1024)} MB")
        out.append({"media_type": im["media_type"], "data": im["data"]})
    return out


async def gone(req: Request) -> None:
    """Returns once the caller's connection is closed."""
    while not await req.is_disconnected():
        await asyncio.sleep(0.1)


async def long_poll(taking, req: Request) -> list[dict]:
    """What `taking` answers, or [] as soon as the caller goes away first: a
    long poll must not outlive the connection that asked for it, or a worker
    being stopped waits for a poll nobody will read (uvicorn's shutdown waits
    for it too)."""
    task = asyncio.ensure_future(taking)
    left = asyncio.ensure_future(gone(req))
    try:
        await asyncio.wait({task, left}, return_when=asyncio.FIRST_COMPLETED)
        return task.result() if task.done() and not task.cancelled() else []
    finally:
        for ending in (task, left):
            ending.cancel()


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
        try:
            images = images_of(b)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        if b.get("request") is None and not b.get("id") and not images:
            return JSONResponse({"error": "request, or the id of a thread to bring back, is required"},
                                status_code=400)
        request = b.get("request")
        if request is None and images:
            request = ""  # images only: the thread still gets a turn
        try:
            t = await host.start(project=b.get("project", "default"),
                                 title=b.get("title") or (b.get("request") or b.get("id") or "thread")[:60],
                                 request=request, parent=b.get("parent"),
                                 model=b.get("model"), id=b.get("id"),
                                 summary=b.get("summary") or "", images=images)
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
        try:
            images = images_of(b)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        t.deliver(b.get("body", ""), sender=b.get("from", "user"), hops=int(b.get("hops", 0)), images=images)
        return JSONResponse({"ok": True}, status_code=202)

    async def stop(req: Request):
        if not thread_or_404(req):
            return JSONResponse({"error": "not found"}, status_code=404)
        await host.stop(req.path_params["id"])
        return JSONResponse({"ok": True})

    async def list_files(req: Request):
        t = thread_or_404(req)
        if not t:
            return JSONResponse({"error": "not found"}, status_code=404)
        try:
            return JSONResponse(files.listing(t.root, req.query_params.get("path", "")))
        except files.FileError as e:
            return JSONResponse({"error": str(e)}, status_code=e.status)

    async def raw_file(req: Request):
        t = thread_or_404(req)
        if not t:
            return JSONResponse({"error": "not found"}, status_code=404)
        try:
            p = files.file_path(t.root, req.query_params.get("path", ""))
        except files.FileError as e:
            return JSONResponse({"error": str(e)}, status_code=e.status)
        return FileResponse(p, media_type="application/octet-stream")

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

    async def get_repo(req: Request):
        return JSONResponse({"base": host.repo.base, "refs": await host.repo.refs()})

    async def post_repo(req: Request):
        try:
            refs = json.loads(req.headers.get("x-git-refs") or "{}")
            base = req.headers.get("x-git-base") or ""
            if not base or not isinstance(refs, dict):
                raise ValueError("x-git-base and x-git-refs (a JSON object) are required")
            await host.repo.receive(await req.body(), base, refs)
        except (ValueError, GitError) as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        return JSONResponse({"ok": True})

    async def thread_repo(req: Request):
        tid = req.path_params["id"]
        if not THREAD_ID.match(tid):
            return JSONResponse({"error": "bad thread id"}, status_code=400)
        have = [h for h in req.query_params.get("have", "").split(",") if h]
        try:
            data, refs = await host.repo.send(tid, have)
        except GitError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return Response(data, media_type="application/octet-stream", headers={"x-git-refs": refs_header(refs)})

    outbox = host.peers.outbox if isinstance(host.peers, OrchestratorPeers) else None

    async def peer_calls(req: Request):
        if outbox is None:
            return JSONResponse([])
        wait = min(max(float(req.query_params.get("wait") or 25), 0.0), 55.0)
        return JSONResponse(await long_poll(outbox.take(wait), req))

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
        Route("/threads/{id}/files", guard(list_files)),
        Route("/threads/{id}/files/raw", guard(raw_file)),
        Route("/repo", guard(get_repo), methods=["GET"]),
        Route("/repo", guard(post_repo), methods=["POST"]),
        Route("/threads/{id}/repo", guard(thread_repo)),
        Route("/peer/calls", guard(peer_calls), methods=["GET"]),
        Route("/peer/calls/{id}", guard(peer_answer), methods=["POST"]),
    ])


def host_from_env() -> ThreadHost:
    workdir = os.environ.get("RUNNER_WORKDIR", "/work/threads")
    return ThreadHost(
        agent=PrimeAgent(env={"DEEPSEEK_API_KEY": get_secret("DEEPSEEK_API_KEY")}),
        web=deepseek_web(),
        peers=OrchestratorPeers(),  # the orchestrator collects these from /peer/calls
        # The orchestrator brings the project's commits (POST /repo).
        repo=Repo(os.path.join(workdir, ".repo")),
        workdir=workdir,
        capacity=int(os.environ.get("RUNNER_CAPACITY", "16")),
    )


def main():
    import sys

    import logging

    import uvicorn
    # prime-agent's own log lines (runner.harness) and thread failures go to the worker log.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(name)s: %(message)s")
    require(["DEEPSEEK_API_KEY"])
    sandbox.check()
    PrimeAgent().check()
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
