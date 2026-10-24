"""HTTP API for the UI, the peer API for runners, and the static ultra UI."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from .context import base_url_from_scope, request_base_url
from .providers import WorkerProvider, default_provider
from .router import Router, default_router
from .service import Orchestrator
from .store import Store, store_from_url

UI_DIR = Path(__file__).resolve().parents[1] / "ui" / "dist"

# Event kinds for the UI, which can't compare strings cheaply.
K_USER, K_ASSISTANT, K_TOOL_CALL, K_TOOL_RESULT, K_STATUS, K_PEER, K_ERROR, K_ROUTE = range(1, 9)


# Kept in the log and the SSE streams, left out of the transcripts the UI shows.
HIDDEN = {"usage"}


def short(s: str, n: int = 600) -> str:
    return s if len(s) <= n else s[:n] + " …"


def display(ev: dict) -> dict:
    """Add `kind` (int) and `text` (one display string) to an event."""
    t, d = ev["type"], ev["payload"]
    if t == "message_in":
        frm = d.get("from", "user")
        kind, label = (K_USER, "You") if frm == "user" else (K_PEER, f"from {frm}")
        text = d.get("body", "")
    elif t == "message_out":
        kind, label, text = K_PEER, f"to {d.get('to', '?')}", d.get("body", "")
    elif t == "created":
        kind, label, text = K_USER, "Request", d.get("request", "")
    elif t == "assistant":
        kind, label, text = K_ASSISTANT, "Agent", d.get("content", "")
    elif t == "tool_call":
        args = d.get("arguments")
        arg = args.get("command") or args.get("path") or json.dumps(args) if isinstance(args, dict) else str(args)
        kind, label, text = K_TOOL_CALL, d.get("name", "tool"), short(str(arg), 300)
    elif t == "tool_result":
        kind, label, text = K_TOOL_RESULT, f"{d.get('name', 'tool')} result", short(str(d.get("result", "")))
    elif t == "state":
        kind, label, text = K_STATUS, "state", d.get("state", "")
    elif t == "turn_done":
        kind, label, text = K_STATUS, "turn done", d.get("summary", "")
    elif t in ("error", "dropped"):
        kind, label, text = K_ERROR, t, d.get("error") or d.get("reason", "")
    elif t == "route":
        kind, label = K_ROUTE, "router"
        text = d.get("summary", "") + (f" — {d['why']}" if d.get("why") else "")
    elif t == "worker":
        kind, label, text = K_STATUS, "worker", f"{d.get('provider')} worker {d.get('id')} up"
    else:
        kind, label, text = K_STATUS, t, short(json.dumps(d), 300)
    return {**ev, "kind": kind, "label": label, "text": text}


def thread_view(t: dict) -> dict:
    return {k: t[k] for k in ("id", "project_id", "parent_id", "title", "request", "summary", "state",
                              "worker_id", "files", "created_at", "updated_at")}


def peer_entry(t: dict) -> dict:
    """A thread as the runner's directory entry shape (runner/API.md)."""
    return {"id": t["id"], "project": t["project_id"], "title": t["title"], "state": t["state"],
            "summary": t["summary"], "files": t["files"], "parent": t["parent_id"]}


class BaseUrlMiddleware:
    """Record the address this request came in on, so workers can call back to
    it and the provider can tell local from reachable-remotely."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        token = request_base_url.set(base_url_from_scope(scope))
        try:
            return await self.app(scope, receive, send)
        finally:
            request_base_url.reset(token)


def create_app(store: Store | None = None, provider: WorkerProvider | None = None,
               router: Router | None = None, public_url: str | None = None,
               shutdown_event: asyncio.Event | None = None) -> Starlette:
    store = store or store_from_url(None)
    stop = shutdown_event if shutdown_event is not None else asyncio.Event()
    holder: dict = {}

    @asynccontextmanager
    async def lifespan(app):
        await store.open()
        orch = Orchestrator(store, provider or default_provider(), router or default_router(),
                            public_url or "")
        holder["orch"] = orch
        orch.peer_app = app
        app.state.orch = orch
        try:
            yield
        finally:
            await orch.close()
            close = getattr(orch.provider, "close", None)
            if close:
                await close()
            await store.close()

    def orch() -> Orchestrator:
        return holder["orch"]

    async def body(req: Request) -> dict:
        try:
            data = await req.json()
        except ValueError:
            return {}
        return data if isinstance(data, dict) else {}

    # ------------------------------------------------------------ UI API

    async def list_projects(req: Request):
        return JSONResponse(await store.list_projects())

    async def create_project(req: Request):
        b = await body(req)
        name = str(b.get("name", "")).strip()
        if not name:
            return JSONResponse({"error": "name is required"}, 400)
        p = await store.create_project(name, str(b.get("repo_url", "")).strip(), str(b.get("model", "")).strip())
        return JSONResponse(p, 201)

    async def get_project(req: Request):
        p = await store.get_project(req.path_params["pid"])
        return JSONResponse(p) if p else JSONResponse({"error": "no such project"}, 404)

    async def post_request(req: Request):
        pid = req.path_params["pid"]
        if await store.get_project(pid) is None:
            return JSONResponse({"error": "no such project"}, 404)
        b = await body(req)
        text = str(b.get("text", "")).strip()
        if not text:
            return JSONResponse({"error": "text is required"}, 400)
        mode = b.get("mode", "auto")
        if mode not in ("auto", "thread", "new"):
            return JSONResponse({"error": "mode is auto, thread or new"}, 400)
        try:
            result = await orch().handle_request(pid, text, mode, b.get("target") or None)
        except (KeyError, RuntimeError) as e:
            return JSONResponse({"error": str(e)}, 409)
        return JSONResponse(result)

    async def list_threads(req: Request):
        return JSONResponse([thread_view(t) for t in await store.list_threads(req.path_params["pid"])])

    async def create_thread(req: Request):
        b = await body(req)
        request = str(b.get("request", "")).strip()
        if not request:
            return JSONResponse({"error": "request is required"}, 400)
        from .router import title_for
        try:
            t = await orch().spawn(req.path_params["pid"], b.get("title") or title_for(request), request)
        except KeyError:
            return JSONResponse({"error": "no such project"}, 404)
        except RuntimeError as e:
            return JSONResponse({"error": str(e)}, 409)
        return JSONResponse(thread_view(t), 201)

    async def get_thread(req: Request):
        t = await store.get_thread(req.path_params["tid"])
        return JSONResponse(thread_view(t)) if t else JSONResponse({"error": "no such thread"}, 404)

    async def thread_message(req: Request):
        b = await body(req)
        text = str(b.get("body") or b.get("text") or "").strip()
        if not text:
            return JSONResponse({"error": "body is required"}, 400)
        try:
            await orch().send(req.path_params["tid"], text, "user")
        except KeyError:
            return JSONResponse({"error": "no such thread"}, 404)
        return JSONResponse({"ok": True}, 202)

    async def thread_stop(req: Request):
        if await store.get_thread(req.path_params["tid"]) is None:
            return JSONResponse({"error": "no such thread"}, 404)
        await orch().stop(req.path_params["tid"])
        return JSONResponse({"ok": True})

    async def transcript(req: Request):
        t = await store.get_thread(req.path_params["tid"])
        if t is None:
            return JSONResponse({"error": "no such thread"}, 404)
        after = int(req.query_params.get("after", "0") or 0)
        events = await store.list_events(t["project_id"], t["id"], after, limit=2000)
        return JSONResponse([display(e) for e in events if e["type"] not in HIDDEN])

    async def project_log(req: Request):
        after = int(req.query_params.get("after", "0") or 0)
        events = await store.list_events(req.path_params["pid"], None, after, limit=2000)
        return JSONResponse([display(e) for e in events if e["type"] not in HIDDEN])

    def sse(project_id: str, thread_id: str | None, after: int):
        async def idle(last: int, seconds: float) -> bool:
            """Wait for events, or return True as soon as shutdown begins."""
            events_wait = asyncio.ensure_future(store.wait_events(project_id, last, seconds))
            stop_wait = asyncio.ensure_future(stop.wait())
            try:
                done, _ = await asyncio.wait({events_wait, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for w in (events_wait, stop_wait):
                    w.cancel()
                await asyncio.gather(events_wait, stop_wait, return_exceptions=True)
            if events_wait in done and not events_wait.cancelled():
                events_wait.result()  # a store error is the stream's error, as before
            return stop.is_set()

        async def gen():
            last = after
            yield ": connected\n\n"
            while not stop.is_set():
                events = await store.list_events(project_id, thread_id, last)
                for e in events:
                    last = e["seq"]
                    yield f"id: {e['seq']}\nevent: {e['type']}\ndata: {json.dumps(display(e))}\n\n"
                if not events:
                    if await idle(last, 15.0):
                        return
                    if not await store.list_events(project_id, thread_id, last, limit=1):
                        yield ": ping\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"cache-control": "no-cache", "x-accel-buffering": "no"})

    def after_of(req: Request) -> int:
        v = req.query_params.get("after") or req.headers.get("last-event-id") or "0"
        return int(v) if v.isdigit() else 0

    async def project_events(req: Request):
        return sse(req.path_params["pid"], None, after_of(req))

    async def thread_events(req: Request):
        t = await store.get_thread(req.path_params["tid"])
        if t is None:
            return JSONResponse({"error": "no such thread"}, 404)
        return sse(t["project_id"], t["id"], after_of(req))

    # ------------------------------------------------------------ peer API (runners)

    def peer_guard(fn):
        async def inner(req: Request):
            auth = req.headers.get("authorization", "")
            token = auth.removeprefix("Bearer ").strip()
            lw = orch().worker_for_token(token) if token else None
            if lw is None:
                return JSONResponse({"error": "unauthorized"}, 401)
            req.state.worker = lw
            return await fn(req)
        return inner

    async def peer_project_ok(req: Request, pid: str) -> bool:
        return req.state.worker.worker.project_id == pid

    async def peer_threads(req: Request):
        pid = req.path_params["pid"]
        if not await peer_project_ok(req, pid):
            return JSONResponse({"error": "forbidden"}, 403)
        q = (req.query_params.get("q") or "").lower()
        threads = [peer_entry(t) for t in await store.list_threads(pid)]
        if q:
            threads = [t for t in threads if q in (t["title"] + " " + t["summary"]).lower()]
        return JSONResponse(threads)

    async def peer_thread(req: Request):
        t = await store.get_thread(req.path_params["tid"])
        if t is None or not await peer_project_ok(req, t["project_id"]):
            return JSONResponse({"error": "no such thread"}, 404)
        n = int(req.query_params.get("last_n", "20") or 20)
        events = await store.list_events(t["project_id"], t["id"], 0, limit=5000)
        recent = [{"seq": e["seq"], "thread": e["thread_id"], "type": e["type"], "ts": e["created_at"],
                   "data": e["payload"]} for e in events[-n:]]
        return JSONResponse({**peer_entry(t), "events": recent})

    async def peer_message(req: Request):
        b = await body(req)
        to, frm = b.get("to"), b.get("from")
        t = await store.get_thread(to) if to else None
        if t is None or not await peer_project_ok(req, t["project_id"]):
            return JSONResponse({"error": "no such thread"}, 404)
        src = await store.get_thread(frm) if frm else None
        hops = int(b.get("hops", 0) or 0)
        await orch().send(to, str(b.get("body", "")), frm or "unknown", hops + 1, b.get("reply_to"))
        if src:
            await store.append_event(t["project_id"], frm, "message_out", {"to": to, "body": b.get("body", "")})
        return JSONResponse({"ok": True})

    async def peer_spawn(req: Request):
        pid = req.path_params["pid"]
        if not await peer_project_ok(req, pid):
            return JSONResponse({"error": "forbidden"}, 403)
        b = await body(req)
        request = str(b.get("request", "")).strip()
        if not request:
            return JSONResponse({"error": "request is required"}, 400)
        from .router import title_for
        try:
            t = await orch().spawn(pid, b.get("title") or title_for(request), request, b.get("parent"))
        except RuntimeError as e:
            return JSONResponse({"error": str(e)}, 409)
        return JSONResponse({"id": t["id"], "title": t["title"]})

    async def peer_summary(req: Request):
        t = await store.get_thread(req.path_params["tid"])
        if t is None or not await peer_project_ok(req, t["project_id"]):
            return JSONResponse({"error": "no such thread"}, 404)
        b = await body(req)
        fields = {"summary": str(b.get("summary", ""))}
        if isinstance(b.get("files"), list):
            fields["files"] = [str(f) for f in b["files"]]
        await store.update_thread(t["id"], **fields)
        return JSONResponse({"ok": True})

    # ------------------------------------------------------------ UI files

    async def index(req: Request):
        f = UI_DIR / "index.html"
        if not f.exists():
            return Response("UI not built: run `pixi run build-ui` in orchestrator/", 503)
        return FileResponse(f)

    async def health(req: Request):
        return JSONResponse({"ok": True})

    routes = [
        Route("/health", health),
        Route("/api/projects", list_projects, methods=["GET"]),
        Route("/api/projects", create_project, methods=["POST"]),
        Route("/api/projects/{pid}", get_project, methods=["GET"]),
        Route("/api/projects/{pid}/requests", post_request, methods=["POST"]),
        Route("/api/projects/{pid}/threads", list_threads, methods=["GET"]),
        Route("/api/projects/{pid}/threads", create_thread, methods=["POST"]),
        Route("/api/projects/{pid}/log", project_log, methods=["GET"]),
        Route("/api/projects/{pid}/events", project_events, methods=["GET"]),
        Route("/api/threads/{tid}", get_thread, methods=["GET"]),
        Route("/api/threads/{tid}/messages", thread_message, methods=["POST"]),
        Route("/api/threads/{tid}/stop", thread_stop, methods=["POST"]),
        Route("/api/threads/{tid}/transcript", transcript, methods=["GET"]),
        Route("/api/threads/{tid}/events", thread_events, methods=["GET"]),
        Route("/api/peer/projects/{pid}/threads", peer_guard(peer_threads), methods=["GET"]),
        Route("/api/peer/projects/{pid}/threads", peer_guard(peer_spawn), methods=["POST"]),
        Route("/api/peer/threads/{tid}", peer_guard(peer_thread), methods=["GET"]),
        Route("/api/peer/threads/{tid}/summary", peer_guard(peer_summary), methods=["PUT"]),
        Route("/api/peer/messages", peer_guard(peer_message), methods=["POST"]),
        Route("/", index),
        Route("/p/{pid}", index),
    ]
    routes.append(Mount("/ui", StaticFiles(directory=UI_DIR, check_dir=False)))
    return Starlette(routes=routes, lifespan=lifespan, middleware=[Middleware(BaseUrlMiddleware)])
