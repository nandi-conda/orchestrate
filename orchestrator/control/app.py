"""HTTP API for the UI and the static ultra UI.

Workers never call in here: the control plane opens every connection to them
(control/service.py, "peer calls")."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, PlainTextResponse, Response, StreamingResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from .providers import WorkerProvider, default_provider
from .router import Router, default_router
from .service import ENDS_STREAM, Orchestrator, describe, tool_arg
from .store import Store, store_from_url

UI_DIR = Path(__file__).resolve().parents[1] / "ui" / "dist"

# Event kinds for the UI, which can't compare strings cheaply.
K_USER, K_ASSISTANT, K_TOOL_CALL, K_TOOL_RESULT, K_STATUS, K_PEER, K_ERROR, K_ROUTE, K_THINKING = range(1, 10)


# Kept in the log and the SSE streams, left out of the transcripts the UI shows.
HIDDEN = {"usage"}


def short(s: str, n: int = 600) -> str:
    return s if len(s) <= n else s[:n] + " …"


def tail(s: str, n: int = 2000) -> str:
    return s if len(s) <= n else "… " + s[-n:]


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
    elif t == "reasoning":
        kind, label, text = K_THINKING, "Thinking", d.get("content", "")
    elif t == "tool_call":
        kind, label, text = K_TOOL_CALL, d.get("name", "tool"), short(tool_arg(d.get("arguments")), 300)
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


def live_row(first: dict, d: dict) -> dict:
    """One row for a model reply still streaming in: its `delta` events so far."""
    if d["kind"] == "content":
        kind, label, text = K_ASSISTANT, "Agent (writing…)", d["text"]
    elif d["kind"] == "tool":
        kind, label, text = K_TOOL_CALL, d.get("name") or "tool", short(d["text"], 300) + " …"
    else:
        kind, label, text = K_THINKING, "Thinking…", tail(d["text"])
    return {**first, "kind": kind, "label": label, "text": text, "live": 1}


def fold(events: list[dict]) -> list[dict]:
    """Events for display. A model call's `delta` events become one live row
    per part (reasoning, reply, each tool call) at the end, until the event
    that ends the call is in; then the deltas are dropped (the control plane
    deletes them too) and the call's own events stand."""
    out: list[dict] = []
    streaming: dict[str | None, list[tuple[dict, dict]]] = {}
    for e in events:
        tid = e["thread_id"]
        if e["type"] == "delta":
            d = e["payload"]
            parts = streaming.setdefault(tid, [])
            if parts and (parts[-1][1]["kind"], parts[-1][1].get("name")) == (d.get("kind"), d.get("name")):
                parts[-1][1]["text"] += d.get("text", "")
            else:
                parts.append((e, {"kind": d.get("kind"), "name": d.get("name"), "text": d.get("text", "")}))
            continue
        if e["type"] in ENDS_STREAM:
            streaming.pop(tid, None)
        if e["type"] not in HIDDEN:
            out.append(display(e))
    for parts in streaming.values():
        out.extend(live_row(first, d) for first, d in parts)
    return out


def thread_view(t: dict) -> dict:
    return {k: t[k] for k in ("id", "project_id", "parent_id", "title", "request", "summary", "activity", "state",
                              "worker_id", "files", "created_at", "updated_at")}


def create_app(store: Store | None = None, provider: WorkerProvider | None = None,
               router: Router | None = None,
               shutdown_event: asyncio.Event | None = None) -> Starlette:
    store = store or store_from_url(None)
    stop = shutdown_event if shutdown_event is not None else asyncio.Event()
    holder: dict = {}

    @asynccontextmanager
    async def lifespan(app):
        await store.open()
        orch = Orchestrator(store, provider or default_provider(), router or default_router())
        holder["orch"] = orch
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

    async def worker_logs(req: Request):
        tid = req.path_params["tid"]
        if await store.get_thread(tid) is None:
            return JSONResponse({"error": "no such thread"}, 404)
        q = req.query_params.get("lines", "300")
        lines = min(int(q), 5000) if q.isdigit() else 300
        try:
            text = await orch().worker_logs(tid, lines)
        except KeyError:
            return PlainTextResponse(f"thread {tid} has no live worker in this control plane "
                                     "(its worker is gone once the control plane restarts)\n", 404)
        except Exception as e:
            return PlainTextResponse(f"could not read the worker log: {describe(e)}\n", 502)
        return PlainTextResponse(text)

    async def transcript(req: Request):
        t = await store.get_thread(req.path_params["tid"])
        if t is None:
            return JSONResponse({"error": "no such thread"}, 404)
        after = int(req.query_params.get("after", "0") or 0)
        events = await store.list_events(t["project_id"], t["id"], after, limit=2000)
        return JSONResponse(fold(events))

    async def project_log(req: Request):
        after = int(req.query_params.get("after", "0") or 0)
        events = await store.list_events(req.path_params["pid"], None, after, limit=2000)
        return JSONResponse(fold(events))

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
        Route("/api/threads/{tid}/worker-logs", worker_logs, methods=["GET"]),
        Route("/api/threads/{tid}/events", thread_events, methods=["GET"]),
        Route("/", index),
        Route("/p/{pid}", index),
    ]
    routes.append(Mount("/ui", StaticFiles(directory=UI_DIR, check_dir=False)))
    return Starlette(routes=routes, lifespan=lifespan)
