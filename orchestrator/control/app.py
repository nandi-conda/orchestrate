"""HTTP API for the UI and the static ultra UI.

Workers never call in here: the control plane opens every connection to them
(control/service.py, "peer calls")."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import re
from contextlib import asynccontextmanager
from pathlib import Path

from urllib.parse import quote

from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, PlainTextResponse, Response, StreamingResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from . import paths
from .providers import WorkerProvider, default_provider
from .gitio import GitError
from .hub import folders, is_folder, remote_url
from .router import Router, default_router
from .runner_client import FileError
from .service import (ENDS_STREAM, ArchivedError, EditError, NotOnWorker, Orchestrator, describe, editable,
                      image_title, tool_arg)
from .store import Store, store_from_url

UI_DIR = Path(__file__).resolve().parents[1] / "ui" / "dist"

# Event kinds for the UI, which can't compare strings cheaply. K_USER is only
# what the person typed; K_REQUEST is the brief a thread was spawned with,
# which the router writes. K_RESULT is a turn's summary, shown as its answer;
# K_STATE is a state change, which the Dialog leaves to the state badges.
(K_USER, K_ASSISTANT, K_TOOL_CALL, K_TOOL_RESULT, K_STATUS, K_PEER, K_ERROR, K_ROUTE, K_THINKING,
 K_RESULT, K_STATE, K_REQUEST) = range(1, 13)


# Kept in the log and the SSE streams, left out of the transcripts the UI shows.
HIDDEN = {"usage"}

# How many events a pane shows at once. Project orchestrator and a thread's transcript
# take the newest: a project worked on for a while has more events than fit in one
# reply, and its view must show what just happened, not what happened first.
PAGE_EVENTS = 2000

# A pasted image arrives as a data: URL. Its bytes live in the store; the events
# and SSE streams carry only its id, so a long transcript and a streaming turn
# stay small. At most MAX_IMAGES per message, each at most MAX_IMAGE_BYTES.
IMAGE_TYPES = ("image/png", "image/jpeg", "image/webp", "image/gif")
IMAGE_ALIASES = {"image/jpg": "image/jpeg"}
MAX_IMAGES = 4
MAX_IMAGE_BYTES = 8 * 1024 * 1024
DATA_URL = re.compile(r"^data:([a-z]+/[a-z0-9.+-]+);base64,(.*)$", re.IGNORECASE | re.DOTALL)
# Worktree files the window shows as images (Files tab, markdown), by extension.
FILE_IMAGE_TYPES = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "gif": "image/gif",
                    "webp": "image/webp", "svg": "image/svg+xml"}


def decode_images(raw) -> list[tuple[str, bytes]]:
    """The (media_type, bytes) pairs in a request's `images`, a list of data:
    URLs. Raises ValueError with a message fit for the person who pasted them."""
    if raw in (None, ""):
        return []
    if not isinstance(raw, list):
        raise ValueError("images must be a list of data: URLs")
    if len(raw) > MAX_IMAGES:
        raise ValueError(f"at most {MAX_IMAGES} images per message")
    out = []
    for item in raw:
        m = DATA_URL.match(item.strip()) if isinstance(item, str) else None
        if m is None:
            raise ValueError("each image must be a data: URL holding base64 image bytes")
        media_type = IMAGE_ALIASES.get(m.group(1).lower(), m.group(1).lower())
        if media_type not in IMAGE_TYPES:
            raise ValueError("images must be png, jpeg, webp or gif")
        try:
            data = base64.b64decode(m.group(2).strip(), validate=True)
        except (ValueError, binascii.Error):
            raise ValueError("an image is not valid base64") from None
        if not data:
            raise ValueError("an image is empty")
        if len(data) > MAX_IMAGE_BYTES:
            raise ValueError(f"each image must be under {MAX_IMAGE_BYTES // (1024 * 1024)} MB")
        out.append((media_type, data))
    return out


async def save_images(store: Store, project_id: str, raw) -> list[dict]:
    """Store a request's images and return the references its events carry."""
    return [await store.add_image(project_id, media_type, data) for media_type, data in decode_images(raw)]


def short(s: str, n: int = 600) -> str:
    return s if len(s) <= n else s[:n] + " …"


def tail(s: str, n: int = 2000) -> str:
    return s if len(s) <= n else "… " + s[-n:]


def display(ev: dict) -> dict:
    """Add `kind` (int) and `text` (one display string) to an event."""
    t, d = ev["type"], ev["payload"]
    if t == "request":
        kind, label, text = K_USER, "You", d.get("text", "")
    elif t == "message_in":
        frm = d.get("from", "user")
        kind, label = (K_USER, "You") if frm == "user" else (K_PEER, f"{frm} → {ev.get('thread_id')}")
        if "edit" in d:
            label = "You · edit"
        text = d.get("body", "")
    elif t == "created":
        kind, label, text = K_REQUEST, "Request", d.get("request", "")
    elif t == "assistant":
        kind, label, text = K_ASSISTANT, "Agent", d.get("content", "")
    elif t == "reasoning":
        kind, label, text = K_THINKING, "Thinking", d.get("content", "")
    elif t == "tool_call":
        kind, label, text = K_TOOL_CALL, d.get("name", "tool"), short(tool_arg(d.get("arguments")), 300)
    elif t == "tool_result":
        kind, label, text = K_TOOL_RESULT, f"{d.get('name', 'tool')} result", short(str(d.get("result", "")))
    elif t == "state":
        kind, label, text = K_STATE, "state", d.get("state", "")
    elif t == "turn_done":
        kind, label, text = K_RESULT, "Result", d.get("summary", "")
    elif t in ("error", "dropped"):
        kind, label, text = K_ERROR, t, d.get("error") or d.get("reason", "")
    elif t == "route":
        kind, label = K_ROUTE, "router"
        text = d.get("summary", "") + (f" — {d['why']}" if d.get("why") else "")
    elif t in ("archived", "unarchived"):
        kind, label, text = K_STATUS, t, f"thread {t}"
    elif t in ("project_archived", "project_unarchived"):
        kind, label = K_STATUS, t.removeprefix("project_")
        text = f"project {label}"
    elif t == "worker_lost":
        kind, label, text = K_ERROR, "worker lost", d.get("error", "")
    elif t == "worker_restarted":
        kind, label, text = K_ERROR, "worker restarted", d.get("error", "")
    elif t == "binding":
        # Leaving a worker, or failing to get on one, says why; the rest is the badges'.
        kind = K_ERROR if d.get("to") == "unbound" else K_STATE
        label, text = d.get("to", "binding"), d.get("reason", "")
    elif t == "worker_state":
        kind = K_STATUS if d.get("to") == "ready" else K_ERROR
        label, text = f"worker {d.get('to')}", f"{d.get('worker')}: {d.get('reason', '')}"
    elif t == "revived":
        if d.get("found"):
            text = f"found again on worker {d.get('worker')}, which still runs it"
        else:
            then = "working on its request again" if d.get("request") else "idle until its next message"
            text = f"back on worker {d.get('worker')}, {then}"
        kind, label = K_STATUS, "revived"
    elif t == "worker":
        kind, label, text = K_STATUS, "worker", f"{d.get('provider')} worker {d.get('id')} up"
    elif t == "saved":
        kind, label, text = K_STATUS, "saved", f"pushed {d.get('branch')} at {str(d.get('commit', ''))[:10]}"
    elif t == "landed":
        kind, label, text = K_STATUS, "landed", f"{d.get('branch')} is on {d.get('base')} at {str(d.get('commit', ''))[:10]}"
    elif t == "land_conflict":
        kind, label = K_ERROR, "land conflict"
        text = f"{d.get('branch')} conflicts with {d.get('base')} in {', '.join(d.get('files') or [])}; the thread resolves it"
    elif t == "edited":
        kind, label, text = K_STATUS, "edited", "you edited a message"
    else:
        kind, label, text = K_STATUS, t, short(json.dumps(d), 300)
    out = {**ev, "kind": kind, "label": label, "text": text, "editable": int(editable(ev))}
    if kind == K_PEER:
        out["from"] = d.get("from", "")
    images = d.get("images") or []
    if images:
        out["images"] = images
    return out


def live_row(first: dict, d: dict) -> dict:
    """One row for a model reply still streaming in: its `delta` events so far."""
    if d["kind"] == "content":
        kind, label, text = K_ASSISTANT, "Agent (writing…)", d["text"]
    elif d["kind"] == "tool":
        kind, label, text = K_TOOL_CALL, d.get("name") or "tool", short(d["text"], 300) + " …"
    else:
        kind, label, text = K_THINKING, "Thinking…", tail(d["text"])
    return {**first, "kind": kind, "label": label, "text": text, "live": 1}


class Streaming:
    """A model call's `delta` events, folded into the live rows a pane shows:
    one row per part (reasoning, the reply, each tool call), which grows until
    the event that ends the call (`ENDS_STREAM`) drops it. The panes and the SSE
    streams both fold with this, so a row looks the same either way."""

    def __init__(self) -> None:
        self.parts: dict[str | None, list[tuple[dict, dict]]] = {}

    def add(self, e: dict) -> tuple[dict, bool]:
        """Fold delta event `e` into its thread's last part, or start a part.
        Returns the thread's live row as it now stands, and whether it started
        the part (which is when the thread's activity moved)."""
        d, parts = e["payload"], self.parts.setdefault(e["thread_id"], [])
        if parts and (parts[-1][1]["kind"], parts[-1][1].get("name")) == (d.get("kind"), d.get("name")):
            parts[-1][1]["text"] += d.get("text", "")
            return live_row(*parts[-1]), False
        parts.append((e, {"kind": d.get("kind"), "name": d.get("name"), "text": d.get("text", "")}))
        return live_row(*parts[-1]), True

    def end(self, tid: str | None) -> None:
        """The event that ends the call is in: its thread has no live rows."""
        self.parts.pop(tid, None)

    def rows(self) -> list[dict]:
        """Every live row, per thread in order, then per part."""
        return [live_row(first, d) for parts in self.parts.values() for first, d in parts]


def fold(events: list[dict]) -> list[dict]:
    """Events for display. A model call's `delta` events become one live row
    per part (reasoning, reply, each tool call) at the end, until the event
    that ends the call is in; then the deltas are dropped (the control plane
    deletes them too) and the call's own events stand."""
    out: list[dict] = []
    by_seq: dict[int, dict] = {}
    live = Streaming()
    for e in events:
        if e["type"] == "edited":
            # An edit shows on the message it edits: its text now, marked edited.
            target = by_seq.get(e["payload"].get("of"))
            if target is not None:
                target["text"], target["label"], target["edited"] = e["payload"].get("text", ""), "You · edited", 1
            continue
        if e["type"] == "delta":
            live.add(e)
            continue
        if e["type"] in ENDS_STREAM:
            live.end(e["thread_id"])
        if e["type"] not in HIDDEN:
            out.append(display(e))
            by_seq[e["seq"]] = out[-1]
    out.extend(live.rows())
    return out


def thread_view(t: dict, orch: Orchestrator) -> dict:
    """A thread for the UI, with its binding and the state of the worker it
    is bound to (states.py), each with why, and its load (load.py)."""
    view = {k: t[k] for k in ("id", "project_id", "parent_id", "title", "request", "summary", "activity", "state",
                              "binding", "binding_reason", "worker_id", "files", "archived_at", "created_at",
                              "updated_at", "load")}
    view["worker_state"], view["worker_reason"] = orch.worker_state(t)
    return view


def create_app(store: Store | None = None, provider: WorkerProvider | None = None,
               router: Router | None = None,
               shutdown_event: asyncio.Event | None = None, repos: str | Path = paths.REPOS) -> Starlette:
    store = store or store_from_url(None)
    stop = shutdown_event if shutdown_event is not None else asyncio.Event()
    holder: dict = {}

    @asynccontextmanager
    async def lifespan(app):
        await store.open()
        orch = Orchestrator(store, provider or default_provider(), router or default_router(), repos)
        holder["orch"] = orch
        app.state.orch = orch
        try:
            await orch.start()
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

    def project_view(p: dict) -> dict:
        """A project and where its hub is: the git repository its threads' work goes to."""
        return {**p, "repo_path": str(orch().hub_path(p["id"]).resolve())}

    async def list_projects(req: Request):
        """Live projects; ?archived=1 lists the archived ones instead."""
        archived = req.query_params.get("archived", "0") == "1"
        return JSONResponse([project_view(p) for p in await store.list_projects(archived)])

    async def create_project(req: Request):
        """A new project: {name, model?}, or one opened from a folder on disk,
        {folder, name?, model?}, named after the folder unless named. The
        folder becomes the project's remote (Orchestrator.create_project); one
        that cannot be is refused with the reason, and nothing is made."""
        b = await body(req)
        folder = str(b.get("folder", "")).strip()
        if folder and not is_folder(folder):
            return JSONResponse({"error": f"{folder} is not a folder path; give its full path, "
                                          "like /home/you/my-app or ~/my-app"}, 400)
        name = str(b.get("name", "")).strip() or (Path(remote_url(folder)).name if folder else "")
        if not name:
            return JSONResponse({"error": "name is required"}, 400)
        try:
            p = await orch().create_project(name, str(b.get("model", "")).strip(), folder)
        except GitError as e:
            return JSONResponse({"error": str(e)}, 400)
        return JSONResponse(project_view(p), 201)

    async def list_folders(req: Request):
        """The folders in a folder on this machine (?path=, the home folder by
        default), for the folder picker: hub.folders."""
        try:
            return JSONResponse(await asyncio.to_thread(folders, req.query_params.get("path", "")))
        except GitError as e:
            return JSONResponse({"error": str(e)}, 400)

    async def project_settings(req: Request):
        """Change any of a project's name, model and remote (`repo_url`, "" to
        remove it). A remote the hub cannot join is refused with git's reason."""
        b = await body(req)
        fields = {k: str(b[k]).strip() for k in ("name", "model", "repo_url") if k in b}
        if fields.get("name") == "":
            return JSONResponse({"error": "name is required"}, 400)
        try:
            p = await orch().update_project(req.path_params["pid"], **fields)
        except KeyError:
            return JSONResponse({"error": "no such project"}, 404)
        except GitError as e:
            return JSONResponse({"error": str(e)}, 400)
        return JSONResponse(project_view(p))

    async def get_project(req: Request):
        p = await store.get_project(req.path_params["pid"])
        return JSONResponse(project_view(p)) if p else JSONResponse({"error": "no such project"}, 404)

    async def project_archive(req: Request):
        try:
            p = await orch().archive_project(req.path_params["pid"])
        except KeyError:
            return JSONResponse({"error": "no such project"}, 404)
        except Exception as e:
            return JSONResponse({"error": f"could not take the project off the workers: {describe(e)}"}, 502)
        return JSONResponse(project_view(p))

    async def project_unarchive(req: Request):
        try:
            p = await orch().unarchive_project(req.path_params["pid"])
        except KeyError:
            return JSONResponse({"error": "no such project"}, 404)
        return JSONResponse(project_view(p))

    async def post_request(req: Request):
        pid = req.path_params["pid"]
        if await store.get_project(pid) is None:
            return JSONResponse({"error": "no such project"}, 404)
        b = await body(req)
        text = str(b.get("text", "")).strip()
        try:
            images = await save_images(store, pid, b.get("images"))
        except ValueError as e:
            return JSONResponse({"error": str(e)}, 400)
        if not text and not images:
            return JSONResponse({"error": "text or an image is required"}, 400)
        mode = b.get("mode", "auto")
        if mode not in ("auto", "thread", "new"):
            return JSONResponse({"error": "mode is auto, thread or new"}, 400)
        try:
            result = await orch().submit(pid, text, mode, b.get("target") or None, images)
        except KeyError as e:
            return JSONResponse({"error": str(e.args[0])}, 404)
        except (RuntimeError, ArchivedError) as e:
            return JSONResponse({"error": str(e)}, 409)
        return JSONResponse(result, 200 if mode != "auto" else 202)

    async def edit_message(req: Request):
        """Edit one of the person's messages, named by its event's seq; the router
        re-assesses it in the background."""
        pid, seq = req.path_params["pid"], req.path_params["seq"]
        b = await body(req)
        text = str(b.get("text", "")).strip()
        if not text:
            return JSONResponse({"error": "text is required"}, 400)
        try:
            result = await orch().edit(pid, seq, text)
        except KeyError as e:
            return JSONResponse({"error": str(e.args[0])}, 404)
        except EditError as e:
            return JSONResponse({"error": str(e)}, 400)
        except ArchivedError as e:
            return JSONResponse({"error": str(e)}, 409)
        return JSONResponse(result, 202 if result["changed"] else 200)

    async def list_threads(req: Request):
        """Live threads; ?archived=1 lists the archived ones instead."""
        archived = req.query_params.get("archived", "0") == "1"
        await orch().recover(req.path_params["pid"])
        return JSONResponse([thread_view(t, orch()) for t in await orch().directory(req.path_params["pid"], archived)])

    async def create_thread(req: Request):
        b = await body(req)
        request = str(b.get("request", "")).strip()
        pid = req.path_params["pid"]
        try:
            images = await save_images(store, pid, b.get("images"))
        except ValueError as e:
            return JSONResponse({"error": str(e)}, 400)
        if not request and not images:
            return JSONResponse({"error": "request or an image is required"}, 400)
        from .router import title_for
        title = b.get("title") or (title_for(request) if request else image_title(images))
        try:
            t = await orch().spawn(pid, title, request, images=images)
        except KeyError:
            return JSONResponse({"error": "no such project"}, 404)
        except (RuntimeError, ArchivedError) as e:
            return JSONResponse({"error": str(e)}, 409)
        return JSONResponse(thread_view(await orch().with_load(t), orch()), 201)

    async def get_thread(req: Request):
        t = await store.get_thread(req.path_params["tid"])
        return JSONResponse(thread_view(await orch().with_load(t), orch())) if t else JSONResponse({"error": "no such thread"}, 404)

    async def thread_message(req: Request):
        tid = req.path_params["tid"]
        t = await store.get_thread(tid)
        if t is None:
            return JSONResponse({"error": "no such thread"}, 404)
        b = await body(req)
        text = str(b.get("body") or b.get("text") or "").strip()
        try:
            images = await save_images(store, t["project_id"], b.get("images"))
        except ValueError as e:
            return JSONResponse({"error": str(e)}, 400)
        if not text and not images:
            return JSONResponse({"error": "body or an image is required"}, 400)
        try:
            await orch().send(tid, text, "user", images=images)
        except KeyError:
            return JSONResponse({"error": "no such thread"}, 404)
        except ArchivedError as e:
            return JSONResponse({"error": str(e)}, 409)
        return JSONResponse({"ok": True}, 202)

    async def thread_stop(req: Request):
        if await store.get_thread(req.path_params["tid"]) is None:
            return JSONResponse({"error": "no such thread"}, 404)
        await orch().stop(req.path_params["tid"])
        return JSONResponse({"ok": True})

    async def thread_respawn(req: Request):
        """Bring the thread back on a worker now (Orchestrator.respawn); 202, the
        revival or its error arrives as the thread's event."""
        try:
            await orch().respawn(req.path_params["tid"])
        except KeyError:
            return JSONResponse({"error": "no such thread"}, 404)
        except ArchivedError as e:
            return JSONResponse({"error": str(e)}, 409)
        return JSONResponse({"ok": True}, 202)

    async def thread_archive(req: Request):
        try:
            t = await orch().archive(req.path_params["tid"])
        except KeyError:
            return JSONResponse({"error": "no such thread"}, 404)
        return JSONResponse(thread_view(await orch().with_load(t), orch()))

    async def thread_unarchive(req: Request):
        try:
            t = await orch().unarchive(req.path_params["tid"])
        except KeyError:
            return JSONResponse({"error": "no such thread"}, 404)
        return JSONResponse(thread_view(await orch().with_load(t), orch()))

    async def worker_logs(req: Request):
        tid = req.path_params["tid"]
        if await store.get_thread(tid) is None:
            return JSONResponse({"error": "no such thread"}, 404)
        q = req.query_params.get("lines", "300")
        lines = min(int(q), 5000) if q.isdigit() else 300
        try:
            text = await orch().worker_logs(tid, lines)
        except Exception as e:
            return PlainTextResponse(f"could not read the worker log: {describe(e)}\n", 502)
        return PlainTextResponse(text)

    def files_error(e: Exception) -> Response:
        if isinstance(e, KeyError):
            return JSONResponse({"error": "no such thread"}, 404)
        if isinstance(e, (ArchivedError, NotOnWorker)):
            return JSONResponse({"error": str(e)}, 409)
        if isinstance(e, FileError):
            return JSONResponse({"error": str(e)}, e.status)
        return JSONResponse({"error": f"could not read the thread's files: {describe(e)}"}, 502)

    async def thread_files(req: Request):
        """A directory of the thread's worktree: ?path=<dir>, the root by default."""
        try:
            return JSONResponse(await orch().list_files(req.path_params["tid"], req.query_params.get("path", "")))
        except Exception as e:
            return files_error(e)

    async def thread_file(req: Request):
        """One file of the thread's worktree (?path=<file>). An image is served
        inline as its type, so the window can show it (the Files tab, and
        images a markdown file or reply refers to); anything else is a
        download. Nothing served runs: the sandbox policy keeps an SVG's
        scripts and links dead even when it is opened on its own."""
        tid, path = req.path_params["tid"], req.query_params.get("path", "")
        try:
            r = await orch().open_file(tid, path)
        except Exception as e:
            return files_error(e)
        name = path.rstrip("/").rsplit("/", 1)[-1] or "file"
        media = FILE_IMAGE_TYPES.get(name.rsplit(".", 1)[-1].lower()) if "." in name else None
        headers = {
            "content-disposition": f"{'inline' if media else 'attachment'}; filename*=UTF-8''{quote(name)}",
            "x-content-type-options": "nosniff",
            "content-security-policy": "default-src 'none'; style-src 'unsafe-inline'; sandbox",
        }
        if "content-length" in r.headers:
            headers["content-length"] = r.headers["content-length"]
        return StreamingResponse(r.aiter_raw(), media_type=media or "application/octet-stream", headers=headers,
                                 background=BackgroundTask(r.aclose))

    async def transcript(req: Request):
        t = await store.get_thread(req.path_params["tid"])
        if t is None:
            return JSONResponse({"error": "no such thread"}, 404)
        return JSONResponse(fold(await store.recent_events(t["project_id"], t["id"], limit=PAGE_EVENTS)))

    async def image(req: Request):
        """A pasted image's bytes. Ids never change, so the browser may keep it."""
        row = await store.get_image(req.path_params["iid"])
        if row is None:
            return JSONResponse({"error": "no such image"}, 404)
        return Response(row["data"], media_type=row["media_type"],
                        headers={"cache-control": "public, max-age=31536000, immutable",
                                 "content-length": str(row["size"]),
                                 "x-content-type-options": "nosniff"})

    async def project_log(req: Request):
        """The newest events in the project: what Project orchestrator shows. A long
        log keeps its newest page, so the pane never freezes on its oldest events."""
        return JSONResponse(fold(await store.recent_events(req.path_params["pid"], limit=PAGE_EVENTS)))

    def sse(project_id: str, thread_id: str | None, after: int):
        def frame(seq: int, typ: str, row: dict) -> str:
            return f"id: {seq}\nevent: {typ}\ndata: {json.dumps(row)}\n\n"

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
            live = Streaming()
            yield ": connected\n\n"
            while not stop.is_set():
                events = await store.list_events(project_id, thread_id, last)
                # A reply still streaming is one row that grows, so the deltas
                # of a batch fold into the row they ended as, by its seq, and a
                # pane patches it instead of refetching everything it shows.
                # `began` marks the delta that started a part: only that one
                # moved something off the pane (the thread list's activity).
                # (On a replay, a whole turn's deltas arrive in one batch.)
                grown: dict[int, dict] = {}
                begun: set[int] = set()
                for e in events:
                    last = e["seq"]
                    if e["type"] == "delta":
                        row, started = live.add(e)
                        grown[row["seq"]] = row
                        if started:
                            begun.add(row["seq"])
                        continue
                    if e["type"] in ENDS_STREAM:
                        live.end(e["thread_id"])
                    yield frame(last, e["type"], display(e))
                for seq, row in grown.items():
                    yield frame(last, "delta", {**row, "began": 1} if seq in begun else row)
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
        Route("/api/folders", list_folders, methods=["GET"]),
        Route("/api/projects/{pid}", get_project, methods=["GET"]),
        Route("/api/projects/{pid}/settings", project_settings, methods=["POST"]),
        Route("/api/projects/{pid}/archive", project_archive, methods=["POST"]),
        Route("/api/projects/{pid}/unarchive", project_unarchive, methods=["POST"]),
        Route("/api/projects/{pid}/requests", post_request, methods=["POST"]),
        Route("/api/projects/{pid}/messages/{seq:int}/edit", edit_message, methods=["POST"]),
        Route("/api/projects/{pid}/threads", list_threads, methods=["GET"]),
        Route("/api/projects/{pid}/threads", create_thread, methods=["POST"]),
        Route("/api/projects/{pid}/log", project_log, methods=["GET"]),
        Route("/api/projects/{pid}/events", project_events, methods=["GET"]),
        Route("/api/images/{iid}", image, methods=["GET"]),
        Route("/api/threads/{tid}", get_thread, methods=["GET"]),
        Route("/api/threads/{tid}/messages", thread_message, methods=["POST"]),
        Route("/api/threads/{tid}/stop", thread_stop, methods=["POST"]),
        Route("/api/threads/{tid}/respawn", thread_respawn, methods=["POST"]),
        Route("/api/threads/{tid}/archive", thread_archive, methods=["POST"]),
        Route("/api/threads/{tid}/unarchive", thread_unarchive, methods=["POST"]),
        Route("/api/threads/{tid}/transcript", transcript, methods=["GET"]),
        Route("/api/threads/{tid}/worker-logs", worker_logs, methods=["GET"]),
        Route("/api/threads/{tid}/events", thread_events, methods=["GET"]),
        Route("/api/threads/{tid}/files", thread_files, methods=["GET"]),
        Route("/api/threads/{tid}/files/raw", thread_file, methods=["GET"]),
        Route("/", index),
        Route("/p/{pid}", index),
    ]
    routes.append(Mount("/ui", StaticFiles(directory=UI_DIR, check_dir=False)))
    return Starlette(routes=routes, lifespan=lifespan)
