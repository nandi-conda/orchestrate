"""An echo runner that speaks runner/API.md, for the tests only.

It has no model. Each turn it says what it got, and:
- a line `spawn: <task>` in a request spawns a child thread through the peer API,
- a line `tell t_xxx: <text>` sends that thread a message,
- a line `list: <query>` lists the project's threads, `read t_xxx` reads one,
- a line `write <path>: <text>` writes text to that file of the thread's workspace
  (`$RUNNER_WORKDIR/<id>`), which /threads/{id}/files lists and serves,
- its objectives are the ones the control plane posts it with, and those a
  message brings (appended); a line `objective <i> <status>: <note>` moves one
  (an `objectives` event, as the real runner's goal turns make: it has no
  model to work them as goals); a turn that ends with them all done ends the
  thread `done`, like the real runner,
- child threads report `Finished: ...` to their parent, like the real runner.

A turn its process is stopped in is interrupted, as the real runner's is
(runner/API.md, "Interrupted turns"): while a turn runs `.team/<id>.turn`
exists in its workdir, and a thread posted while it does resumes that turn
with RESUME from `runner` instead of taking a request. (The real runner also
checks the thread's session is still the one the turn went on from; this one
does not move threads between workers on its own, so it leaves that out.)

Like the real runner it hosts threads of any number of projects. Its git flow
is the real runner's (runner/API.md, "Repository"): a worktree per thread in
its project's repository, which the control plane fills
(`POST /projects/{project}/repo`), and at the
end of every turn a commit, a snapshot and a session that the control plane
takes, landing the branch (peer call `save`). Its session is the messages a thread received, one
per line, so a thread brought back on another worker says how many it had.

Peer calls wait on /peer/calls for the control plane to collect, as in
runner/API.md.

The tests start it through LocalProvider(command=ECHO_RUNNER) (tests/doubles.py);
the control plane itself never does.
"""

from __future__ import annotations

import argparse
import sys
import asyncio
import json
import os
import re
import secrets
import shutil
import signal
import tempfile
import threading
import time
from pathlib import Path

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from control.gitio import Checkout, GitError, refs_header

SPAWN = re.compile(r"^\s*spawn:\s*(.+)$", re.M)
TELL = re.compile(r"^\s*tell\s+(t_[0-9a-zA-Z]+):\s*(.+)$", re.M)
LIST = re.compile(r"^\s*list:(.*)$", re.M)
READ = re.compile(r"^\s*read\s+(t_[0-9a-zA-Z]+)\s*$", re.M)
WRITE = re.compile(r"^\s*write\s+([^:\n]+):\s?(.*)$", re.M)
OBJECTIVE = re.compile(r"^\s*objective\s+(\d+)\s+(pending|in_progress|done):\s?(.*)$", re.M)
ENDED = ("done", "stopped", "failed")
THREAD_ID = re.compile(r"^[\w-]+$")
RESUME = "Your last turn was cut short: the runner restarted while it ran. Finish that turn."


async def gone(req: Request) -> None:
    """Returns once the caller's connection is closed."""
    while not await req.is_disconnected():
        await asyncio.sleep(0.1)


async def long_poll(taking, req: Request) -> list[dict]:
    """What `taking` answers, or [] as soon as the caller goes away first: a
    long poll must not outlive the connection that asked for it, or a worker
    being stopped waits for a poll nobody will read (uvicorn's shutdown waits
    for it too). The real runner's server.py does the same."""
    task = asyncio.ensure_future(taking)
    left = asyncio.ensure_future(gone(req))
    try:
        await asyncio.wait({task, left}, return_when=asyncio.FIRST_COMPLETED)
        return task.result() if task.done() and not task.cancelled() else []
    finally:
        for ending in (task, left):
            ending.cancel()


class Host:
    def __init__(self, capacity: int, delay: float, workdir: str) -> None:
        self.capacity = capacity
        self.delay = delay
        self.workdir = Path(workdir).resolve()
        self.repos: dict[str, Checkout] = {}  # one per project
        self.opened: set[str] = set()
        self.opening = asyncio.Lock()  # requests come in at once; one makes a repository
        self.threads: dict[str, dict] = {}
        self.events: list[dict] = []
        self.cond = asyncio.Condition()
        self.calls: dict[str, dict] = {}  # peer calls queued, not yet handed out
        self.turn_locks: dict[str, asyncio.Lock] = {}
        self.waiting: dict[str, asyncio.Future] = {}
        self.calls_cond = asyncio.Condition()

    async def repo(self, project: str) -> Checkout:
        """The project's repository on this worker, made on first use."""
        if not THREAD_ID.match(project or ""):
            raise GitError(f"bad project id {project!r}")
        async with self.opening:
            if project not in self.repos:
                self.repos[project] = Checkout(self.workdir / ".repos" / project)
            if project not in self.opened:
                await self.repos[project].open()
                self.opened.add(project)
        return self.repos[project]

    async def prune(self, keep: set[str]) -> list[str]:
        """Take every project not in `keep` off this worker (runner/API.md)."""
        root = self.workdir / ".repos"
        on_disk = {d.name for d in root.iterdir()} if root.exists() else set()
        gone = sorted(p for p in on_disk | set(self.repos) if p not in keep)
        for project in gone:
            for tid in [tid for tid, t in self.threads.items() if t["project"] == project]:
                t = self.threads.pop(tid)
                t["state"] = "stopped"
                for path in (self.workdir / tid, self.session_dir(t)):
                    shutil.rmtree(path, ignore_errors=True)
            self.repos.pop(project, None)
            self.opened.discard(project)
            shutil.rmtree(root / project, ignore_errors=True)
        return gone

    async def peer_call(self, op: str, thread: str, **args) -> str:
        """Thread `thread`'s peer call, whose answer is a tool result."""
        reply = await self.peer(op, thread, **args)
        return json.dumps(reply.get("result") if reply.get("ok") else {"error": reply.get("error")})

    async def peer(self, op: str, thread: str, **args) -> dict:
        cid = f"pc_{secrets.token_hex(5)}"
        fut = asyncio.get_running_loop().create_future()
        self.waiting[cid] = fut
        async with self.calls_cond:
            self.calls[cid] = {"id": cid, "thread": thread, "op": op, "args": args}
            self.calls_cond.notify_all()
        try:
            return await asyncio.wait_for(fut, 60)
        finally:
            self.waiting.pop(cid, None)

    async def take_calls(self, wait: float) -> list[dict]:
        """The calls queued for the control plane, waiting up to `wait` seconds
        for one."""
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

    def turn_file(self, tid: str) -> Path:
        return self.workdir / ".team" / f"{tid}.turn"

    async def turn(self, t: dict, text: str, images: list | None = None) -> None:
        """One turn at a time per thread, as the real runner takes them: a
        message that comes in during a turn is the next one."""
        async with self.turn_locks.setdefault(t["id"], asyncio.Lock()):
            await self._turn(t, text, images)

    async def _turn(self, t: dict, text: str, images: list | None = None) -> None:
        await self.set_state(t, "working")
        self.turn_file(t["id"]).parent.mkdir(parents=True, exist_ok=True)
        self.turn_file(t["id"]).write_text("{}")
        await asyncio.sleep(self.delay)
        # Stream the reply the way the real runner does (runner/API.md, `delta`).
        log = self.session_dir(t) / "messages.txt"
        earlier = log.read_text().count("\n") if log.exists() else 0
        with log.open("a") as f:
            f.write(json.dumps(text) + "\n")
        reply = f"Echo: {text}" + (f" (resumed after {earlier} earlier messages)" if t["resumed"] else "")
        t["resumed"] = False
        await self.emit(t["id"], "delta", {"kind": "reasoning", "text": "Echoing the request."})
        for i in range(0, len(reply), 16):
            await self.emit(t["id"], "delta", {"kind": "content", "text": reply[i:i + 16]})
            await asyncio.sleep(self.delay / 4)
        await self.emit(t["id"], "reasoning", {"content": "Echoing the request."})
        await self.emit(t["id"], "assistant", {"content": reply})
        for task in SPAWN.findall(text):
            await self.emit(t["id"], "tool_call", {"id": "c1", "name": "spawn_thread",
                                                   "arguments": {"request": task}})
            r = await self.peer_call("spawn_thread", t["id"], parent=t["id"], project=t["project"], title=task[:40],
                                     request=task)
            await self.emit(t["id"], "tool_result", {"id": "c1", "name": "spawn_thread", "result": r})
        for path, body in WRITE.findall(text):
            f = self.inside(t, path)
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(body + "\n")
        for to, body in TELL.findall(text):
            await self.peer_call("send_message", t["id"], sender=t["id"], to=to, body=body)
        for query in LIST.findall(text):
            r = await self.peer_call("list_threads", t["id"], project=t["project"], query=query.strip() or None)
            await self.emit(t["id"], "tool_result", {"id": "c2", "name": "list_threads", "result": r})
        for tid in READ.findall(text):
            r = await self.peer_call("read_thread", t["id"], thread_id=tid, last_n=5)
            await self.emit(t["id"], "tool_result", {"id": "c3", "name": "read_thread", "result": r})
        for i, status, note in OBJECTIVE.findall(text):
            t["objectives"] = [{**o, "status": status, "note": note} if n == int(i) else o
                               for n, o in enumerate(t["objectives"])]  # a new list: the last event keeps its own
            await self.emit(t["id"], "objectives", {"objectives": t["objectives"]})
        summary = f"echoed {len(text)} chars"
        t["summary"] = summary
        await self.save(t)
        await self.emit(t["id"], "turn_done", {"summary": summary, "final": f"Echo: {text}"})
        await self.peer_call("set_summary", t["id"], thread_id=t["id"], summary=summary, files=[])
        if t.get("parent") and not text.startswith("Finished:"):
            await self.peer_call("report", t["id"], thread_id=t["id"], summary=summary)
        self.turn_file(t["id"]).unlink(missing_ok=True)
        if t["state"] != "stopped":
            await self.set_state(t, "idle")
            if t["objectives"] and all(o["status"] == "done" for o in t["objectives"]):
                await self.set_state(t, "done")

    async def save(self, t: dict) -> None:
        """Commit and snapshot the worktree; the control plane takes both and
        lands the branch."""
        tid, branch = t["id"], f"thread/{t['id']}"
        try:
            repo = await self.repo(t["project"])
            commit = await repo.save(self.workdir / tid, tid, f"{t['title']}\n\nThread: {tid}")
            await repo.save_session(tid, self.session_dir(t))
            reply = await self.peer("save", tid, thread_id=tid, land=True)
            if not reply.get("ok"):
                raise GitError(reply.get("error") or "save failed")
            result = reply["result"]
            if result.get("landed"):
                await repo.follow(self.workdir / tid, tid)
        except (GitError, asyncio.TimeoutError) as e:
            await self.emit(tid, "error", {"error": f"saving the work failed: {e or type(e).__name__}"})
            return
        if result.get("landed"):
            await self.emit(tid, "landed", {"branch": branch, "base": result["base"], "commit": result["commit"]})
        elif result.get("conflict") is not None:
            await self.emit(tid, "land_conflict", {"branch": branch, "base": result["base"], "files": result["conflict"]})
        else:
            await self.emit(tid, "error", {"error": f"landing {branch} failed: {result.get('message')}"})

    def session_dir(self, t: dict) -> Path:
        return self.workdir / ".sessions" / t["id"]

    def inside(self, t: dict, path: str) -> Path:
        """`path` in the thread's workspace; ValueError if it leaves it or names .git."""
        root = (self.workdir / t["id"]).resolve()
        p = (root / path).resolve()
        if (p != root and root not in p.parents) or ".git" in Path(path).parts:
            raise ValueError(f"path escapes the workspace: {path}")
        return p

    def entry(self, t: dict) -> dict:
        return {k: t[k] for k in ("id", "project", "title", "state", "summary", "objectives", "files", "parent")}


def create_app(host: Host, token: str) -> Starlette:
    def authed(req: Request) -> bool:
        return not token or req.headers.get("authorization") == f"Bearer {token}"

    boot = secrets.token_hex(8)  # this runner process (runner/API.md, /health)

    async def health(req: Request):
        running = sum(1 for t in host.threads.values() if t["state"] in ("queued", "working"))
        return JSONResponse({"ok": True, "threads": len(host.threads), "running": running,
                             "capacity": host.capacity, "boot": boot})

    async def list_threads(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        return JSONResponse([host.entry(t) for t in host.threads.values()])

    async def get_thread(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        t = host.threads.get(req.path_params["tid"])
        if t is None:
            return JSONResponse({"error": "not found"}, 404)
        return JSONResponse(host.entry(t))

    async def create_thread(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        b = await req.json()
        if not isinstance(b.get("project"), str) or not b["project"]:
            return JSONResponse({"error": "project is required"}, 400)
        if not b.get("request") and not b.get("id") and not b.get("images"):
            return JSONResponse({"error": "request, or the id of a thread to bring back, is required"}, 400)
        tid = b.get("id") or f"t_{secrets.token_hex(5)}"
        if tid in host.threads and host.threads[tid]["state"] not in ENDED:
            return JSONResponse({"error": "exists"}, 409)
        t = {"id": tid, "project": b["project"], "title": b.get("title") or (b.get("request") or tid)[:40],
             "state": "queued", "summary": b.get("summary") or "", "objectives": b.get("objectives") or [],
             "files": [], "parent": b.get("parent"),
             "resumed": not (b.get("request") or b.get("images"))}
        host.threads[tid] = t
        await host.emit(tid, "created", host.entry(t))

        async def prepare():  # after the answer, as the runner's ThreadHost._prepare; messages wait for it
            async with host.turn_locks.setdefault(tid, asyncio.Lock()):
                await make_ready()

        async def make_ready():
            try:
                repo = await host.repo(t["project"])
                await host.emit(tid, "preparing", {"step": "making its worktree"})
                await repo.worktree(host.workdir / tid, tid)
                await host.emit(tid, "preparing", {"step": "taking its session"})
                await repo.session(tid, host.session_dir(t))
                host.session_dir(t).mkdir(parents=True, exist_ok=True)
            except GitError as e:
                await host.emit(tid, "error", {"error": str(e)})
                await host.set_state(t, "failed")
                return
            cut = host.turn_file(tid).exists()
            asked = bool(b.get("request") or b.get("images"))
            await host.emit(tid, "prepared", {"resumed": cut, "idle": not cut and not asked})
            if cut:  # the request, if any, is the cut turn's: the thread has it already
                await host.emit(tid, "message_in", {"from": "runner", "body": RESUME, "images": []})
                await host._turn(t, f"[from runner] {RESUME}")
            elif asked:
                await host.emit(tid, "message_in", {"from": "user", "body": b.get("request") or "",
                                                    "images": b.get("images") or []})
                await host._turn(t, b.get("request") or "", b.get("images") or [])
            else:  # resumed: idle until its next message (runner/API.md, "Resuming a thread")
                await host.set_state(t, "idle")

        asyncio.create_task(prepare())
        return JSONResponse(host.entry(t), 201)

    async def message(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        t = host.threads.get(req.path_params["tid"])
        if t is None:
            return JSONResponse({"error": "no such thread"}, 404)
        if t["state"] in ENDED:
            return JSONResponse({"error": f"thread {t['id']} has ended ({t['state']})"}, 409)
        b = await req.json()
        if b.get("objectives"):
            t["objectives"] = t["objectives"] + b["objectives"]
            await host.emit(t["id"], "objectives", {"objectives": t["objectives"]})
        await host.emit(t["id"], "message_in", {"from": b.get("from", "user"), "body": b["body"],
                                                "images": b.get("images") or []})
        asyncio.create_task(host.turn(t, b["body"], b.get("images") or []))
        return JSONResponse({"ok": True}, 202)

    async def stop(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        t = host.threads.get(req.path_params["tid"])
        if t is None:
            return JSONResponse({"error": "no such thread"}, 404)
        host.turn_file(t["id"]).unlink(missing_ok=True)
        await host.set_state(t, "stopped")
        return JSONResponse({"ok": True})

    def file_of(req: Request):
        """(thread, path in its workspace), or an error response."""
        t = host.threads.get(req.path_params["tid"])
        if t is None:
            return None, JSONResponse({"error": "no such thread"}, 404)
        try:
            p = host.inside(t, req.query_params.get("path", ""))
        except ValueError as e:
            return None, JSONResponse({"error": str(e)}, 400)
        if not p.exists():
            return None, JSONResponse({"error": "no such file or directory"}, 404)
        return p, None

    async def list_files(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        p, err = file_of(req)
        if err:
            return err
        if not p.is_dir():
            return JSONResponse({"error": "not a directory"}, 400)
        root = (host.workdir / req.path_params["tid"]).resolve()
        entries = sorted(({"name": c.name, "dir": c.is_dir(), "size": 0 if c.is_dir() else c.stat().st_size}
                          for c in p.iterdir() if c.name != ".git"), key=lambda e: (not e["dir"], e["name"]))
        rel = p.relative_to(root).as_posix()
        return JSONResponse({"path": "" if rel == "." else rel, "entries": entries})

    async def raw_file(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        p, err = file_of(req)
        if err:
            return err
        if not p.is_file():
            return JSONResponse({"error": "not a file"}, 400)
        return FileResponse(p, media_type="application/octet-stream")

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

    async def get_repo(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        try:
            repo = await host.repo(req.path_params["project"])
        except GitError as e:
            return JSONResponse({"error": str(e)}, 400)
        return JSONResponse({"base": repo.base, "refs": await repo.refs()})

    async def post_repo(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        try:
            repo = await host.repo(req.path_params["project"])
            refs = json.loads(req.headers.get("x-git-refs") or "{}")
            base = req.headers.get("x-git-base") or ""
            if not base or not isinstance(refs, dict):
                raise ValueError("x-git-base and x-git-refs (a JSON object) are required")
            await repo.receive(await req.body(), base, refs)
        except (ValueError, GitError) as e:
            return JSONResponse({"error": str(e)}, 400)
        return JSONResponse({"ok": True})

    async def thread_repo(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        tid = req.path_params["tid"]
        if not THREAD_ID.match(tid):
            return JSONResponse({"error": "bad thread id"}, 400)
        try:
            repo = await host.repo(req.path_params["project"])
        except GitError as e:
            return JSONResponse({"error": str(e)}, 400)
        try:
            data, refs = await repo.send(tid, [h for h in req.query_params.get("have", "").split(",") if h])
        except GitError as e:
            return JSONResponse({"error": str(e)}, 404)
        return Response(data, media_type="application/octet-stream", headers={"x-git-refs": refs_header(refs)})

    async def prune(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        keep = (await req.json()).get("keep")
        if not isinstance(keep, list):
            return JSONResponse({"error": "keep (a list of project ids) is required"}, 400)
        return JSONResponse({"removed": await host.prune(set(keep))})

    async def peer_calls(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        wait = min(float(req.query_params.get("wait") or 25), 55.0)
        return JSONResponse(await long_poll(host.take_calls(wait), req))

    async def peer_answer(req: Request):
        if not authed(req):
            return JSONResponse({"error": "unauthorized"}, 401)
        fut = host.waiting.get(req.path_params["cid"])
        if fut is None or fut.done():
            return JSONResponse({"error": "no such call"}, 404)
        fut.set_result(await req.json())
        return JSONResponse({"ok": True})

    return Starlette(routes=[
        Route("/health", health),
        Route("/peer/calls", peer_calls, methods=["GET"]),
        Route("/peer/calls/{cid}", peer_answer, methods=["POST"]),
        Route("/threads", list_threads, methods=["GET"]),
        Route("/threads", create_thread, methods=["POST"]),
        Route("/threads/{tid}", get_thread, methods=["GET"]),
        Route("/threads/{tid}/messages", message, methods=["POST"]),
        Route("/threads/{tid}/stop", stop, methods=["POST"]),
        Route("/threads/{tid}/events", thread_events),
        Route("/threads/{tid}/files", list_files),
        Route("/threads/{tid}/files/raw", raw_file),
        Route("/projects/prune", prune, methods=["POST"]),
        Route("/projects/{project}/threads/{tid}/repo", thread_repo),
        Route("/projects/{project}/repo", get_repo, methods=["GET"]),
        Route("/projects/{project}/repo", post_repo, methods=["POST"]),
        Route("/events", all_events),
    ])


def watch_parent(every: float = 1.0) -> None:
    """Exit once the process that started us is gone.

    Nothing else knows a devrunner's port, so with its control plane gone it is
    only a listener nobody will call: it stands for a worker that cannot be
    reached. A control plane that stops its workers (the app's lifespan, or
    LocalProvider.close) leaves nothing behind, but one that is killed without
    the chance -- a test process that dies, a window killed mid-start -- used to
    leave its runner listening for as long as the machine was up. Being
    reparented is how a survivor knows: the pid we started under is no longer
    our parent's.
    """
    started_with = os.getppid()
    while True:
        time.sleep(every)
        if os.getppid() != started_with:
            os.kill(os.getpid(), signal.SIGTERM)  # uvicorn's graceful shutdown
            return


def claim_workdir(workdir: str) -> int:
    """Hold the workdir's runner lock while this process lives, as the real
    runner does (runner/src/runner/thread.py, claim_workdir): one runner per
    workdir, and LocalProvider stops the holder before it starts another."""
    import fcntl

    Path(workdir).mkdir(parents=True, exist_ok=True)
    fd = os.open(Path(workdir) / ".runner.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit(f"devrunner: another runner (pid {os.read(fd, 32).decode().strip()}) serves {workdir}")
    os.ftruncate(fd, 0)
    os.write(fd, f"{os.getpid()}\n".encode())
    return fd


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=int(os.environ.get("RUNNER_PORT", "8080")))
    a = p.parse_args()
    workdir = os.environ.get("RUNNER_WORKDIR") or tempfile.mkdtemp(prefix="devrunner-")
    claim_workdir(workdir)
    host = Host(int(os.environ.get("RUNNER_CAPACITY", "16")), float(os.environ.get("DEVRUNNER_DELAY", "0.3")),
                workdir)
    print(f"devrunner: echo runner on port {a.port}", file=sys.stderr, flush=True)
    # /events streams stay open for subscribers (the control plane's relay), and
    # so does the peer long poll, so bound the wait for them; otherwise Ctrl+C
    # never completes (the real runner, runner/server.py).
    threading.Thread(target=watch_parent, name="parent-watch", daemon=True).start()
    uvicorn.run(create_app(host, os.environ.get("RUNNER_TOKEN", "")),
                host=os.environ.get("RUNNER_HOST", "127.0.0.1"), port=a.port, log_level="warning",
                timeout_graceful_shutdown=int(os.environ.get("RUNNER_GRACEFUL_SHUTDOWN", "5")))


if __name__ == "__main__":
    main()
