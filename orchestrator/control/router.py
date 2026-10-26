"""The router: decide where a request typed in a project's main window goes.

A plan spawns new threads, sends messages to existing ones, or both:

    {"spawn": [{"title": "...", "request": "..."}],
     "send":  [{"thread": "t_42", "message": "..."}],
     "why": "..."}

When the person edits a request they sent, `revise` plans the edited text.
Every thread the request already reached gets the edit as a correction from
the person (the control plane sends it), so a revision plans only what those
corrections leave out: threads for parts none of them covers, and notes to
threads about what changed for them. An empty revision is a fine answer.

`ModelRouter` asks a fast OpenAI-compatible model (deepseek-flash by default)
with the thread directory as context, each thread with its load (load.py), so
busy and backed-up threads get no new work: it goes to a new thread.
It leans toward spawning. A request it cannot plan (the model unreachable,
an answer that is not a plan) is an error the project shows; there is no
model-less router outside the tests (tests/doubles.py).
"""

from __future__ import annotations

import json
import re
from typing import Protocol

import httpx

LOAD = """Each thread has a `load` (how much it already has on its plate):
`level` is "idle" (between turns, nothing waiting: it takes work at once),
"busy" (in a turn), "backed_up" (`backlog` messages it has not got to are
waiting on it) or "stalled" (its worker is not answering, so anything sent
waits); `busy_for` is seconds into its current turn.
"""

REPLY_FORMAT = """Reply with JSON only:
{"spawn": [{"title": "<3-6 word title>", "request": "<the task for that thread>"}],
 "send": [{"thread": "<existing thread id>", "message": "<what to tell it>"}],
 "why": "<one sentence>"}"""

SYSTEM = """You route requests in a software project to agent threads.
Each thread is a long-lived agent working in its own git worktree.
Threads are cheap: prefer spawning a new thread. Reuse a thread only when the
request clearly continues that thread's work and the thread is idle. Split a
request with several independent parts into several new threads. A request can
also spawn threads and notify existing threads whose work it touches.
""" + LOAD + """Send a busy thread a message only when it is about the work it is doing now
(a correction, something it needs); new work, even work that follows on from
it, goes to a new thread instead. Send a backed_up or stalled thread nothing
it could do without: its messages wait behind the rest.

""" + REPLY_FORMAT


REVISE = """You route requests in a software project to agent threads.
Each thread is a long-lived agent working in its own git worktree.
The person edited a request they had sent. `previous` is what it said,
`request` is what it says now, and `reached` lists the threads already working
from it. Each of those threads is sent the edit as a correction and decides
itself what to keep, drop or undo, so plan only what the corrections leave out:
spawn a thread for each part of the new text that no reached thread covers, and
send a reached or other thread a message only to say what changed for it (for
example that a part it had now has its own thread). An empty plan is a fine
answer when the corrections cover the edit.
""" + LOAD + """Send a busy thread only what concerns the work it is doing now, and a
backed_up or stalled thread nothing it could do without.

""" + REPLY_FORMAT


class Router(Protocol):
    async def plan(self, request: str, directory: list[dict]) -> dict: ...

    async def revise(self, request: str, previous: str, reached: list[str], directory: list[dict]) -> dict:
        """Plan an edited request; `reached` are the threads the earlier text reached."""


def title_for(text: str) -> str:
    words = re.sub(r"\s+", " ", text).strip().split(" ")
    title = " ".join(words[:6])
    return (title[:60] + "…") if len(title) > 60 else (title or "New thread")


def clean_plan(plan: dict, directory: list[dict]) -> dict:
    """Keep only well-formed entries that point at real threads."""
    known = {t["id"] for t in directory}
    spawn = [{"title": str(s.get("title") or title_for(str(s.get("request", ""))))[:80],
              "request": str(s["request"])}
             for s in plan.get("spawn") or [] if isinstance(s, dict) and s.get("request")]
    send = [{"thread": s["thread"], "message": str(s["message"])}
            for s in plan.get("send") or []
            if isinstance(s, dict) and s.get("thread") in known and s.get("message")]
    return {"spawn": spawn, "send": send, "why": str(plan.get("why", ""))}


class ModelRouter:
    def __init__(self, api_key: str, model: str = "deepseek-flash",
                 base_url: str = "https://api.deepseek.com") -> None:
        self.model = model
        self.http = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=30.0,
                                      headers={"authorization": f"Bearer {api_key}"})

    async def _ask(self, system: str, content: dict, directory: list[dict]) -> dict:
        threads = [{k: t.get(k) for k in ("id", "title", "state", "summary", "files", "load")} for t in directory]
        r = await self.http.post("/chat/completions", json={
            "model": self.model,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "system", "content": system},
                         {"role": "user", "content": json.dumps({"threads": threads, **content})}],
        })
        r.raise_for_status()
        return clean_plan(json.loads(r.json()["choices"][0]["message"]["content"]), directory)

    async def plan(self, request: str, directory: list[dict]) -> dict:
        plan = await self._ask(SYSTEM, {"request": request}, directory)
        if not plan["spawn"] and not plan["send"]:
            raise ValueError(f"the router model ({self.model}) planned nothing for the request")
        return plan

    async def revise(self, request: str, previous: str, reached: list[str], directory: list[dict]) -> dict:
        return await self._ask(REVISE, {"previous": previous, "request": request, "reached": reached},
                               directory)


ROUTER_MODEL = "deepseek-flash"
MODEL_BASE_URL = "https://api.deepseek.com"


def default_router() -> Router:
    """The router asks the model; the control plane does not start without
    DEEPSEEK_API_KEY (control/__main__.py, desktop/__main__.py, the Android
    app's Keys screen)."""
    from .secrets import get_secret, missing_message

    key = get_secret("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError(missing_message(["DEEPSEEK_API_KEY"]))
    return ModelRouter(key, ROUTER_MODEL, MODEL_BASE_URL)
