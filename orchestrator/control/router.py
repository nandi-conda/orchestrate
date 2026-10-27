"""The router: decide where a request typed in a project's main window goes.

A plan spawns new threads, sends messages to existing ones, or both:

    {"spawn": [{"title": "...", "request": "...", "workers": "sprites"}],
     "send":  [{"thread": "t_42", "message": "...", "workers": "local"}],
     "why": "..."}

The router also decides where each thread's workers run (providers.PLACES):
on Sprites unless the request asks to run on the person's own computer
("run it locally"). `workers` on a send moves that thread there. A thread
spawned without one (a new thread typed straight in, or one the plan left
it off) is asked about with `place` when it is first put on a worker.

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

PLACES = """Each thread's work runs on workers in one of `places`: "sprites" (cloud
machines) unless the person asks for their own computer ("run locally", "on my
machine", "on this computer", "don't use sprites"), then "local". A place with a
non-empty `why` is unavailable: never pick it; pick the other one. Each thread
has its `workers` in the directory. Give every spawned thread "workers". Give a
send "workers" only when the request asks to move that thread to the other place.
"""

REPLY_FORMAT = """Reply with JSON only:
{"spawn": [{"title": "<3-6 word title>", "request": "<the task for that thread>", "workers": "sprites|local"}],
 "send": [{"thread": "<existing thread id>", "message": "<what to tell it>", "workers": "<only to move it>"}],
 "why": "<one sentence>"}"""

PLACE = """A request in a software project starts an agent thread. Decide where its
workers run, one of `places`: "sprites" (cloud machines) unless the request asks
for the person's own computer ("run locally", "on my machine", "on this computer",
"don't use sprites"), then "local". A place with a non-empty `why` is unavailable:
never pick it. Reply with JSON only: {"workers": "sprites|local"}"""

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
""" + PLACES + """
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
""" + PLACES + """
""" + REPLY_FORMAT


class Router(Protocol):
    async def plan(self, request: str, directory: list[dict], places: list[dict]) -> dict:
        """Plan a request; `places` is where workers can run (Places.offered)."""

    async def revise(self, request: str, previous: str, reached: list[str], directory: list[dict],
                     places: list[dict]) -> dict:
        """Plan an edited request; `reached` are the threads the earlier text reached."""

    async def place(self, request: str, places: list[dict]) -> str:
        """Where a new thread with this request runs: an available place's id."""


def title_for(text: str) -> str:
    words = re.sub(r"\s+", " ", text).strip().split(" ")
    title = " ".join(words[:6])
    return (title[:60] + "…") if len(title) > 60 else (title or "New thread")


def available(places: list[dict]) -> set[str]:
    return {p["id"] for p in places if not p["why"]}


def clean_plan(plan: dict, directory: list[dict], places: list[dict]) -> dict:
    """Keep only well-formed entries that point at real threads, each with
    `workers` an available place, or "" (spawn: asked with `place` when it
    is first put on a worker; send: not moved)."""
    known, can = {t["id"] for t in directory}, available(places)

    def where(s: dict) -> str:
        return s["workers"] if s.get("workers") in can else ""
    spawn = [{"title": str(s.get("title") or title_for(str(s.get("request", ""))))[:80],
              "request": str(s["request"]), "workers": where(s)}
             for s in plan.get("spawn") or [] if isinstance(s, dict) and s.get("request")]
    send = [{"thread": s["thread"], "message": str(s["message"]), "workers": where(s)}
            for s in plan.get("send") or []
            if isinstance(s, dict) and s.get("thread") in known and s.get("message")]
    return {"spawn": spawn, "send": send, "why": str(plan.get("why", ""))}


class ModelRouter:
    def __init__(self, api_key: str, model: str = "deepseek-flash",
                 base_url: str = "https://api.deepseek.com") -> None:
        self.model = model
        self.http = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=30.0,
                                      headers={"authorization": f"Bearer {api_key}"})

    async def _json(self, system: str, content: dict) -> dict:
        r = await self.http.post("/chat/completions", json={
            "model": self.model,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "system", "content": system},
                         {"role": "user", "content": json.dumps(content)}],
        })
        r.raise_for_status()
        return json.loads(r.json()["choices"][0]["message"]["content"])

    async def _ask(self, system: str, content: dict, directory: list[dict], places: list[dict]) -> dict:
        threads = [{k: t.get(k) for k in ("id", "title", "state", "summary", "files", "load", "workers")}
                   for t in directory]
        got = await self._json(system, {"threads": threads, "places": places, **content})
        return clean_plan(got, directory, places)

    async def plan(self, request: str, directory: list[dict], places: list[dict]) -> dict:
        plan = await self._ask(SYSTEM, {"request": request}, directory, places)
        if not plan["spawn"] and not plan["send"]:
            raise ValueError(f"the router model ({self.model}) planned nothing for the request")
        return plan

    async def revise(self, request: str, previous: str, reached: list[str], directory: list[dict],
                     places: list[dict]) -> dict:
        return await self._ask(REVISE, {"previous": previous, "request": request, "reached": reached},
                               directory, places)

    async def place(self, request: str, places: list[dict]) -> str:
        got = (await self._json(PLACE, {"request": request, "places": places})).get("workers")
        if got not in available(places):
            raise ValueError(f"the router model ({self.model}) picked no place workers can run in: {got!r}")
        return got


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
