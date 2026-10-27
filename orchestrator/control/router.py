"""The router: decide where a request typed in a project's main window goes.

A plan spawns new threads, sends messages to existing ones, or both:

    {"spawn": [{"title": "...", "request": "...", "workers": "sprites", "where": "..."}],
     "send":  [{"thread": "t_42", "message": "...", "workers": "local", "where": "..."}],
     "why": "..."}

The router also decides where each thread's workers run (providers.PLACES),
from what the thread's work must touch, and says why in `where`: each place
comes with what work there `reaches` (providers.REACHES). Work on the
project's code runs on Sprites; work on the person's own computer (its
processes, apps, files outside the project, hardware, logs, services) runs
there. The person saying where decides. A thread spawned to carry on or help
with an existing thread's work runs where that thread runs. `workers` on a
send moves that thread there. A thread spawned without one (a new thread
typed straight in, or one the plan left it off) is asked about with `place`
when it is first put on a worker.

When the person edits a request they sent, `revise` plans the edited text.
Every thread the request already reached gets the edit as a correction from
the person (the control plane sends it), so a revision plans only what those
corrections leave out: threads for parts none of them covers, and notes to
threads about what changed for them. An empty revision is a fine answer.

`ModelRouter` asks a fast OpenAI-compatible model (deepseek-flash by default)
to call a function whose schema is the plan, with the thread directory as
context, each thread with its load (load.py), so busy and backed-up threads
get no new work: it goes to a new thread. It leans toward spawning. A request
it cannot plan (the model unreachable or refusing the call, an answer that is
not one call of that function, a plan that does nothing) is an error the
project shows, with what the model said; there is no
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
waiting on it) or "stalled" (its worker is not answering: a message moves it
to another worker and cuts its turn short); `busy_for` is seconds into its
current turn.
"""

WHERE = """Each thread's work runs on workers in one of `places`; `reaches` says what
work there can get at. Decide from what the work must touch to get done, not
from the words it uses. Work on the project itself (its code, tests, builds,
docs, research on the internet) runs on "sprites". Work that needs anything
only the person's own computer has runs on "local": a process or app running
there (why Chrome crashes, what is eating memory), its files outside the
project, its hardware, devices, desktop, system logs, crash dumps or services,
its local network. When the person says where ("run it locally", "on my
machine", "on sprites"), that decides. A place with a non-empty `why` is
unavailable: never pick it; pick the other one and say what the work will lack
there. Say in "where", in one sentence, what in the work decided its place.
"""

PLACES = WHERE + """Each thread has its `workers` in the directory. Give every spawned thread
"workers". A thread spawned to continue, follow on from or help with an
existing thread's work (that thread busy, backed_up or stalled, say) runs where
that thread runs: its `workers`, unless its work needs the other place.
A send's "workers" is "" unless the request means that thread's work now needs
the other place (its "where" says why; "" when it stays).
"""

REPLY_FORMAT = """Answer by calling `route` once: `spawn` lists the new threads (each with a
3-6 word title, its task, its workers and where), `send` the messages to existing
threads, and `why` says in one sentence why."""

PLACE = """A request in a software project starts an agent thread. Decide where its
workers run.
""" + WHERE + """Answer by calling `place` once, with "workers" and "where"."""

WHERE_FIELD = "one sentence: what in its work decided where it runs"

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

    async def place(self, request: str, places: list[dict]) -> dict:
        """Where a new thread with this request runs: {"workers": an
        available place's id, "where": why there}."""


def title_for(text: str) -> str:
    words = re.sub(r"\s+", " ", text).strip().split(" ")
    title = " ".join(words[:6])
    return (title[:60] + "…") if len(title) > 60 else (title or "New thread")


def available(places: list[dict]) -> set[str]:
    return {p["id"] for p in places if not p["why"]}


def clean_plan(plan: dict, directory: list[dict], places: list[dict]) -> dict:
    """Keep only well-formed entries that point at real threads, each with
    `workers` an available place, or "" (spawn: asked with `place` when it
    is first put on a worker; send: not moved), and `where` why there."""
    known, can = {t["id"] for t in directory}, available(places)

    def placed(s: dict) -> dict:
        workers = s["workers"] if s.get("workers") in can else ""
        return {"workers": workers, "where": str(s.get("where") or "") if workers else ""}
    spawn = [{"title": str(s.get("title") or title_for(str(s.get("request", ""))))[:80],
              "request": str(s["request"]), **placed(s)}
             for s in plan.get("spawn") or [] if isinstance(s, dict) and s.get("request")]
    send = [{"thread": s["thread"], "message": str(s["message"]), **placed(s)}
            for s in plan.get("send") or []
            if isinstance(s, dict) and s.get("thread") in known and s.get("message")]
    return {"spawn": spawn, "send": send, "why": str(plan.get("why", ""))}


def plan_tool(directory: list[dict], places: list[dict]) -> dict:
    """The `route` function the model must call: its schema is the plan's, so
    the answer has the plan's shape (strict mode). A spawn's `workers` is an
    available place; a send's is one, or "" to leave the thread where it is.
    `send` is offered only when there are threads to send to, and names one
    of them. `where` is why a thread runs (or moves) where it does."""
    can = sorted(available(places))
    # Nowhere to run: the thread is refused when it is first put on a worker.
    spawn = {"type": "object", "additionalProperties": False,
             "required": ["title", "request", "workers", "where"],
             "properties": {"title": {"type": "string", "description": "3-6 word title"},
                            "request": {"type": "string", "description": "the task for that thread"},
                            "workers": {"type": "string", "enum": can or [""]},
                            "where": {"type": "string", "description": WHERE_FIELD}}}
    props = {"spawn": {"type": "array", "items": spawn},
             "why": {"type": "string", "description": "one sentence"}}
    if directory:
        props["send"] = {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "required": ["thread", "message", "workers", "where"],
            "properties": {"thread": {"type": "string", "enum": [t["id"] for t in directory]},
                           "message": {"type": "string", "description": "what to tell it"},
                           "workers": {"type": "string", "enum": [""] + can,
                                       "description": "\"\" unless its work now needs the other place"},
                           "where": {"type": "string", "description": "why it moves; \"\" when it stays"}}}}
    return {"name": "route", "description": "Route the request: the threads to spawn and the messages to send.",
            "parameters": {"type": "object", "additionalProperties": False,
                           "required": list(props), "properties": props}}


def place_tool(places: list[dict]) -> dict:
    return {"name": "place", "description": "Say where the new thread's workers run, and why.",
            "parameters": {"type": "object", "additionalProperties": False, "required": ["workers", "where"],
                           "properties": {"workers": {"type": "string",
                                                      "enum": sorted(available(places)) or [""]},
                                          "where": {"type": "string", "description": WHERE_FIELD}}}}


class ModelRouter:
    """Asks the model to call one function (`route` or `place`) whose JSON
    schema is the answer's shape: a forced tool call in strict mode, which
    DeepSeek serves on its /beta base URL and only with thinking off (a
    forced tool_choice is refused in thinking mode, and routing is meant to
    be fast). The arguments of that call are the answer."""

    def __init__(self, api_key: str, model: str = "deepseek-flash",
                 base_url: str = "https://api.deepseek.com/beta") -> None:
        self.model = model
        self.http = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=30.0,
                                      headers={"authorization": f"Bearer {api_key}"})

    async def _call(self, system: str, content: dict, tool: dict) -> dict:
        r = await self.http.post("/chat/completions", json={
            "model": self.model,
            "thinking": {"type": "disabled"},
            "max_tokens": 4096,
            "tools": [{"type": "function", "function": {**tool, "strict": True}}],
            "tool_choice": {"type": "function", "function": {"name": tool["name"]}},
            "messages": [{"role": "system", "content": system},
                         {"role": "user", "content": json.dumps(content)}],
        })
        if r.is_error:
            raise RuntimeError(f"the router model ({self.model}) answered {r.status_code}: {r.text[:500]}")
        choice = r.json()["choices"][0]
        calls = choice["message"].get("tool_calls") or []
        if choice.get("finish_reason") == "length" or len(calls) != 1 \
                or calls[0]["function"]["name"] != tool["name"]:
            raise ValueError(f"the router model ({self.model}) did not call {tool['name']} once "
                             f"(finish_reason {choice.get('finish_reason')!r}): {json.dumps(choice['message'])[:500]}")
        return json.loads(calls[0]["function"]["arguments"])

    async def _ask(self, system: str, content: dict, directory: list[dict], places: list[dict]) -> dict:
        threads = [{k: t.get(k) for k in ("id", "title", "state", "summary", "files", "load", "workers")}
                   for t in directory]
        got = await self._call(system, {"threads": threads, "places": places, **content},
                               plan_tool(directory, places))
        return clean_plan(got, directory, places)

    async def plan(self, request: str, directory: list[dict], places: list[dict]) -> dict:
        plan = await self._ask(SYSTEM, {"request": request}, directory, places)
        if not plan["spawn"] and not plan["send"]:
            raise ValueError(f"the router model ({self.model}) planned nothing for the request: {plan['why']!r}")
        return plan

    async def revise(self, request: str, previous: str, reached: list[str], directory: list[dict],
                     places: list[dict]) -> dict:
        return await self._ask(REVISE, {"previous": previous, "request": request, "reached": reached},
                               directory, places)

    async def place(self, request: str, places: list[dict]) -> dict:
        got = await self._call(PLACE, {"request": request, "places": places}, place_tool(places))
        if got.get("workers") not in available(places):
            raise ValueError(f"the router model ({self.model}) picked no place workers can run in: "
                             f"{got.get('workers')!r}")
        return {"workers": got["workers"], "where": str(got.get("where") or "")}


ROUTER_MODEL = "deepseek-flash"
MODEL_BASE_URL = "https://api.deepseek.com/beta"


def default_router() -> Router:
    """The router asks the model; the control plane does not start without
    DEEPSEEK_API_KEY (control/__main__.py, desktop/__main__.py, the Android
    app's Keys screen)."""
    from .secrets import get_secret, missing_message

    key = get_secret("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError(missing_message(["DEEPSEEK_API_KEY"]))
    return ModelRouter(key, ROUTER_MODEL, MODEL_BASE_URL)
