"""The router: decide where a request typed in a project's main window goes.

A plan spawns new threads, sends messages to existing ones, answers the
person itself, or any of these:

    {"spawn": [{"title": "...", "request": "...", "workers": "sprites", "where": "..."}],
     "send":  [{"thread": "t_42", "message": "...", "kind": "steer", "workers": "local", "where": "..."}],
     "answer": "...",
     "why": "..."}

A quick question needs no thread: when what the router is given (the
threads, their state, summaries, files and load, the places) or what any
engineer knows answers it, the router answers it in `answer` (markdown, shown
in the project's chat), and starts no worker for it. A question about the
project's code or files, or one that needs the internet, a command or any work
done, goes to a thread like any other request.

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
context, each thread with its load (load.py). A thread does one job at a
time: each send is "steer" (about the job the thread is on: a correction,
guidance, something it needs; a busy thread takes it mid-turn) or "work" (a
new job: only an idle thread takes it; the control plane gives work for a
busy one to a new thread). `kind` decides the same for a message sent
straight to a busy thread, by the person or by another thread. It leans toward spawning. The router
answers a quick question itself (`answer`). A request it cannot plan (the model unreachable or refusing the call, an answer that is
not one call of that function, a plan that neither answers nor does
anything) is an error the
project shows, with what the model said; there is no
model-less router outside the tests (tests/doubles.py).
"""

from __future__ import annotations

import json
import re
from typing import Protocol

import httpx

LOAD = """Each thread has a `load` (how much it already has on its plate):
`level` is "idle" (between turns: it takes a new job at once), "busy" (in a
turn) or "stalled" (its worker is not answering); `busy_for` is
seconds into its current turn. A thread does one job at a time and work never
waits. Each send has a `kind`: "steer" is about the job the thread is on now (a
correction, guidance, an answer it needs), and a thread in a turn takes it at
once, mid-turn; "work" is a new job, even one that follows on from its work,
and only an idle thread can take it: new work for any other thread goes to a
new thread (spawn it).
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
existing thread's work (that thread busy or stalled, say) runs where
that thread runs: its `workers`, unless its work needs the other place.
A send's "workers" is "" unless the request means that thread's work now needs
the other place (its "where" says why; "" when it stays).
"""

ANSWER = """A quick question needs no thread. When the request is a question that what you
are given here (the threads, their state, summaries, files and load, the
places) or general engineering knowledge answers in full, answer it yourself in
"answer", in markdown, short and direct, and spawn nothing for it. Anything you
would have to look up in the project's code or files, on the internet, by
running something, or that asks for work done goes to a thread, and "answer" is
"" unless another part of the request is such a question. Never guess at what a
thread or the code does beyond what you are given. `images` says how many
images came with the request: you cannot see them, so a question about them
goes to a thread.
"""

REPLY_FORMAT = """Answer by calling `route` once: `spawn` lists the new threads (each with a
3-6 word title, its task, its workers and where), `send` the messages to existing
threads, `answer` your own answer to the person ("" when none), and `why` says
in one sentence why."""

PLACE = """A request in a software project starts an agent thread. Decide where its
workers run.
""" + WHERE + """Answer by calling `place` once, with "workers" and "where"."""

WHERE_FIELD = "one sentence: what in its work decided where it runs"
KIND_FIELD = "steer: about the job it is on now; work: a new job"

KIND = """A message was sent to an agent thread that is in a turn, working on a job.
A thread does one job at a time: it can be steered, but not given more work.
Decide whether the message steers the job the thread is on now ("steer": a
correction, guidance, a change to what it was asked, an answer or result it is
waiting for, something it needs to finish) or is a new job ("work": anything it
would do after its current job, even work that follows on from it). New work
goes to a new thread. `thread` is the busy thread (its title, the request that
started it, its summary); `message` is what was sent and `from` who sent it.
Answer by calling `kind` once."""

SYSTEM = """You route requests in a software project to agent threads.
Each thread is a long-lived agent working in its own git worktree.
Threads are cheap: prefer spawning a new thread. Reuse a thread only when the
request clearly continues that thread's work and the thread is idle. Split a
request with several independent parts into several new threads. A request can
also spawn threads and notify existing threads whose work it touches.
""" + ANSWER + LOAD + PLACES + """
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
""" + ANSWER + LOAD + PLACES + """
""" + REPLY_FORMAT


class Router(Protocol):
    async def plan(self, request: str, directory: list[dict], places: list[dict], images: int = 0) -> dict:
        """Plan a request; `places` is where workers can run (Places.offered),
        `images` how many images came with it."""

    async def revise(self, request: str, previous: str, reached: list[str], directory: list[dict],
                     places: list[dict]) -> dict:
        """Plan an edited request; `reached` are the threads the earlier text reached."""

    async def place(self, request: str, places: list[dict]) -> dict:
        """Where a new thread with this request runs: {"workers": an
        available place's id, "where": why there}."""

    async def kind(self, message: str, sender: str, thread: dict) -> dict:
        """Whether a message to a busy thread steers its job or is new work:
        {"kind": "steer" | "work", "why"}."""


def title_for(text: str) -> str:
    words = re.sub(r"\s+", " ", text).strip().split(" ")
    title = " ".join(words[:6])
    return (title[:60] + "…") if len(title) > 60 else (title or "New thread")


def available(places: list[dict]) -> set[str]:
    return {p["id"] for p in places if not p["why"]}


KINDS = ("steer", "work")


def clean_plan(plan: dict, directory: list[dict], places: list[dict]) -> dict:
    """Keep only well-formed entries that point at real threads, each with
    `workers` an available place, or "" (spawn: asked with `place` when it
    is first put on a worker; send: not moved), and `where` why there. A
    send's `kind` is "steer" or else "work". `answer` is the router's own
    answer to the person, "" when it gave none."""
    known, can = {t["id"] for t in directory}, available(places)

    def placed(s: dict) -> dict:
        workers = s["workers"] if s.get("workers") in can else ""
        return {"workers": workers, "where": str(s.get("where") or "") if workers else ""}
    spawn = [{"title": str(s.get("title") or title_for(str(s.get("request", ""))))[:80],
              "request": str(s["request"]), **placed(s)}
             for s in plan.get("spawn") or [] if isinstance(s, dict) and s.get("request")]
    send = [{"thread": s["thread"], "message": str(s["message"]),
             "kind": "steer" if s.get("kind") == "steer" else "work", **placed(s)}
            for s in plan.get("send") or []
            if isinstance(s, dict) and s.get("thread") in known and s.get("message")]
    return {"spawn": spawn, "send": send, "answer": str(plan.get("answer") or "").strip(),
            "why": str(plan.get("why", ""))}


def plan_tool(directory: list[dict], places: list[dict]) -> dict:
    """The `route` function the model must call: its schema is the plan's, so
    the answer has the plan's shape (strict mode). A spawn's `workers` is an
    available place; a send's is one, or "" to leave the thread where it is.
    `send` is offered only when there are threads to send to, and names
    one of them, with its `kind`. `where` is why a thread runs (or moves) where it does."""
    can = sorted(available(places))
    # Nowhere to run: the thread is refused when it is first put on a worker.
    spawn = {"type": "object", "additionalProperties": False,
             "required": ["title", "request", "workers", "where"],
             "properties": {"title": {"type": "string", "description": "3-6 word title"},
                            "request": {"type": "string", "description": "the task for that thread"},
                            "workers": {"type": "string", "enum": can or [""]},
                            "where": {"type": "string", "description": WHERE_FIELD}}}
    props = {"spawn": {"type": "array", "items": spawn},
             "answer": {"type": "string",
                        "description": "markdown answer to a quick question you can answer from what you are "
                                       "given; \"\" when a thread must do it"},
             "why": {"type": "string", "description": "one sentence"}}
    if directory:
        props["send"] = {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "required": ["thread", "message", "kind", "workers", "where"],
            "properties": {"thread": {"type": "string", "enum": [t["id"] for t in directory]},
                           "message": {"type": "string", "description": "what to tell it"},
                           "kind": {"type": "string", "enum": list(KINDS), "description": KIND_FIELD},
                           "workers": {"type": "string", "enum": [""] + can,
                                       "description": "\"\" unless its work now needs the other place"},
                           "where": {"type": "string", "description": "why it moves; \"\" when it stays"}}}}
    return {"name": "route", "description": "Route the request: the threads to spawn and the messages to send.",
            "parameters": {"type": "object", "additionalProperties": False,
                           "required": list(props), "properties": props}}


def kind_tool() -> dict:
    return {"name": "kind", "description": "Say whether the message steers the thread's current job or is new work.",
            "parameters": {"type": "object", "additionalProperties": False, "required": ["kind", "why"],
                           "properties": {"kind": {"type": "string", "enum": list(KINDS), "description": KIND_FIELD},
                                          "why": {"type": "string", "description": "one sentence"}}}}


def place_tool(places: list[dict]) -> dict:
    return {"name": "place", "description": "Say where the new thread's workers run, and why.",
            "parameters": {"type": "object", "additionalProperties": False, "required": ["workers", "where"],
                           "properties": {"workers": {"type": "string",
                                                      "enum": sorted(available(places)) or [""]},
                                          "where": {"type": "string", "description": WHERE_FIELD}}}}


# How a turn that made its call whole ends: "tool_calls", or "stop" for a
# forced tool_choice (OpenAI-compatible servers report either).
FINISHED = ("tool_calls", "stop")


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
        said = json.dumps(choice["message"])[:500]
        # The call is whole only when the model ended its turn: anything else
        # (length, content_filter, insufficient_system_resource, aborted) cut it
        # off wherever it was, mid-string included.
        if choice.get("finish_reason") not in FINISHED:
            raise ValueError(f"the router model ({self.model}) stopped before finishing its {tool['name']} call "
                             f"(finish_reason {choice.get('finish_reason')!r}): {said}")
        if len(calls) != 1 or calls[0]["function"]["name"] != tool["name"]:
            raise ValueError(f"the router model ({self.model}) did not call {tool['name']} once "
                             f"(finish_reason {choice.get('finish_reason')!r}): {said}")
        arguments = calls[0]["function"]["arguments"]
        try:
            return json.loads(arguments)
        except json.JSONDecodeError as e:
            raise ValueError(f"the router model ({self.model}) called {tool['name']} with arguments that are "
                             f"not JSON ({e}; finish_reason {choice.get('finish_reason')!r}): {arguments[:500]}") from None

    async def _ask(self, system: str, content: dict, directory: list[dict], places: list[dict]) -> dict:
        threads = [{k: t.get(k) for k in ("id", "title", "state", "summary", "files", "load", "workers")}
                   for t in directory]
        got = await self._call(system, {"threads": threads, "places": places, **content},
                               plan_tool(directory, places))
        return clean_plan(got, directory, places)

    async def plan(self, request: str, directory: list[dict], places: list[dict], images: int = 0) -> dict:
        plan = await self._ask(SYSTEM, {"request": request, "images": images}, directory, places)
        if not plan["spawn"] and not plan["send"] and not plan["answer"]:
            raise ValueError(f"the router model ({self.model}) planned nothing for the request: {plan['why']!r}")
        return plan

    async def revise(self, request: str, previous: str, reached: list[str], directory: list[dict],
                     places: list[dict]) -> dict:
        return await self._ask(REVISE, {"previous": previous, "request": request, "reached": reached},
                               directory, places)

    async def kind(self, message: str, sender: str, thread: dict) -> dict:
        about = {k: thread.get(k) for k in ("id", "title", "request", "summary")}
        got = await self._call(KIND, {"thread": about, "message": message, "from": sender}, kind_tool())
        if got.get("kind") not in KINDS:
            raise ValueError(f"the router model ({self.model}) said neither steer nor work: {got.get('kind')!r}")
        return {"kind": got["kind"], "why": str(got.get("why") or "")}

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
