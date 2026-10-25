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
with the thread directory as context. `HeuristicRouter` needs no model and is
used when no key is set, or when the model's answer can't be used.
Both lean toward spawning.
"""

from __future__ import annotations

import json
import re
from typing import Protocol

import httpx

SYSTEM = """You route requests in a software project to agent threads.
Each thread is a long-lived agent working in its own git worktree.
Threads are cheap: prefer spawning a new thread. Reuse a thread only when the
request clearly continues that thread's work. Split a request with several
independent parts into several new threads. A request can also spawn threads
and notify existing threads whose work it touches.

Reply with JSON only:
{"spawn": [{"title": "<3-6 word title>", "request": "<the task for that thread>"}],
 "send": [{"thread": "<existing thread id>", "message": "<what to tell it>"}],
 "why": "<one sentence>"}"""


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

Reply with JSON only:
{"spawn": [{"title": "<3-6 word title>", "request": "<the task for that thread>"}],
 "send": [{"thread": "<existing thread id>", "message": "<what to tell it>"}],
 "why": "<one sentence>"}"""


class Router(Protocol):
    async def plan(self, request: str, directory: list[dict]) -> dict: ...

    async def revise(self, request: str, previous: str, reached: list[str], directory: list[dict]) -> dict:
        """Plan an edited request; `reached` are the threads the earlier text reached."""


def title_for(text: str) -> str:
    words = re.sub(r"\s+", " ", text).strip().split(" ")
    title = " ".join(words[:6])
    return (title[:60] + "…") if len(title) > 60 else (title or "New thread")


ITEM = re.compile(r"^\s*(?:\d+[.)]|[-*•])\s+(.*\S)\s*$")
MENTION = re.compile(r"(?<!\w)@(t_[0-9a-zA-Z]+)")


class HeuristicRouter:
    """No model: `@t_xxx` sends to that thread, a bulleted or numbered list
    fans out one thread per item, anything else spawns one thread."""

    async def plan(self, request: str, directory: list[dict]) -> dict:
        known = {t["id"] for t in directory}
        targets = [m for m in MENTION.findall(request) if m in known]
        if targets:
            body = MENTION.sub("", request).strip()
            return {"spawn": [], "send": [{"thread": t, "message": body} for t in targets],
                    "why": "the request names existing threads"}
        items = [m.group(1) for line in request.splitlines() if (m := ITEM.match(line))]
        if len(items) >= 2:
            return {"spawn": [{"title": title_for(i), "request": i} for i in items], "send": [],
                    "why": f"{len(items)} independent items, one thread each"}
        return {"spawn": [{"title": title_for(request), "request": request}], "send": [],
                "why": "new work gets a new thread"}


    async def revise(self, request: str, previous: str, reached: list[str], directory: list[dict]) -> dict:
        """Items of a list that are new in the edit get a thread each; the
        rest is left to the corrections. An edit that reached no thread is
        planned like a new request."""
        if not reached:
            return await self.plan(request, directory)
        old = {m.group(1) for line in previous.splitlines() if (m := ITEM.match(line))}
        new = [m.group(1) for line in request.splitlines() if (m := ITEM.match(line)) and m.group(1) not in old]
        if new:
            return {"spawn": [{"title": title_for(i), "request": i} for i in new], "send": [],
                    "why": f"{len(new)} new item{'s' if len(new) > 1 else ''} in the edit, one thread each"}
        return {"spawn": [], "send": [], "why": "the threads on it take the edit as a correction"}


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
                 base_url: str = "https://api.deepseek.com", fallback: Router | None = None) -> None:
        self.model = model
        self.http = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=30.0,
                                      headers={"authorization": f"Bearer {api_key}"})
        self.fallback = fallback or HeuristicRouter()

    async def _ask(self, system: str, content: dict, directory: list[dict]) -> dict:
        threads = [{k: t.get(k) for k in ("id", "title", "state", "summary", "files")} for t in directory]
        r = await self.http.post("/chat/completions", json={
            "model": self.model,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "system", "content": system},
                         {"role": "user", "content": json.dumps({"threads": threads, **content})}],
        })
        r.raise_for_status()
        return clean_plan(json.loads(r.json()["choices"][0]["message"]["content"]), directory)

    async def plan(self, request: str, directory: list[dict]) -> dict:
        try:
            plan = await self._ask(SYSTEM, {"request": request}, directory)
        except (httpx.HTTPError, KeyError, ValueError, TypeError) as e:
            plan = await self.fallback.plan(request, directory)
            plan["why"] += f" (router model unavailable: {type(e).__name__})"
            return plan
        if not plan["spawn"] and not plan["send"]:
            return await self.fallback.plan(request, directory)
        return plan

    async def revise(self, request: str, previous: str, reached: list[str], directory: list[dict]) -> dict:
        try:
            return await self._ask(REVISE, {"previous": previous, "request": request, "reached": reached},
                                   directory)
        except (httpx.HTTPError, KeyError, ValueError, TypeError) as e:
            plan = await self.fallback.revise(request, previous, reached, directory)
            plan["why"] += f" (router model unavailable: {type(e).__name__})"
            return plan


ROUTER_MODEL = "deepseek-flash"
MODEL_BASE_URL = "https://api.deepseek.com"


def default_router() -> Router:
    """The router asks a model when the DeepSeek key is stored, and decides by
    rule when it is not, so the UI works without credentials."""
    from .secrets import get_secret

    key = get_secret("DEEPSEEK_API_KEY")
    return ModelRouter(key, ROUTER_MODEL, MODEL_BASE_URL) if key else HeuristicRouter()
