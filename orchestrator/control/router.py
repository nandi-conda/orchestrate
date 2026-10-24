"""The router: decide where a request typed in a project's main window goes.

A plan spawns new threads, sends messages to existing ones, or both:

    {"spawn": [{"title": "...", "request": "..."}],
     "send":  [{"thread": "t_42", "message": "..."}],
     "why": "..."}

`ModelRouter` asks a fast OpenAI-compatible model (deepseek-flash by default)
with the thread directory as context. `HeuristicRouter` needs no model and is
used when no key is set, or when the model's answer can't be used.
Both lean toward spawning.
"""

from __future__ import annotations

import json
import os
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


class Router(Protocol):
    async def plan(self, request: str, directory: list[dict]) -> dict: ...


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

    async def plan(self, request: str, directory: list[dict]) -> dict:
        threads = [{k: t.get(k) for k in ("id", "title", "state", "summary", "files")} for t in directory]
        try:
            r = await self.http.post("/chat/completions", json={
                "model": self.model,
                "response_format": {"type": "json_object"},
                "messages": [{"role": "system", "content": SYSTEM},
                             {"role": "user", "content": json.dumps({"threads": threads, "request": request})}],
            })
            r.raise_for_status()
            plan = clean_plan(json.loads(r.json()["choices"][0]["message"]["content"]), directory)
        except (httpx.HTTPError, KeyError, ValueError, TypeError) as e:
            plan = await self.fallback.plan(request, directory)
            plan["why"] += f" (router model unavailable: {type(e).__name__})"
            return plan
        if not plan["spawn"] and not plan["send"]:
            return await self.fallback.plan(request, directory)
        return plan


def router_from_env() -> Router:
    from .secrets import get_secret

    key = get_secret("DEEPSEEK_API_KEY")
    if not key:
        return HeuristicRouter()
    return ModelRouter(key, os.environ.get("ROUTER_MODEL", "deepseek-flash"),
                       os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com"))
