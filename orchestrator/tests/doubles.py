"""Stand-ins for the model-backed parts, so the tests need no credentials:
the echo runner (tests/devrunner.py) and a rule-based router. The control
plane itself never uses them."""

import re
import sys

from control.providers import Places
from control.router import title_for

# LocalProvider's command for the echo runner; it runs from orchestrator/.
ECHO_RUNNER = [sys.executable, "-m", "tests.devrunner"]


ITEM = re.compile(r"^\s*(?:\d+[.)]|[-*•])\s+(.*\S)\s*$")
MENTION = re.compile(r"(?<!\w)@(t_[0-9a-zA-Z]+)")
LOCALLY = re.compile(r"\blocally\b", re.I)
ON_SPRITES = re.compile(r"\bon sprites\b", re.I)


def where(request: str, places: list[dict]) -> str:
    """"local" when the request says to run locally, "sprites" when it says
    on sprites, else "" (asked with `place` when the thread is put on a worker)."""
    can = {p["id"] for p in places if not p["why"]}
    want = "local" if LOCALLY.search(request) else "sprites" if ON_SPRITES.search(request) else ""
    return want if want in can else ""


def said(workers: str) -> str:
    return "the request says where" if workers else ""


OBJECTIVE_LINES = re.compile(r"^\s*objectives:\s*(.+)$", re.M)


def objectives_in(text: str) -> list[str]:
    """A request's objectives: its `objectives: a | b` line, else the request itself."""
    m = OBJECTIVE_LINES.search(text)
    return [o.strip() for o in m.group(1).split("|")] if m else [(text.strip().splitlines() or ["its work"])[0][:300]]


class HeuristicRouter:
    """No model: "question: ..." is answered with no thread, `@t_xxx` sends that thread work, a bulleted or numbered list
    fans out one thread per item, anything else spawns one thread. "run
    locally" puts the request's threads on this computer (moves a thread a
    message goes to), "on sprites" on Sprites; `place` picks Sprites when it
    is available, else this computer. A thread's objectives are its
    request's `objectives: a | b` line, else its request (objectives_in)."""

    async def plan(self, request: str, directory: list[dict], places: list[dict], images: int = 0) -> dict:
        if request.lower().startswith("question:") and not images:
            return {"spawn": [], "send": [], "answer": f"Answer: {request[9:].strip()}",
                    "why": "a quick question, answered from the directory"}
        known = {t["id"] for t in directory}
        workers = where(request, places)
        targets = [m for m in MENTION.findall(request) if m in known]
        if targets:
            body = MENTION.sub("", request).strip()
            return {"spawn": [], "send": [{"thread": t, "message": body, "kind": "work",
                                           "objectives": objectives_in(body), "workers": workers,
                                           "where": said(workers)} for t in targets],
                    "why": "the request names existing threads"}
        items = [m.group(1) for line in request.splitlines() if (m := ITEM.match(line))]
        if len(items) >= 2:
            return {"spawn": [{"title": title_for(i), "request": i, "objectives": [i], "workers": workers,
                               "where": said(workers)} for i in items],
                    "send": [], "why": f"{len(items)} independent items, one thread each"}
        return {"spawn": [{"title": title_for(request), "request": request, "objectives": objectives_in(request),
                           "workers": workers, "where": said(workers)}], "send": [],
                "why": "new work gets a new thread"}

    async def assess(self, message: str, sender: str, thread: dict, busy: bool) -> dict:
        """New work only when the message says so ("new job: ..."), with its
        objectives; else it steers, bringing none."""
        if message.lower().startswith("new job:"):
            return {"kind": "work", "objectives": objectives_in(message[8:]), "why": "it says it is a new job"}
        return {"kind": "steer", "objectives": [], "why": "it is about the job the thread is on"}

    async def objectives(self, request: str, directory: list[dict]) -> dict:
        return {"objectives": objectives_in(request), "why": "what its request asks"}

    async def place(self, request: str, places: list[dict]) -> dict:
        can = [p["id"] for p in places if not p["why"]]
        workers = where(request, places)
        return {"workers": workers or ("sprites" if "sprites" in can else can[0]), "where": said(workers)}


    async def revise(self, request: str, previous: str, reached: list[str], directory: list[dict],
                     places: list[dict]) -> dict:
        """Items of a list that are new in the edit get a thread each; the
        rest is left to the corrections. An edit that reached no thread is
        planned like a new request."""
        if not reached:
            return await self.plan(request, directory, places)
        old = {m.group(1) for line in previous.splitlines() if (m := ITEM.match(line))}
        new = [m.group(1) for line in request.splitlines() if (m := ITEM.match(line)) and m.group(1) not in old]
        if new:
            workers = where(request, places)
            return {"spawn": [{"title": title_for(i), "request": i, "objectives": [i], "workers": workers,
                               "where": said(workers)} for i in new], "send": [],
                    "why": f"{len(new)} new item{'s' if len(new) > 1 else ''} in the edit, one thread each"}
        return {"spawn": [], "send": [], "why": "the threads on it take the edit as a correction"}


def local(provider) -> Places:
    """Places with `provider` as this computer's, the only place."""
    return Places({"local": provider})
