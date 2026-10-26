"""Stand-ins for the model-backed parts, so the tests need no credentials:
the echo runner (tests/devrunner.py) and a rule-based router. The control
plane itself never uses them."""

import re
import sys

from control.router import title_for

# LocalProvider's command for the echo runner; it runs from orchestrator/.
ECHO_RUNNER = [sys.executable, "-m", "tests.devrunner"]


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
