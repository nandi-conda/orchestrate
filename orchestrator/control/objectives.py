"""The project's objectives: what the person's requests ask to get done.

The orchestrator owns them. Its router plans them from each request (one per
outcome, never one a thread already owns: work toward an owned objective goes
to its owner) and delegates each to one thread, which owns it from then on:
the thread is posted with its objectives, and a message that brings a thread
more work carries the objectives for it (runner/API.md). A thread only moves
its own (its agent works them one by one as its goal, runner/src/runner/thread.py).

They are kept on their owners, as each thread's runner last said them
(threads.objectives): the project's objectives are its threads' together.
"""

from __future__ import annotations

import secrets

MAX = 20  # objectives one thread is given (the runner's limit)
TEXT_MAX = 300


def new_id() -> str:
    return "o_" + secrets.token_hex(4)


def made(texts: list[str]) -> list[dict]:
    """New objectives, pending, from the router's texts."""
    return [{"id": new_id(), "text": t, "status": "pending", "note": ""} for t in texts]


def texts(items, at_least: int = 0) -> list[str]:
    """The router's objective texts, cleaned: strings, stripped, each at most
    TEXT_MAX characters, at most MAX. Fewer than `at_least` is an error."""
    out = [str(i).strip()[:TEXT_MAX] for i in items or [] if isinstance(i, str) and i.strip()][:MAX]
    if len(out) < at_least:
        raise ValueError("no objectives")
    return out


def with_ids(items: list[dict]) -> list[dict]:
    """Objectives from before they had ids, each given one."""
    return [o if o.get("id") else {**o, "id": new_id()} for o in items]
