"""A thread's objectives: the project's objectives the orchestrator delegated
to it.

The orchestrator makes every objective (its router plans them from the
person's requests, without repeating one a thread already owns) and gives
each to one thread: with the thread when it is posted, and with the message
that brings the thread more work. The thread's agent only moves its own
through the team skill (team.py): each is `pending`, `in_progress` or `done`,
with a short note on where it is at, and they are in the order to work them.
A turn that ends with every objective done ends the thread (`done`), and the
control plane archives it.
"""

from __future__ import annotations

STATES = ("pending", "in_progress", "done")
MAX = 20
TEXT_MAX = 300
NOTE_MAX = 500


class ObjectiveError(ValueError):
    pass


def objective(item) -> dict:
    """One objective, `{"id", "text", "status", "note"}`: `id` and `text` as
    the orchestrator made them, `status` (pending when left out) and `note`."""
    if not isinstance(item, dict):
        raise ObjectiveError("an objective is a dict with id, text, status and note")
    oid = str(item.get("id") or "").strip()
    if not oid:
        raise ObjectiveError("an objective needs its id")
    text = str(item.get("text") or "").strip()
    if not text:
        raise ObjectiveError("an objective needs its text")
    if len(text) > TEXT_MAX:
        raise ObjectiveError(f"an objective's text is at most {TEXT_MAX} characters")
    status = item.get("status") or "pending"
    if status not in STATES:
        raise ObjectiveError(f"an objective's status is one of {', '.join(STATES)}, not {status!r}")
    note = str(item.get("note") or "").strip()
    if len(note) > NOTE_MAX:
        raise ObjectiveError(f"an objective's note is at most {NOTE_MAX} characters")
    return {"id": oid, "text": text, "status": status, "note": note}


def objectives(items) -> list[dict]:
    """A list of objectives, each id once, at most MAX."""
    if not isinstance(items, list):
        raise ObjectiveError("objectives are a list")
    if len(items) > MAX:
        raise ObjectiveError(f"a thread has at most {MAX} objectives")
    out = [objective(i) for i in items]
    ids = [o["id"] for o in out]
    if len(set(ids)) != len(ids):
        raise ObjectiveError("each objective's id is in the list once")
    return out


def accomplished(items: list[dict]) -> bool:
    """Every objective is done (and there is at least one)."""
    return bool(items) and all(o["status"] == "done" for o in items)
