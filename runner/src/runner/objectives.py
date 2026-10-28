"""A thread's objectives: the things it is trying to accomplish.

Its agent sets them from its request on its first turn and keeps them current
through the team skill (team.py): each is `pending`, `in_progress` or `done`,
with a short note on where it is at. A turn that ends with every objective done
ends the thread (`done`), and the control plane archives it.
"""

from __future__ import annotations

STATES = ("pending", "in_progress", "done")
MAX = 20
TEXT_MAX = 300
NOTE_MAX = 500


class ObjectiveError(ValueError):
    pass


def objective(item) -> dict:
    """One objective, `{"text", "status", "note"}`, from a string (pending,
    no note) or a dict with `text` and optionally `status` and `note`."""
    if isinstance(item, str):
        item = {"text": item}
    if not isinstance(item, dict):
        raise ObjectiveError("an objective is a string or a dict with text, status and note")
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
    return {"text": text, "status": status, "note": note}


def objectives(items) -> list[dict]:
    """A whole list of objectives: at least one, at most MAX."""
    if not isinstance(items, list):
        raise ObjectiveError("objectives are a list")
    if not items:
        raise ObjectiveError("a thread has at least one objective")
    if len(items) > MAX:
        raise ObjectiveError(f"a thread has at most {MAX} objectives")
    return [objective(i) for i in items]


def accomplished(items: list[dict]) -> bool:
    """Every objective is done (and there is at least one)."""
    return bool(items) and all(o["status"] == "done" for o in items)
