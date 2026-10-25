"""What the pane shows, from the events the control plane serves (already
folded and labelled by control/app.py: each has `kind`, `label`, `text` and,
for a reply still streaming, `live`). Pure functions, no Qt."""

from __future__ import annotations

# Event kinds, as control/app.py numbers them.
(K_USER, K_ASSISTANT, K_TOOL_CALL, K_TOOL_RESULT, K_STATUS, K_PEER, K_ERROR, K_ROUTE, K_THINKING,
 K_RESULT, K_STATE) = range(1, 12)

# Kinds whose text is markdown.
MARKDOWN = {K_ASSISTANT, K_PEER, K_RESULT}


def is_tool(e: dict) -> bool:
    return e.get("kind") in (K_TOOL_CALL, K_TOOL_RESULT)


def is_thought(e: dict) -> bool:
    """Finished reasoning (a live row still streams on its own)."""
    return e.get("kind") == K_THINKING and not e.get("live")


def hidden(e: dict, activity: bool) -> bool:
    """Left out of the Dialog: tool rows (under Tools) and state changes (the
    badges show them). Project activity also leaves out reasoning and replies
    still streaming: it is about requests, routing and answers, and the
    working rows say what each thread is doing."""
    if is_tool(e) or e.get("kind") == K_STATE:
        return True
    return activity and (e.get("kind") == K_THINKING or bool(e.get("live")))


def dialog_items(events: list[dict], activity: bool) -> list[tuple[str, list[dict]]]:
    """The Dialog's entries in order: ("event", [e]) for one row, or
    ("thinking", [steps]) for a run of finished reasoning from one thread,
    shown back to back with only hidden rows between them."""
    items: list[tuple[str, list[dict]]] = []
    i, n = 0, len(events)
    while i < n:
        e = events[i]
        if hidden(e, activity):
            i += 1
        elif is_thought(e):
            steps, j = [e], i + 1
            while j < n:
                f = events[j]
                if is_thought(f):
                    if f.get("thread_id") != e.get("thread_id"):
                        break
                    steps.append(f)
                elif not hidden(f, False):
                    break
                j += 1
            items.append(("thinking", steps))
            i = j
        else:
            items.append(("event", [e]))
            i += 1
    return items


def counts(events: list[dict], activity: bool) -> tuple[int, int]:
    """(rows the Dialog says, tool rows) for the tab labels."""
    return (sum(1 for e in events if not hidden(e, activity)), sum(1 for e in events if is_tool(e)))


def size_text(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1048576:
        return f"{n // 1024} KB"
    return f"{n // 1048576} MB"


def join_path(base: str, name: str) -> str:
    return f"{base}/{name}" if base else name
