"""How loaded a thread is: the signal the router weighs when it decides whether
a request goes to an existing thread or to a new one, and what the UI shows
beside each thread.

A thread does one job at a time, and work never queues behind it: it can be
steered (a message about the job it is on reaches its turn at once), but new
work for a thread that is not idle goes to a new thread (Orchestrator.send,
router.py). Its load is made from what the control plane already knows of it:

    running   it is in a turn (runner state queued, working or waiting), being
              put on a worker to start one (binding "binding"), or just handed
              a job (Orchestrator.taken).
    busy_for  seconds since its current turn began (threads.busy_since), 0
              between turns.
    worker    the state of the worker it is bound to (states.WORKER).

and comes to one level:

    idle     not in a turn: takes a new job at once.
    busy     in a turn: takes only steering; new work goes to a new thread.
    stalled  its worker is not answering (unreachable, or being reattached):
             a message to it moves it to a ready worker, cutting short the
             turn it had under way there. Or its worker is broken (or being
             replaced): it takes nothing until Respawn has replaced it.
"""

from __future__ import annotations

from .states import RUNNING

LEVELS = ("idle", "busy", "stalled")

# Worker states (states.WORKER) in which nothing sent to its threads gets through
# (a message moves its thread to a ready worker instead, but off a broken one,
# which only Respawn mends).
WORKER_AWAY = {"unreachable", "reattaching", "broken", "replacing"}


def running(thread: dict, taken: bool = False) -> bool:
    """Whether a thread is on a job: new work for it goes to a new thread."""
    return taken or thread["state"] in RUNNING or thread["binding"] == "binding"


def load_of(thread: dict, worker_state: str, at: float, taken: bool = False) -> dict:
    """A thread's load at time `at`: {"level", "busy_for"}."""
    on = running(thread, taken)
    since = thread.get("busy_since")
    busy_for = max(0, int(at - since)) if on and since else 0
    if worker_state in WORKER_AWAY:
        level = "stalled"
    elif on:
        level = "busy"
    else:
        level = "idle"
    return {"level": level, "busy_for": busy_for}
