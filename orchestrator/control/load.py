"""How loaded a thread is: the signal the router weighs when it decides whether
a request goes to an existing thread or to a new one, and what the UI shows
beside each thread.

A thread's load is made from what the control plane already knows of it:

    running   it is in a turn (runner state queued, working or waiting), or
              being put on a worker to start one (binding "binding").
    backlog   the messages to it it has not got to (Store.backlog): those not
              yet delivered to its runner, and those that came in during its
              current turn, which steer that turn and so are still to do.
    busy_for  seconds since its current turn began (threads.busy_since), 0
              between turns.
    worker    the state of the worker it is bound to (states.WORKER).

and comes to one level:

    idle       not in a turn and nothing waiting: takes new work at once.
    busy       in a turn, or one message on its way to it.
    backed_up  BACKLOG or more messages it has not got to: what it is sent now
               waits behind them.
    stalled    its worker is not answering (unreachable, or being reattached):
               a message to it moves it to a ready worker, cutting short the
               turn it had under way there.

The router gives new work only to idle threads whose work it continues; busy
threads get only what concerns their current turn, and backed-up or stalled
threads none, so that work goes to a new thread (router.py).
"""

from __future__ import annotations

from .states import RUNNING

# Messages a thread has not got to at which it counts as backed up.
BACKLOG = 2

LEVELS = ("idle", "busy", "backed_up", "stalled")

# Worker states (states.WORKER) in which nothing sent to its threads gets through
# (a message moves its thread to a ready worker instead).
WORKER_AWAY = {"unreachable", "reattaching"}


def load_of(thread: dict, backlog: int, worker_state: str, at: float) -> dict:
    """A thread's load at time `at`: {"level", "backlog", "busy_for"}."""
    running = thread["state"] in RUNNING or thread["binding"] == "binding"
    since = thread.get("busy_since")
    busy_for = max(0, int(at - since)) if running and since else 0
    if worker_state in WORKER_AWAY:
        level = "stalled"
    elif backlog >= BACKLOG:
        level = "backed_up"
    elif running or backlog:
        level = "busy"
    else:
        level = "idle"
    return {"level": level, "backlog": backlog, "busy_for": busy_for}
