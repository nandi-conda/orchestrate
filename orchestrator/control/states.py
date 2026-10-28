"""The states a worker and a thread's hold on a worker can be in, and the
only moves between them.

Every change of either goes through Machine.check (Orchestrator._worker_to and
Orchestrator._bind_to), so a move not listed here raises IllegalTransition
rather than leaving a thread or a worker somewhere no code expects. Each move
carries a reason, which the store keeps beside the state and the event log
shows.

Every state has a path back to the machine's happy state, checked when this
module loads. The one exception is a retired state: a lost worker is gone
(its provider no longer has it, and it is destroyed), and its threads take the
path back instead, unbound and then bound on another worker.

Worker (one per LiveWorker):

    ready        its runner answers /health. Happy.
    unreachable  /health fails and its provider still has it (a sprite waking,
                 this machine's network down). Back to ready when it answers;
                 reattached when it has not answered for READY_TIMEOUT, or at
                 once by Respawn of one of its threads.
    reattaching  the provider is starting its runner again, on the same disk.
                 Ready when that answers, unreachable when it fails.
    broken       its provider has it but says it cannot run a runner on it
                 (providers.WorkerBroken: a sprite Fly reports corrupted). Its
                 threads are put on no worker until Respawn of one of them
                 replaces it: its disk is not given up unasked.
    replacing    being destroyed, disk and all, and made afresh in its slot.
                 Ready when the new one answers, broken again when that fails.
    lost         its provider says it is gone. Retired.

    A ready worker whose runner restarted (a new boot id) moves ready -> ready:
    the new runner runs none of its threads. Those mid-turn go bound ->
    binding -> bound on it again, and the runner resumes the cut turn; the
    rest are unbound.

Binding (one per thread):

    unbound      on no worker: never put on one, its worker lost or its runner
                 restarted, its start failed, or an earlier control plane had it.
                 Bound again by its next message or by Respawn.
    binding      being put on a worker: one picked or made, the hub shipped to
                 it, the thread posted. Bound when that is done, unbound when
                 it fails or Respawn starts it over.
    bound        a live worker's runner has it. Happy.

    Leaving bound for unbound cuts a running turn short: a thread queued,
    working or waiting is failed (Orchestrator._bind_to). A runner or control
    plane restart does not unbind such a thread: it is bound again on its
    worker, where the turn resumes (runner/API.md, "Interrupted turns").
"""

from __future__ import annotations

from collections import deque


class IllegalTransition(RuntimeError):
    """A state change the machine does not allow: a bug in the control plane."""


class Machine:
    def __init__(self, name: str, happy: str, moves: dict[str, set[str]], retired: set[str] = frozenset()) -> None:
        self.name = name
        self.happy = happy
        self.moves = {k: frozenset(v) for k, v in moves.items()}
        self.retired = frozenset(retired)
        self.states = frozenset(self.moves)
        for frm, tos in self.moves.items():
            unknown = tos - self.states
            if unknown:
                raise ValueError(f"{name}: {frm} moves to unknown {sorted(unknown)}")
        stuck = sorted(s for s in self.states - self.retired if not self.reaches_happy(s))
        if stuck:
            raise ValueError(f"{name}: no path back to {happy} from {stuck}")

    def reaches_happy(self, state: str) -> bool:
        seen, todo = {state}, deque([state])
        while todo:
            s = todo.popleft()
            if s == self.happy:
                return True
            for nxt in self.moves[s] - seen:
                seen.add(nxt)
                todo.append(nxt)
        return False

    def check(self, subject: str, frm: str, to: str) -> None:
        if to not in self.moves.get(frm, ()):
            raise IllegalTransition(f"{self.name} of {subject} cannot move from {frm} to {to}")


WORKER = Machine("worker", "ready", {
    "ready": {"ready", "unreachable", "lost"},
    "unreachable": {"ready", "reattaching", "lost"},
    "reattaching": {"ready", "unreachable", "broken", "lost"},
    "broken": {"replacing"},
    "replacing": {"ready", "broken"},
    "lost": set(),
}, retired={"lost"})

BINDING = Machine("binding", "bound", {
    "unbound": {"binding"},
    "binding": {"bound", "unbound"},
    "bound": {"binding", "unbound"},
})

# Runner states in which a thread's turn is under way; cut short when it is unbound.
RUNNING = {"queued", "working", "waiting"}
