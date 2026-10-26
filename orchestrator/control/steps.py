"""Timed steps of slow work: putting a thread on a worker, making a worker.

Each step is logged (to the control plane's stdout) when it starts, every
STILL_EVERY seconds while it runs, and when it ends, with how long it took,
so work that stalls always says what it is waiting on. The lines are kept
too, for the thread's Worker log tab (Orchestrator.worker_logs).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable

import httpx

log = logging.getLogger("orchestrator")

# How often a step that is still running says so.
STILL_EVERY = 15.0
# Lines a Steps keeps for the Worker log tab.
KEPT = 200


def describe(e: BaseException) -> str:
    """An exception as one line that says what failed: httpx's timeouts carry
    no message, so name the request they were for."""
    text = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
    if isinstance(e, httpx.RequestError):
        try:
            text += f" on {e.request.method} {e.request.url}"
        except RuntimeError:  # no request attached
            pass
    return text


class Steps:
    """The steps of one piece of work, named `name` in every line it logs.
    `on_step`, when given, is told each step as it starts and, with how long
    it has run, while it runs (a thread's activity shows it)."""

    def __init__(self, name: str, on_step: Callable[[str], Awaitable[None]] | None = None,
                 level: int = logging.INFO) -> None:
        self.name = name
        self.level = level  # of the lines it logs; a failure is a warning whatever it is
        self.on_step = on_step
        self.began = time.monotonic()
        self.started_at = time.time()
        self.lines: deque[str] = deque(maxlen=KEPT)
        self.running: list[str] = []  # the steps under way, outermost first
        # The steps of other work this work waits on now (a worker being made
        # for it): the Worker log tab shows them with this work's own.
        self.waits_on: Steps | None = None

    def note(self, text: str, level: int | None = None) -> None:
        now = time.time()
        stamp = time.strftime("%H:%M:%S", time.localtime(now)) + f".{int(now * 1000) % 1000:03d}"
        self.lines.append(f"{stamp} +{time.monotonic() - self.began:6.1f}s  {text}")
        log.log(self.level if level is None else level, "%s: %s", self.name, text)

    async def _tell(self, what: str) -> None:
        if self.on_step is None:
            return
        try:
            await self.on_step(what)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("%s: could not show step %r: %s", self.name, what, describe(e))

    async def _still(self, what: str, t0: float) -> None:
        while True:
            await asyncio.sleep(STILL_EVERY)
            took = time.monotonic() - t0
            self.note(f"still {what} ({took:.0f}s so far)", max(self.level, logging.INFO))
            await self._tell(f"{what} ({took:.0f}s)")

    @asynccontextmanager
    async def step(self, what: str) -> AsyncIterator[None]:
        """Run the body as step `what` ("shipping the hub to worker w1")."""
        t0 = time.monotonic()
        self.running.append(what)
        self.note(f"{what} ...")
        await self._tell(what)
        still = asyncio.create_task(self._still(what, t0))
        try:
            yield
        except asyncio.CancelledError:
            self.note(f"{what}: called off after {time.monotonic() - t0:.1f}s", logging.WARNING)
            raise
        except BaseException as e:
            self.note(f"{what}: failed after {time.monotonic() - t0:.1f}s: {describe(e)}", logging.WARNING)
            raise
        else:
            self.note(f"{what}: done in {time.monotonic() - t0:.1f}s")
        finally:
            still.cancel()
            self.running.remove(what)
            if self.running:  # back to the step around this one
                await self._tell(self.running[-1])

    def text(self) -> str:
        """The lines kept, then those of the work this waits on, for the Worker log tab."""
        out = "\n".join(self.lines)
        if self.waits_on is not None:
            out += f"\n\n{self.waits_on.name}, which it waits on:\n" + self.waits_on.text()
        return out
