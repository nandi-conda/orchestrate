"""Threads run the real prime-agent and its kernel in the real sandbox; only
the model is scripted, with prime-agent's faux provider
(PRIME_AGENT_FAUX_SCRIPT: a list of responses, each text and tool calls)."""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from runner import PrimeAgent
from runner.sandbox import View, bwrap, bwrap_args, thread_env

# Tests get their temp dirs here rather than under TMPDIR (see `tmp_path`).
SHORT_TMP = "/tmp"


@pytest.fixture
def tmp_path():
    """A test's own directory, at a short path.

    A thread keeps its team socket at `<workdir>/.team/<thread>.sock`, and a
    unix socket address is at most 107 bytes. pytest's own tmp_path sits under
    `TMPDIR/pytest-of-<user>/pytest-<n>/<test name>/`, so a deep TMPDIR (a Delta
    terminal's, say) puts that socket past the limit: every thread then fails to
    start, and the wait for it runs out its timeout.
    """
    d = Path(tempfile.mkdtemp(prefix="runner-test-", dir=SHORT_TMP))
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


def py(code: str, text: str = "") -> dict:
    """A model response that runs `code` in the thread's Python kernel."""
    blocks = [{"type": "text", "text": text}] if text else []
    return {"content": blocks + [{"type": "toolCall", "name": "python_repl", "arguments": {"code": code}}]}


def sh(command: str) -> dict:
    """A model response that runs a shell command, and prints its exit code and output."""
    return py(f"r = await bash({command!r}, yield_after=120)\nprint(r.exit_code, r.output)")


class Scripted(PrimeAgent):
    """prime-agent with a scripted model: each thread started takes the next
    script, in the order threads start."""

    def __init__(self, *scripts: list):
        super().__init__(env={"DEEPSEEK_API_KEY": "faux", "OPENGATEWAY_API_TOKEN": "faux"})
        object.__setattr__(self, "scripts", list(scripts))

    async def start(self, place, *, model, append_prompt):
        script = {"responses": self.scripts.pop(0)}
        return await PrimeAgent(env={"PRIME_AGENT_FAUX_SCRIPT": json.dumps(script)}).start(
            place, model=model, append_prompt=append_prompt)


class Web:
    """DeepSeek's search and fetch, answered here."""

    def __init__(self):
        self.calls = []

    async def search(self, query, count=10, question=None):
        self.calls.append(("search", query, count, question))
        return [{"title": "Bubblewrap", "url": "https://github.com/containers/bubblewrap", "summary": "sandboxing"}]

    async def fetch(self, url):
        self.calls.append(("fetch", url))
        return {"title": "Bubblewrap", "content": "Unprivileged sandboxing tool"}


def in_view(view: View, command: str) -> subprocess.CompletedProcess:
    """A command in a thread's sandbox, as its kernel's commands run."""
    return subprocess.run([bwrap(), *bwrap_args(view), "bash", "-c", command], env=thread_env(view),
                          capture_output=True, text=True)


def results(t) -> list[str]:
    return [e["data"]["result"] for e in t.events if e["type"] == "tool_result"]


def types(t) -> list[str]:
    return [e["type"] for e in t.events]


async def until(cond, timeout: float = 60.0):
    async def wait():
        while not cond():
            await asyncio.sleep(0.05)
    await asyncio.wait_for(wait(), timeout)


async def started(t, timeout: float = 60.0):
    """Wait until the thread's agent runs (its team socket is up, so its
    sandbox takes commands). A thread that failed to start is not waited for:
    it fails here, with what it reported, instead of after the timeout."""
    async def wait():
        while t.harness is None:
            if t.task is not None and t.task.done():
                errors = [e["data"]["error"] for e in t.events if e["type"] == "error"]
                raise AssertionError(f"thread {t.id} did not start ({t.state}): {errors or 'no error reported'}")
            await asyncio.sleep(0.02)
    await asyncio.wait_for(wait(), timeout)
