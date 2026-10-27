"""A turn the runner's shutdown cuts short is interrupted, not failed, and
resumes when the thread is started on the worker again (thread.py,
"Interrupted turns"). prime-agent is stood in for by a script that speaks its
RPC and runs a turn until it is killed, so the cut lands mid-turn."""

from __future__ import annotations

import asyncio
import json
import signal
import sys
from pathlib import Path

import pytest

from runner import PrimeAgent
from runner.harness import Harness, exit_reason
from runner.thread import RESUME, ThreadHost

from conftest import until

# Answers every command; a prompt starts a turn that never ends.
TURN_FOREVER = r"""
import json, sys
for line in sys.stdin:
    cmd = json.loads(line)
    print(json.dumps({"type": "response", "id": cmd["id"], "success": True, "data": {"isStreaming": True}}),
          flush=True)
    if cmd["type"] == "prompt":
        print(json.dumps({"type": "agent_start"}), flush=True)
"""


class Forever(PrimeAgent):
    """prime-agent whose every turn runs until its process is killed."""

    async def start(self, place, *, model, append_prompt):
        for d in (place.view.root, place.view.home, place.view.tmp):
            Path(d).mkdir(parents=True, exist_ok=True)
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-c", TURN_FOREVER, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        object.__setattr__(self, "proc", proc)
        return Harness(proc)


@pytest.fixture(autouse=True)
def a_pixi(monkeypatch):
    monkeypatch.setenv("PIXI_EXE", "/bin/true")  # a thread's view puts pixi back; these never run it


def host(tmp_path, agent) -> ThreadHost:
    return ThreadHost(agent=agent, web=None, workdir=str(tmp_path))


async def working(tmp_path, agent):
    h = host(tmp_path, agent)
    t = await h.start(project="p", title="T", request="do it", id="t_cut")
    await until(lambda: t.state == "working")
    return h, t


async def test_a_turn_the_runners_shutdown_cuts_short_is_interrupted_and_resumes(tmp_path):
    agent = Forever()
    h, t = await working(tmp_path, agent)
    assert (tmp_path / ".team" / "t_cut.turn").exists()
    h.shut_down("the runner got SIGTERM")
    agent.proc.send_signal(signal.SIGTERM)  # a service manager signals every process of the service
    await asyncio.wait_for(t.task, 10)
    assert "error" not in [e["type"] for e in t.events] and t.state == "working"
    assert (tmp_path / ".team" / "t_cut.turn").exists()

    # The runner again, on the same disk: the thread posted again resumes the cut turn.
    agent = Forever()
    again = host(tmp_path, agent)
    t = await again.start(project="p", title="T", request="do it", id="t_cut")
    await until(lambda: t.state == "working")
    assert t.resumed and {"resumed": True, "idle": False} in [e["data"] for e in t.events if e["type"] == "prepared"]
    ins = [e["data"] for e in t.events if e["type"] == "message_in"]
    assert ins == [{"from": "runner", "body": RESUME}]  # not the request again: its session has it
    await again.stop("t_cut")
    assert not (tmp_path / ".team" / "t_cut.turn").exists()  # stopped on purpose: nothing to resume
    later = host(tmp_path, Forever())
    t = await later.start(project="p", title="T", id="t_cut")
    await until(lambda: t.state == "idle")
    assert not t.resumed
    await later.stop("t_cut")


async def test_an_agent_killed_while_the_runner_runs_on_fails_the_thread_and_says_by_what(tmp_path):
    agent = Forever()
    h, t = await working(tmp_path, agent)
    agent.proc.send_signal(signal.SIGTERM)
    await asyncio.wait_for(t.task, 10)
    errors = [e["data"]["error"] for e in t.events if e["type"] == "error"]
    assert t.state == "failed" and len(errors) == 1
    assert "prime-agent was terminated by SIGTERM" in errors[0]
    assert not (tmp_path / ".team" / "t_cut.turn").exists()  # a failed turn is not resumed
    for tid in list(h.threads):
        await h.stop(tid)


async def test_a_turn_file_from_another_session_is_dropped(tmp_path):
    """The thread went on on another worker since its turn here was cut: its
    session is newer than the one the turn file names, so nothing resumes."""
    h = host(tmp_path, Forever())
    place = h._place("t_old", "p")
    (tmp_path / ".team").mkdir()
    (tmp_path / ".team" / "t_old.turn").write_text(json.dumps({"session": "abc"}))
    assert not h._interrupted(place, "def")
    assert not (tmp_path / ".team" / "t_old.turn").exists()
    (tmp_path / ".team" / "t_old.turn").write_text(json.dumps({"session": "abc"}))
    assert h._interrupted(place, "abc")


def test_exit_reasons_name_the_signal():
    assert exit_reason(143) == "was terminated by SIGTERM (exit 143)"
    assert exit_reason(-15) == "was terminated by SIGTERM (exit -15)"
    assert exit_reason(-9) == "was terminated by SIGKILL (exit -9)"
    assert exit_reason(1) == "exited (1)"
