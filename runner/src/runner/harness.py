"""A thread's agent: prime-agent, the harness its model works in.

prime-agent (https://github.com/PrimeIntellect-ai/prime-agent) is a binary
that runs the agent loop (provider calls, the session file, compaction) and a
Python kernel the model drives with its one tool, `python_repl`: commands
(`await bash(...)`), file edits and skills are all Python there. A thread is
one `prime-agent --mode rpc` process: a JSON line per command on its stdin,
one per response or event on its stdout.

The binary runs outside the sandbox, since it holds the model's API key. The
kernel, which runs everything the model writes, is inside it: the binary
starts the kernel as `$PRIME_AGENT_KERNEL_PYTHON -m rlm.repl`, and for a
thread that is a wrapper written here (`write_kernel`). The wrapper execs
bwrap with the thread's View and a fresh environment, then the thread's own
venv on the kernel environment (`runner/.pixi/envs/kernel`, put back
read-only in the view). The venv is the thread's, so the `uv pip install`
prime-agent's prompt offers installs there.

The thread's session (the conversation prime-agent keeps, with its
compaction and local harness state) lives in the thread's home; the runner
ships it to the orchestrator as a git ref every turn (repo.py), so the thread
resumes on any worker.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import signal
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .sandbox import View, bwrap, bwrap_args, no_capabilities, thread_env

log = logging.getLogger("runner.harness")

VERSION = "0.10.1"
RUNNER = Path(__file__).resolve().parents[2]
HARNESS = RUNNER / ".harness" / f"prime-agent-{VERSION}"  # deploy/install-harness.sh
KERNEL = RUNNER / ".pixi" / "envs" / "kernel"  # pixi.toml, feature `kernel`
TEAM_SKILL = RUNNER / "skills" / "team"
# The models threads run on, as prime-agent's models.json: the binary runs
# --offline, so it has no catalog to look them up in. IMAGE_MODEL takes a
# turn that attaches images when the thread's model accepts text only.
MODELS = RUNNER / "models.json"
IMAGE_MODEL = "deepseek/deepseek-flash"
# prime-agent's bundled skills threads do without: they need its daemon
# (messaging and observing sessions, factories, heartbeats), a Serper key
# (websearch), or accounts a worker has none of (MCP connections, Prime
# Intellect's products). Threads reach each other through the team skill.
DROPPED_SKILLS = ("agent-message", "agent-observe", "factory", "rlm-heartbeat", "websearch", "mcp",
                  "prime-intellect")
# What the kernel gets of the environment the binary starts it with: the
# binary's own settings for it (the agent dir, the session's dir and id).
KERNEL_VARS = ("RLM_", "PRIME_AGENT_", "PI_")
NOT_KERNEL_VARS = ("PRIME_AGENT_KERNEL_PYTHON", "PRIME_AGENT_KERNEL_OWNER_PID")  # the wrapper; a pid outside
# The parts of a thread's agent dir that are its session: the session file,
# and what the session keeps beside it (the kernel's local harness state).
SESSION = ("sessions", "session-artifacts")
LINE_LIMIT = 256 * 1024 * 1024  # a message_update line carries the whole message so far


class HarnessError(RuntimeError):
    pass


def exit_reason(code: int | None) -> str:
    """How a prime-agent process ended, as its exit code says: a signal that
    ended it is named (asyncio gives -N for one it died of, a shell 128+N)."""
    if code is None:
        return "is still running"
    n = -code if code < 0 else code - 128 if code > 128 else 0
    try:
        name = signal.Signals(n).name if n else ""
    except ValueError:
        name = ""
    return f"was terminated by {name} (exit {code})" if name else f"exited ({code})"


@dataclass(frozen=True)
class Place:
    """Where a thread's harness keeps its things on the worker."""
    view: View
    socket: str   # the team socket, outside the view and bound into it (team.py)
    kernel: str   # the kernel wrapper, outside the view
    turn: str     # there while a turn is under way (thread.py, "Interrupted turns"), outside the view

    @property
    def agent_dir(self) -> Path:
        return Path(self.view.home) / ".prime-agent"

    @property
    def sessions(self) -> Path:
        return self.agent_dir / "sessions"

    @property
    def venv(self) -> Path:
        return Path(self.view.home) / ".venv"

    def session_file(self) -> Path | None:
        files = sorted(self.sessions.glob("*.jsonl"))
        return files[0] if files else None


@dataclass(frozen=True)
class PrimeAgent:
    """How threads' prime-agent processes start. `env` goes to the binary on
    top of the runner's environment: the model provider's API key (prime-agent
    reads DEEPSEEK_API_KEY for its `deepseek` provider)."""
    env: Mapping[str, str] = field(default_factory=dict)
    binary: Path = HARNESS / "prime-agent"
    kernel: Path = KERNEL

    def check(self) -> None:
        if not os.access(self.binary, os.X_OK):
            raise HarnessError(f"prime-agent {VERSION} is not installed at {self.binary}: run `pixi run harness`")
        if not (self.kernel / "bin" / "python").exists():
            raise HarnessError(f"the kernel environment is not installed at {self.kernel}: "
                               "run `pixi install -e kernel`")

    def prepare(self, place: Place) -> None:
        """The thread's agent dir (settings, models, the team skill), its venv and its
        kernel wrapper."""
        for d in (place.view.root, place.view.home, place.view.tmp, place.sessions):
            Path(d).mkdir(parents=True, exist_ok=True)
        settings = {"skills": [f"-{name}/SKILL.md" for name in DROPPED_SKILLS], "imageModel": IMAGE_MODEL}
        (place.agent_dir / "settings.json").write_text(json.dumps(settings, indent=1))
        shutil.copyfile(MODELS, place.agent_dir / "models.json")
        skill = place.agent_dir / "skills" / "team"
        shutil.rmtree(skill, ignore_errors=True)
        shutil.copytree(TEAM_SKILL, skill)
        if not (place.venv / "bin" / "python").exists():
            import subprocess

            subprocess.run([str(self.kernel / "bin" / "python"), "-m", "venv", "--without-pip",
                            "--system-site-packages", str(place.venv)], check=True)
        write_kernel(place, self.kernel)

    async def start(self, place: Place, *, model: dict | None, append_prompt: str) -> "Harness":
        """Start the thread's prime-agent, resuming its session when it has one."""
        self.prepare(place)
        args = [str(self.binary), "--mode", "rpc", "--offline", "--cwd", place.view.root,
                "--session-dir", str(place.sessions), "--append-system-prompt", append_prompt]
        session = place.session_file()
        if session is not None:
            args += ["--resume", str(session)]
        spec = model or {}
        args += ["--provider", spec.get("provider") or "deepseek",
                 "--model", spec.get("model") or os.environ.get("DEEPSEEK_MODEL", "deepseek-flash")]
        env = {**os.environ, **self.env, "HOME": place.view.home, "TMPDIR": place.view.tmp,
               "PRIME_AGENT_CODING_AGENT_DIR": str(place.agent_dir), "PRIME_AGENT_KERNEL_PYTHON": place.kernel,
               "DO_NOT_TRACK": "1"}
        proc = await asyncio.create_subprocess_exec(
            *args, env=env, cwd=place.view.root, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, limit=LINE_LIMIT, start_new_session=True, preexec_fn=no_capabilities)
        return Harness(proc)


def write_kernel(place: Place, kernel: Path) -> None:
    """The thread's kernel wrapper: bwrap with the thread's view, the
    environment made afresh (thread_env, the venv, the team socket) plus what
    the binary sets for its kernel, then the venv's Python with the binary's
    arguments."""
    venv = str(place.venv)
    env = thread_env(place.view)
    env.update({"PATH": os.pathsep.join([f"{venv}/bin", str(kernel / "bin"), env["PATH"]]),
                "VIRTUAL_ENV": venv, "ORCHESTRATE_TEAM_SOCKET": place.socket})
    command = [bwrap(), *bwrap_args(place.view), f"{venv}/bin/python"]
    script = f"""#!{sys.executable} -I
# The kernel of thread {Path(place.view.root).name}, in its sandbox (runner/src/runner/harness.py).
import os, sys
env = {env!r}
env.update({{k: v for k, v in os.environ.items() if k.startswith({KERNEL_VARS!r}) and k not in {NOT_KERNEL_VARS!r}}})
os.execve({command[0]!r}, {command!r} + sys.argv[1:], env)
"""
    path = Path(place.kernel)
    path.write_text(script)
    path.chmod(0o700)


class Harness:
    """One running prime-agent: its RPC commands, and its events in `events`
    (None once the process has exited)."""

    def __init__(self, proc: asyncio.subprocess.Process):
        self.proc = proc
        self.events: asyncio.Queue[dict | None] = asyncio.Queue()
        self.pending: dict[str, asyncio.Future] = {}
        self.stderr: deque[str] = deque(maxlen=40)
        self._n = 0
        self._readers = [asyncio.create_task(self._read()), asyncio.create_task(self._read_stderr())]

    async def _read(self) -> None:
        try:
            while line := await self.proc.stdout.readline():
                try:
                    msg = json.loads(line)
                except ValueError:
                    log.warning("prime-agent wrote a line that is not JSON: %r", line[:200])
                    continue
                fut = self.pending.pop(msg.get("id"), None) if msg.get("type") == "response" else None
                if fut is not None:
                    if not fut.done():
                        fut.set_result(msg)
                else:
                    self.events.put_nowait(msg)
        finally:
            await self.proc.wait()
            error = HarnessError(f"prime-agent {self.ended()}")
            for fut in self.pending.values():
                if not fut.done():
                    fut.set_exception(error)
            self.pending.clear()
            self.events.put_nowait(None)

    async def _read_stderr(self) -> None:
        while line := await self.proc.stderr.readline():
            text = line.decode(errors="replace").rstrip()
            self.stderr.append(text)
            log.info("prime-agent %s: %s", self.proc.pid, text)

    def tail(self) -> str:
        return "\n".join(self.stderr) or "no output"

    def ended(self) -> str:
        """How the process ended, and the last it wrote to stderr."""
        return f"{exit_reason(self.proc.returncode)}: {self.tail()}"

    async def command(self, type: str, **fields: Any) -> Any:
        """Send one RPC command; its response's data. Raises HarnessError when
        prime-agent refuses it or has exited."""
        if self.proc.returncode is not None or self.proc.stdin.is_closing():
            raise HarnessError(f"prime-agent {self.ended()}")
        self._n += 1
        cid = f"c{self._n}"
        fut = asyncio.get_running_loop().create_future()
        self.pending[cid] = fut
        self.proc.stdin.write(json.dumps({"id": cid, "type": type, **fields}).encode() + b"\n")
        await self.proc.stdin.drain()
        reply = await fut
        if not reply.get("success"):
            raise HarnessError(f"{type}: {reply.get('error') or 'refused'}")
        return reply.get("data")

    async def prompt(self, message: str, images: list[dict] | None = None) -> None:
        """A message for the model. While a turn runs it steers that turn (the
        model reads it before its next call); otherwise it starts one."""
        await self.command("prompt", message=message, streamingBehavior="steer",
                           images=[{"type": "image", "data": im["data"], "mimeType": im["media_type"]}
                                   for im in images or []])

    async def busy(self) -> bool:
        """Whether a turn is running, or has messages queued that start one."""
        state = await self.command("get_state")
        return bool(state.get("isStreaming") or state.get("isCompacting")
                    or (state.get("sessionActions") or {}).get("queuedCount"))

    async def abort(self) -> None:
        await self.command("abort")

    async def close(self) -> None:
        """End the process (its kernel, in a sandbox that dies with it, ends too)."""
        if self.proc.returncode is None:
            self.proc.stdin.close()
            try:
                await asyncio.wait_for(self.proc.wait(), 5)
            except asyncio.TimeoutError:
                try:
                    os.killpg(self.proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        await asyncio.gather(*self._readers, return_exceptions=True)
