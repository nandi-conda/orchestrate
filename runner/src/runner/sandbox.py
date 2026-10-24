"""Sandbox interface: where a thread's commands and file edits actually happen."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

MAX_OUTPUT = 30_000


@dataclass
class ExecResult:
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool = False


class Sandbox(Protocol):
    root: str

    async def exec(self, cmd: str, timeout: float = 120.0) -> ExecResult: ...
    async def read_file(self, path: str) -> bytes: ...
    async def write_file(self, path: str, data: bytes) -> None: ...
    async def list_dir(self, path: str) -> list[str]: ...


class SandboxError(RuntimeError):
    pass


def _clip(b: bytes) -> str:
    s = b.decode("utf-8", errors="replace")
    if len(s) > MAX_OUTPUT:
        half = MAX_OUTPUT // 2
        s = s[:half] + f"\n... [{len(s) - MAX_OUTPUT} chars cut] ...\n" + s[-half:]
    return s


class LocalSandbox:
    """Runs in the current machine, confined to one directory.

    The directory confinement only stops accidents; the worker itself (a Sprite,
    a container) is the real isolation boundary.
    """

    def __init__(self, root: str | os.PathLike, env: dict[str, str] | None = None):
        self.root = str(Path(root).resolve())
        Path(self.root).mkdir(parents=True, exist_ok=True)
        self.env = env

    def _path(self, path: str) -> Path:
        p = (Path(self.root) / path).resolve()
        if p != Path(self.root) and Path(self.root) not in p.parents:
            raise SandboxError(f"path escapes the workspace: {path}")
        return p

    async def exec(self, cmd: str, timeout: float = 120.0) -> ExecResult:
        env = {**os.environ, **(self.env or {})}
        proc = await asyncio.create_subprocess_exec(
            "bash", "-lc", cmd, cwd=self.root, env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout)
        except asyncio.TimeoutError:
            try:
                os.killpg(proc.pid, 9)
            except ProcessLookupError:
                pass
            out, err = await proc.communicate()
            return ExecResult(-1, _clip(out), _clip(err), timed_out=True)
        return ExecResult(proc.returncode or 0, _clip(out), _clip(err))

    async def read_file(self, path: str) -> bytes:
        return self._path(path).read_bytes()

    async def write_file(self, path: str, data: bytes) -> None:
        p = self._path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    async def list_dir(self, path: str) -> list[str]:
        p = self._path(path)
        return sorted(f"{c.name}/" if c.is_dir() else c.name for c in p.iterdir())
