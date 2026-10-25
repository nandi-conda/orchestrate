"""A thread's worktree as files to browse and download (GET /threads/{id}/files).

Paths are relative to the thread's workspace root and never leave it, symlinks
included. Git's own data (`.git`) is not part of what a thread made, so it is
neither listed nor served."""

from __future__ import annotations

from pathlib import Path


class FileError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def resolve(root: str, path: str) -> Path:
    """`path` (relative, "" for the root) inside `root`, or FileError 400/404."""
    base = Path(root).resolve()
    parts = [p for p in path.replace("\\", "/").split("/") if p not in ("", ".")]
    if ".git" in parts:
        raise FileError(400, "git's own files are not served")
    p = base.joinpath(*parts).resolve()
    if p != base and base not in p.parents:
        raise FileError(400, f"path escapes the workspace: {path}")
    if not p.exists():
        raise FileError(404, f"no such file or directory: {path or '.'}")
    return p


def listing(root: str, path: str) -> dict:
    """The directory at `path`: `{"path", "entries": [{"name", "dir", "size"}]}`,
    directories first, then by name."""
    p = resolve(root, path)
    if not p.is_dir():
        raise FileError(400, f"not a directory: {path}")
    base = Path(root).resolve()
    entries = []
    for c in p.iterdir():
        if c.name == ".git":
            continue
        is_dir = c.is_dir()
        size = 0 if is_dir else (c.stat().st_size if c.exists() else 0)  # a dangling link has no size
        entries.append({"name": c.name, "dir": is_dir, "size": size})
    entries.sort(key=lambda e: (not e["dir"], e["name"]))
    rel = p.relative_to(base).as_posix()
    return {"path": "" if rel == "." else rel, "entries": entries}


def file_path(root: str, path: str) -> Path:
    """The regular file at `path`, to send as it is."""
    p = resolve(root, path)
    if not p.is_file():
        raise FileError(400, f"not a file: {path}")
    return p
