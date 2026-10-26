"""Where the control plane keeps its data: one directory per user,
`~/.orchestrate`, whichever checkout or working directory it runs from.

- `orchestrator.db`: the SQLite store (unless `--db` names another, or Postgres)
- `repos/<project id>.git`: each project's hub
- `workers/<worker id>/`: a local worker's disk, with its `runner.log`

Before this, the data lived in the checkout (`orchestrator/orchestrator.db`
and `orchestrator/.orchestrator/`). `migrate()` moves it here once.
"""

import logging
import shutil
from pathlib import Path

DATA = Path.home() / ".orchestrate"
DB = DATA / "orchestrator.db"
REPOS = DATA / "repos"
WORKERS = DATA / "workers"

log = logging.getLogger("orchestrator.paths")

# The checkout's orchestrator/ directory, where the pixi tasks used to keep
# it, and what moves from there to where.
_OLD = Path(__file__).resolve().parents[1]
_MOVES = [(".orchestrator/repos", "repos"), (".orchestrator/workers", "workers"),
          *((f"orchestrator.db{suffix}", f"orchestrator.db{suffix}") for suffix in ("", "-wal", "-shm"))]


def migrate(old: Path = _OLD, data: Path = DATA) -> list[tuple[Path, Path]]:
    """Move the data a control plane kept in the checkout to `data`, the
    moves made. Data in both places is refused before anything moves: which
    one is wanted is the user's call. Worktrees that point at the old place
    are made again by their repositories (a hub's landings, a worker's
    threads), from what the hub holds."""
    moves = [(old / src, data / dst) for src, dst in _MOVES if (old / src).exists()]
    both = [str(dst) for _, dst in moves if dst.exists()]
    if both:
        raise RuntimeError(f"orchestrator data is in {old} and in {data} ({', '.join(both)}); keep one of "
                           f"them and remove the other")
    data.mkdir(parents=True, exist_ok=True)
    for src, dst in moves:
        shutil.move(src, dst)
        log.info("moved %s to %s", src, dst)
    leftover = old / ".orchestrator"
    if moves and leftover.is_dir() and not any(leftover.iterdir()):
        leftover.rmdir()
    return moves
