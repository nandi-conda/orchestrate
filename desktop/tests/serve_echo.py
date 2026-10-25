"""A control plane on the echo runner, as a process: what the window starts
in the tests instead of `pixi run serve`. Usage: serve_echo.py PORT DIR [EXIT_CODE]"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "orchestrator"))

if len(sys.argv) > 3:
    raise SystemExit(int(sys.argv[3]))

import uvicorn  # noqa: E402

from control.app import create_app  # noqa: E402
from control.providers import LocalProvider  # noqa: E402
from control.router import HeuristicRouter  # noqa: E402
from control.store import SqliteStore  # noqa: E402

port, work = int(sys.argv[1]), Path(sys.argv[2])
app = create_app(SqliteStore(str(work / "db.sqlite")),
                 LocalProvider(workdir=str(work / "workers"), use_devrunner=True), HeuristicRouter())
uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning", timeout_graceful_shutdown=2)
