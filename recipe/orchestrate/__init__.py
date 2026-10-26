"""The conda package's commands. The package installs the checkout's layout
under this directory (orchestrator/control, orchestrator/desktop,
orchestrator/ui/dist, runner/), because the control plane finds its UI and the
runner a local worker runs by where its own files are, as in a checkout.

- `orchestrate`: the desktop window, as `pixi run desktop`
- `orchestrate-serve`: the control plane for a browser, as `pixi run serve`
"""

import runpy
import sys
from pathlib import Path

ORCHESTRATOR = Path(__file__).resolve().parent / "orchestrator"


def _run(module: str) -> None:
    sys.path.insert(0, str(ORCHESTRATOR))
    runpy.run_module(module, run_name="__main__")


def desktop() -> None:
    _run("desktop")


def serve() -> None:
    _run("control")
