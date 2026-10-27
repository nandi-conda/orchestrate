import argparse
import faulthandler
import logging
import signal
import sys

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QApplication

from control import paths
from control.app import create_app
from control.secrets import require, warn_optional
from control.store import store_from_url
from control.sync import Sync

from .server import ControlPlane
from .window import MainWindow, prepare_qt


def main() -> None:
    p = argparse.ArgumentParser(description="orchestrator desktop window: the ultra UI, with the control plane "
                                            "running in this process")
    p.add_argument("--port", type=int, default=8000,
                   help="the control plane's port (its UI is in a browser there too)")
    p.add_argument("--db", default=str(paths.DB),
                   help="SQLite path, or a postgresql:// URL, as for `python -m control`")
    a, rest = p.parse_known_args()
    faulthandler.enable()  # a crash in Qt or Chromium says where, instead of exiting silently
    logging.basicConfig(level=logging.INFO)
    paths.migrate()
    logging.getLogger("httpx").setLevel(logging.WARNING)  # a line per request to every worker otherwise
    require(["DEEPSEEK_API_KEY"])  # the router and every worker's model
    warn_optional(["SPRITES_TOKEN", "GITHUB_TOKEN"])
    try:
        sync = Sync.from_env()  # projects shared with other computers in the same space
    except RuntimeError as e:
        raise SystemExit(str(e)) from None
    prepare_qt()
    app = QApplication([sys.argv[0], *rest])
    app.setApplicationName("Orchestrator")
    control = ControlPlane(a.port, lambda shutdown: create_app(store=store_from_url(a.db), shutdown_event=shutdown,
                                                               sync=sync))
    w = MainWindow(control)
    w.show()
    # Ctrl+C in the terminal closes the window (and stops the control plane).
    # Python handles signals only while it runs, so a timer gives it the chance.
    signal.signal(signal.SIGINT, lambda *_: w.close())
    tick = QTimer()
    tick.timeout.connect(lambda: None)
    tick.start(250)
    code = app.exec()
    control.stop()
    del w  # the web page goes before the application, as Chromium's teardown needs
    sys.exit(code)


main()
