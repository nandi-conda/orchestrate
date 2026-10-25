import argparse
import signal
import sys

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QApplication

from .api import Api
from .server import ControlPlane, serve_command
from .window import MainWindow


def main() -> None:
    p = argparse.ArgumentParser(description="orchestrator desktop client")
    p.add_argument("--port", type=int, default=8000,
                   help="port of the control plane this window starts (its web UI is there too)")
    p.add_argument("--url", help="connect to a control plane someone else runs instead of starting one")
    a, rest = p.parse_known_args()
    app = QApplication([sys.argv[0], *rest])
    app.setApplicationName("Orchestrator")
    if a.url:
        api = Api(a.url)
        control = ControlPlane(api, None)
    else:
        api = Api(f"http://127.0.0.1:{a.port}")
        control = ControlPlane(api, serve_command(a.port))
    w = MainWindow(api, control)
    w.show()
    # Ctrl+C in the terminal closes the window (and stops its control plane).
    # Python handles signals only while it runs, so a timer gives it the chance.
    signal.signal(signal.SIGINT, lambda *_: w.close())
    tick = QTimer()
    tick.timeout.connect(lambda: None)
    tick.start(250)
    code = app.exec()
    control.stop()
    sys.exit(code)


main()
