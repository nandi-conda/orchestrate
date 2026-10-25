import argparse
import signal
import sys

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QApplication

from .server import ControlPlane, serve_command
from .window import MainWindow


def main() -> None:
    p = argparse.ArgumentParser(description="orchestrator desktop window: the ultra UI of a control plane it starts")
    p.add_argument("--port", type=int, default=8000,
                   help="port of the control plane this window starts (its UI is in a browser there too)")
    p.add_argument("--url", help="connect to a control plane someone else runs instead of starting one")
    a, rest = p.parse_known_args()
    app = QApplication([sys.argv[0], *rest])
    app.setApplicationName("Orchestrator")
    if a.url:
        control = ControlPlane(a.url, None)
    else:
        control = ControlPlane(f"http://127.0.0.1:{a.port}", serve_command(a.port))
    w = MainWindow(control)
    w.show()
    # Ctrl+C in the terminal closes the window (and stops its control plane).
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
