import argparse
import sys

from PySide6.QtWidgets import QApplication

from .api import Api
from .window import MainWindow


def main() -> None:
    p = argparse.ArgumentParser(description="orchestrator desktop client")
    p.add_argument("--url", default="http://127.0.0.1:8000",
                   help="the control plane (`pixi run serve` in orchestrator/)")
    a, rest = p.parse_known_args()
    app = QApplication([sys.argv[0], *rest])
    app.setApplicationName("Orchestrator")
    w = MainWindow(Api(a.url))
    w.show()
    sys.exit(app.exec())


main()
