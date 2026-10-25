"""The window: a web view on the control plane's ultra UI, which is the only
UI. PySide draws the window and does what a browser tab would: it keeps
every navigation in this view (links, window.open, target=_blank) and saves
downloads to the downloads folder."""

from __future__ import annotations

import html
from pathlib import Path

from PySide6.QtCore import QStandardPaths, QTimer, QUrl
from PySide6.QtWebEngineCore import QWebEngineDownloadRequest, QWebEnginePage
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWidgets import QMainWindow

from .server import DOWN, UP, ControlPlane

# The status page's Retry link; the page never loads it, the window acts on it.
RETRY = QUrl("http://retry.orchestrate.invalid/")


def free_path(folder: Path, name: str) -> Path:
    """folder/name, or folder/"name (n)" when that is taken: a download never
    overwrites a file."""
    p = folder / name
    stem, suffix, n = p.stem, p.suffix, 1
    while p.exists():
        p = folder / f"{stem} ({n}){suffix}"
        n += 1
    return p


def status_page(state: str, text: str) -> str:
    """What the window shows until the control plane answers."""
    retry = '<p><a href="%s">Retry</a></p>' % RETRY.toString() if state == DOWN else ""
    color = "#a4262c" if state == DOWN else "inherit"
    return f"""<!doctype html><meta charset="utf-8"><title>Orchestrator</title>
<style>body {{ font: 15px system-ui, sans-serif; margin: 48px; color-scheme: light dark; }}
p {{ max-width: 720px; line-height: 1.5; }}</style>
<h1>Orchestrator</h1><p style="color: {color}">{html.escape(text)}</p>{retry}"""


class Page(QWebEnginePage):
    """Keeps the UI in one window: a new window is this view instead, and the
    status page's Retry asks the control plane to start again."""

    def __init__(self, view: QWebEngineView, control: ControlPlane) -> None:
        super().__init__(view)
        self.view = view
        self.control = control

    def createWindow(self, kind):
        return self

    def acceptNavigationRequest(self, url, kind, is_main_frame) -> bool:
        if url == RETRY:
            # Starting shows the status page again, which must not happen
            # inside this navigation's own callback.
            QTimer.singleShot(0, self.control.start)
            return False
        return super().acceptNavigationRequest(url, kind, is_main_frame)


class MainWindow(QMainWindow):
    def __init__(self, control: ControlPlane) -> None:
        super().__init__()
        self.control = control
        self.download_dir = Path(QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DownloadLocation))
        self.setWindowTitle("Orchestrator")
        self.resize(1280, 820)
        self.view = QWebEngineView(self)
        self.view.setPage(Page(self.view, control))
        self.view.titleChanged.connect(self.setWindowTitle)
        self.view.page().profile().downloadRequested.connect(self._download)
        self.setCentralWidget(self.view)
        control.changed.connect(self._control_changed)
        control.start()

    def _control_changed(self, state: str, text: str) -> None:
        if state == UP:
            self.statusBar().showMessage(text, 8000)
            self.view.load(QUrl(self.control.base_url + "/"))
        else:
            self.view.setHtml(status_page(state, text))

    def _download(self, d: QWebEngineDownloadRequest) -> None:
        dest = free_path(self.download_dir, d.downloadFileName())
        d.setDownloadDirectory(str(dest.parent))
        d.setDownloadFileName(dest.name)
        d.isFinishedChanged.connect(lambda: self._downloaded(d, dest))
        d.accept()
        self.statusBar().showMessage(f"Saving {dest.name}…")

    def _downloaded(self, d: QWebEngineDownloadRequest, dest: Path) -> None:
        if d.state() == QWebEngineDownloadRequest.DownloadState.DownloadCompleted:
            self.statusBar().showMessage(f"Saved {dest.name} to {dest.parent}", 8000)
        else:
            self.statusBar().showMessage(f"Could not save {dest.name}: {d.interruptReasonString()}", 8000)

    def closeEvent(self, event) -> None:
        self.control.stop()
        super().closeEvent(event)
