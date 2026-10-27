"""The window: a web view on the control plane's ultra UI, which is the only
UI. PySide draws the window and does what a browser tab would: it keeps the
UI's own navigation in this view (window.open and target=_blank included),
opens links anywhere else in the default browser, saves downloads to the
downloads folder, lets the UI write to the clipboard (a reply's copy button),
and shows the OS's folder dialog for the UI's Browse… buttons (Host)."""

from __future__ import annotations

import html
import os
import sys
from pathlib import Path

from PySide6.QtCore import QDir, QFile, QIODevice, QObject, QStandardPaths, Qt, QTimer, QUrl, Signal, Slot
from PySide6.QtGui import QDesktopServices, QIcon
from PySide6.QtQuick import QQuickWindow, QSGRendererInterface
from PySide6.QtWebChannel import QWebChannel
from PySide6.QtWebEngineCore import (QWebEngineDownloadRequest, QWebEnginePage, QWebEnginePermission,
                                     QWebEngineScript)
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWidgets import QFileDialog, QMainWindow, QWidget

from .server import DOWN, UP, ControlPlane

# The status page's Retry link; the page never loads it, the window acts on it.
RETRY = QUrl("http://retry.orchestrate.invalid/")
# The app icon, the same file the UI serves as its favicon.
ICON = Path(__file__).resolve().parents[1] / "ui" / "icon.svg"
# The built UI (`pixi run build-ui`): the status page takes its stylesheets from it.
UI_DIST = Path(__file__).resolve().parents[1] / "ui" / "dist"


def prepare_qt() -> None:
    """How the window is drawn; call before the QApplication is made.

    With no OpenGL, EGL or GLX in this process at all: Qt Quick's software
    renderer, Chromium without its GPU process, and on Linux no GL in Qt's
    platform plugins, so a Wayland window is drawn into shared-memory buffers
    and an X11 window never touches GLX. The page is a dashboard. The GL this
    environment's Qt would find is what broke: conda-forge's Mesa has no
    Wayland EGL ("qt.qpa.wayland: EGL not available"), and the system's GLX
    can fail to match its Qt ("Could not initialize GLX", then an abort).

    Dialogs on Linux go through the XDG desktop portal, so the folder dialog
    is the desktop's own (GNOME's, KDE's, …) on Wayland and X11 alike.
    Chromium is pointed at the system's D-Bus: conda-forge's libdbus looks
    for it inside the environment (<env>/var/run/dbus), where it never is.
    Variables already set still decide."""
    if sys.platform == "linux":
        os.environ.setdefault("DBUS_SYSTEM_BUS_ADDRESS", "unix:path=/run/dbus/system_bus_socket")
        os.environ.setdefault("QT_WAYLAND_CLIENT_BUFFER_INTEGRATION", "none")
        os.environ.setdefault("QT_XCB_GL_INTEGRATION", "none")
        os.environ.setdefault("QT_QPA_PLATFORMTHEME", "xdgdesktopportal")
    QQuickWindow.setGraphicsApi(QSGRendererInterface.GraphicsApi.Software)
    flags = os.environ.get("QTWEBENGINE_CHROMIUM_FLAGS", "")
    os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = f"{flags} --disable-gpu".strip()


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
    """What the window shows until the control plane answers, in the UI's own
    look: its stylesheets and icon from ui/dist (setHtml gives the page that
    folder as its base, see MainWindow._show_pending). The steps of starting
    are filled in by setSteps (MainWindow._push_steps), the one that is
    running with how long it has run, a failed one with why."""
    retry = f'<p><a class="retry" href="{RETRY.toString()}">Retry</a></p>' if state == DOWN else ""
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Orchestrator</title>
<link rel="stylesheet" href="ui.css"><link rel="stylesheet" href="app.css">
<style>
.boot {{ max-width: 640px; margin: 0 auto; padding: 72px 16px; }}
.boot-head {{ display: flex; align-items: center; gap: 12px; margin-bottom: 20px; }}
.boot-head img {{ width: 36px; height: 36px; }}
.boot-head h1 {{ font-size: 24px; margin: 0; }}
.boot-text {{ line-height: 1.5; margin: 0 0 16px; overflow-wrap: anywhere; }}
.boot-text.down {{ color: var(--destructive); }}
.boot-steps {{ list-style: none; margin: 0; padding: 0; border-top: 1px solid var(--border); }}
.boot-steps li {{ display: grid; grid-template-columns: 20px 1fr auto; gap: 8px; align-items: baseline;
  padding: 8px 0; border-bottom: 1px solid var(--border); }}
.boot-steps .done {{ color: var(--muted-foreground); }}
.boot-steps .failed {{ color: var(--destructive); }}
.boot-steps .why {{ grid-column: 2 / 4; overflow-wrap: anywhere; }}
.spin {{ display: inline-block; width: 12px; height: 12px; border: 2px solid var(--border);
  border-top-color: var(--foreground); border-radius: 50%; animation: spin 0.8s linear infinite; }}
@keyframes spin {{ to {{ transform: rotate(360deg); }} }}
.retry {{ display: inline-block; margin-top: 20px; padding: 6px 14px; border: 1px solid var(--border);
  border-radius: 6px; color: var(--foreground); text-decoration: none; }}
.retry:hover {{ background: var(--muted); }}
</style></head><body><div class="boot">
<div class="boot-head"><img src="icon.svg" alt=""><h1>Orchestrator</h1></div>
<p class="boot-text{' down' if state == DOWN else ''}">{html.escape(text)}</p>
<ol class="boot-steps" id="steps"></ol>{retry}
</div><script>
let steps = [];
const esc = (s) => s.replace(/[&<>"]/g, (c) => ({{"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}})[c]);
function render() {{
  const now = Date.now() / 1000;
  document.getElementById("steps").innerHTML = steps.map((s) => {{
    const cls = s.error ? "failed" : s.done ? "done" : "";
    const mark = s.error ? "✕" : s.done ? "✓" : '<span class="spin"></span>';
    const took = s.done || s.error ? "" : `${{Math.floor(now - s.at)}}s`;
    const why = s.error ? `<span class="why mono">${{esc(s.error)}}</span>` : "";
    return `<li class="${{cls}}"><span>${{mark}}</span><span>${{esc(s.what)}}</span>`
         + `<span class="mono muted">${{took}}</span>${{why}}</li>`;
  }}).join("");
}}
window.setSteps = (s) => {{ steps = s; render(); }};
setInterval(render, 1000);
</script></body></html>"""


def host_script() -> QWebEngineScript:
    """host.js, with the qwebchannel.js it needs from Qt's resources, run in
    the page before the UI's own scripts."""
    f = QFile(":/qtwebchannel/qwebchannel.js")
    if not f.open(QIODevice.OpenModeFlag.ReadOnly):
        raise RuntimeError("Qt's qwebchannel.js is missing from QtWebChannel's resources")
    channel = bytes(f.readAll()).decode()
    f.close()
    s = QWebEngineScript()
    s.setName("orchestrate-host")
    s.setSourceCode(channel + "\n" + (Path(__file__).with_name("host.js")).read_text())
    s.setInjectionPoint(QWebEngineScript.InjectionPoint.DocumentCreation)
    s.setWorldId(QWebEngineScript.ScriptWorldId.MainWorld)
    s.setRunsOnSubFrames(False)
    return s


class Host(QObject):
    """What the window does for the UI's page (desktop/host.js): show the OS's
    folder dialog. The answer comes back as folderPicked(id, path), "" when
    cancelled, so the page never waits on a dialog inside a call."""

    folderPicked = Signal(int, str)

    def __init__(self, window: QWidget) -> None:
        super().__init__(window)
        self.window_ = window

    @Slot(int, str)
    def pickFolder(self, id: int, start: str) -> None:
        start = os.path.expanduser(start.strip())
        d = QFileDialog(self.window_, "Choose a folder", start if os.path.isdir(start) else QDir.homePath())
        d.setFileMode(QFileDialog.FileMode.Directory)
        d.setOption(QFileDialog.Option.ShowDirsOnly)
        d.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        picked = [""]
        d.fileSelected.connect(lambda path: picked.__setitem__(0, path))
        d.finished.connect(lambda _: self.folderPicked.emit(id, picked[0]))
        d.open()


class NewWindow(QWebEnginePage):
    """What a new window (window.open, target=_blank) gets instead of one: it
    takes the first navigation, sends it where a link of the page would go
    (this view, or the default browser), and is gone."""

    def __init__(self, page: "Page") -> None:
        super().__init__(page)
        self.page_ = page

    def acceptNavigationRequest(self, url, kind, is_main_frame) -> bool:
        if self.page_.external(url):
            QDesktopServices.openUrl(url)
        else:
            self.page_.view.load(url)
        self.deleteLater()
        return False


class Page(QWebEnginePage):
    """Keeps the UI in one window: a new window is this view instead. A link
    off the control plane (a URL in an agent's reply, say) opens in the
    default browser, and the status page's Retry asks the control plane to
    start again."""

    def __init__(self, view: QWebEngineView, control: ControlPlane) -> None:
        super().__init__(view)
        self.view = view
        self.control = control
        self.permissionRequested.connect(self._permission)

    def _permission(self, p: QWebEnginePermission) -> None:
        """The UI may use the clipboard (navigator.clipboard, which a browser
        grants a localhost page on a click; Qt asks the window instead, and an
        unanswered request leaves the page's promise pending forever). Anything
        else, or any other origin, is refused."""
        if (p.permissionType() == QWebEnginePermission.PermissionType.ClipboardReadWrite
                and not self.external(p.origin()) and p.origin().scheme() in ("http", "https")):
            p.grant()
        else:
            p.deny()

    def external(self, url: QUrl) -> bool:
        """A web or mail link anywhere but the control plane."""
        if url.scheme() == "mailto":
            return True
        if url.scheme() not in ("http", "https"):
            return False  # the status page (data:), about:blank
        home = QUrl(self.control.base_url)
        return (url.scheme(), url.host(), url.port()) != (home.scheme(), home.host(), home.port())

    def createWindow(self, kind):
        return NewWindow(self)

    def acceptNavigationRequest(self, url, kind, is_main_frame) -> bool:
        if url == RETRY:
            # Starting shows the status page again, which must not happen
            # inside this navigation's own callback.
            QTimer.singleShot(0, self.control.start)
            return False
        if self.external(url):
            QDesktopServices.openUrl(url)
            return False
        return super().acceptNavigationRequest(url, kind, is_main_frame)


class MainWindow(QMainWindow):
    def __init__(self, control: ControlPlane) -> None:
        super().__init__()
        self.control = control
        self.download_dir = Path(QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DownloadLocation))
        self.setWindowTitle("Orchestrator")
        self.setWindowIcon(QIcon(str(ICON)))
        self.resize(1280, 820)
        self.view = QWebEngineView(self)
        self.view.setPage(Page(self.view, control))
        self.host = Host(self)
        self.channel = QWebChannel(self)
        self.channel.registerObject("host", self.host)
        self.view.page().setWebChannel(self.channel)
        self.view.page().scripts().insert(host_script())
        self.view.titleChanged.connect(self.setWindowTitle)
        self.view.page().profile().downloadRequested.connect(self._download)
        self.setCentralWidget(self.view)
        # What the view should show next, held while a page is still loading:
        # a load started meanwhile can lose to the one in flight (the status
        # page can land after the UI it should have given way to).
        self.loading = False
        self.pending: tuple[str, str] | None = None
        self.view.loadStarted.connect(lambda: setattr(self, "loading", True))
        self.view.loadFinished.connect(self._loaded)
        self.showing_status = False  # the status page, not the UI, is in the view
        control.changed.connect(self._control_changed)
        control.stepped.connect(self._stepped)
        self.steps = "[]"  # the control plane's latest steps of starting, as JSON
        control.start()

    def _control_changed(self, state: str, text: str) -> None:
        self.pending = (state, text)
        if not self.loading:
            self._show_pending()

    def _stepped(self, steps: str) -> None:
        self.steps = steps
        if self.showing_status and not self.loading:
            self._push_steps()

    def _push_steps(self) -> None:
        self.view.page().runJavaScript(f"setSteps({self.steps})")

    def _loaded(self, ok: bool) -> None:
        self.loading = False
        if self.pending is not None:
            self._show_pending()
        elif self.showing_status:
            self._push_steps()

    def _show_pending(self) -> None:
        (state, text), self.pending = self.pending, None
        self.loading = True
        self.showing_status = state != UP
        if state == UP:
            self.statusBar().showMessage(text, 8000)
            self.view.load(QUrl(self.control.base_url + "/"))
        else:
            self.view.setHtml(status_page(state, text), QUrl.fromLocalFile(str(UI_DIST) + "/"))

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
