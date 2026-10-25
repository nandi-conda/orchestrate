"""The desktop client: one window. The project list and each project replace
each other in it, and a thread's Dialog, Tools, Files and Worker log are tabs
of its pane, so nothing opens another window."""

from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QStandardPaths, Qt, QTimer, Signal
from PySide6.QtWidgets import (QButtonGroup, QFrame, QHBoxLayout, QLineEdit, QListWidget, QListWidgetItem,
                               QMainWindow, QPlainTextEdit, QPushButton, QSizePolicy, QSplitter, QStackedWidget,
                               QTabWidget, QVBoxLayout, QWidget)

from .api import Api, q
from .model import K_ERROR, K_ROUTE, K_TOOL_CALL, counts, dialog_items, is_tool, join_path, size_text
from .widgets import STYLE, Column, EventRow, badge, label, one_line, restyle, thread_tag

AUTO, TO_THREAD, NEW_THREAD = range(3)
DIALOG, TOOLS, FILES, WLOG = range(4)
# Events arriving together are fetched once, this long after the first.
REFRESH_MS = 150
WLOG_LINES = 300


def vbox(w: QWidget | None = None, margins: tuple = (0, 0, 0, 0), spacing: int = 6) -> QVBoxLayout:
    lay = QVBoxLayout(w) if w is not None else QVBoxLayout()
    lay.setContentsMargins(*margins)
    lay.setSpacing(spacing)
    return lay


def hbox(w: QWidget | None = None, margins: tuple = (0, 0, 0, 0), spacing: int = 6) -> QHBoxLayout:
    lay = QHBoxLayout(w) if w is not None else QHBoxLayout()
    lay.setContentsMargins(*margins)
    lay.setSpacing(spacing)
    return lay


def button(text: str, on_click, flat: bool = False) -> QPushButton:
    b = QPushButton(text)
    b.setFlat(flat)
    b.clicked.connect(on_click)
    return b


def free_path(folder: Path, name: str) -> Path:
    """folder/name, or folder/"name (n)" when that is taken: a download never
    overwrites a file."""
    p = folder / name
    stem, suffix, n = p.stem, p.suffix, 1
    while p.exists():
        p = folder / f"{stem} ({n}){suffix}"
        n += 1
    return p


# ---------------------------------------------------------------- projects

class ProjectsPage(QWidget):
    """The project list, with a form for a new project."""

    open_project = Signal(str)

    def __init__(self, api: Api) -> None:
        super().__init__()
        self.api = api
        self.projects: list[dict] = []
        self.creating = False
        lay = vbox(self, (32, 24, 32, 24), 12)
        top = hbox()
        top.addWidget(label("Projects", "h1"))
        top.addStretch(1)
        self.new_button = button("New project", self.show_form)
        top.addWidget(self.new_button)
        lay.addLayout(top)
        self.error = label(name="error", wrap=True)
        self.error.hide()
        lay.addWidget(self.error)

        self.form = QFrame()
        self.form.setFrameShape(QFrame.Shape.StyledPanel)
        f = vbox(self.form, (12, 12, 12, 12))
        f.addWidget(label("New project: a repo plus the threads working on it.", "muted"))
        f.addWidget(label("Name"))
        self.name = QLineEdit()
        self.name.setPlaceholderText("my-app")
        self.name.returnPressed.connect(self.create)
        f.addWidget(self.name)
        f.addWidget(label("Git repo URL (optional)"))
        self.repo = QLineEdit()
        self.repo.setPlaceholderText("https://github.com/you/repo")
        self.repo.returnPressed.connect(self.create)
        f.addWidget(self.repo)
        actions = hbox()
        actions.addStretch(1)
        actions.addWidget(button("Cancel", self.hide_form))
        self.create_button = button("Create", self.create)
        self.create_button.setDefault(True)
        actions.addWidget(self.create_button)
        f.addLayout(actions)
        self.form.hide()
        lay.addWidget(self.form)

        self.status = label("Loading…", "muted")
        lay.addWidget(self.status)
        self.list = QListWidget()
        self.list.setSpacing(2)
        self.list.itemClicked.connect(lambda it: self.open_project.emit(it.data(Qt.ItemDataRole.UserRole)))
        lay.addWidget(self.list, 1)

    def refresh(self) -> None:
        self.status.setText("Loading…")
        self.status.show()
        self.api.get_json("/api/projects", self._got)

    def _got(self, data, err: str) -> None:
        self.set_error(err)
        if not err:
            self.projects = data
        self.list.clear()
        for p in self.projects:
            it = QListWidgetItem()
            it.setData(Qt.ItemDataRole.UserRole, p["id"])
            it.setToolTip("Open")
            row = QWidget()
            r = vbox(row, (10, 8, 10, 8), 2)
            name = label(p["name"])
            name.setStyleSheet("font-weight: 600; font-size: 15px;")
            r.addWidget(name)
            r.addWidget(label(p.get("repo_url") or "no repo", "muted"))
            r.addWidget(label(p["id"], "threadId"))
            it.setSizeHint(row.sizeHint())
            self.list.addItem(it)
            self.list.setItemWidget(it, row)
        self.status.setText("No projects yet. A project is a repo plus the threads working on it; "
                            "create one with New project.")
        self.status.setVisible(not self.projects)

    def set_error(self, err: str) -> None:
        self.error.setText(err)
        self.error.setVisible(bool(err))

    def show_form(self) -> None:
        self.form.show()
        self.name.setFocus()

    def hide_form(self) -> None:
        self.form.hide()

    def create(self) -> None:
        name = self.name.text().strip()
        if not name or self.creating:
            return
        self.creating = True
        self.create_button.setText("Creating…")
        self.api.post_json("/api/projects", {"name": name, "repo_url": self.repo.text().strip()}, self._created)

    def _created(self, data, err: str) -> None:
        self.creating = False
        self.create_button.setText("Create")
        self.set_error(err)
        if err:
            return
        self.name.clear()
        self.repo.clear()
        self.form.hide()
        self.open_project.emit(data["id"])


# ---------------------------------------------------------------- one project

class ThreadRow(QWidget):
    """A thread in the list: title and state, what it is doing now (else its
    summary or request), and its id."""

    def __init__(self, t: dict) -> None:
        super().__init__()
        lay = vbox(self, (14 if t.get("parent_id") else 4, 4, 4, 4), 1)
        top = hbox()
        title = label(t["title"])
        title.setStyleSheet("font-weight: 600;")
        title.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        top.addWidget(title, 1)
        top.addWidget(badge(t["state"]))
        lay.addLayout(top)
        if t.get("activity"):
            now = hbox()
            now.addWidget(label("●", "dot"))
            act = label(t["activity"], "mono")
            act.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
            now.addWidget(act, 1)
            lay.addLayout(now)
        else:
            summary = label(one_line(t.get("summary") or t.get("request") or "", 120), "threadSummary", wrap=True)
            lay.addWidget(summary)
        lay.addWidget(label(t["id"], "threadId"))


class ProjectPage(QWidget):
    """One project: its threads on the left; on the right the pane (Project
    activity, or the selected thread) and the request box."""

    home = Signal()
    title_changed = Signal(str)

    def __init__(self, api: Api) -> None:
        super().__init__()
        self.api = api
        self.download_dir = Path(QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DownloadLocation))
        self.gen = 0  # bumped on entering and leaving, so late replies for another project are dropped
        self.stream = None
        self.refresh_timer = QTimer(self)
        self.refresh_timer.setSingleShot(True)
        self.refresh_timer.timeout.connect(self.refresh)
        self._build()
        self._reset()

    # ------------------------------------------------------------ layout

    def _build(self) -> None:
        lay = vbox(self, spacing=0)
        bar = QFrame()
        b = hbox(bar, (12, 8, 12, 8), 12)
        b.addWidget(button("← All projects", self.home.emit, flat=True))
        self.name = label("Project", "h1")
        b.addWidget(self.name)
        self.repo = label("", "muted")
        b.addWidget(self.repo)
        b.addStretch(1)
        lay.addWidget(bar)

        split = QSplitter(Qt.Orientation.Horizontal)
        lay.addWidget(split, 1)

        side = QWidget()
        s = vbox(side, (8, 8, 4, 8))
        head = hbox()
        self.list_title = label("Threads")
        self.list_title.setStyleSheet("font-weight: 600;")
        head.addWidget(self.list_title)
        self.list_count = badge("0")
        head.addWidget(self.list_count)
        head.addStretch(1)
        self.archived_button = button("Archived", self.toggle_archived, flat=True)
        head.addWidget(self.archived_button)
        s.addLayout(head)
        self.thread_list = QListWidget()
        self.thread_list.itemClicked.connect(self._clicked_thread)
        s.addWidget(self.thread_list, 1)
        split.addWidget(side)

        main = QWidget()
        m = vbox(main, (8, 8, 8, 8))
        split.addWidget(main)
        split.setStretchFactor(1, 1)
        split.setSizes([320, 900])

        head = hbox()
        self.pane_title = label("Project activity")
        self.pane_title.setStyleSheet("font-weight: 600; font-size: 15px;")
        head.addWidget(self.pane_title)
        self.pane_state = badge("")
        head.addWidget(self.pane_state)
        self.pane_id = label("", "threadId")
        head.addWidget(self.pane_id)
        head.addStretch(1)
        self.stop_button = button("Stop thread", self.stop, flat=True)
        self.archive_button = button("Archive", self.archive, flat=True)
        self.archive_button.setToolTip("Stops the thread if it is running and hides it from the list")
        self.unarchive_button = button("Unarchive", self.unarchive, flat=True)
        for w in (self.stop_button, self.archive_button, self.unarchive_button):
            head.addWidget(w)
        m.addLayout(head)

        self.now = QFrame()
        self.now.setObjectName("now")
        n = hbox(self.now, (10, 4, 10, 4), 8)
        n.addWidget(label("●", "dot"))
        now_label = label("Now")
        now_label.setStyleSheet("font-weight: 600;")
        n.addWidget(now_label)
        self.now_text = label("", "mono")
        self.now_text.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        n.addWidget(self.now_text, 1)
        m.addWidget(self.now)

        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        m.addWidget(self.tabs, 1)

        dialog = QWidget()
        d = vbox(dialog, (0, 6, 0, 0), 4)
        self.working = QWidget()
        self.working_rows = vbox(self.working, (12, 0, 12, 0), 0)
        d.addWidget(self.working)
        self.dialog = Column()
        d.addWidget(self.dialog, 1)
        self.tabs.addTab(dialog, "Dialog")
        self.tools = Column()
        self.tools.rows.setSpacing(0)
        self.tabs.addTab(self.tools, "Tools")
        self.tabs.addTab(self._files_tab(), "Files")
        self.tabs.addTab(self._wlog_tab(), "Worker log")
        self.tabs.currentChanged.connect(self._tab_changed)

        self.route = QFrame()
        self.route.setObjectName("route")
        r = hbox(self.route, (10, 4, 10, 4), 8)
        route_label = label("Router")
        route_label.setStyleSheet("font-weight: 600;")
        r.addWidget(route_label)
        self.route_text = label("")
        self.route_text.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        r.addWidget(self.route_text, 1)
        m.addWidget(self.route)
        self.error = label(name="error", wrap=True)
        m.addWidget(self.error)

        modes = hbox(spacing=0)
        self.mode_group = QButtonGroup(self)
        self.mode_buttons = {}
        for mode, text in ((AUTO, "Router decides"), (TO_THREAD, "This thread"), (NEW_THREAD, "New thread")):
            mb = QPushButton(text)
            mb.setObjectName("mode")
            mb.setCheckable(True)
            self.mode_group.addButton(mb, mode)
            self.mode_buttons[mode] = mb
            modes.addWidget(mb)
        modes.addStretch(1)
        self.mode_group.idClicked.connect(self._set_mode)
        m.addLayout(modes)

        row = hbox()
        self.input = QLineEdit()
        self.input.returnPressed.connect(self.send)
        row.addWidget(self.input, 1)
        self.send_button = button("Send", self.send)
        row.addWidget(self.send_button)
        m.addLayout(row)

    def _files_tab(self) -> QWidget:
        w = QWidget()
        lay = vbox(w, (8, 8, 8, 8))
        bar = hbox()
        self.files_path = label("/", "mono")
        bar.addWidget(self.files_path, 1)
        self.files_up = button("Up", self.files_go_up, flat=True)
        bar.addWidget(self.files_up)
        self.files_refresh = button("Refresh", self.fetch_files, flat=True)
        bar.addWidget(self.files_refresh)
        lay.addLayout(bar)
        self.files_error = label(name="error", wrap=True)
        lay.addWidget(self.files_error)
        self.files_list = QListWidget()
        self.files_list.itemClicked.connect(self._clicked_file)
        lay.addWidget(self.files_list, 1)
        self.files_status = label("", "muted", wrap=True)
        lay.addWidget(self.files_status)
        return w

    def _wlog_tab(self) -> QWidget:
        w = QWidget()
        lay = vbox(w, (8, 8, 8, 8))
        bar = hbox()
        bar.addWidget(label(f"Last {WLOG_LINES} lines of this thread's worker", "muted"), 1)
        self.wlog_refresh = button("Refresh", self.fetch_wlog, flat=True)
        bar.addWidget(self.wlog_refresh)
        lay.addLayout(bar)
        self.wlog_error = label(name="error", wrap=True)
        lay.addWidget(self.wlog_error)
        self.wlog_text = QPlainTextEdit()
        self.wlog_text.setReadOnly(True)
        self.wlog_text.setStyleSheet("font-family: monospace;")
        lay.addWidget(self.wlog_text, 1)
        return w

    # ------------------------------------------------------------ state

    def _reset(self) -> None:
        self.pid = ""
        self.project: dict | None = None
        self.threads: list[dict] = []
        self.transcript: list[dict] = []
        self.plog: list[dict] = []
        self.sel = ""
        self.mode = AUTO
        self.show_archived = False
        self.sending = False
        self.last_text = ""
        self.route_line = ""
        self.err = ""
        self.wlog: str | None = None
        self.wlog_err = ""
        self.wlog_loading = False
        self.files: dict | None = None
        self.fpath = ""
        self.files_err = ""
        self.files_loading = False
        self.input.clear()
        self.dialog.opened.clear()
        self.tools.opened.clear()
        self.files_status.clear()
        self.tabs.blockSignals(True)
        self.tabs.setCurrentIndex(DIALOG)
        self.tabs.blockSignals(False)

    def enter(self, pid: str) -> None:
        self.leave()
        self.pid = pid
        self.api.get_json(f"/api/projects/{pid}", self._call(self._got_project))
        self.fetch_threads()
        self.fetch_log()
        self.stream = self.api.stream(f"/api/projects/{pid}/events")
        self.stream.event.connect(self.on_event)
        self.render()

    def leave(self) -> None:
        """Close the open project's stream and forget its state."""
        self.gen += 1
        self.refresh_timer.stop()
        if self.stream is not None:
            self.stream.close()
            self.stream = None
        self._reset()
        self.render()

    def _call(self, done):
        """done, for a reply to this project only."""
        gen = self.gen
        return lambda data, err: gen == self.gen and done(data, err)

    def set_error(self, err: str) -> None:
        self.err = err
        self.render_composer()

    def take(self, attr: str, data, err: str) -> bool:
        """Keep a fresh reply in attr; on error keep the old one and show the error."""
        if err:
            self.set_error(err)
            return False
        self.err = ""
        setattr(self, attr, data)
        return True

    def selected(self) -> dict | None:
        return next((t for t in self.threads if t["id"] == self.sel), None)

    def title_of(self, tid: str) -> str:
        return next((t["title"] for t in self.threads if t["id"] == tid), tid)

    # ------------------------------------------------------------ fetches

    def _got_project(self, data, err: str) -> None:
        if self.take("project", data, err):
            self.title_changed.emit(data["name"])
            self.render_head()

    def fetch_threads(self) -> None:
        path = f"/api/projects/{self.pid}/threads" + ("?archived=1" if self.show_archived else "")
        self.api.get_json(path, self._call(self._got_threads))

    def _got_threads(self, data, err: str) -> None:
        if self.take("threads", data, err):
            self.render_threads()
            self.render_pane()

    def fetch_log(self) -> None:
        self.api.get_json(f"/api/projects/{self.pid}/log", self._call(self._got_log))

    def _got_log(self, data, err: str) -> None:
        if self.take("plog", data, err) and not self.sel:
            self.render_pane()

    def fetch_transcript(self, to_end: bool = False) -> None:
        tid = self.sel
        if not tid:
            return

        def done(data, err):
            if tid == self.sel and self.take("transcript", data, err):
                self.render_pane(to_end)
        self.api.get_json(f"/api/threads/{tid}/transcript", self._call(done))

    def fetch_wlog(self) -> None:
        tid = self.sel
        if not tid:
            return
        self.wlog_loading = True

        def done(text, err):
            if tid != self.sel:
                return
            self.wlog_loading = False
            self.wlog_err = err
            if not err:
                self.wlog = text
            self.render_wlog()
        self.api.get_text(f"/api/threads/{tid}/worker-logs?lines={WLOG_LINES}", self._call(done))
        self.render_wlog()

    def fetch_files(self) -> None:
        """The directory fpath of the selected thread's worktree, from its worker."""
        tid = self.sel
        if not tid:
            return
        self.files_loading = True

        def done(data, err):
            if tid != self.sel:
                return
            self.files_loading = False
            self.files_err = err
            if not err:
                self.files = data
                self.fpath = data["path"]  # the listing names its own, normalized, path
            self.render_files()
        self.api.get_json(f"/api/threads/{tid}/files?path={q(self.fpath)}", self._call(done))
        self.render_files()

    def refresh(self) -> None:
        if not self.pid:
            return
        self.fetch_threads()
        if self.sel:
            self.fetch_transcript()
        else:
            self.fetch_log()

    def on_event(self, e: dict) -> None:
        if e.get("kind") == K_ROUTE:
            self.route_line = e.get("text", "")
            self.render_composer()
        elif e.get("kind") == K_ERROR and not e.get("thread_id"):
            self.set_error(e.get("text", ""))
        if not self.refresh_timer.isActive():
            self.refresh_timer.start(REFRESH_MS)

    # ------------------------------------------------------------ actions

    def select(self, tid: str) -> None:
        self.sel = tid
        if self.mode == AUTO and not self.show_archived:
            self.mode = TO_THREAD
        self.transcript = []
        self.dialog.opened.clear()
        self.tools.opened.clear()
        self.wlog, self.wlog_err = None, ""
        self.files, self.fpath, self.files_err = None, "", ""
        self.files_status.clear()
        self.fetch_transcript(to_end=True)
        if self.tabs.currentIndex() == WLOG:
            self.fetch_wlog()
        if self.tabs.currentIndex() == FILES:
            self.fetch_files()
        self.render()

    def show_activity(self) -> None:
        """Select nothing: the pane shows the project's activity."""
        self.sel = ""
        if self.mode == TO_THREAD:
            self.mode = AUTO
        if self.tabs.currentIndex() in (WLOG, FILES):
            self.tabs.blockSignals(True)
            self.tabs.setCurrentIndex(DIALOG)
            self.tabs.blockSignals(False)
        self.fetch_log()
        self.render()

    def open_thread(self, tid: str) -> None:
        """Select tid ("" for Project activity) from a click. Selecting
        rebuilds the rows that were clicked, so it runs after the click is
        handled, not inside it."""
        QTimer.singleShot(0, lambda: self.select(tid) if tid else self.show_activity())

    def _clicked_thread(self, item: QListWidgetItem) -> None:
        self.open_thread(item.data(Qt.ItemDataRole.UserRole))

    def _set_mode(self, mode: int) -> None:
        self.mode = mode
        self.render_composer()

    def send(self) -> None:
        """The box clears at once: the control plane takes the request and
        returns, and what becomes of it arrives on the event stream."""
        text = self.input.text().strip()
        if not text or self.sending:
            return
        self.sending = True
        self.last_text = text
        self.input.clear()
        self.err = ""
        body: dict = {"text": text}
        if self.mode == TO_THREAD and self.sel:
            body.update(mode="thread", target=self.sel)
        elif self.mode == NEW_THREAD:
            body["mode"] = "new"
        else:
            body["mode"] = "auto"
            self.route_line = "routing…"
        self.api.post_json(f"/api/projects/{self.pid}/requests", body, self._call(self._sent))
        self.render_composer()

    def _sent(self, data, err: str) -> None:
        # A spawned thread never takes the window with it: the pane stays
        # where it is, and a thread opens only when it is clicked.
        self.sending = False
        self.err = err
        if err and not self.input.text():
            self.input.setText(self.last_text)
        self.render_composer()

    def _post_thread(self, action: str, done) -> None:
        self.api.post_json(f"/api/threads/{self.sel}/{action}", {}, self._call(done))

    def stop(self) -> None:
        if self.sel:
            self._post_thread("stop", lambda d, e: (self.set_error(e), self.fetch_threads()))

    def archive(self) -> None:
        def done(data, err):
            # The control plane stopped the thread if it was running; it leaves
            # the list, so the pane goes back to the project's activity.
            self.set_error(err)
            if not err:
                self.show_activity()
                self.fetch_threads()
        if self.sel:
            self._post_thread("archive", done)

    def unarchive(self) -> None:
        def done(data, err):
            # Back to the live list, with the thread still selected.
            self.set_error(err)
            if not err:
                self.show_archived = False
                self.fetch_threads()
                self.fetch_transcript()
                self.render()
        if self.sel:
            self._post_thread("unarchive", done)

    def toggle_archived(self) -> None:
        self.show_archived = not self.show_archived
        self.threads = []
        self.show_activity()
        self.fetch_threads()

    def _tab_changed(self, index: int) -> None:
        if index == WLOG and self.wlog is None:
            self.fetch_wlog()
        if index == FILES and self.files is None:
            self.fetch_files()

    def files_go_up(self) -> None:
        self.fpath = join_path(self.fpath, "..")
        self.fetch_files()

    def _clicked_file(self, item: QListWidgetItem) -> None:
        name, is_dir = item.data(Qt.ItemDataRole.UserRole)
        path = join_path(self.fpath, name)
        if is_dir:
            self.fpath = path
            QTimer.singleShot(0, self.fetch_files)  # it rebuilds the list that was clicked
            return
        self.download(path, name)

    def download(self, path: str, name: str) -> None:
        """Save a file of the selected thread's worktree to the downloads folder."""
        dest = free_path(self.download_dir, name)
        self.files_status.setText(f"Saving {name}…")

        def done(err):
            self.files_status.setText(err or f"Saved {name} to {dest}")
        self.api.download(f"/api/threads/{self.sel}/files/raw?path={q(path)}", dest, done)

    # ------------------------------------------------------------ view

    def render(self, to_end: bool = False) -> None:
        self.render_head()
        self.render_threads()
        self.render_pane(to_end)
        self.render_composer()

    def render_head(self) -> None:
        p = self.project or {}
        self.name.setText(p.get("name", "Project"))
        self.repo.setText(p.get("repo_url", ""))

    def render_threads(self) -> None:
        self.list_title.setText("Archived" if self.show_archived else "Threads")
        self.list_count.setText(str(len(self.threads)))
        self.archived_button.setText("← Threads" if self.show_archived else "Archived")
        lst = self.thread_list
        scroll = lst.verticalScrollBar().value()
        lst.clear()
        top = QListWidgetItem()
        top.setData(Qt.ItemDataRole.UserRole, "")
        row = QWidget()
        r = vbox(row, (4, 4, 4, 4), 1)
        title = label("Project activity")
        title.setStyleSheet("font-weight: 600;")
        r.addWidget(title)
        r.addWidget(label("Router decisions and every thread's events", "threadSummary"))
        top.setSizeHint(row.sizeHint())
        lst.addItem(top)
        lst.setItemWidget(top, row)
        if not self.sel:
            lst.setCurrentItem(top)
        for t in self.threads:
            it = QListWidgetItem()
            it.setData(Qt.ItemDataRole.UserRole, t["id"])
            w = ThreadRow(t)
            it.setSizeHint(w.sizeHint())
            lst.addItem(it)
            lst.setItemWidget(it, w)
            if t["id"] == self.sel:
                lst.setCurrentItem(it)
        if self.show_archived and not self.threads:
            empty = QListWidgetItem("No archived threads.")
            empty.setFlags(Qt.ItemFlag.NoItemFlags)
            lst.addItem(empty)
        lst.verticalScrollBar().setValue(scroll)

    def render_pane(self, to_end: bool = False) -> None:
        activity = not self.sel
        events = self.plog if activity else self.transcript
        t = self.selected()
        if activity:
            self.pane_title.setText("Project activity")
        else:
            self.pane_title.setText(t["title"] if t else self.sel)
        self.pane_state.setVisible(t is not None)
        if t is not None:
            self.pane_state.setText(t["state"])
            self.pane_state.setProperty("state", t["state"])
            restyle(self.pane_state)
        self.pane_id.setText(self.sel)
        archived = t is not None and t.get("archived_at") is not None
        self.stop_button.setVisible(not activity and not archived)
        self.archive_button.setVisible(not activity and not archived)
        self.unarchive_button.setVisible(archived)
        now = (t or {}).get("activity") or ""
        self.now_text.setText(now)
        self.now.setVisible(bool(now))

        said, tools = counts(events, activity)
        self.tabs.setTabText(DIALOG, f"Dialog  {said}")
        self.tabs.setTabText(TOOLS, f"Tools  {tools}")
        self.tabs.setTabVisible(FILES, not activity)
        self.tabs.setTabVisible(WLOG, not activity)

        self._render_working(activity)
        self._render_dialog(events, activity, to_end)
        self._render_tools(events, activity, to_end)
        self.render_files()
        self.render_wlog()

    def _render_working(self, activity: bool) -> None:
        """Project activity's live line per working thread: its title and what
        it is doing, instead of every thread's reasoning."""
        while self.working_rows.count():
            w = self.working_rows.takeAt(0).widget()
            w.setParent(None)
            w.deleteLater()
        rows = [t for t in self.threads if t.get("activity")] if activity else []
        for t in rows:
            b = QPushButton(f"●  {t['title']}    {one_line(t['activity'], 120)}")
            b.setObjectName("working")
            b.setFlat(True)
            b.clicked.connect(lambda _=False, tid=t["id"]: self.open_thread(tid))
            self.working_rows.addWidget(b)
        self.working.setVisible(bool(rows))

    def _tag(self, e: dict, activity: bool):
        tid = e.get("thread_id")
        if not activity or not tid:
            return None
        return thread_tag(self.title_of(tid), lambda: self.open_thread(tid))

    def _render_dialog(self, events: list[dict], activity: bool, to_end: bool) -> None:
        items = dialog_items(events, activity)
        titles = tuple((t["id"], t["title"]) for t in self.threads) if activity else ()
        sig = (self.sel, titles, tuple((e.get("seq"), e.get("text"), e.get("live")) for _, es in items for e in es))

        def build():
            rows = []
            for what, es in items:
                if what == "thinking":
                    first, last = es[0], es[-1]
                    title = "Thinking" + (f"   {len(es)} steps" if len(es) > 1 else "")
                    body = "\n\n".join(f"{i + 1}. {e.get('text', '')}" for i, e in enumerate(es))
                    rows.append(self.dialog.fold(("thinking", first.get("seq")), title, last.get("text", ""),
                                                 body, self._tag(first, activity)))
                else:
                    rows.append(EventRow(es[0], self._tag(es[0], activity)))
            if not rows:
                if events:
                    text = "Nothing said yet. Tool calls are under Tools."
                elif activity:
                    text = "Nothing yet. Type a request below; the router spawns threads for it."
                else:
                    text = "Waiting for this thread's first event…"
                rows.append(label(text, "muted"))
            return rows
        self.dialog.show_rows(sig, build, to_end)

    def _render_tools(self, events: list[dict], activity: bool, to_end: bool) -> None:
        tools = [e for e in events if is_tool(e)]
        sig = (self.sel, tuple((e.get("seq"), e.get("text"), e.get("live")) for e in tools))

        def build():
            rows = []
            for e in tools:
                f = self.tools.fold(("tool", e.get("seq")), str(e.get("label", "")), e.get("text", ""),
                                    e.get("text", ""), self._tag(e, activity))
                if e.get("kind") != K_TOOL_CALL:  # a result sits under its call
                    f.setContentsMargins(22, 0, 0, 0)
                rows.append(f)
            if not rows:
                rows.append(label("No tool calls yet.", "muted"))
            return rows
        self.tools.show_rows(sig, build, to_end)

    def render_files(self) -> None:
        self.files_path.setText("/" + self.fpath)
        self.files_up.setVisible(bool(self.fpath))
        self.files_refresh.setText("Loading…" if self.files_loading else "Refresh")
        self.files_error.setText(self.files_err)
        self.files_error.setVisible(bool(self.files_err))
        self.files_list.clear()
        if self.files_err or self.files is None:
            return
        entries = self.files["entries"]
        if not entries:
            empty = QListWidgetItem("This directory is empty.")
            empty.setFlags(Qt.ItemFlag.NoItemFlags)
            self.files_list.addItem(empty)
        for e in entries:
            text = e["name"] + "/" if e["dir"] else f"{e['name']}    {size_text(e['size'])}"
            it = QListWidgetItem(text)
            it.setData(Qt.ItemDataRole.UserRole, (e["name"], e["dir"]))
            it.setToolTip("Open" if e["dir"] else f"Download to {self.download_dir}")
            self.files_list.addItem(it)

    def render_wlog(self) -> None:
        self.wlog_refresh.setText("Loading…" if self.wlog_loading else "Refresh")
        self.wlog_error.setText(self.wlog_err)
        self.wlog_error.setVisible(bool(self.wlog_err))
        text = "" if self.wlog_err or self.wlog is None else (self.wlog or "(empty)")
        if self.wlog_text.toPlainText() != text:
            self.wlog_text.setPlainText(text)
            bar = self.wlog_text.verticalScrollBar()
            bar.setValue(bar.maximum())

    def render_composer(self) -> None:
        # Project activity shows the router's decisions in its log; with a
        # thread open, the last one shows here instead.
        self.route_text.setText(self.route_line)
        self.route_text.setToolTip(self.route_line)
        self.route.setVisible(bool(self.route_line) and bool(self.sel))
        self.error.setText(self.err)
        self.error.setVisible(bool(self.err))
        to_thread = bool(self.sel) and not self.show_archived
        self.mode_buttons[TO_THREAD].setVisible(to_thread)
        if self.mode == TO_THREAD and not to_thread:
            self.mode = AUTO
        self.mode_buttons[self.mode].setChecked(True)
        if self.mode == TO_THREAD:
            self.input.setPlaceholderText("Message this thread…")
        else:
            self.input.setPlaceholderText("What should get done? A list fans out into one thread per item.")
        self.send_button.setText("Sending…" if self.sending else "Send")


# ---------------------------------------------------------------- window

class MainWindow(QMainWindow):
    def __init__(self, api: Api) -> None:
        super().__init__()
        self.api = api
        self.setStyleSheet(STYLE)
        self.resize(1280, 820)
        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)
        self.projects = ProjectsPage(api)
        self.project = ProjectPage(api)
        self.stack.addWidget(self.projects)
        self.stack.addWidget(self.project)
        self.projects.open_project.connect(self.open_project)
        self.project.home.connect(self.show_projects)
        self.project.title_changed.connect(lambda name: self.setWindowTitle(f"{name} · Orchestrator"))
        self.show_projects()

    def show_projects(self) -> None:
        self.project.leave()
        self.setWindowTitle("Orchestrator")
        self.stack.setCurrentWidget(self.projects)
        self.projects.refresh()

    def open_project(self, pid: str) -> None:
        self.setWindowTitle("Orchestrator")
        self.stack.setCurrentWidget(self.project)
        self.project.enter(pid)
        self.project.input.setFocus()
