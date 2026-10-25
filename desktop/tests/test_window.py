"""The window against the control plane and its echo runner, end to end:
projects, routing, the thread list, the pane's tabs, archiving and SSE."""

from PySide6.QtCore import Qt

from orchestrate_desktop.api import Api
from orchestrate_desktop.widgets import EventRow, Fold
from orchestrate_desktop.window import AUTO, DIALOG, FILES, NEW_THREAD, TO_THREAD, TOOLS, WLOG, MainWindow

WAIT = 20000


def thread_ids(page) -> list[str]:
    lst = page.thread_list
    return [lst.item(i).data(Qt.ItemDataRole.UserRole) for i in range(1, lst.count())
            if lst.item(i).data(Qt.ItemDataRole.UserRole)]


def click_thread(qtbot, page, tid: str) -> None:
    lst = page.thread_list
    item = next(lst.item(i) for i in range(lst.count()) if lst.item(i).data(Qt.ItemDataRole.UserRole) == tid)
    lst.itemClicked.emit(item)
    qtbot.waitUntil(lambda: page.sel == tid, timeout=WAIT)


def send(page, text: str) -> None:
    page.input.setText(text)
    page.send_button.click()


def dialog_texts(page) -> list[str]:
    return [w.text.text() for w in page.dialog.widgets() if isinstance(w, EventRow)]


def make_window(qtbot, url, tmp_path) -> MainWindow:
    w = MainWindow(Api(url))
    qtbot.addWidget(w)
    w.project.download_dir = tmp_path
    w.show()
    return w


def open_new_project(qtbot, w, name="demo"):
    pp = w.projects
    qtbot.waitUntil(lambda: not pp.status.text().startswith("Loading"), timeout=WAIT)
    pp.new_button.click()
    pp.name.setText(name)
    pp.create_button.click()
    page = w.project
    qtbot.waitUntil(lambda: w.stack.currentWidget() is page and page.project is not None, timeout=WAIT)
    return page


def test_route_threads_dialog_tools_and_back_to_projects(qtbot, control_plane, tmp_path):
    w = make_window(qtbot, control_plane, tmp_path)
    page = open_new_project(qtbot, w)
    assert page.name.text() == "demo" and w.windowTitle() == "demo · Orchestrator"
    assert page.mode == AUTO and not page.mode_buttons[TO_THREAD].isVisibleTo(page)

    # A list fans out into one thread per item; the stream brings them in.
    send(page, "- login page\n- export bug")
    assert page.input.text() == ""
    qtbot.waitUntil(lambda: len(thread_ids(page)) == 2
                    and all(t["state"] == "idle" for t in page.threads), timeout=WAIT)
    qtbot.waitUntil(lambda: page.route_line.startswith("started"), timeout=WAIT)
    assert not page.sel, "a spawned thread never takes the pane with it"
    assert any("login page" in t for t in dialog_texts(page))

    # Opening a thread: its transcript, This thread mode, Files and Worker log tabs.
    login = next(t["id"] for t in page.threads if t["title"] == "login page")
    click_thread(qtbot, page, login)
    assert page.mode == TO_THREAD and page.mode_buttons[TO_THREAD].isVisibleTo(page)
    assert page.tabs.isTabVisible(FILES) and page.tabs.isTabVisible(WLOG)
    qtbot.waitUntil(lambda: any("login page" in t for t in dialog_texts(page)), timeout=WAIT)
    assert page.route.isVisibleTo(page) and page.route_text.text().startswith("started")

    # Message the thread: its spawn line goes through the peer calls and the
    # child reports back; tool calls are under Tools, folded.
    send(page, "spawn: write tests")
    qtbot.waitUntil(lambda: any(t.get("parent_id") == login for t in page.threads), timeout=WAIT)
    qtbot.waitUntil(lambda: any(t.startswith("Finished:") for t in dialog_texts(page)), timeout=WAIT)
    page.tabs.setCurrentIndex(TOOLS)
    folds = [f for f in page.tools.widgets() if isinstance(f, Fold)]
    assert folds and page.tabs.tabText(TOOLS) == f"Tools  {len(folds)}"
    assert not folds[0].body.isVisibleTo(page)
    folds[0].button.click()
    assert folds[0].body.isVisibleTo(page)

    page.tabs.setCurrentIndex(WLOG)
    qtbot.waitUntil(lambda: page.wlog is not None, timeout=WAIT)
    assert not page.wlog_err

    # Project activity again: Files and Worker log go, the mode goes back.
    page.thread_list.itemClicked.emit(page.thread_list.item(0))
    qtbot.waitUntil(lambda: page.sel == "", timeout=WAIT)
    assert page.mode == AUTO and page.tabs.currentIndex() == DIALOG
    assert not page.tabs.isTabVisible(FILES)

    pid = page.pid
    page.home.emit()
    assert w.stack.currentWidget() is w.projects and page.stream is None and w.windowTitle() == "Orchestrator"
    qtbot.waitUntil(lambda: w.projects.list.count() == 1, timeout=WAIT)
    item = w.projects.list.item(0)
    assert item.data(Qt.ItemDataRole.UserRole) == pid
    w.projects.list.itemClicked.emit(item)
    qtbot.waitUntil(lambda: page.pid == pid and len(thread_ids(page)) == 3, timeout=WAIT)


def test_files_browse_and_download(qtbot, control_plane, tmp_path):
    w = make_window(qtbot, control_plane, tmp_path / "dl")
    (tmp_path / "dl").mkdir()
    page = open_new_project(qtbot, w)
    page.mode_buttons[NEW_THREAD].click()
    assert page.mode == NEW_THREAD
    send(page, "build hello\nwrite out/hello world: compiled\nwrite README.md: hi")
    qtbot.waitUntil(lambda: bool(page.threads) and page.threads[0]["state"] == "idle", timeout=WAIT)
    click_thread(qtbot, page, page.threads[0]["id"])
    page.tabs.setCurrentIndex(FILES)
    qtbot.waitUntil(lambda: page.files is not None and page.files_list.count() == 2, timeout=WAIT)
    assert [page.files_list.item(i).text() for i in range(2)] == ["out/", "README.md    3 B"]

    page.files_list.itemClicked.emit(page.files_list.item(0))
    qtbot.waitUntil(lambda: page.fpath == "out" and page.files_list.count() == 1, timeout=WAIT)
    assert page.files_path.text() == "/out" and page.files_up.isVisibleTo(page)
    page.files_list.itemClicked.emit(page.files_list.item(0))
    qtbot.waitUntil(lambda: page.files_status.text().startswith("Saved"), timeout=WAIT)
    assert (tmp_path / "dl" / "hello world").read_bytes() == b"compiled\n"
    page.files_list.itemClicked.emit(page.files_list.item(0))  # never overwrites
    qtbot.waitUntil(lambda: (tmp_path / "dl" / "hello world (1)").exists(), timeout=WAIT)

    page.files_up.click()
    qtbot.waitUntil(lambda: page.fpath == "" and page.files_list.count() == 2, timeout=WAIT)


def test_archive_and_unarchive(qtbot, control_plane, tmp_path):
    w = make_window(qtbot, control_plane, tmp_path)
    page = open_new_project(qtbot, w)
    page.mode = NEW_THREAD
    send(page, "tidy up")
    qtbot.waitUntil(lambda: bool(page.threads) and page.threads[0]["state"] == "idle", timeout=WAIT)
    tid = page.threads[0]["id"]
    click_thread(qtbot, page, tid)
    assert page.archive_button.isVisibleTo(page) and not page.unarchive_button.isVisibleTo(page)

    page.archive_button.click()
    qtbot.waitUntil(lambda: page.sel == "" and thread_ids(page) == [], timeout=WAIT)

    page.archived_button.click()
    assert page.list_title.text() == "Archived"
    qtbot.waitUntil(lambda: thread_ids(page) == [tid], timeout=WAIT)
    click_thread(qtbot, page, tid)
    assert page.unarchive_button.isVisibleTo(page) and not page.stop_button.isVisibleTo(page)
    assert not page.mode_buttons[TO_THREAD].isVisibleTo(page), "an archived thread takes no messages"

    page.unarchive_button.click()
    qtbot.waitUntil(lambda: not page.show_archived and thread_ids(page) == [tid], timeout=WAIT)
    assert page.sel == tid and page.list_title.text() == "Threads"
