"""The window: it runs the control plane in its own process, shows the ultra
UI, keeps the UI's navigation in its one view, opens other links in the
default browser, and saves downloads to the downloads folder."""

import json
import socket
import threading
import time

import httpx
import pytest
from PySide6.QtCore import QUrl

from conftest import echo_app, free_port
from desktop.server import DOWN, STARTING, UP, ControlPlane
from desktop.window import MainWindow

WAIT = 30000


def js(qtbot, w, code: str):
    """Run code in the page and return its value, or None when the page went
    away first (a navigation drops the call)."""
    with qtbot.waitCallback(timeout=3000, raising=False) as cb:
        w.view.page().runJavaScript(code, 0, cb)
    return cb.args[0] if cb.called else None


def page_text(qtbot, w) -> str:
    return js(qtbot, w, "document.body ? document.body.innerText : ''") or ""


def window(qtbot, tmp_path, port=None) -> MainWindow:
    w = MainWindow(ControlPlane(port or free_port(), echo_app(tmp_path)))
    (tmp_path / "dl").mkdir(exist_ok=True)
    w.download_dir = tmp_path / "dl"
    qtbot.addWidget(w)
    w.show()
    return w


@pytest.fixture
def up(qtbot, tmp_path):
    """A window whose control plane is up, with the UI loaded."""
    w = window(qtbot, tmp_path)
    assert w.control.state in (STARTING, UP)
    qtbot.waitUntil(lambda: w.control.state == UP, timeout=WAIT)
    qtbot.waitUntil(lambda: "Projects" in page_text(qtbot, w), timeout=WAIT)
    yield w, w.control.base_url
    w.close()


def test_the_control_plane_runs_in_the_windows_process(qtbot, up):
    w, url = up
    assert w.view.url().toString() == url + "/"
    assert w.control.thread in threading.enumerate()
    with httpx.Client(base_url=url) as c:
        pid = c.post("/api/projects", json={"name": "from-the-test"}).json()["id"]
    w.view.reload()
    qtbot.waitUntil(lambda: "from-the-test" in page_text(qtbot, w), timeout=WAIT)

    # A new window (window.open, target=_blank) on the UI is this view.
    js(qtbot, w, f"window.open({json.dumps(url + '/p/' + pid)}, '_blank'); true")
    qtbot.waitUntil(lambda: w.view.url().toString() == f"{url}/p/{pid}", timeout=WAIT)

    thread = w.control.thread
    w.close()
    assert not thread.is_alive()
    with pytest.raises(httpx.ConnectError):
        httpx.get(url + "/health")


def test_links_off_the_control_plane_open_in_the_default_browser(qtbot, up, monkeypatch):
    w, url = up
    opened = []
    monkeypatch.setattr("desktop.window.QDesktopServices.openUrl", lambda u: opened.append(u.toString()))
    page = w.view.page()
    assert not page.external(QUrl(url + "/p/x")) and page.external(QUrl("https://example.com/"))
    assert page.external(QUrl("mailto:a@example.com")) and not page.external(QUrl("data:text/html,x"))

    js(qtbot, w, """const a = document.createElement('a'); a.href = 'https://example.com/docs?x=1';
                    document.body.appendChild(a); a.click(); true""")
    qtbot.waitUntil(lambda: opened == ["https://example.com/docs?x=1"], timeout=WAIT)
    js(qtbot, w, "window.open('https://example.org/', '_blank'); true")  # a new window, off the UI
    qtbot.waitUntil(lambda: opened[-1:] == ["https://example.org/"], timeout=WAIT)
    assert w.view.url().toString() == url + "/", "the UI stays where it was"


def test_a_files_download_lands_in_the_downloads_folder(qtbot, up, tmp_path):
    w, url = up
    with httpx.Client(base_url=url, timeout=20) as c:
        pid = c.post("/api/projects", json={"name": "demo"}).json()["id"]
        tid = c.post(f"/api/projects/{pid}/requests",
                     json={"text": "build\nwrite out/hello: compiled", "mode": "new"}).json()["spawned"][0]["id"]
        deadline = time.time() + 20
        while c.get(f"/api/threads/{tid}").json()["state"] != "idle":
            assert time.time() < deadline
            time.sleep(0.1)
    # What the Files tab's link does: <a href=… download=…>, clicked.
    link = f"/api/threads/{tid}/files/raw?path=out%2Fhello"
    js(qtbot, w, f"""const a = document.createElement('a'); a.href = {json.dumps(link)}; a.download = 'hello';
                     document.body.appendChild(a); a.click(); true""")
    qtbot.waitUntil(lambda: (tmp_path / "dl" / "hello").exists()
                    and w.statusBar().currentMessage().startswith("Saved"), timeout=WAIT)
    assert (tmp_path / "dl" / "hello").read_bytes() == b"compiled\n"
    # Never overwrites. (A second scripted click would be held back by
    # Chromium's guard against downloads without a user gesture.)
    w.view.page().download(QUrl(url + link), "hello")
    qtbot.waitUntil(lambda: (tmp_path / "dl" / "hello (1)").exists(), timeout=WAIT)
    assert w.view.url().toString() == url + "/", "a download leaves the page where it was"


def test_a_taken_port_is_refused_with_how_to_free_it(qtbot, tmp_path):
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        w = window(qtbot, tmp_path, port)
        assert w.control.state == DOWN and w.control.thread is None
        assert f"Port {port} is already in use" in w.control.text and f"fuser -k {port}/tcp" in w.control.text
        qtbot.waitUntil(lambda: "already in use" in page_text(qtbot, w), timeout=WAIT)
    # Retry, once the port is free.
    js(qtbot, w, "document.querySelector('a').click(); true")
    qtbot.waitUntil(lambda: w.control.state == UP, timeout=WAIT)
    w.close()


def test_connections_the_last_run_left_dont_hold_the_port(qtbot, tmp_path):
    """After a run ends, its closed connections sit in TIME_WAIT on the port
    with no process holding them (lsof shows nothing); the next run starts."""
    with socket.socket() as last:
        last.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        last.bind(("127.0.0.1", 0))
        last.listen()
        port = last.getsockname()[1]
        client = socket.create_connection(("127.0.0.1", port))
        accepted, _ = last.accept()
        accepted.close()  # the server side closes first, so it is the side left in TIME_WAIT
    client.close()
    w = window(qtbot, tmp_path, port)
    qtbot.waitUntil(lambda: w.control.state == UP, timeout=WAIT)
    w.close()


def test_a_control_plane_that_stops_shows_down(qtbot, up):
    w, url = up
    w.control.server.should_exit = True
    qtbot.waitUntil(lambda: w.control.state == DOWN, timeout=WAIT)
    assert "stopped" in w.control.text
    qtbot.waitUntil(lambda: "stopped" in page_text(qtbot, w), timeout=WAIT)


def click(qtbot, w, element: str) -> None:
    """A real mouse click (a user gesture) on the centre of the element that
    the JS expression element finds. It waits until the element is what is
    under that point, at the same place on two looks in a row: Chromium hit
    tests its last committed layout, and a click sent while a render is still
    landing goes to whatever was there before."""
    from PySide6.QtCore import QPoint, Qt
    from PySide6.QtTest import QTest
    where = (f"(() => {{ const e = {element}; if (!e) return '';"
             " const b = e.getBoundingClientRect(), x = b.left + b.width / 2, y = b.top + b.height / 2;"
             " const hit = document.elementFromPoint(x, y);"
             " return hit && e.contains(hit) ? JSON.stringify([Math.round(x), Math.round(y)]) : ''; })()")
    seen = [""]

    def settled() -> bool:
        seen.append(js(qtbot, w, where) or "")
        return seen[-1] != "" and seen[-1] == seen[-2]
    qtbot.waitUntil(settled, timeout=WAIT)
    x, y = json.loads(seen[-1])
    QTest.mouseClick(w.view.focusProxy(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, QPoint(x, y))


def test_copying_a_reply_puts_its_markdown_on_the_clipboard(qtbot, up, tmp_path):
    import asyncio

    from PySide6.QtGui import QGuiApplication

    from control.store import SqliteStore

    w, url = up
    reply = "## Done\n\nChanged **two** files:\n\n- `a.py`\n- `b.py`"

    async def seed():
        s = SqliteStore(str(tmp_path / "db.sqlite"))
        await s.open()
        p = await s.create_project("copy")
        t = await s.create_thread(p["id"], "a thread", "do it")
        await s.append_event(p["id"], t["id"], "assistant", {"content": reply})
        await s.close()
        return p["id"]

    pid = asyncio.run(seed())
    QGuiApplication.clipboard().clear()
    w.view.load(QUrl(f"{url}/p/{pid}"))
    qtbot.waitUntil(lambda: "a thread" in page_text(qtbot, w), timeout=WAIT)
    click(qtbot, w, "document.querySelectorAll('.thread')[1]")
    click(qtbot, w, "document.querySelector('.ev-assistant .ev-copy')")
    qtbot.waitUntil(lambda: QGuiApplication.clipboard().text() == reply, timeout=WAIT)
    qtbot.waitUntil(lambda: js(qtbot, w, "!!document.querySelector('.ev-assistant .icon-check')"), timeout=WAIT)


def test_a_markdown_file_opens_rendered_in_the_files_tab(qtbot, up, tmp_path):
    w, url = up
    with httpx.Client(base_url=url, timeout=20) as c:
        pid = c.post("/api/projects", json={"name": "docs"}).json()["id"]
        tid = c.post(f"/api/projects/{pid}/requests",
                     json={"text": "plan\nwrite notes/plan.md: Ship **it** today", "mode": "new"}).json()["spawned"][0]["id"]
        deadline = time.time() + 20
        while c.get(f"/api/threads/{tid}").json()["state"] != "idle":
            assert time.time() < deadline
            time.sleep(0.1)
    w.view.load(QUrl(f"{url}/p/{pid}"))
    qtbot.waitUntil(lambda: js(qtbot, w, "document.querySelectorAll('.thread').length > 1"), timeout=WAIT)
    click(qtbot, w, "document.querySelectorAll('.thread')[1]")
    click(qtbot, w, "[...document.querySelectorAll('.tab')].find(t => t.textContent === 'Files')")
    click(qtbot, w, "[...document.querySelectorAll('.file')].find(f => f.textContent === 'notes/')")
    click(qtbot, w, "[...document.querySelectorAll('.file')].find(f => f.textContent.startsWith('plan.md'))")
    qtbot.waitUntil(lambda: js(qtbot, w, "(document.querySelector('.fview strong') || {}).textContent") == "it",
                    timeout=WAIT)
    assert w.view.url().toString() == f"{url}/p/{pid}", "the file opens in the window's own pane"
    # Download stays one click away, and saves the file as it is.
    click(qtbot, w, "document.getElementById('fview-download')")
    qtbot.waitUntil(lambda: (tmp_path / "dl" / "plan.md").exists()
                    and w.statusBar().currentMessage().startswith("Saved"), timeout=WAIT)
    assert (tmp_path / "dl" / "plan.md").read_text() == "Ship **it** today\n"
    # Back returns to the directory.
    click(qtbot, w, "[...document.querySelectorAll('.pane-actions .btn')].find(b => b.textContent === 'Back')")
    qtbot.waitUntil(lambda: js(qtbot, w, "!document.querySelector('.fview') && !!document.querySelector('.files')"),
                    timeout=WAIT)
