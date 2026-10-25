"""The window: it starts and stops its control plane, shows the ultra UI,
keeps new windows in its one view, and saves downloads to the downloads folder."""

import json
import sys
import time
from pathlib import Path

import httpx
import pytest
from PySide6.QtCore import QUrl

from conftest import free_port
from orchestrate_desktop.server import DOWN, STARTING, UP, ControlPlane
from orchestrate_desktop.window import MainWindow

SERVE = Path(__file__).with_name("serve_echo.py")
WAIT = 30000


def js(qtbot, w, code: str):
    """Run code in the page and return its value, or None when the page went
    away first (a navigation drops the call)."""
    with qtbot.waitCallback(timeout=3000, raising=False) as cb:
        w.view.page().runJavaScript(code, 0, cb)
    return cb.args[0] if cb.called else None


def page_text(qtbot, w) -> str:
    return js(qtbot, w, "document.body ? document.body.innerText : ''") or ""


@pytest.fixture
def owned(qtbot, tmp_path):
    """A window that started its own control plane, with the UI loaded."""
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    (tmp_path / "dl").mkdir()
    w = MainWindow(ControlPlane(url, [sys.executable, str(SERVE), str(port), str(tmp_path)]))
    w.download_dir = tmp_path / "dl"
    qtbot.addWidget(w)
    w.show()
    assert w.control.proc is not None and w.control.state in (STARTING, UP)
    qtbot.waitUntil(lambda: w.control.state == UP, timeout=WAIT)
    qtbot.waitUntil(lambda: "Projects" in page_text(qtbot, w), timeout=WAIT)
    yield w, url
    w.close()


def test_the_window_shows_the_ultra_ui_of_the_control_plane_it_started(qtbot, owned):
    w, url = owned
    assert w.view.url().toString() == url + "/"
    with httpx.Client(base_url=url) as c:
        pid = c.post("/api/projects", json={"name": "from-the-test"}).json()["id"]
    w.view.reload()
    qtbot.waitUntil(lambda: "from-the-test" in page_text(qtbot, w), timeout=WAIT)
    assert "stops when this window closes" in w.control.text

    # A new window (window.open, target=_blank) is this view.
    js(qtbot, w, f"window.open({json.dumps(url + '/p/' + pid)}, '_blank'); true")
    qtbot.waitUntil(lambda: w.view.url().toString() == f"{url}/p/{pid}", timeout=WAIT)

    proc = w.control.proc
    w.close()
    assert proc.poll() is not None and w.control.proc is None


def test_a_files_download_lands_in_the_downloads_folder(qtbot, owned, tmp_path):
    w, url = owned
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
    click = f"""const a = document.createElement('a'); a.href = {json.dumps(link)}; a.download = 'hello';
                document.body.appendChild(a); a.click(); true"""
    js(qtbot, w, click)
    qtbot.waitUntil(lambda: (tmp_path / "dl" / "hello").exists()
                    and w.statusBar().currentMessage().startswith("Saved"), timeout=WAIT)
    assert (tmp_path / "dl" / "hello").read_bytes() == b"compiled\n"
    # Never overwrites. (A second scripted click would be held back by
    # Chromium's guard against downloads without a user gesture.)
    w.view.page().download(QUrl(url + link), "hello")
    qtbot.waitUntil(lambda: (tmp_path / "dl" / "hello (1)").exists(), timeout=WAIT)
    assert w.view.url().toString() == url + "/", "a download leaves the page where it was"


def test_a_control_plane_that_exits_says_so_with_retry(qtbot, tmp_path):
    port = free_port()
    w = MainWindow(ControlPlane(f"http://127.0.0.1:{port}", [sys.executable, str(SERVE), str(port), str(tmp_path), "3"]))
    qtbot.addWidget(w)
    w.show()
    qtbot.waitUntil(lambda: w.control.state == DOWN, timeout=WAIT)
    qtbot.waitUntil(lambda: "exited with code 3" in page_text(qtbot, w), timeout=WAIT)
    assert f"127.0.0.1:{port}" in page_text(qtbot, w)
    js(qtbot, w, "document.querySelector('a').click(); true")  # Retry starts it again
    qtbot.waitUntil(lambda: w.control.proc is not None or w.control.state == STARTING, timeout=WAIT)
    qtbot.waitUntil(lambda: w.control.state == DOWN, timeout=WAIT)


def test_connecting_to_nothing_names_the_url(qtbot):
    url = f"http://127.0.0.1:{free_port()}"
    w = MainWindow(ControlPlane(url, None))
    qtbot.addWidget(w)
    w.show()
    qtbot.waitUntil(lambda: w.control.state == DOWN, timeout=WAIT)
    assert w.control.text.startswith(f"No control plane at {url}: ")
    qtbot.waitUntil(lambda: f"No control plane at {url}" in page_text(qtbot, w), timeout=WAIT)
