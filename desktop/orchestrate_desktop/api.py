"""The control plane's HTTP API (orchestrator/README.md, "HTTP API") on Qt's
event loop: every call is asynchronous and answers through a callback, so the
window never waits on the network."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

from PySide6.QtCore import QObject, QTimer, QUrl, Signal
from PySide6.QtNetwork import QNetworkAccessManager, QNetworkReply, QNetworkRequest

# (data, error): data is the parsed reply when error is "".
Done = Callable[[Any, str], None]

# An SSE stream that ends is opened again after this long, resuming after the
# last event it delivered (the control plane's ?after=seq).
RECONNECT_MS = 1000


def q(path: str) -> str:
    """A worktree path as a query value."""
    return quote(path, safe="")


class Api(QObject):
    def __init__(self, base_url: str, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self.base_url = base_url.rstrip("/")
        self.net = QNetworkAccessManager(self)

    def request(self, path: str) -> QNetworkRequest:
        return QNetworkRequest(QUrl(self.base_url + path))

    def get_json(self, path: str, done: Done) -> None:
        self._finish(self.net.get(self.request(path)), done, json_body=True)

    def post_json(self, path: str, body: dict, done: Done) -> None:
        req = self.request(path)
        req.setHeader(QNetworkRequest.KnownHeaders.ContentTypeHeader, "application/json")
        self._finish(self.net.post(req, json.dumps(body).encode()), done, json_body=True)

    def get_text(self, path: str, done: Done) -> None:
        self._finish(self.net.get(self.request(path)), done, json_body=False)

    def download(self, path: str, dest: Path, done: Callable[[str], None]) -> None:
        """Save the body of GET path to dest; done gets "" or the error."""
        reply = self.net.get(self.request(path))

        def finished() -> None:
            reply.deleteLater()
            data = bytes(reply.readAll().data())
            err = self._error(reply, data)
            if not err:
                try:
                    dest.write_bytes(data)
                except OSError as e:
                    err = f"could not save {dest}: {e}"
            done(err)

        reply.finished.connect(finished)

    def stream(self, path: str) -> "EventStream":
        return EventStream(self, path)

    def _finish(self, reply: QNetworkReply, done: Done, json_body: bool) -> None:
        def finished() -> None:
            reply.deleteLater()
            data = bytes(reply.readAll().data())
            err = self._error(reply, data)
            if err:
                done(None, err)
            elif json_body:
                done(json.loads(data or b"null"), "")
            else:
                done(data.decode("utf-8", "replace"), "")

        reply.finished.connect(finished)

    @staticmethod
    def _error(reply: QNetworkReply, data: bytes) -> str:
        """The control plane's own words for a failed call ({"error": ...} or
        a plain text body), else Qt's."""
        status = reply.attribute(QNetworkRequest.Attribute.HttpStatusCodeAttribute)
        if status is None:
            return reply.errorString() if reply.error() != QNetworkReply.NetworkError.NoError else ""
        if status < 400:
            return ""
        text = data.decode("utf-8", "replace").strip()
        try:
            parsed = json.loads(text)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict) and parsed.get("error"):
            return str(parsed["error"])
        return text or f"HTTP {status}"


class EventStream(QObject):
    """A server-sent event stream (/api/projects/<id>/events). It delivers
    each event's data, parsed; when the connection ends it opens again after
    the last event seen, like a browser's EventSource."""

    event = Signal(dict)

    def __init__(self, api: Api, path: str) -> None:
        super().__init__(api)
        self.api = api
        self.path = path
        self.last_id = 0
        self.buf = b""
        self.closed = False
        self.reply: QNetworkReply | None = None
        self.retry = QTimer(self)
        self.retry.setSingleShot(True)
        self.retry.timeout.connect(self._open)
        self._open()

    def close(self) -> None:
        self.closed = True
        self.retry.stop()
        if self.reply is not None:
            reply, self.reply = self.reply, None
            reply.abort()
            reply.deleteLater()
        self.deleteLater()

    def _open(self) -> None:
        if self.closed:
            return
        sep = "&" if "?" in self.path else "?"
        req = self.api.request(f"{self.path}{sep}after={self.last_id}")
        req.setRawHeader(b"Accept", b"text/event-stream")
        self.buf = b""
        reply = self.api.net.get(req)
        reply.readyRead.connect(lambda: self._read(reply))
        reply.finished.connect(lambda: self._ended(reply))
        self.reply = reply

    def _read(self, reply: QNetworkReply) -> None:
        if reply is not self.reply:
            return
        self.buf += bytes(reply.readAll().data())
        self.buf = self.buf.replace(b"\r\n", b"\n")
        while b"\n\n" in self.buf:
            block, self.buf = self.buf.split(b"\n\n", 1)
            self._dispatch(block.decode("utf-8", "replace"))

    def _dispatch(self, block: str) -> None:
        data: list[str] = []
        for line in block.split("\n"):
            if line.startswith(":"):
                continue
            name, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if name == "id" and value.isdigit():
                self.last_id = int(value)
            elif name == "data":
                data.append(value)
        if data:
            self.event.emit(json.loads("\n".join(data)))

    def _ended(self, reply: QNetworkReply) -> None:
        if reply is not self.reply:
            return
        self.reply = None
        reply.deleteLater()
        if not self.closed:
            self.retry.start(RECONNECT_MS)
