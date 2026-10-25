"""Small building blocks for the window: badges, collapsible rows, event rows
and a scrolling column that keeps to its end while new rows arrive."""

from __future__ import annotations

from typing import Callable

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (QFrame, QHBoxLayout, QLabel, QPushButton, QScrollArea, QSizePolicy, QToolButton,
                               QVBoxLayout, QWidget)

from .model import MARKDOWN, K_ASSISTANT, K_ERROR, K_PEER, K_RESULT, K_ROUTE, K_STATUS, K_THINKING, K_USER

KIND_NAMES = {K_USER: "user", K_ASSISTANT: "assistant", K_STATUS: "status", K_PEER: "peer", K_ERROR: "error",
              K_ROUTE: "route", K_THINKING: "thinking", K_RESULT: "result"}

STYLE = """
QLabel#h1 { font-size: 20px; font-weight: 600; }
QLabel#muted, QLabel#threadId, QLabel#threadSummary, QLabel#evLabel { color: palette(placeholder-text); }
QLabel#mono, QLabel#threadId { font-family: monospace; }
QLabel#error { background: #fdecea; color: #a4262c; border: 1px solid #f1b7b4; border-radius: 6px; padding: 6px 10px; }
QLabel#badge { border-radius: 8px; padding: 1px 8px; font-size: 11px; background: palette(mid); color: palette(bright-text); }
QLabel#badge[state="working"] { background: #2563eb; color: white; }
QLabel#badge[state="failed"] { background: #dc2626; color: white; }
QLabel#badge[state="idle"], QLabel#badge[state="queued"] { background: palette(midlight); color: palette(text); }
QLabel#badge[state="stopped"], QLabel#badge[state="done"] { background: transparent; color: palette(text); border: 1px solid palette(mid); }
QFrame#now { background: rgba(37, 99, 235, 0.07); border-radius: 6px; }
QLabel#dot { color: #22c55e; }
QFrame#ev { border: 1px solid palette(midlight); border-radius: 8px; }
QFrame#ev[kind="user"] { background: palette(alternate-base); }
QFrame#ev[kind="assistant"], QFrame#ev[kind="result"] { border-color: rgba(37, 99, 235, 0.35); }
QFrame#ev[kind="result"] { background: rgba(37, 99, 235, 0.05); }
QFrame#ev[kind="error"] { border-color: #f1b7b4; background: #fdecea; }
QFrame#ev[kind="thinking"] { border-style: dashed; }
QFrame#ev[kind="status"] { border: 0; }
QLabel#evLabel { font-size: 11px; font-weight: 600; }
QFrame#route { background: palette(alternate-base); border-radius: 6px; }
QToolButton#fold { border: 0; text-align: left; color: palette(placeholder-text); }
QToolButton#fold:hover { color: palette(text); }
QLabel#foldBody { font-family: monospace; padding: 2px 0 6px 22px; }
QPushButton#threadTag, QPushButton#working { border: 0; text-align: left; color: palette(link); }
QPushButton#threadTag:hover, QPushButton#working:hover { text-decoration: underline; }
QPushButton#mode { padding: 4px 10px; }
QPushButton#mode:checked { background: palette(highlight); color: palette(highlighted-text); }
"""


def label(text: str = "", name: str = "", wrap: bool = False) -> QLabel:
    w = QLabel(text)
    if name:
        w.setObjectName(name)
    w.setWordWrap(wrap)
    return w


def badge(state: str) -> QLabel:
    w = label(state, "badge")
    w.setProperty("state", state)
    return w


def one_line(s: str, n: int = 160) -> str:
    s = " ".join(s.split())
    return s if len(s) <= n else s[:n] + " …"


def restyle(w: QWidget) -> None:
    """Apply a changed dynamic property to the style sheet."""
    w.style().unpolish(w)
    w.style().polish(w)


class Fold(QWidget):
    """One line until clicked, then its body under it (a tool call or result,
    or a run of reasoning). Its open state is kept by the column it is in."""

    toggled = Signal(bool)

    def __init__(self, title: str, preview: str, body: str, opened: bool, tag: QWidget | None = None) -> None:
        super().__init__()
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)
        head = QHBoxLayout()
        head.setSpacing(6)
        self.button = QToolButton()
        self.button.setObjectName("fold")
        self.button.setCheckable(True)
        self.button.setChecked(opened)
        self.button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.button.setText(title)
        head.addWidget(self.button)
        if tag is not None:
            head.addWidget(tag)
        self.preview = label(one_line(preview, 140), "muted")
        self.preview.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        head.addWidget(self.preview, 1)
        lay.addLayout(head)
        self.body = label(body, "foldBody", wrap=True)
        self.body.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        lay.addWidget(self.body)
        self.button.toggled.connect(self._show)
        self._show(opened)

    def _show(self, opened: bool) -> None:
        self.button.setArrowType(Qt.ArrowType.DownArrow if opened else Qt.ArrowType.RightArrow)
        self.body.setVisible(opened)
        self.preview.setVisible(not opened)
        self.toggled.emit(opened)


class EventRow(QFrame):
    """One Dialog entry: its label (and thread, in Project activity) over its
    text, which is rendered markdown for replies, messages and results."""

    def __init__(self, e: dict, tag: QWidget | None = None) -> None:
        super().__init__()
        kind = e.get("kind")
        self.setObjectName("ev")
        self.setProperty("kind", KIND_NAMES.get(kind, "status"))
        lay = QVBoxLayout(self)
        lay.setContentsMargins(12, 6, 12, 8)
        lay.setSpacing(2)
        head = QHBoxLayout()
        head.setSpacing(8)
        name = str(e.get("label", "")).upper() + (" ●" if e.get("live") else "")
        head.addWidget(label(name, "evLabel"))
        if tag is not None:
            head.addWidget(tag)
        head.addStretch(1)
        lay.addLayout(head)
        self.text = label(wrap=True)
        self.text.setTextFormat(Qt.TextFormat.MarkdownText if kind in MARKDOWN else Qt.TextFormat.PlainText)
        self.text.setText(str(e.get("text", "")))
        self.text.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        lay.addWidget(self.text)


def thread_tag(title: str, on_click: Callable[[], None]) -> QPushButton:
    """A thread named by its title; clicking it opens the thread."""
    b = QPushButton(title)
    b.setObjectName("threadTag")
    b.setFlat(True)
    b.setCursor(Qt.CursorShape.PointingHandCursor)
    b.clicked.connect(on_click)
    return b


class Column(QScrollArea):
    """A scrolling column of rows, rebuilt whenever what it shows changes. It
    stays at its end while it was there, so a streaming reply stays in view."""

    def __init__(self) -> None:
        super().__init__()
        self.setWidgetResizable(True)
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.inner = QWidget()
        self.rows = QVBoxLayout(self.inner)
        self.rows.setContentsMargins(12, 8, 12, 8)
        self.rows.setSpacing(6)
        self.rows.addStretch(1)
        self.setWidget(self.inner)
        self.shown: object = None
        self.opened: set = set()  # keys of the folds that are open
        self.stick = True
        bar = self.verticalScrollBar()
        bar.rangeChanged.connect(lambda lo, hi: self.stick and bar.setValue(hi))
        bar.valueChanged.connect(lambda v: setattr(self, "stick", v >= bar.maximum() - 24))

    def show_rows(self, signature: object, build: Callable[[], list[QWidget]], to_end: bool = False) -> None:
        """Replace the rows with build()'s, unless signature says they are what is shown."""
        if to_end:
            self.stick = True
        if signature == self.shown:
            return
        self.shown = signature
        while self.rows.count() > 1:
            w = self.rows.takeAt(0).widget()
            w.setParent(None)
            w.deleteLater()
        for i, w in enumerate(build()):
            self.rows.insertWidget(i, w)

    def fold(self, key: object, title: str, preview: str, body: str, tag: QWidget | None = None) -> Fold:
        f = Fold(title, preview, body, key in self.opened, tag)
        f.toggled.connect(lambda on: self.opened.add(key) if on else self.opened.discard(key))
        return f

    def widgets(self) -> list[QWidget]:
        return [self.rows.itemAt(i).widget() for i in range(self.rows.count() - 1)]
