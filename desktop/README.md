# desktop

A desktop window for the orchestrator. PySide draws the window; what is in it
is the ultra UI from `../orchestrator`, the same one the browser shows. There
is one UI, and this is a window for it.

The window does what a browser tab would, and keeps everything in itself:

- Links, `window.open` and `target=_blank` open in this window, never another.
- Downloads (the Files tab) are saved to your downloads folder, never over an
  existing file (`name (1)`, `name (2)`, …), with a line at the bottom saying
  where.

## Run it locally

You need [pixi](https://pixi.sh). One command:

```sh
git clone https://github.com/nandi-conda/orchestrate && cd orchestrate
pixi run desktop
```

The window starts the control plane itself (`pixi run serve` in
`../orchestrator`, on port 8000) and owns it: it shows the UI once the control
plane answers, and closing the window (or Ctrl+C in the terminal) stops it,
which also destroys its sprites. The control plane's output, including which
secrets it found, goes to the same terminal. The first run installs both
environments and builds the UI, so it takes a while; the window says what it
is waiting for. While it runs, the same UI is at http://127.0.0.1:8000 in a
browser too.

Secrets are the control plane's (see `../orchestrator/README.md`); without
any, it runs threads on the local echo runner.

- `pixi run desktop --port 8100` starts it on another port, e.g. when 8000 is taken.
- `pixi run desktop --url http://host:8000` shows a control plane someone else
  runs instead of starting one.

If the control plane cannot start or cannot be reached, the window says so
with the URL, and **Retry** tries again.

Platforms: linux-64, osx-arm64 and win-64. conda-forge has no Qt WebEngine for
osx-64, and ultra has no linux-aarch64 build.

## Tests

```sh
pixi run test       # builds the UI, then pytest-qt, offscreen
```

The tests let the window start the real control plane from `../orchestrator`
as its own process, on the echo runner and the rule-based router
(`tests/serve_echo.py`), and check that it shows the ultra UI, keeps a new
window in its view, saves a Files download to the downloads folder without
overwriting, stops the control plane when it closes, and says so with the URL
when the control plane exits or is not there. They need no credentials and no
display. Offscreen there is no GPU, so they run Chromium with `--disable-gpu`
(and without its sandbox, which refuses to run as root).

## Layout

| Path | What |
| --- | --- |
| `orchestrate_desktop/window.py` | the window: a web view on the UI, downloads, new windows, the status page |
| `orchestrate_desktop/server.py` | starting, watching and stopping the control plane the window owns |
