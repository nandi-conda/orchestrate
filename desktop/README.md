# desktop

A PySide6 desktop client for the control plane in `../orchestrator`. It speaks
the same HTTP API and event stream as the web UI, which stays as it is, and
mirrors it in one native window:

- **Projects**: the list, and **New project** (name, optional git repo URL).
  Opening one replaces the list in the same window; **← All projects** goes back.
- **Threads** on the left: each thread's state, what it is doing now (else its
  summary), and its id, children indented. **Project activity** at the top is
  the router's decisions and every thread's events, with a live line per
  working thread. **Archived** lists archived threads.
- **The pane**: the selected thread's title, state, **Stop thread** and
  **Archive** (or **Unarchive**), the **Now** line while it works, and tabs:
  - **Dialog**: requests, messages and replies, markdown rendered, each turn's
    **Result**, reasoning folded to one line until clicked.
  - **Tools**: one folded line per tool call and result.
  - **Files**: the thread's worktree, a directory at a time. Click a directory
    to open it (**Up** goes back); click a file to save it to your downloads
    folder (never over an existing file: `name (1)`, `name (2)`, …).
  - **Worker log**: the last 300 lines its runner wrote, with **Refresh**.
- **The request box**: **Router decides**, **This thread** or **New thread**,
  then Enter. The box clears at once; what the router does arrives on the
  event stream, and its last decision shows above the box while a thread is open.

Nothing opens another window. The client never waits on the network: every call
is asynchronous on Qt's event loop, and the project's SSE stream refreshes the
view as events arrive, resuming after the last event if the connection drops.

## Run it locally

You need [pixi](https://pixi.sh). One command:

```sh
git clone https://github.com/nandi-conda/orchestrate && cd orchestrate
pixi run desktop
```

The window starts the control plane itself (`pixi run serve` in
`../orchestrator`, on port 8000) and owns it: the projects show once it
answers, and closing the window (or Ctrl+C in the terminal) stops it, which
also destroys its sprites. The control plane's output, including which
secrets it found, goes to the same terminal. The first run installs both
environments and builds the web UI, so it takes a while; the window says
what it is waiting for. While it runs, the web UI is at http://127.0.0.1:8000
too.

Secrets are the control plane's (see `../orchestrator/README.md`); without
any, it runs threads on the local echo runner.

- `pixi run desktop --port 8100` starts it on another port, e.g. when 8000 is taken.
- `pixi run desktop --url http://host:8000` connects to a control plane someone
  else runs instead of starting one.

If the control plane cannot start or cannot be reached, the window says so
with the URL, and **Retry** tries again.

## Tests

```sh
pixi run test       # pytest-qt, offscreen
```

The tests run the real control plane from `../orchestrator` with its echo
runner and rule-based router, in-process and as a process the window starts
and stops, and drive the window against it:
routing a list into threads, a thread spawning a child, the Dialog and Tools
tabs, browsing and downloading files, the worker log, archiving and
unarchiving, and the connection states (starting, a control plane that exits,
nothing at the URL). They need no credentials, no display and no network beyond
127.0.0.1.

## Layout

| Path | What |
| --- | --- |
| `orchestrate_desktop/server.py` | starting, watching and stopping the control plane the window owns |
| `orchestrate_desktop/api.py` | the control plane's HTTP API and SSE stream on `QNetworkAccessManager` |
| `orchestrate_desktop/model.py` | what the Dialog and Tools tabs show (pure, no Qt) |
| `orchestrate_desktop/widgets.py` | badges, folded rows, event rows, the scrolling column, the style sheet |
| `orchestrate_desktop/window.py` | the window: project list, project page, pane, request box |
