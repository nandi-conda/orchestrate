# The desktop window

```sh
pixi run desktop          # from the repo root or orchestrator/
```

The same UI in a desktop window instead of a browser tab. Qt (PySide6) only
draws the window; the ultra UI fills it. The control plane runs **in the same
process**, on a thread next to the window, so it ends when the window does,
however the window ends. Closing the window or pressing Ctrl+C in the terminal
stops it gracefully. Workers are let go, not destroyed: the next control plane
attaches to them again.

While the window is open, the UI is also at `http://127.0.0.1:8000` in a
browser. `--port N` and `--db …` work as for `pixi run serve`.

It runs on linux-64, osx-arm64 and win-64 (the orchestrator's `desktop` pixi
environment). conda-forge has no Qt WebEngine for osx-64.

## Behaviour

- The UI's own links, `window.open` and `target=_blank` stay in the window.
- Links anywhere else (a URL in an agent's reply) open in your default browser.
- Markdown files and images in the Files tab open in the pane; other files
  download to your Downloads folder, never over an existing file
  (`name (1)`, …), with a line at the bottom saying where.
- **Browse…** (Open folder, and a project's Settings → Remote) shows your
  OS's folder dialog, open at the folder the field names. The window gives
  the page this through a Qt web channel (`desktop/host.js`); on Linux the
  dialog comes from the XDG desktop portal (`QT_QPA_PLATFORMTHEME=xdgdesktopportal`),
  so it is GNOME's, KDE's or your desktop's own, on Wayland and X11 alike.
- If the port is taken, the window says so, and how to free it
  (`fuser -k 8000/tcp`, or `kill $(lsof -t -i :8000)`), with a **Retry**.
- Until the control plane answers, the window shows a page in the app's own
  look with each step of starting as it runs (opening the database, looking
  up where workers can run, setting up the router, loading projects and
  threads), how long the running one has taken, and, if one fails, why, with
  a **Retry**. None of these steps waits on the network: the sweep of each
  place's workers and catching up with the sync server run once the window
  shows the UI.
- What the control plane is doing is printed to the terminal as `desktop: …`
  lines, and every worker's log follows with a `<worker> |` prefix.

## Watched by Claude Code

```sh
pixi run desktop-fix      # from the repo root
```

Starts [Claude Code](https://code.claude.com) (the `claude` command, which has
to be on your `PATH`) in the checkout, with
[`.agents/desktop-fix.md`](https://github.com/nandi-conda/orchestrate/blob/main/.agents/desktop-fix.md)
as its task. Claude runs `pixi run desktop` itself and watches everything it
prints, worker logs included, keeping the whole output in
`~/.orchestrate/desktop-fix.log`. Each error, warning or traceback it sees, it
traces to the code, fixes with a test, runs the suites, commits only that fix
to `main` and pushes it, then restarts the window so the fix is live. Your own
uncommitted changes in the checkout are left alone.

What it cannot fix in code it tells you instead: a missing secret by its exact
name, a full disk, the network. A fix that would change what a feature does
waits for your answer. The two startup lines in the next section are expected
and ignored.

File edits are accepted without asking (`--permission-mode acceptEdits`);
commands such as the tests, `git` and the restart still ask, as Claude Code's
own permission settings say.

## Rendering without a GPU

The window is drawn on the CPU, with no OpenGL, EGL or GLX in the process: Qt
Quick's software renderer, Chromium with `--disable-gpu`, and on Linux
`QT_WAYLAND_CLIENT_BUFFER_INTEGRATION=none` (native Wayland, drawn into
shared-memory buffers) and `QT_XCB_GL_INTEGRATION=none` (X11 without GLX).
conda-forge's Mesa has no Wayland EGL platform, and the system's GL drivers
need not match the environment's Qt, so any GL use aborted the window or kept
it from appearing. Variables you set yourself still win.

Two lines at startup are expected and harmless:

```text
Failed to load client buffer integration: "none"
Failed to create RHI for backend: OpenGL
```
