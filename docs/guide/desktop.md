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
- What the control plane is doing is printed to the terminal as `desktop: …`
  lines, and every worker's log follows with a `<worker> |` prefix.

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
