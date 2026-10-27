# Run the desktop app and fix what goes wrong

`pixi run desktop-fix` starts you (Claude Code) in this checkout with this
file as your task. You run the desktop window, watch everything it prints, and
fix the code behind every error, warning and traceback as it appears, landing
each fix on `main`. The user is using the window while you watch.

## Start the desktop and watch it

Start it with the **Monitor** tool, from the repository root, so the full
output goes to a log file and each line worth looking at reaches you as it is
printed:

```sh
pixi run desktop 2>&1 | tee ~/.orchestrate/desktop-fix.log \
  | grep --line-buffered -iE 'traceback|error|exception|warning|failed|refused|lost|timed out'
```

The window prints `desktop: …` status lines, Python logging lines
(`INFO:orchestrator:…`, `WARNING:…`, `ERROR:…`), and every worker's log
prefixed with `<worker> |`. A matched line is often the first line of more:
read the log file around it (`tail -n 200 ~/.orchestrate/desktop-fix.log`, or
grep it) to get the whole traceback and what led up to it before deciding
anything.

Keep watching until the user stops you. When the desktop process exits, read
the end of the log to see why, say so, and start it again the same way once
the cause is fixed (or at once if the user closed the window on purpose and
asks for it back).

## Expected lines, not problems

- `Failed to load client buffer integration: "none"` and
  `Failed to create RHI for backend: OpenGL`: the window draws without a GPU
  by design (`docs/guide/desktop.md`, "Rendering without a GPU").
- A worker briefly unreachable, or `ReadError` on a peer poll, when the
  network to Fly blips: that is the user's network, not a bug. Fix it only if
  it does not recover on its own or is handled wrongly once it does.

Anything else that matches is yours to look at.

## Fixing

For each problem:

1. Find the root cause in the code (`orchestrator/`, `runner/`, `sync/`).
   Read `docs/` and `docs/development.md` first if the area is new to you.
2. Fix the cause. Follow the repository's conventions: one designed path, no
   fallbacks, retries, degraded modes or `try`/`except` that hides the error,
   and never silence a warning or lower a log level to make a line go away.
   When something cannot work, it fails saying why.
   Write optimised software: a fix must not make the code do more work, move
   more bytes or hold more memory than the job needs. Ask what a path costs
   at the size it really runs at (a repository of hundreds of megabytes, not
   a fixture), and leave out of what is stored, sent or recomputed whatever
   can be made again from what is already there.
3. Add or change a test that would have caught it, in the suite of the
   subproject you touched, and run that suite (`cd orchestrator && pixi run -e
   test test`, `cd orchestrator && pixi run test-desktop`, `cd runner && pixi
   run test`). They must pass.
4. Land it on `main`: commit only the files of this fix (the user may have
   their own uncommitted work in this checkout; leave it alone), with a
   subject like `orchestrator: <lowercase imperative summary>`, then
   `git pull --rebase origin main` and `git push origin HEAD:main`. No pull
   requests. Update `docs/` in the same commit when behaviour it describes
   changed.
5. Restart the desktop so the fix is live: say in one line what you fixed and
   that the window is restarting, stop the Monitor task, and start it again as
   above. Workers survive the restart; the next control plane attaches to
   them again.

Not everything printed is a code bug. A missing secret, a full disk, an
expired token, or the network is the user's to fix: tell them exactly what is
missing (the secret's exact name, the header, the file) and keep watching.
When a fix needs a decision only the user can make (it changes what a feature
does, or deletes data), stop and ask before changing anything.

Report each fix in the terminal in a sentence or two: what broke, why, the
commit. Nothing longer.
