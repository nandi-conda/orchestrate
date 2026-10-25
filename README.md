# orchestrate

A project-based agent orchestrator. Each project has one main window; each
request becomes one or more agent threads running on remote workers.

| Directory | What | Docs |
| --- | --- | --- |
| [`orchestrator/`](orchestrator) | control plane + ultra UI | [README](orchestrator/README.md) |
| [`runner/`](runner) | agent runner that hosts many threads per worker | [README](runner/README.md), [API](runner/API.md) |
| [`desktop/`](desktop) | desktop window (PySide) showing the control plane's ultra UI | [README](desktop/README.md) |

They're separate pixi projects and deploy separately. The root
[`pixi.toml`](pixi.toml) is a front door that delegates to them, so you can
drive everything from the repo root:

```sh
pixi run setup                        # install what every project needs
pixi run set-key DEEPSEEK_API_KEY     # once: store the key in the system keyring
pixi run set-key SPRITES_TOKEN        # once: run workers on Fly Sprites, even for a local UI
pixi run serve                        # just the control plane + web UI on http://127.0.0.1:8000
pixi run desktop                      # the UI in a desktop window; starts and owns the control plane
pixi run test                         # every test suite: runner, orchestrator, desktop
pixi run worker                       # a runner service on :8080, for a standalone worker
```

With `SPRITES_TOKEN` stored, workers are sprites, including when the control
plane runs on your laptop. The control plane is never reachable from a worker,
by design: it opens every connection, and it also collects the workers' peer
calls (list, read, message and spawn threads across workers), so nothing has to
be exposed or tunnelled.

The same tasks with their full lists are still there: `cd orchestrator && pixi run <task>`
and `cd runner && pixi run <task>`.
