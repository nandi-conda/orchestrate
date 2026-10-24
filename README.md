# orchestrate

A project-based agent orchestrator. Each project has one main window; each
request becomes one or more agent threads running on remote workers.

| Directory | What | Docs |
| --- | --- | --- |
| [`orchestrator/`](orchestrator) | control plane + ultra UI | [README](orchestrator/README.md) |
| [`runner/`](runner) | agent runner that hosts many threads per worker | [README](runner/README.md), [API](runner/API.md) |

They're separate pixi projects and deploy separately. The root
[`pixi.toml`](pixi.toml) is a front door that delegates to both, so you can
drive everything from the repo root:

```sh
pixi run setup                        # install what both projects need
pixi run set-key DEEPSEEK_API_KEY     # once: store the key in the system keyring
pixi run serve                        # build the UI, then serve http://127.0.0.1:8000
pixi run test                         # both test suites: runner, then orchestrator
pixi run worker                       # a runner service on :8080, for a standalone worker
```

The same tasks with their full lists are still there: `cd orchestrator && pixi run <task>`
and `cd runner && pixi run <task>`.
