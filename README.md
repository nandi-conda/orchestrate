# orchestrate

A project-based agent orchestrator. Each project has one main window; each
request becomes one or more agent threads running on remote workers.

| Directory | What | Docs |
| --- | --- | --- |
| [`orchestrator/`](orchestrator) | control plane + ultra UI | [README](orchestrator/README.md) |
| [`runner/`](runner) | agent runner that hosts many threads per worker | [README](runner/README.md), [API](runner/API.md) |

They're separate pixi projects and deploy separately. To try it locally:

```sh
cd runner && pixi install && pixi run set-key DEEPSEEK_API_KEY && cd ..
cd orchestrator && pixi run serve     # http://127.0.0.1:8000
```
