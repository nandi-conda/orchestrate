# runner

Hosts many agent threads in one process. Each thread has its own git worktree,
calls a pluggable model (DeepSeek first) and runs tools in a pluggable sandbox
(a local shell first). Threads can list, read, message and spawn each other.
A thread's commands run in a bubblewrap sandbox that holds its worktree and
nothing of the runner, so the agent sees only the project
([API.md](API.md#sandbox)). Linux only (linux-64, linux-aarch64).

```sh
pixi run set-key DEEPSEEK_API_KEY             # once: store the key in the system keyring
pixi run test                                   # offline tests (scripted model)
DEEPSEEK_API_KEY=... pixi run run "add a README" --repo /path/to/repo   # or a URL
DEEPSEEK_API_KEY=... RUNNER_TOKEN=... pixi run serve   # HTTP service on :8080
```

`--repo` seeds the run: threads start from its default branch, and finished
work lands on `origin/<default>` in the workdir's own repository
(`<workdir>/.repo`); nothing is pushed back to the source. Under an
orchestrator the worker has no remote at all: the orchestrator moves commits
to and from its per-project repository ([API.md](API.md#repository)).

`pixi install -e prod` gives the runtime without test tools. The HTTP contract
and environment variables are in [API.md](API.md).

## Layout

| File | What |
| --- | --- |
| `src/runner/model.py` | `Model` protocol, `OpenAICompatModel` (DeepSeek), `ScriptedModel` for tests |
| `src/runner/sandbox.py` | `Sandbox` protocol, `View` (what a thread's commands see), `LocalSandbox` (bubblewrap) |
| `src/runner/tools.py` | work tools (bash, files), peer tools (threads, messages), web tools (search, fetch) |
| `src/runner/web.py` | `DeepSeekWeb`: DeepSeek Search & Fetch API, same `DEEPSEEK_API_KEY` as the model |
| `src/runner/thread.py` | `AgentThread` loop and `ThreadHost` (workspaces, events, capacity) |
| `src/runner/repo.py` | the worker's repository, per-thread worktrees, snapshots, bundles to and from the orchestrator |
| `src/runner/peers.py` | `OrchestratorPeers` (queued for the orchestrator to collect) and `LocalPeers` (same process) |
| `src/runner/server.py` | Starlette app |
| `src/runner/cli.py` | one-shot local run |

## Deploying a worker

On any Linux box with git, curl, unprivileged user namespaces and, if a container runtime masks parts of `/proc`, passwordless sudo (bubblewrap needs them, see [API.md](API.md#sandbox)): `deploy/bootstrap.sh` installs pixi, checks out
this repo and installs the `prod` environment; `deploy/start.sh` runs the service in
the foreground. On Sprites, run bootstrap once through exec, checkpoint, then register
`deploy/start.sh` as a sprite service with `http_port` 8080 and the env vars from API.md.
