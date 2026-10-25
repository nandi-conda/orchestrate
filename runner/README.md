# runner

Hosts many agent threads in one process. Each thread has its own git worktree,
calls a pluggable model (DeepSeek first) and runs tools in a pluggable sandbox
(a local shell first). Threads can list, read, message and spawn each other.

```sh
pixi run set-key DEEPSEEK_API_KEY             # once: store the key in the system keyring
pixi run test                                   # offline tests (scripted model)
DEEPSEEK_API_KEY=... pixi run run "add a README" --repo /path/to/bare.git   # or a URL, with GITHUB_TOKEN
DEEPSEEK_API_KEY=... RUNNER_TOKEN=... pixi run serve   # HTTP service on :8080
```

`--repo` is cloned, and finished work lands on its default branch, so give it
a URL or a bare repo (git refuses pushes to a checked-out branch).
How commits get from threads to the repo is in [API.md](API.md#commits).

`pixi install -e prod` gives the runtime without test tools. The HTTP contract
and environment variables are in [API.md](API.md).

## Layout

| File | What |
| --- | --- |
| `src/runner/model.py` | `Model` protocol, `OpenAICompatModel` (DeepSeek), `ScriptedModel` for tests |
| `src/runner/sandbox.py` | `Sandbox` protocol, `LocalSandbox` |
| `src/runner/tools.py` | work tools (bash, files), peer tools (threads, messages), web tools (search, fetch) |
| `src/runner/web.py` | `DeepSeekWeb`: DeepSeek Search & Fetch API, same `DEEPSEEK_API_KEY` as the model |
| `src/runner/thread.py` | `AgentThread` loop and `ThreadHost` (workspaces, events, capacity) |
| `src/runner/repo.py` | the project clone, per-thread worktrees, saving and landing commits |
| `src/runner/peers.py` | `OrchestratorPeers` (queued for the orchestrator to collect) and `LocalPeers` (same process) |
| `src/runner/server.py` | Starlette app |
| `src/runner/cli.py` | one-shot local run |

## Deploying a worker

On any Linux box with git and curl: `deploy/bootstrap.sh` installs pixi, checks out
this repo and installs the `prod` environment; `deploy/start.sh` runs the service in
the foreground. On Sprites, run bootstrap once through exec, checkpoint, then register
`deploy/start.sh` as a sprite service with `http_port` 8080 and the env vars from API.md.
