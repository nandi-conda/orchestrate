# runner

Hosts many agent threads in one process. Each thread has its own git worktree,
calls a pluggable model (DeepSeek first) and runs tools in a pluggable sandbox
(a local shell first). Threads can list, read, message and spawn each other.

```sh
pixi run test                                   # offline tests (scripted model)
DEEPSEEK_API_KEY=... pixi run run "add a README" --repo /path/to/git/repo
DEEPSEEK_API_KEY=... RUNNER_TOKEN=... pixi run serve   # HTTP service on :8080
```

`pixi install -e prod` gives the runtime without test tools. The HTTP contract
and environment variables are in [API.md](API.md).

## Layout

| File | What |
| --- | --- |
| `src/runner/model.py` | `Model` protocol, `OpenAICompatModel` (DeepSeek), `ScriptedModel` for tests |
| `src/runner/sandbox.py` | `Sandbox` protocol, `LocalSandbox` |
| `src/runner/tools.py` | work tools (bash, files) and peer tools (threads, messages) |
| `src/runner/thread.py` | `AgentThread` loop and `ThreadHost` (worktrees, events, capacity) |
| `src/runner/peers.py` | `HttpPeers` (orchestrator) and `LocalPeers` (same process) |
| `src/runner/server.py` | Starlette app |
| `src/runner/cli.py` | one-shot local run |
