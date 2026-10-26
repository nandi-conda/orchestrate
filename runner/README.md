# runner

Hosts many agent threads in one process. Each thread is a
[prime-agent](https://github.com/PrimeIntellect-ai/prime-agent) harness (DeepSeek
by default) with its own git worktree. Its Python kernel, where everything the
model writes runs, is in a bubblewrap sandbox that holds the worktree and
nothing of the runner, so the agent sees only the project
([API.md](API.md#sandbox)). Threads list, read, message and spawn each other
through the team skill (`skills/team`). Linux only (linux-64, linux-aarch64).

```sh
pixi run set-key DEEPSEEK_API_KEY             # once: store the key in the system keyring
pixi run test                                   # offline tests (prime-agent's scripted model)
DEEPSEEK_API_KEY=... pixi run run "add a README" --repo /path/to/repo   # or a URL
DEEPSEEK_API_KEY=... RUNNER_TOKEN=... pixi run serve   # HTTP service on :8080
```

`--repo` seeds the run: threads start from its default branch, and finished
work lands on `origin/<default>` in the workdir's own repository
(`<workdir>/.repo`); nothing is pushed back to the source. Under an
orchestrator the worker has no remote at all: the orchestrator moves commits
to and from its per-project repository ([API.md](API.md#repository)).

`serve`, `run` and `test` first install the pinned prime-agent binary into
`.harness/` (`pixi run harness`, checksummed) and the kernel environment
(`pixi install -e kernel`: the Python the kernel runs, prime-agent's runtime
and skills, the team skill). `pixi install -e prod` gives the runtime without
test tools. The HTTP contract
and environment variables are in [API.md](API.md).

## Layout

| File | What |
| --- | --- |
| `src/runner/harness.py` | `PrimeAgent` (how a thread's prime-agent starts, its kernel wrapper), `Harness` (its RPC) |
| `src/runner/sandbox.py` | `View` (what a thread's kernel sees), bubblewrap arguments, the kernel's environment |
| `src/runner/team.py` | the runner's side of the team skill: a thread's socket for peer and web calls |
| `skills/team/` | the team skill: its SKILL.md and the `team` module in the kernel |
| `src/runner/web.py` | `DeepSeekWeb`: DeepSeek Search & Fetch API, same `DEEPSEEK_API_KEY` as the model |
| `src/runner/thread.py` | `AgentThread` (prime-agent events to runner events, turns) and `ThreadHost` (workspaces, capacity) |
| `src/runner/repo.py` | the worker's repository, per-thread worktrees, snapshots, sessions, bundles to and from the orchestrator |
| `src/runner/peers.py` | `OrchestratorPeers` (queued for the orchestrator to collect) and `LocalPeers` (same process) |
| `src/runner/server.py` | Starlette app |
| `src/runner/cli.py` | one-shot local run |

## Deploying a worker

On any Linux box with git, curl, unprivileged user namespaces and, if a container runtime masks parts of `/proc`, passwordless sudo (bubblewrap needs them, see [API.md](API.md#sandbox)): `deploy/bootstrap.sh` installs pixi, checks out
this repo and installs the `prod` and `kernel` environments and prime-agent;
`deploy/start.sh` runs the service in the foreground. On Sprites, run bootstrap once through exec, checkpoint, then register
`deploy/start.sh` as a sprite service with `http_port` 8080 and the env vars from API.md.
