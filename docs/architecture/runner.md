# The runner and prime-agent

A **worker** is one runner process (`runner/`) on a Linux machine. It hosts
many threads (capacity 16 by default, reported in `/health` for scheduling),
each with its own worktree, sandbox and agent. The control plane drives it
over the [runner HTTP API](../reference/runner-api.md); the runner never
connects out to the control plane.

## Workers on Fly Sprites

With `SPRITES_TOKEN` stored, each worker is a [Fly Sprite](https://sprites.dev)
named `orch-<project>-w<n>`. Workers are not per thread: a project gets one,
and another only when every worker it has is at capacity. New threads go to
the least loaded worker.

Making a worker (`SpritesProvider.create`):

1. `POST /v1/sprites` creates the sprite (URL auth `public` at the proxy; the
   runner itself requires its `RUNNER_TOKEN`).
2. The commit to run is resolved with `git ls-remote` on this checkout's
   `origin` at its current branch.
3. A bootstrap script, run through exec, checks that commit out at
   `/opt/orchestrate` and runs `runner/deploy/bootstrap.sh`: install pixi, the
   runner's `prod` and `kernel` environments, and the pinned prime-agent binary
   (`deploy/install-harness.sh`, checksummed).
4. `runner/deploy/start.sh` is registered as the sprite service `runner` on
   port 8080, with `DEEPSEEK_API_KEY`, the worker's `RUNNER_TOKEN`,
   `RUNNER_WORKDIR=/home/sprite/threads`, `RUNNER_CAPACITY` and
   `PYTHONUNBUFFERED=1`, then started (or restarted, when a service from an
   earlier control plane is there, so the runner is always this checkout's
   with this control plane's key and token).
5. The control plane waits for `/health`, then checks the runner runs the
   expected commit and accepts the token. Either mismatch is an error carrying
   the end of the worker's log.

Sprites keep their default open outbound network; the orchestrator sets no
network policy. A sprite pauses when nothing is connected to it and wakes cold
on the next request, which restarts the runner service. That is a normal event
in the [worker lifecycle](../worker-lifecycle.md), not a failure.

Any other Linux box works as a worker with the same two scripts: it needs git,
curl, unprivileged user namespaces, and passwordless sudo if a container
runtime masks parts of `/proc`.

### Local workers

Without `SPRITES_TOKEN`, `LocalProvider` starts runners as subprocesses on this
machine under `.orchestrator/workers/<id>/`: the real runner from `../runner`
when `DEEPSEEK_API_KEY` is stored (Linux only), the echo runner otherwise.

## A thread is a prime-agent process

Each thread is one [prime-agent](https://github.com/PrimeIntellect-ai/prime-agent)
process (version pinned in `runner/src/runner/harness.py` and
`deploy/install-harness.sh`, currently 0.10.1) started as
`prime-agent --mode rpc --offline`: one JSON command per line on stdin
(`prompt`, `abort`, ...), one event per line on stdout (`agent_start`,
`message_update`, `tool_execution_start`, `turn_end`, ...). The runner turns
those into its own events (`delta`, `assistant`, `tool_call`, `turn_done`, ...).

- **One tool.** The model's only tool is `python_repl`. Shell commands
  (`await bash(...)`), file edits and skills are Python in its kernel.
- **Models.** prime-agent's `deepseek` provider, with `deepseek-flash` by
  default (`$DEEPSEEK_MODEL`) or the project's model. It runs `--offline`, so
  it knows only the models declared in `runner/models.json`
  (`deepseek-flash`, `deepseek-v4-pro`); a new model must be added there. A turn
  with images on a text-only model goes to `imageModel`,
  `deepseek/deepseek-flash`.
- **Skills.** prime-agent's bundled skills that need its daemon, a Serper key
  or Prime Intellect accounts are turned off (`DROPPED_SKILLS`). The **team
  skill** (`runner/skills/team`) takes their place.
- **Slash commands.** A message starting with `/` is a prime-agent slash
  command.
- **Sessions.** prime-agent's conversation file (with its compactions) and
  harness state live in the thread's home. The runner commits them as
  `refs/sessions/<id>` every turn and the control plane takes them into the
  hub, so the thread resumes with `--resume` on any worker, exactly as the
  model last had it, images included.

### The team skill

The `team` module is ready in every thread's kernel; all functions are async:

| Call | Does |
| --- | --- |
| `team.list_threads(query=None)` | the project's other threads: id, title, state, summary, files, parent |
| `team.read_thread(id, last_n=20)` | a thread's directory entry and recent events |
| `team.send_message(to, body, reply_to=None)` | a message in another thread's inbox |
| `team.spawn_thread(title, request)` | a new thread for independent work; its result comes back as a message |
| `team.set_summary(summary)` | this thread's status line in the directory |
| `team.web_search(query, count=10, question=None)` | DeepSeek Search API results |
| `team.web_fetch(url)` | a page as Markdown |

The module talks to the runner over a unix socket bound into the sandbox. Peer
calls are queued for the control plane to [collect](pull-mode.md#peer-calls);
`web_search` and `web_fetch` are answered by the runner itself, with the same
`DEEPSEEK_API_KEY`. A failed call raises `team.TeamError`.

## The sandbox

The prime-agent binary runs outside the sandbox, since it holds the model's
key. Its kernel, where everything the model writes runs, is started through a
per-thread wrapper (`PRIME_AGENT_KERNEL_PYTHON`) that execs
[bubblewrap](https://github.com/containers/bubblewrap) with this view:

| Path | In the sandbox |
| --- | --- |
| `/` | the machine's system, read-only |
| the thread's worktree | writable; the kernel starts here |
| the worker repository's `.git` | writable, so the thread can commit |
| the thread's home | writable: its venv and prime-agent's agent dir |
| `/tmp` | the thread's own temp directory |
| the kernel's pixi environment | read-only |
| the pixi the runner runs under | read-only, on `PATH`; its cache and global tools go in the thread's home |
| the orchestrate checkout, the runner's environment, the worker user's home, other threads | empty |
| `/proc` | its own pid namespace |

The kernel's environment is built fresh: `PATH`, `HOME`, `TMPDIR`, locale and
terminal variables, the team socket and prime-agent's own variables. No token,
key, `RUNNER_*` or pixi variable reaches it. `uv pip install` installs into the
thread's venv, which is for the agent's scratch work: the project's own
dependencies, environments and tasks are pixi's (every project is a pixi
project, see [Project repos](git.md#every-project-is-a-pixi-project)), and the
system prompt tells the agent so. Snapshots leave pixi's environments out
(everything in a `.pixi/` directory's subdirectories); `pixi install` or
`pixi run` makes them again from `pixi.lock`.

bubblewrap needs unprivileged user namespaces and a fully visible `/proc`.
Container runtimes mask parts of `/proc` (crun on Sprites masks `kcore`,
`keys`, `timer_list`, `scsi`), so `deploy/start.sh` unmounts those with `sudo`
at every start; the runner also clears ambient capabilities before starting
bubblewrap, which refuses them. `serve` and `run` check at startup that a
sandbox can be made, and exit with bubblewrap's error if not. This is why the
runner is Linux only.

The exact paths are in the [runner API reference](../reference/runner-api.md#sandbox).

## Logs

Every worker's runner log streams to the control plane's stdout, each line
prefixed with its worker (`orch-p_ab12-w0 | INFO: runner.harness: ...`);
attaching a worker prints its last 50 lines first. For a sprite the control
plane follows `GET /v1/sprites/<name>/services/runner/logs` (NDJSON); for a
local worker, its `runner.log`. Tracebacks, prime-agent's stderr, failed model
calls (the whole message but its content) and failed tool calls all land
there. The same log is in each thread's **Worker log** tab
(`/api/threads/<id>/worker-logs?lines=N`). When a worker is lost, the control
plane keeps why and the last 300 lines, since destroying it takes its log.

To read a live sprite's log without the UI:

```sh
curl -sS -H "Authorization: Bearer $SPRITES_TOKEN" \
  "https://api.sprites.dev/v1/sprites/orch-<project>-w<n>/fs/read?path=/.sprite/logs/services/runner.log&workingDir=/" | tail -n 300
```
