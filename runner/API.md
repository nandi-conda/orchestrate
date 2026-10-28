# Runner HTTP API

One runner process per worker (a Sprite, a container, a laptop). It hosts many
threads, of any number of projects: workers are a pool the orchestrator shares
between all its projects. Default port 8080. Every route except `/health` needs
`Authorization: Bearer $RUNNER_TOKEN` when `RUNNER_TOKEN` is set.

## Routes

| Method | Path | Body | Returns |
| --- | --- | --- | --- |
| GET | `/health` | | `{"ok", "threads", "running", "capacity", "commit", "boot", "spec"}`: `commit` is the orchestrate checkout the runner runs from (null outside a git checkout); `boot` is random per runner process, so a control plane can tell the runner restarted (its threads are gone from memory, its event `seq` starts again at 1); `spec` is `$RUNNER_SPEC`, the control plane's fingerprint of how it started this runner ("" when unset) |
| GET | `/threads` | | list of directory entries |
| POST | `/threads` | `{"project", "request"?, "images"?, "title"?, "parent"?, "model"?, "id"?, "summary"?, "objectives"?}` | 201 + directory entry, `queued`, at once: the thread's worktree and session are made after (`preparing`, `prepared` events, below); 400 without `project` or with objectives that are not valid (below); 409 if `id` is running here |
| GET | `/threads/{id}` | | directory entry + `"events"` (count) |
| POST | `/threads/{id}/messages` | `{"body", "images"? = [], "from"? = "user", "hops"? = 0}` | 202; 409 once the thread has ended (`done`, `stopped`, `failed`): start it again first |
| POST | `/threads/{id}/stop` | | `{"ok": true}` |
| GET | `/threads/{id}/events` | | SSE stream for one thread |
| GET | `/threads/{id}/files` | `?path=<dir>` (default: the root) | `{"path", "entries": [{"name", "dir", "size"}]}`, directories first; 409 while its worktree is being made |
| GET | `/threads/{id}/files/raw` | `?path=<file>` | the file's bytes, `application/octet-stream`; 409 while its worktree is being made |
| GET | `/projects/{project}/threads/{id}/repo` | `?have=<sha>,<sha>` | a git bundle of `thread/<id>`, its snapshot and its session; header `x-git-refs` (below) |
| GET | `/projects/{project}/repo` | | `{"base", "refs": {"<refname>": "<sha>"}}`: the project's repository's refs on this worker |
| POST | `/projects/{project}/repo` | a git bundle (may be empty); headers `x-git-base`, `x-git-refs` | `{"ok": true}`; 400 for a ref outside `refs/remotes/origin/`, `refs/remotes/origin-snapshots/` and `refs/remotes/origin-sessions/` |
| POST | `/projects/prune` | `{"keep": ["<project>", ...]}` | `{"removed": [...]}`: every other project is taken off this worker (below) |
| GET | `/events` | | SSE stream for every thread on this worker |
| GET | `/peer/calls` | `?wait=25` | queued peer calls (below) |
| POST | `/peer/calls/{id}` | `{"ok", "result"}` or `{"ok": false, "error"}` | `{"ok": true}`; 404 if unknown or already answered |

- `id` lets the orchestrator pick thread ids so they match its own records. Omitted, the runner makes `t_<10 hex>`. An `id` whose thread on this worker has ended (`done`, `stopped`, `failed`) is started afresh.
- `request`, `id` or `images` is required. An `id` with no `request` brings a thread back: see [Resuming a thread](#resuming-a-thread).
- `images` are pasted pictures: `[{"media_type": "image/png", "data": "<base64>"}]`, at most 4 per message, each at most 8 MB, types `image/png`, `image/jpeg`, `image/webp` or `image/gif`. They become image parts of the user message the model reads; on a thread whose model takes text only (`deepseek-v4-pro`), prime-agent sends that turn to its `imageModel`, `deepseek/deepseek-flash` (`IMAGE_MODEL` in `src/runner/harness.py`). They need no text: `request` (or a message's `body`) may be empty. On `/threads` they belong to the request; on a message, to that message. The orchestrator validates them too, and a worker reachable on its own checks its own.
- `model` is `{"provider", "model"}`, passed to prime-agent as `--provider` and `--model`. It runs `--offline`, so the models it knows are the ones `runner/models.json` declares: `deepseek` (`deepseek-flash`, `deepseek-v4-pro`, key `DEEPSEEK_API_KEY`) and `opengateway` (`deepseek/deepseek-v4.1-flash-ultrafast` at `https://apis.opengateway.ai/v1`, key `OPENGATEWAY_API_TOKEN`, the provider's `apiKey`). A model it does not declare, or whose key the runner has not got, is refused with 409 before a thread is made. Omitted: `deepseek` with `$DEEPSEEK_MODEL` (`deepseek-flash` when unset).
- A message (or `request`) starting with `/` is a prime-agent slash command.
- `files` paths are relative to the thread's worktree. A path that leaves it (`..`, a symlink out) or names `.git` is 400, as is a file listed as a directory or the reverse; a missing one is 404. `.git` is never listed.
- `from` on a message is `"user"` or the sending thread id; non-user messages reach the model as `[from <id>] <body>`. A message posted while a turn runs steers it: the model reads it before its next call.
- A message whose `hops` exceeds the limit (6) is dropped with a `dropped` event. The orchestrator should increment `hops` when it forwards a thread-to-thread message.

### Directory entry

```json
{"id": "t_1a2b3c4d5e", "project": "p", "title": "Fix export", "state": "working",
 "summary": "owns ui/export.py",
 "objectives": [{"text": "Find why Safari ignores the click", "status": "done", "note": "handler bound on click only"},
                {"text": "Fix it and add a test", "status": "in_progress", "note": "test written"}],
 "files": ["ui/export.py"], "parent": null}
```

`state` is one of `queued`, `working`, `idle`, `done`, `failed`, `stopped`. Threads are long-lived: after a turn they go `idle` and wake on the next message.

`objectives` are what the thread is trying to accomplish (`src/runner/objectives.py`): its agent sets them from its request on its first turn and keeps them current with the team skill (`set_objectives`, `update_objective`). Each is `{"text", "status", "note"}`, `status` one of `pending`, `in_progress`, `done`, `note` where it is at; one to 20 of them, `[]` until the agent sets them. A turn that ends with every objective done, and no message waiting, finishes the thread: it goes `done` and its task ends (the orchestrator archives it). A thread brought back is posted with its `objectives` as they last stood.

### SSE

Each event is sent as `id: <seq>`, `event: <type>`, `data: <json>`. Resume with `?after=<seq>` or the `Last-Event-ID` header; seq is per worker and increases across all threads. A `: ping` comment every 15 s keeps proxies open.

```json
{"seq": 12, "thread": "t_1a2b3c4d5e", "type": "tool_call", "ts": 1761339600.1,
 "data": {"id": "call_0", "name": "python_repl", "arguments": {"code": "r = await bash('pytest -q')"}, "step": 1}}
```

| type | data |
| --- | --- |
| `created` | directory entry |
| `preparing` | `{"step"}`: a step of making the thread's worktree and session after `POST /threads` answered ("making its worktree", "taking its session") |
| `prepared` | `{"resumed", "idle"}`: its worktree and session are made; `resumed` when it resumes a turn cut short here (below), `idle` when it waits for its next message; else it works on its request. Messages posted before this steer the first turn from its start (the control plane sends a thread on a job only steering). A failure is an `error` and state `failed` instead |
| `state` | `{"state"}` |
| `message_in` | `{"from", "body"}` |
| `delta` | `{"kind", "text", "name"?}`: the model's reply while it streams. `kind` is `reasoning`, `content` or `tool` (`name` is the tool, `python_repl`; `text` is a piece of its JSON arguments, empty when the call starts). Batched about every 0.25 s. A `reasoning` or `content` part starts at its first non-blank character. |
| `reasoning` | `{"content"}`: the model's whole reasoning for one step, once it is done; none when it is blank |
| `assistant` | `{"content"}`: the model's reply text for one step; none when it is blank (DeepSeek writes `"\n\n"` before a tool call) |
| `tool_call` | `{"id", "name", "arguments", "step"}`: the model's one tool is `python_repl` (`{"code"}`); `step` numbers the model call within its turn |
| `tool_result` | `{"id", "name", "result"}` (cut to 4000 chars) |
| `usage` | prime-agent's token counts for one model call |
| `turn_done` | `{"summary", "final"}` |
| `objectives` | `{"objectives"}`: the thread's objectives, whole, each time its agent changes them |
| `saved` | `{"branch", "commit"}`: a turn a model call failed in ended; leftovers committed, the orchestrator took the branch and snapshot but did not land it |
| `landed` | `{"branch", "base", "commit"}`: a turn ended and the branch is now the tip of `base` in the orchestrator's repository |
| `land_conflict` | `{"branch", "base", "files"}`: rebasing onto `base` conflicted; the thread gets a message from `runner` and resolves it |
| `dropped` | `{"from", "reason"}` |
| `error` | `{"error"}` |

## Resuming a thread

A thread lives on after its worker: the orchestrator takes its branch, a
snapshot of its worktree and its session every turn. The session is
prime-agent's: the conversation file (with its compactions) and the kernel's
harness state, kept in the thread's home and committed by the runner as
`refs/sessions/<id>` (a commit of those directories, whose parent is the
previous turn's). To bring a thread back on any worker (its old one was
deleted, the orchestrator restarted, or the thread stopped or failed), the
orchestrator sends its refs (`POST /projects/{project}/repo`), then posts `/threads` with the
thread's `id`, `title`, `parent`, `summary` and `objectives`, and usually no `request`:

- The worktree is the worker's own for that thread when it still has one
  holding `origin/thread/<id>`; else it is made from `origin/thread/<id>`
  with the files of `origin-snapshots/<id>`, else it is a new branch off
  `origin/<base>`.
- The session is the worker's own when its `refs/sessions/<id>` already
  contains `origin-sessions/<id>`; else it is checked out from
  `origin-sessions/<id>`. prime-agent resumes it (`--resume`), so the model
  sees its whole conversation, images included, as it last stood.
- Without `request` the thread goes `idle` (a `state` event, after `prepared`) and waits for
  its next message; nothing runs until one is posted.

### Interrupted turns

A turn under way when the runner stops (SIGTERM from its service manager, a
crash) is interrupted, not failed: on a signal the runner marks itself
stopping before its threads' prime-agents die, logs each cut turn ("cut short
by the runner's shutdown") and sends no `error` for it. While a turn runs,
`<workdir>/.team/<id>.turn` exists, naming the session commit it started
from. Posting the thread to this worker again (any `request` is ignored)
resumes the turn when that file is there and the thread's session is still
that commit: the worktree and the session file are as the cut left them, and
the model gets a `message_in` from `runner` saying the turn was cut short and
to finish it. Its `prepared` event says `"resumed": true`. A thread that went on on
another worker since has a newer session, and its stale file is dropped.

A runner owns its workdir and its threads' prime-agents. It holds an
exclusive flock on `<workdir>/.runner.lock` (its pid written in it) while it
lives, and refuses to start when another runner holds it; whoever restarts a
worker's runner stops the holder first (the control plane's LocalProvider
sends it SIGTERM, then SIGKILL after 15s). Each prime-agent is killed by the
kernel the moment its runner ends, however it ends (PR_SET_PDEATHSIG), so no
prime-agent outlives its runner holding a session's lease: a resumed turn's
prime-agent always gets its session.

## Peer calls (collected by the orchestrator)

The orchestrator is never reachable from a worker, by design: it may run on a
laptop behind NAT, and it opens every connection. The team skill's calls
still reach it, by queuing on this runner for the orchestrator to collect:

1. `GET /peer/calls?wait=25` (max 55) long-polls. It returns as soon as a call
   is queued, with every call due: `[{"id", "thread", "op", "args"}]`
   (`thread` is the thread making the call, as the runner knows it from the
   socket the call came on), or `[]` after
   `wait` seconds with none, or when the connection asking is closed (an
   orchestrator that went away; a poll outliving it would hold the worker open
   while it stops).
2. The orchestrator runs each call (concurrently) as the calling thread,
   limited to that thread's project: a worker holds threads of many projects,
   and none sees another project's.
3. `POST /peer/calls/{id}` with `{"ok": true, "result": ...}` or
   `{"ok": false, "error": "..."}` wakes the waiting tool; an error becomes the
   tool's result. 404 if the call is unknown or already answered.

A call handed out but not answered within 60 s is handed out again, so a lost
poll response only delays it. A tool waits up to 15 minutes for its answer
(spawning can wait on a new worker). With no orchestrator polling, peer tools
wait and then fail; the CLI (`pixi run run`) uses in-process peers instead.

A thread makes these calls from its kernel with the team skill
(`skills/team`, `await team.send_message(...)`), over a unix socket of its own
bound into its sandbox; the runner queues them here. `web_search`,
`web_fetch`, `objectives`, `set_objectives` and `update_objective` go through
the same socket but are answered by the runner itself (an `objectives` event
carries each change).

| `op` (the tool) | `args` | `result` |
| --- | --- | --- |
| `list_threads` | `{"project", "query"}` (`query` may be null; `project` is the caller's) | list of directory entries (the thread itself is left out of what it sees) |
| `read_thread` | `{"thread_id", "last_n"}` | directory entry + `"events"`: recent event list |
| `send_message` | `{"sender", "to", "body", "reply_to"}` | `{"ok": true}` |
| `spawn_thread` | `{"parent", "project", "title", "request"}` | `{"id", "title"}` |
| `set_summary` | `{"thread_id", "summary", "files"}` | anything |
| `save` | `{"thread_id", "land"}` | `{"landed", "base", "commit"?, "conflict"?, "message"?}`: see [Repository](#repository) |

A thread also calls `set_summary` after each turn, and when it has a `parent`, sends
`Finished: <summary>` to the parent through `send_message`. The orchestrator routes
`send_message` to whichever worker holds `to` (`POST /threads/{to}/messages` with
`hops + 1`), and `spawn_thread` to the least loaded worker of its pool.

## Environment

Secrets (`DEEPSEEK_API_KEY`, `OPENGATEWAY_API_TOKEN`, `RUNNER_TOKEN`) are read from the environment first, then from the system keyring under service `orchestrate` with the variable name as the username (`pixi run set-key DEEPSEEK_API_KEY`). An item added by hand in a keyring GUI is found too when its label is the variable name; only unlocked collections are searched. `serve` and `run` print the missing ones, with the keyring backend in use, and exit if a required secret is in neither place.

| Var | Default | Meaning |
| --- | --- | --- |
| `DEEPSEEK_API_KEY` | | required: prime-agent's `deepseek` provider and the web calls send it as `Authorization: Bearer` |
| `OPENGATEWAY_API_TOKEN` | | for threads on the `opengateway` provider: prime-agent sends it as `Authorization: Bearer` to `https://apis.opengateway.ai/v1` |
| `DEEPSEEK_MODEL` | `deepseek-flash` | default model |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com` | for web search and fetch |
| `RUNNER_TOKEN` | | bearer token the orchestrator uses to call this runner |
| `RUNNER_PORT` / `RUNNER_HOST` | `8080` / `0.0.0.0` | |
| `RUNNER_WORKDIR` | `/work/threads` | where workspaces go: `<id>/` (the worktree), `.home/<id>/`, `.tmp/<id>/`, `.repos/<project>/`, `.cache/<project>/` |
| `RUNNER_CAPACITY` | `16` | reported in `/health` for scheduling |
| `RUNNER_SPEC` | | reported in `/health` as `spec`: the control plane's fingerprint of the service it started |

## Sandbox

A thread is one [prime-agent](https://github.com/PrimeIntellect-ai/prime-agent)
process (`--mode rpc`, version pinned in `deploy/install-harness.sh`). The
binary runs the agent loop and holds the model's key, outside the sandbox.
Everything the model writes runs in its Python kernel (`python_repl`; shell
commands are `await bash(...)` there), and the kernel runs in
[bubblewrap](https://github.com/containers/bubblewrap) (`bwrap`, from the
runner's pixi environment): prime-agent starts the kernel through a wrapper the
runner writes per thread (`PRIME_AGENT_KERNEL_PYTHON`), which execs bwrap with
this view:

| Path | In the view |
| --- | --- |
| `/` | the machine's system, read-only (its git, compilers, shells) |
| `$RUNNER_WORKDIR/<id>` | the thread's worktree, writable; the kernel starts here |
| `$RUNNER_WORKDIR/.repos/<project>/.git` | its project's repository's git dir, writable, so the thread can commit |
| `$RUNNER_WORKDIR/.home/<id>` | the thread's `HOME`, writable: its venv (`.venv`) and prime-agent's agent dir (`.prime-agent`: settings, `models.json`, skills, the session) |
| `/tmp` | `$RUNNER_WORKDIR/.tmp/<id>`, kept for the thread |
| `runner/.pixi/envs/kernel`, prime-agent's bundled skills | read-only: the kernel's Python and packages |
| `$PIXI_EXE` | read-only, at its own path and on `PATH`: the pixi the runner runs under (every project is a pixi project); it keeps its global tools in the thread's `HOME` |
| `$RUNNER_WORKDIR/.cache/<project>` | pixi's cache for the project, writable and shared by its threads on this worker (`PIXI_CACHE_DIR`): the packages, and the worktrees' environments (`PIXI_CONFIG_FILE` sets `detached-environments`), in one mount so the environments hard-link to the packages instead of copying them |
| `$RUNNER_WORKDIR/.team/<id>.sock` | the thread's team socket |
| the rest of the orchestrate checkout and the runner's pixi environment, the worker user's home, the rest of `$RUNNER_WORKDIR` | empty: the runner's code, other threads and projects, the keyring and caches are not there |
| `/proc` | its own pid namespace: the runner's processes, and their environments, are not in it |

The kernel's environment is made afresh: `PATH` (the thread's venv, the kernel
environment, pixi's directory, the system's), `VIRTUAL_ENV`, `HOME`, `TMPDIR=/tmp`, `USER`,
`LOGNAME`, `SHELL`, `LANG`, `LC_*`, `TERM`, `TZ`, `ORCHESTRATE_TEAM_SOCKET`,
and prime-agent's own variables for its kernel (`RLM_*`, `PRIME_AGENT_*`). No
token, key, `RUNNER_*` or pixi variable reaches it. The venv is the thread's
(over the kernel environment's packages), so `uv pip install` installs there.
The kernel ends with the thread's prime-agent.

prime-agent's skills that need its daemon, a Serper key or Prime Intellect
accounts are turned off (`DROPPED_SKILLS` in `harness.py`); the team skill
takes their place. The Files API resolves paths inside the worktree (symlinks
included) and refuses anything outside it.

`serve` and `run` check at startup that bubblewrap can make a sandbox and exit
with bubblewrap's error if not. It needs unprivileged user namespaces and a
fully visible `/proc`: the kernel refuses a user namespace its own `/proc` while
anything is mounted over parts of the machine's. Container runtimes do that
(crun on Sprites masks `/proc/kcore`, `keys`, `timer_list`, `scsi`), so
`deploy/start.sh` unmounts whatever is mounted under `/proc` (with `sudo`)
before it starts the service. They also clear the ambient capabilities a
non-root service may hold before starting bubblewrap, which refuses them
(Sprites gives its user a set). The runner is Linux-only for this reason.

## Repository

The orchestrator keeps one git repository per project (its hub); it is the
only place commits live beyond a worker. The worker has a repository of its
own per project, `$RUNNER_WORKDIR/.repos/<project>`, made when the orchestrator
first sends that project's commits, with no remote: the worker reaches nothing, and
the orchestrator moves commits both ways as git bundles over this API. A
bundle carries objects only; ref tips travel as JSON in `x-git-refs`
(`{"<refname>": "<sha>"}`), since a bundle leaves out a ref whose tip the
other side already has.

- `POST /projects/{project}/repo` brings the hub's commits: the default branch as
  `refs/remotes/origin/<base>` (`x-git-base` names it), a thread's branch as
  `refs/remotes/origin/thread/<id>` and its snapshot as
  `refs/remotes/origin-snapshots/<id>`, its session as
  `refs/remotes/origin-sessions/<id>`. The orchestrator sends the base
  before it starts a thread, and the thread's branch, snapshot and session before it
  brings one back. It leaves out of the bundle what `GET /projects/{project}/repo` says
  the worker has.
- `GET /projects/{project}/threads/{id}/repo?have=...` gives the thread's `refs/heads/thread/<id>`,
  `refs/snapshots/<id>` and `refs/sessions/<id>`, without the commits `have` lists (the hub's tips;
  ones the worker does not know are ignored). The body is empty when the hub
  has everything.

Threads of a project on one worker share its repository's objects and refs; each has its
own worktree, index and branch. A turn's work leaves the worker on one path:

1. A thread commits on `thread/<id>` as it likes; there is no remote to push to.
2. At the end of every turn the runner commits anything left uncommitted and
   makes a snapshot: a commit of the whole worktree, untracked and ignored
   files included, whose parent is the branch tip (`refs/snapshots/<id>`), and
   commits the session (`refs/sessions/<id>`). It then asks the orchestrator
   to `save` (a peer call), which takes all three with
   `GET /projects/{project}/threads/{id}/repo`.
3. `save` has `land: true` unless a model call failed in the turn (the work
   is then only taken, and the runner logs `saved`): the orchestrator rebases the branch onto the default branch in its hub (and,
   when the project has a GitHub repository, pushes the new tip there first),
   sends back the new `origin/<base>` and `origin/thread/<id>`, and answers
   `{"landed": true, "base", "commit"}`. The worktree's branch follows
   (`git reset --keep`) and the runner logs `landed`. No merge commits.
4. A conflicting rebase is answered `{"landed": false, "base", "conflict":
   [files]}`, with the latest `origin/<base>` sent first. The thread gets a
   message from `runner` (`land_conflict`), rebases onto `origin/<base>`,
   resolves, and ends the turn, which lands again. A run of conflicts ends at the hop limit.

The CLI (`pixi run run --repo <path or URL>`) has no orchestrator: `--repo`
seeds the worker repository (its branches become `origin/*`, its HEAD the
base) and landing moves `origin/<base>` there.

A project leaves a worker with `POST /projects/prune`: the orchestrator sends
the projects it still has (not archived) whenever it attaches to a worker and
after it archives one, and the runner stops and forgets the threads of every
other project, and deletes its repository and their worktrees, homes, `/tmp`
and team sockets. Nothing is lost: the hub has every turn's branch, snapshot
and session, so an unarchived project's threads come back from it.
