# Runner HTTP API

One runner process per worker (a Sprite, a container, a laptop). It hosts many
threads. Default port 8080. Every route except `/health` needs
`Authorization: Bearer $RUNNER_TOKEN` when `RUNNER_TOKEN` is set.

## Routes

| Method | Path | Body | Returns |
| --- | --- | --- | --- |
| GET | `/health` | | `{"ok", "threads", "running", "capacity"}` |
| GET | `/threads` | | list of directory entries |
| POST | `/threads` | `{"request"?, "images"?, "title"?, "project"?, "parent"?, "model"?, "id"?, "history"?, "summary"?}` | 201 + directory entry; 409 if `id` is running here |
| GET | `/threads/{id}` | | directory entry + `"events"` (count) |
| POST | `/threads/{id}/messages` | `{"body", "images"? = [], "from"? = "user", "hops"? = 0}` | 202 |
| POST | `/threads/{id}/stop` | | `{"ok": true}` |
| GET | `/threads/{id}/events` | | SSE stream for one thread |
| GET | `/threads/{id}/files` | `?path=<dir>` (default: the root) | `{"path", "entries": [{"name", "dir", "size"}]}`, directories first |
| GET | `/threads/{id}/files/raw` | `?path=<file>` | the file's bytes, `application/octet-stream` |
| GET | `/threads/{id}/repo` | `?have=<sha>,<sha>` | a git bundle of `thread/<id>` and its snapshot; header `x-git-refs` (below) |
| GET | `/repo` | | `{"base", "refs": {"<refname>": "<sha>"}}`: the worker repository's refs |
| POST | `/repo` | a git bundle (may be empty); headers `x-git-base`, `x-git-refs` | `{"ok": true}`; 400 for a ref outside `refs/remotes/origin/` and `refs/remotes/origin-snapshots/` |
| GET | `/events` | | SSE stream for every thread on this worker |
| GET | `/peer/calls` | `?wait=25` | queued peer calls (below) |
| POST | `/peer/calls/{id}` | `{"ok", "result"}` or `{"ok": false, "error"}` | `{"ok": true}`; 404 if unknown or already answered |

- `id` lets the orchestrator pick thread ids so they match its own records. Omitted, the runner makes `t_<10 hex>`. An `id` whose thread on this worker has ended (`stopped`, `failed`) is started afresh.
- `request`, `history` or `images` is required. `history` resumes a thread: see [Resuming a thread](#resuming-a-thread).
- `images` are pasted pictures: `[{"media_type": "image/png", "data": "<base64>"}]`, at most 4 per message, each at most 8 MB, types `image/png`, `image/jpeg`, `image/webp` or `image/gif`. They become image parts of the user message the model reads, so they need no text: `request` (or a message's `body`) may be empty. On `/threads` they belong to the request; on a message, to that message. The orchestrator validates them too, and a worker reachable on its own checks its own.
- `model` is `{"provider": "deepseek", "model": "deepseek-v4-pro"}` or `{"provider": "openai-compat", "model", "base_url", "api_key_env"}`. Omitted: DeepSeek with `$DEEPSEEK_MODEL`.
- `files` paths are relative to the thread's worktree. A path that leaves it (`..`, a symlink out) or names `.git` is 400, as is a file listed as a directory or the reverse; a missing one is 404. `.git` is never listed.
- `from` on a message is `"user"` or the sending thread id; non-user messages reach the model as `[from <id>] <body>`.
- A message whose `hops` exceeds the limit (6) is dropped with a `dropped` event. The orchestrator should increment `hops` when it forwards a thread-to-thread message.

### Directory entry

```json
{"id": "t_1a2b3c4d5e", "project": "p", "title": "Fix export", "state": "working",
 "summary": "owns ui/export.py", "files": ["ui/export.py"], "parent": null}
```

`state` is one of `queued`, `working`, `idle`, `failed`, `stopped`. Threads are long-lived: after a turn they go `idle` and wake on the next message.

### SSE

Each event is sent as `id: <seq>`, `event: <type>`, `data: <json>`. Resume with `?after=<seq>` or the `Last-Event-ID` header; seq is per worker and increases across all threads. A `: ping` comment every 15 s keeps proxies open.

```json
{"seq": 12, "thread": "t_1a2b3c4d5e", "type": "tool_call", "ts": 1761339600.1,
 "data": {"id": "call_0", "name": "bash", "arguments": {"command": "pytest -q"}}}
```

| type | data |
| --- | --- |
| `created` | directory entry |
| `state` | `{"state"}` |
| `message_in` | `{"from", "body"}` |
| `delta` | `{"kind", "text", "name"?}`: the model's reply while it streams. `kind` is `reasoning`, `content` or `tool` (`name` is the tool; `text` is a piece of its JSON arguments, empty when the call starts). Batched about every 0.25 s. |
| `reasoning` | `{"content"}`: the model's whole reasoning for one step, once it is done |
| `assistant` | `{"content"}` |
| `tool_call` | `{"id", "name", "arguments", "step"}`: `step` numbers the model call within its turn |
| `tool_result` | `{"id", "name", "result"}` (cut to 4000 chars) |
| `usage` | provider token counts |
| `turn_done` | `{"summary", "final"}` |
| `saved` | `{"branch", "commit"}`: a turn ended without `finish`; leftovers committed, the orchestrator took the branch and snapshot |
| `landed` | `{"branch", "base", "commit"}`: a turn ended with `finish` and the branch is now the tip of `base` in the orchestrator's repository |
| `land_conflict` | `{"branch", "base", "files"}`: rebasing onto `base` conflicted; the thread gets a message from `runner` and resolves it |
| `dropped` | `{"from", "reason"}` |
| `error` | `{"error"}` |

## Resuming a thread

A thread lives on after its worker: the orchestrator takes its branch and a
snapshot of its worktree every turn, and keeps its events. To bring it back on any worker (its old one was
deleted, the orchestrator restarted, or the thread stopped or failed), the
orchestrator posts `/threads` with the thread's `id`, `title`, `parent`,
`summary` and `history`, and usually no `request`:

- `history` is the thread's earlier events, oldest first, in the SSE shape
  (`{"type", "data"}`; other keys are ignored). `message_in`, `reasoning`,
  `assistant`, `tool_call` and `tool_result` rebuild the conversation after the
  system prompt; other types are skipped. Only messages the thread received
  belongs in it. A `message_in` may also carry `images` (`[{"media_type", "data"}]`, the bytes base64-encoded): the orchestrator inlines a request's and a message's images there, since a worker cannot reach the control plane to fetch them. A tool call with no result is answered as stopped. Tool
  results are the events' copies, cut to 4000 characters.
- The worktree is the worker's own for that thread when it still has one
  holding `origin/thread/<id>`; else it is made from `origin/thread/<id>`
  with the files of `origin-snapshots/<id>` (the orchestrator sends both
  first, `POST /repo`), else it is a new branch off `origin/<base>`.
- Without `request` the thread goes `idle` (a `state` event) and waits for
  its next message; nothing runs until one is posted.

## Peer calls (collected by the orchestrator)

The orchestrator is never reachable from a worker, by design: it may run on a
laptop behind NAT, and it opens every connection. The inter-thread tools still
reach it, by queuing their calls on this runner for the orchestrator to collect:

1. `GET /peer/calls?wait=25` (max 55) long-polls. It returns as soon as a call
   is queued, with every call due: `[{"id", "op", "args"}]`, or `[]` after
   `wait` seconds with none.
2. The orchestrator runs each call (concurrently) as this worker, limited to
   the worker's project.
3. `POST /peer/calls/{id}` with `{"ok": true, "result": ...}` or
   `{"ok": false, "error": "..."}` wakes the waiting tool; an error becomes the
   tool's result. 404 if the call is unknown or already answered.

A call handed out but not answered within 60 s is handed out again, so a lost
poll response only delays it. A tool waits up to 15 minutes for its answer
(spawning can wait on a new worker). With no orchestrator polling, peer tools
wait and then fail; the CLI (`pixi run run`) uses in-process peers instead.

| `op` (the tool) | `args` | `result` |
| --- | --- | --- |
| `list_threads` | `{"project", "query"}` (`query` may be null) | list of directory entries |
| `read_thread` | `{"thread_id", "last_n"}` | directory entry + `"events"`: recent event list |
| `send_message` | `{"sender", "to", "body", "reply_to"}` | `{"ok": true}` |
| `spawn_thread` | `{"parent", "project", "title", "request"}` | `{"id", "title"}` |
| `set_summary` | `{"thread_id", "summary", "files"}` | anything |
| `save` | `{"thread_id", "land"}` | `{"landed", "base", "commit"?, "conflict"?, "message"?}`: see [Repository](#repository) |

A thread also calls `set_summary` after each turn, and when it has a `parent`, sends
`Finished: <summary>` to the parent through `send_message`. The orchestrator routes
`send_message` to whichever worker holds `to` (`POST /threads/{to}/messages` with
`hops + 1`), and `spawn_thread` to the least loaded worker.

## Environment

Secrets (`DEEPSEEK_API_KEY`, `RUNNER_TOKEN`, and an `openai-compat` model's `api_key_env`) are read from the environment first, then from the system keyring under service `orchestrate` with the variable name as the username (`pixi run set-key DEEPSEEK_API_KEY`). An item added by hand in a keyring GUI is found too when its label is the variable name; only unlocked collections are searched. `serve` and `run` print the missing ones, with the keyring backend in use, and exit if a required secret is in neither place.

| Var | Default | Meaning |
| --- | --- | --- |
| `DEEPSEEK_API_KEY` | | required for DeepSeek, sent as `Authorization: Bearer` |
| `DEEPSEEK_MODEL` | `deepseek-v4-pro` | default model |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com` | |
| `RUNNER_TOKEN` | | bearer token the orchestrator uses to call this runner |
| `RUNNER_PORT` / `RUNNER_HOST` | `8080` / `0.0.0.0` | |
| `RUNNER_WORKDIR` | `/work/threads` | where workspaces go: `<id>/` (the worktree), `.home/<id>/`, `.tmp/<id>/`, `.repo/` |
| `RUNNER_CAPACITY` | `16` | reported in `/health` for scheduling |
| `RUNNER_MAX_STEPS` | `60` | model calls per turn |

## Sandbox

A thread works on its project and sees nothing else of the worker. Every `bash`
command runs in [bubblewrap](https://github.com/containers/bubblewrap)
(`bwrap`, from the runner's pixi environment) with this view:

| Path | In the view |
| --- | --- |
| `/` | the machine's system, read-only (its git, compilers, shells) |
| `$RUNNER_WORKDIR/<id>` | the thread's worktree, writable; commands start here |
| `$RUNNER_WORKDIR/.repo/.git` | the worker repository's git dir, writable, so the thread can commit |
| `$RUNNER_WORKDIR/.home/<id>` | the thread's `HOME`, writable, kept between commands |
| `/tmp` | `$RUNNER_WORKDIR/.tmp/<id>`, kept between commands |
| the orchestrate checkout the runner runs from, its pixi environment, the worker user's home, the rest of `$RUNNER_WORKDIR` | empty: the runner's code, other threads, the keyring and caches are not there |
| `/proc` | its own pid namespace: the runner's processes, and their environments, are not in it |

The environment is made afresh for each command: `PATH` (without the hidden
directories), `HOME`, `TMPDIR=/tmp`, `USER`, `LOGNAME`, `SHELL`, `LANG`,
`LC_*`, `TERM`, `TZ`. No token, key, `RUNNER_*` or pixi variable reaches it.
Processes end with the command that started them.

`read_file`, `write_file`, `edit_file` and `list_dir` resolve paths inside the
worktree (symlinks included) and refuse anything outside it, as does the
Files API.

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
own, `$RUNNER_WORKDIR/.repo`, with no remote: the worker reaches nothing, and
the orchestrator moves commits both ways as git bundles over this API. A
bundle carries objects only; ref tips travel as JSON in `x-git-refs`
(`{"<refname>": "<sha>"}`), since a bundle leaves out a ref whose tip the
other side already has.

- `POST /repo` brings the hub's commits: the default branch as
  `refs/remotes/origin/<base>` (`x-git-base` names it), a thread's branch as
  `refs/remotes/origin/thread/<id>` and its snapshot as
  `refs/remotes/origin-snapshots/<id>`. The orchestrator sends the base
  before it starts a thread, and the thread's branch and snapshot before it
  brings one back. It leaves out of the bundle what `GET /repo` says the
  worker has.
- `GET /threads/{id}/repo?have=...` gives the thread's `refs/heads/thread/<id>`
  and `refs/snapshots/<id>`, without the commits `have` lists (the hub's tips;
  ones the worker does not know are ignored). The body is empty when the hub
  has everything.

Threads on one worker share the repository's objects and refs; each has its
own worktree, index and branch. A turn's work leaves the worker on one path:

1. A thread commits on `thread/<id>` as it likes; there is no remote to push to.
2. At the end of every turn the runner commits anything left uncommitted and
   makes a snapshot: a commit of the whole worktree, untracked and ignored
   files included, whose parent is the branch tip (`refs/snapshots/<id>`). It
   then asks the orchestrator to `save` (a peer call), which takes both with
   `GET /threads/{id}/repo` (`saved`).
3. When the turn ended with `finish`, `save` has `land: true`: the
   orchestrator rebases the branch onto the default branch in its hub (and,
   when the project has a GitHub repository, pushes the new tip there first),
   sends back the new `origin/<base>` and `origin/thread/<id>`, and answers
   `{"landed": true, "base", "commit"}`. The worktree's branch follows
   (`git reset --keep`) and the runner logs `landed`. No merge commits.
4. A conflicting rebase is answered `{"landed": false, "base", "conflict":
   [files]}`, with the latest `origin/<base>` sent first. The thread gets a
   message from `runner` (`land_conflict`), rebases onto `origin/<base>`,
   resolves, and calls `finish` again. A run of conflicts ends at the hop limit.

The CLI (`pixi run run --repo <path or URL>`) has no orchestrator: `--repo`
seeds the worker repository (its branches become `origin/*`, its HEAD the
base) and landing moves `origin/<base>` there.
