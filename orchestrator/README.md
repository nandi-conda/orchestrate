# orchestrator

The control plane and the ultra UI. Everything happens in one browser window:
the project list, each project, and each thread's transcript, tool calls and
worker log. A request typed in a project goes to a router, which spawns new threads or messages
existing ones. Threads run on runner workers (see `../runner`), and the control
plane relays their events back to the window as they happen.

## Run it locally

You need [pixi](https://pixi.sh). The orchestrator works on linux-64, osx-64,
osx-arm64 and win-64; ultra's compiler (lpython 0.22) has no linux-aarch64 build.

```sh
git clone https://github.com/nandi-conda/orchestrate && cd orchestrate

# once: store the DeepSeek key in the system keyring (or export DEEPSEEK_API_KEY)
cd runner && pixi install && pixi run set-key DEEPSEEK_API_KEY && cd ..

cd orchestrator
pixi run serve            # builds the UI, then serves http://127.0.0.1:8000
```

Open http://127.0.0.1:8000, create a project and give it the path or URL of a
git repo. It opens in the same window at `/p/<project id>`, and
**← All projects**, Back and Forward move between the list and projects
without reloading or opening anything new. Type a request and press Enter:

- **Router decides** (default): the router spawns threads or messages
  existing ones, leaning toward spawning. A bulleted or numbered list fans out
  into one thread per item.
- **This thread**: sends the message straight to the selected thread.
- **New thread**: always starts a fresh thread.

Pressing Enter never waits on the router, a worker or the runner: the box
clears at once and the request shows in the log. The router's decision then
shows above the request box ("routing…" until the model answers). A new thread
appears in the list right away and says "starting on a worker" until its worker
is up (a sprite can take a while); messages sent to it meanwhile are delivered,
in order, once it starts. **Project activity** in the
thread list shows every event in the project, and each thread shows its own
transcript, including messages from other threads. A request that spawns
threads never moves the window into one of them: you stay where you are, the
new threads show up in the list, and their events stream into Project activity.
You're only ever talking to the project, and you open a thread only by
clicking it.

With `DEEPSEEK_API_KEY` stored (keyring, service `orchestrate`), the
orchestrator starts the real runner from `../runner` as a local subprocess,
one worker per project, and the router uses `deepseek-flash`. Without the key it
falls back to an echo runner (`control.devrunner`) and a rule-based router. That
way you can click through the whole UI with no credentials.

The echo runner understands two lines, so you can test thread-to-thread
traffic without a model: `spawn: <task>` starts a child thread, which reports
`Finished: …` back to its parent, and `tell t_xxx: <text>` messages another
thread.

## Tests

```sh
pixi run test             # store, router, the whole loop against the echo runner, the markdown renderer, Postgres if installed
```

The Postgres test starts a throwaway server with `initdb`, which refuses to run
as root, so it is skipped there.

## Configuration

There are no environment variables to set. Three secrets come from the system
keyring (service `orchestrate`, username = the name), and everything else is
worked out at runtime.

| Secret | What | Sent as |
| --- | --- | --- |
| `DEEPSEEK_API_KEY` | router model, and passed to every worker. Sprites need it; the local fallback (used only when `SPRITES_TOKEN` is absent) runs the echo runner without it | `Authorization: Bearer <key>` to `https://api.deepseek.com` |
| `SPRITES_TOKEN` | stored, workers always run on Sprites; without it they fall back to this machine | `Authorization: Bearer <token>` to `https://api.sprites.dev/v1` |
| `GITHUB_TOKEN` | GitHub fine-grained token, Contents: Read and write on the projects' repos. Passed to every worker, which clones the project's `repo_url`, pushes `thread/<id>` branches and lands finished work on the default branch | git `http.<repo url>.extraHeader`: `Authorization: Basic base64("x-access-token:<token>")` |

Store one with `pixi run set-key NAME`. Missing ones are printed at startup with
the exact name, the header they're sent as, and the keyring backend that was
searched. A password added by hand in a keyring GUI (for example
`DEEPSEEK_API_KEY` in the Login keyring) also works: an item labelled exactly
like the variable is accepted, as long as its collection is unlocked. An
environment variable of the same name is read first.

What the control plane works out for itself:

| | How |
| --- | --- |
| Where workers run | Sprites whenever `SPRITES_TOKEN` is stored, even when the browser is on this machine. Local only when no token is stored |
| What a sprite checks out | This checkout's `origin` remote and current branch, with a warning when the sprite would not get this exact commit |
| Which router | The model when `DEEPSEEK_API_KEY` is stored, the rule-based one otherwise |
| Storage | SQLite in `orchestrator.db`, or `--db postgresql://…` for several control planes |

Flags: `--host` (default `127.0.0.1`), `--port` (default `8000`), `--db`.

The UI API is not authenticated, so keep `--host` on loopback unless the
network in front of it is trusted. Workers never need to reach it.

### Workers never connect to the control plane

The control plane is not reachable from workers, by design: it can sit on a
laptop behind NAT, and it opens every connection. It drives each worker over
the worker's own URL (create threads, post messages, stream events), and it
also carries the workers' peer calls:

1. A thread's peer tool (`list_threads`, `read_thread`, `send_message`,
   `spawn_thread`, `set_summary`) queues the call on its worker.
2. The control plane holds a long poll on each worker's `GET /peer/calls`,
   which returns the moment a call is queued.
3. It runs every call concurrently, scoped to the worker's project
   (`Orchestrator.PEER_OPS` in `control/service.py`), and posts the answer to
   `POST /peer/calls/{id}`.

So threads on different workers, local or on Sprites, list, read, message and
spawn each other in one round trip from the control plane. The protocol is in
`runner/API.md`, "Peer calls".

### Storage

`control/store.py` defines a `Store` interface with two backends:

- **SQLite** is the default. It needs no setup and fits a single control plane process.
- **Postgres** (`--db postgresql://…`) lets several control plane instances
  share state. Appends are announced with `LISTEN/NOTIFY`, so an event written
  by one instance wakes SSE streams on all of them.

Runners never touch the database, and never connect to the control plane.

### Sprites

When `SPRITES_TOKEN` is stored, each worker is a sprite named
`orch-<project>-w<n>`, even when the browser reached the control plane on
localhost. The provider clones this repo to `/opt/orchestrate`, runs
`runner/deploy/bootstrap.sh`, and registers `runner/deploy/start.sh` as the
sprite service on port 8080. Sprites keep their default open outbound
network; the orchestrator sets no network policy.

```sh
cd orchestrator
pixi run set-key SPRITES_TOKEN
pixi run serve
```

A laptop session now starts billable sprites as soon as it spawns a thread;
that is the point of storing the token. Remove `SPRITES_TOKEN` to fall back to
local workers (the echo runner when `DEEPSEEK_API_KEY` is absent). The sprite
checks out this checkout's `origin` and current branch, so push before
expecting local commits to be there; that clone of the orchestrate code uses no
credentials, so this repository must be publicly readable.

### Project repos and commits

A project's `repo_url` is cloned once per worker; each thread works in its own
worktree on `thread/<id>`. Every turn ends with the thread's branch pushed to
origin, and a turn that ends with `finish` lands it on the default branch
(rebase, fast-forward push; conflicts go back to the thread). So destroying
sprites when the control plane stops loses nothing a thread had at the end of
its last turn; only a turn still running then is cut short.
Details: [runner/API.md](../runner/API.md#commits).

### Messaging a thread that runs nowhere

A thread outlives its worker. When a message (yours, the router's or another
thread's) is for a thread no live worker runs (the control plane restarted and
its sprites are gone, or the thread stopped or failed), the control plane
brings it back first: it picks a worker as for a new thread and posts the
thread to it with its conversation rebuilt from the event log (the request,
the messages it received, its replies, tool calls and tool results), and the
runner continues the worktree from the pushed `thread/<id>`. The thread's log
shows a `revived` line, then the message is delivered. `Orchestrator.live_worker`
is that one path; anything that needs the thread's worker calls it.
Details: [runner/API.md](../runner/API.md#resuming-a-thread).

### Worker logs

Each thread's pane has a **Worker log** tab next to Dialog and Tools: the last
300 lines its runner wrote to stdout and stderr, with a Refresh button. The same
text is served as plain text at `/api/threads/<id>/worker-logs?lines=N`.
The control plane reads them itself: from `/.sprite/logs/services/runner.log`
on a sprite (Sprites `GET /v1/sprites/<name>/fs/read`), or from
`.orchestrator/workers/<worker>/runner.log` for a local worker. Stopping the
control plane destroys its sprites, so read a sprite's log while it is still
running. To read it without the UI:

```sh
curl -sS -H "Authorization: Bearer $SPRITES_TOKEN" \
  "https://api.sprites.dev/v1/sprites/orch-<project>-w<n>/fs/read?path=/.sprite/logs/services/runner.log&workingDir=/" | tail -n 300
```

The spawn path has been exercised against the live API: create, bootstrap,
`/health`, an agent turn, and destroy.

### Archiving threads

**Archive** in a thread's pane stops the thread if it is still on its runner,
then hides it from the thread list, the router and other threads' peer calls.
**Archived** at the top of the thread list shows the archived threads; pick one
and press **Unarchive** to bring it back (it stays stopped). An archived thread
takes no messages. The state is `threads.archived_at`, added to an existing
SQLite or Postgres database the first time the control plane opens it.

## Layout

| Path | What |
| --- | --- |
| `control/app.py` | Starlette app: UI API, SSE streams, static UI |
| `control/service.py` | `Orchestrator`: workers, spawning, messaging, event relay, peer calls collected from workers, request handling |
| `control/router.py` | `ModelRouter` (DeepSeek), `HeuristicRouter` |
| `control/store.py` | `Store` interface, `SqliteStore`, `PostgresStore` |
| `control/providers.py` | `WorkerProvider`, `LocalProvider`, `SpritesProvider`, `AutoProvider` (Sprites whenever a token is stored, local otherwise) |
| `control/context.py` | the address the current request came in on |
| `control/runner_client.py` | HTTP client for `runner/API.md` |
| `control/devrunner.py` | echo runner speaking the same API |
| `control/secrets.py` | env-then-keyring lookup (shared convention with the runner) |
| `ui/app.py` | the ultra app (LPython → wasm) |
| `ui/orch.py`, `ui/imports.js` | extra `@jscall` imports: POST, SSE, named windows |

## HTTP API

| Method | Path | |
| --- | --- | --- |
| GET/POST | `/api/projects` | list, create `{name, repo_url?, model?}` |
| POST | `/api/projects/{id}/requests` | `{text, mode: auto\|thread\|new, target?}`; returns at once. `auto` → 202 `{queued}`, the plan follows as a `route` event; `thread`/`new` → the plan |
| GET/POST | `/api/projects/{id}/threads` | directory of live threads, `?archived=1` for archived ones; force a new thread `{request, title?}` |
| GET | `/api/projects/{id}/log` | every event in the project (JSON) |
| GET | `/api/projects/{id}/events` | SSE, resumable with `?after=seq` or `Last-Event-ID` |
| GET | `/api/threads/{id}` | one thread |
| GET | `/api/threads/{id}/transcript` | its events (JSON) |
| GET | `/api/threads/{id}/worker-logs` | its worker's runner log, `?lines=300` (text) |
| GET | `/api/threads/{id}/events` | SSE |
| POST | `/api/threads/{id}/messages` | `{body}` |
| POST | `/api/threads/{id}/stop` | |
| POST | `/api/threads/{id}/archive` | stops it if running, then archives it; the thread |
| POST | `/api/threads/{id}/unarchive` | the thread |
