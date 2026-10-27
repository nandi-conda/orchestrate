# orchestrator

The control plane and the ultra UI. Everything happens in one browser window:
the project list, each project, and each thread's transcript, tool calls,
files and worker log. A request typed in a project goes to a router, which spawns new threads or messages
existing ones. Threads run on runner workers (see `../runner`), and the control
plane relays their events back to the window as they happen.

## Run it locally

You need [pixi](https://pixi.sh). The orchestrator works on linux-64, osx-64,
osx-arm64 and win-64; ultra's compiler (lpython 0.22) has no linux-aarch64 build.
The runner (`../runner`) is Linux-only, since it sandboxes threads with
bubblewrap, so local workers with the real runner need a Linux machine; Sprites
are Linux.

```sh
git clone https://github.com/nandi-conda/orchestrate && cd orchestrate

# once: store the DeepSeek key in the system keyring (or export DEEPSEEK_API_KEY)
cd runner && pixi install && pixi run set-key DEEPSEEK_API_KEY && cd ..

cd orchestrator
pixi run serve            # builds the UI, then serves http://127.0.0.1:8000
```

Open http://127.0.0.1:8000 and create a project (a name is all it needs; a
GitHub remote can be attached later in its Settings). It opens in the same window at `/p/<project id>`, and
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
in order, once it starts. **Project orchestrator** in the
thread list shows the project's events, newest last (the newest 2,000: a project
worked on for a while has more than a pane holds), and each thread shows its own
transcript, including messages from other threads. A request that spawns
threads never moves the window into one of them: you stay where you are, the
new threads show up in the list, and their events stream into Project orchestrator.
You're only ever talking to the project, and you open a thread only by
clicking it.

**Editing a message.** Your own messages (the blue bubbles: requests and
messages you sent straight to a thread) have an edit button. Save, and the
bubble shows the new text marked edited, while the router re-assesses it in
the background, after any routing still queued for the original:

- every thread the message reached (the threads its routing spawned or told,
  or the thread you messaged) gets a correction from you with both versions,
  and decides itself what to keep, drop or undo. Nothing is stopped: a thread
  mid-turn reads the correction before its next model call, and a finished one
  comes back and acts on it;
- the router plans the new text knowing those threads, so it starts threads
  only for parts none of them covers, and can tell a thread what changed for it. Its decision
  shows as an "edit:" line in Project orchestrator.

**Pasted images.** Paste a screenshot or a copied picture into the request box
and it becomes an attachment above it: up to 4, each up to 8 MB, and a huge
screenshot is scaled down in the browser before it is sent. Send it with words
or on its own — an image-only request skips the router and starts one new
thread, titled for the image. Images show in the transcript and open enlarged
on a click.

With `DEEPSEEK_API_KEY` stored (keyring, service `orchestrate`), the
orchestrator starts the real runner from `../runner` as local subprocesses,
a pool of up to 3 workers shared by every project, and the router uses `deepseek-flash`. The key is
required: without it `pixi run serve` and `pixi run desktop` stop at startup
and say how to store it (`export DEEPSEEK_API_KEY=…` or
`pixi run set-key DEEPSEEK_API_KEY`). There is no model-less mode; the echo
runner and rule-based router in `tests/` are for the tests only.

## The desktop window

```sh
pixi run desktop          # from the repo root or here
```

The same UI in a desktop window instead of a browser tab. Qt (PySide6) draws
the window and the ultra UI fills it. The control plane runs in the same
process, on a thread next to the window, so it ends when the window does,
however the window ends; closing the window (or Ctrl+C in the terminal) stops
it gracefully. Workers are let go, not destroyed: the next control plane
attaches to them again. While it runs, the UI is at
http://127.0.0.1:8000 in a browser too. `--port N` and `--db …` are as for
`python -m control`.

- The UI's own links, `window.open` and `target=_blank` stay in the window.
- Links anywhere else (a URL in an agent's reply) open in your default browser.
- Markdown files in the Files tab open rendered in the pane; other files
  download to your downloads folder, never over an existing file
  (`name (1)`, …), with a line at the bottom saying where.
- If the port is taken, the window says so, and how to free it
  (`fuser -k 8000/tcp`, or `kill $(lsof -t -i :8000)`), with a Retry.
  What the control plane is doing is also printed to the terminal as
  `desktop: …` lines.

The window is drawn on the CPU, with no OpenGL, EGL or GLX in the process:
Qt Quick's software renderer, Chromium with `--disable-gpu`, and on Linux
`QT_WAYLAND_CLIENT_BUFFER_INTEGRATION=none` (native Wayland, drawn into
shared-memory buffers) and `QT_XCB_GL_INTEGRATION=none` (X11 without GLX).
conda-forge's Mesa has no Wayland EGL platform and the system's GL drivers need
not match the environment's Qt, so any GL use aborted the window or kept it
from appearing. Expect two harmless lines at startup:
`Failed to load client buffer integration: "none"` and
`Failed to create RHI for backend: OpenGL`. Variables already set still decide.

It runs on linux-64, osx-arm64 and win-64 (the `desktop` environment):
conda-forge has no Qt WebEngine for osx-64.

## Tests

```sh
pixi run test             # store, router, the whole loop against the echo runner, the markdown renderer, Postgres if installed
pixi run test-desktop     # the desktop window, offscreen, against the real UI on the echo runner
```

Each test has its own directory, ports and workers, so `test` runs them side by
side (`-n auto`, one per core).

The Postgres test starts a throwaway server with `initdb`, which refuses to run
as root, so it is skipped there.

## Configuration

There are no environment variables to set. Three secrets come from the system
keyring (service `orchestrate`, username = the name), and everything else is
worked out at runtime.

| Secret | What | Sent as |
| --- | --- | --- |
| `DEEPSEEK_API_KEY` | **required**: router model, and passed to every worker. Without it the control plane does not start | `Authorization: Bearer <key>` to `https://api.deepseek.com` |
| `SPRITES_TOKEN` | lets threads run on Sprites; without it the router is told Sprites is off and why, and puts every thread on this computer | `Authorization: Bearer <token>` to `https://api.sprites.dev/v1` |
| `GITHUB_TOKEN` | GitHub fine-grained token, Contents: Read and write on the projects' repos. Used by the control plane only: for a project with a remote (`repo_url`, set in its settings) it fetches the remote's default branch into the project's hub (`~/.orchestrate/repos/<id>.git`) and pushes finished threads' work to it. Workers never get it | git `http.<repo url>.extraHeader`: `Authorization: Basic base64("x-access-token:<token>")` |

Store one with `pixi run set-key NAME`. Missing ones are printed at startup with
the exact name, the header they're sent as, and the keyring backend that was
searched. A password added by hand in a keyring GUI (for example
`DEEPSEEK_API_KEY` in the Login keyring) also works: an item labelled exactly
like the variable is accepted, as long as its collection is unlocked. An
environment variable of the same name is read first.

What the control plane works out for itself:

| | How |
| --- | --- |
| Where workers run | The router decides per thread: Sprites, unless the request says to run locally ("run it locally"), then this computer. Each place has its own pool of 3. It never picks a place the control plane cannot run workers in |
| What a sprite checks out | This checkout's `origin` remote and current branch, with a warning when the sprite would not get this exact commit |
| Which router | `deepseek-flash`; a request it cannot plan shows as an error in the project |
| Where its data lives | `~/.orchestrate/`: the SQLite store, project hubs (`repos/`) and local workers (`workers/`). Data an older control plane kept in the checkout (`orchestrator.db`, `.orchestrator/`) is moved there once at startup; data in both places stops startup until one is removed |
| Storage | SQLite in `~/.orchestrate/orchestrator.db`, or `--db postgresql://…` for several control planes |

Flags: `--host` (default `127.0.0.1`), `--port` (default `8000`), `--db`.

The UI API is not authenticated, so keep `--host` on loopback unless the
network in front of it is trusted. Workers never need to reach it.

### Workers never connect to the control plane

The control plane is not reachable from workers, by design: it can sit on a
laptop behind NAT, and it opens every connection. It drives each worker over
the worker's own URL (create threads, post messages, stream events), and it
also carries the workers' peer calls:

1. A thread's team-skill call (`team.list_threads`, `read_thread`,
   `send_message`, `spawn_thread`, `set_summary`, from its kernel) queues the
   call on its worker.
2. The control plane holds a long poll on each worker's `GET /peer/calls`,
   which returns the moment a call is queued.
3. It runs every call concurrently, as the thread that made it and scoped to
   that thread's project, since a worker holds threads of every project
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

Threads on Sprites (every thread whose request does not say to run locally)
share a pool of 3 sprites, named for their slot (`orch-w1`..`orch-w3`) and labeled
`orchestrator`, even when the browser reached the control plane on localhost.
A thread that finds every worker full waits for room; no fourth is made. At
startup the control plane destroys every other sprite of ours (labeled, or
named `orch-p-<id>-w<n>` as each project's own workers were before the pool).
See `docs/worker-lifecycle.md`, "The worker pool". The provider clones this repo to `/opt/orchestrate`, runs
`runner/deploy/bootstrap.sh`, and registers `runner/deploy/start.sh` as the
sprite service on port 8080. Sprites keep their default open outbound
network; the orchestrator sets no network policy.

```sh
cd orchestrator
pixi run set-key SPRITES_TOKEN
pixi run serve
```

A laptop session starts billable sprites as soon as it spawns a thread, unless
the request says to run locally. The sprite
checks out this checkout's `origin` and current branch, so push before
expecting local commits to be there; that clone of the orchestrate code uses no
credentials, so this repository must be publicly readable.

### Project repos and commits

Every project has its own git repository on the control plane's machine, a
bare repository at `~/.orchestrate/repos/<project id>.git` (the hub; its path is
under **Settings** in the project window). It starts from a commit that makes
the project a pixi project, "Start the project": every project is one, and a
main branch that is not (an older hub, an attached remote's history) gets a
commit that makes it one. Each thread works in its own worktree on `thread/<id>` on
its worker; every turn ends with the thread's branch and a snapshot of its
whole worktree (ignored files too) taken into the hub and the branch landed
on the hub's main branch (a turn a model call failed in is only taken) (rebase, fast-forward;
conflicts go back to the thread). Workers never reach the hub: the control
plane moves commits to and from them as git bundles. So losing a worker
loses nothing a thread had at the end of its last turn; only a turn still
running on it is cut short.
Details: [runner/API.md](../runner/API.md#repository), `control/hub.py`.

A project can get a remote (a GitHub repository) at any time from its
**Settings**; a new project has none. With a remote, the hub's main branch
follows the remote's default branch (and takes its name), and every landing is
pushed there first. Thread branches and snapshots stay on this machine.
Attaching or changing the remote joins the two histories:

| The remote | What happens |
| --- | --- |
| has no commits | it gets the project's main branch |
| is ahead of the project, or nothing has landed in the project yet | the project's main branch takes the remote's commits; threads begun before land on top of them |
| is behind the project | it gets the commits it is missing |
| has a different history | refused; the project keeps the remote it had |

Removing the remote keeps everything in the hub; landings stay local from then
on, and attaching it again pushes what it missed.

### Worker and thread states

The full lifecycle, with diagrams, what you see in the UI and what Respawn
does in each state: [docs/worker-lifecycle.md](../docs/worker-lifecycle.md).

Workers, and each thread's hold on a worker (its binding), are two state
machines, defined in one place: [control/states.py](control/states.py). Every
change goes through `Orchestrator._worker_to` or `Orchestrator._bind_to`,
which check it against the machine (a move it does not list raises
`IllegalTransition`, a control plane bug), keep the reason beside the state in
the store, and log it: `worker_state` events in the project log, `binding`
events in the thread's. Every state has a path back to the happy one; only a
lost worker is retired, and its threads take the path back on another worker.

| Worker | Means | Way back to ready |
| --- | --- | --- |
| `ready` | its runner answers `/health` | |
| `unreachable` | `/health` fails, its provider still has it (a sprite waking, this machine's network down) | it answers again; reattached after 300 s without an answer, or at once by Respawn |
| `reattaching` | the provider is starting its runner again, same disk and token | ready when it answers; unreachable if it fails |
| `lost` | its provider says it is gone; destroyed | retired: its threads are unbound and come back on the pool; its slot is filled by a new worker of the same name |

| Binding | Means | Way back to bound |
| --- | --- | --- |
| `bound` | a live worker's runner has the thread | |
| `binding` | being put on a worker: picked or made, the hub shipped, the thread posted | bound when that is done; unbound when it fails or Respawn starts it over |
| `unbound` | on no worker: its worker lost or its runner restarted, its start failed, or an earlier control plane had it | its next message, or Respawn |

Unbinding a thread that was mid-turn (queued, working, waiting) fails it.
A runner that restarted (a new boot id in `/health`) unbinds its threads; they
keep the worker as theirs and come back on it, in their worktree.

Workers are watched every 10 s, before every delivery and when one is picked
for a thread. A delivery to a thread whose worker is unreachable waits for it
to be ready again (or reattached), and fails with the worker's state and why
if a reattach fails.

**Respawn** (the thread's button, `POST /api/threads/<id>/respawn`) is a way
back from every state: a thread being put on a worker has that called off and
started over; one on an unreachable worker has the worker reattached; one
bound is stopped on its runner and posted again; one unbound is bound. A
thread whose worker is not ready, or that is on none, shows a bar under its
title with the state, why, and that Respawn brings it back.

Binding a thread puts it on the worker it was last on while that one is not
lost, so its worktree there (uncommitted and ignored files too) is the one it
continues in; else on a picked worker after sending its branch, snapshot and
session from the hub: the runner continues the worktree from `thread/<id>` and
the snapshot, and prime-agent resumes the session (the conversation as the
model last had it, compactions and images included) from `sessions/<id>`. A
thread none of whose turns was saved is given its request again; the rest
wait idle for their next message. `Orchestrator.live_worker` is the one path
anything that needs the thread's worker takes.
Details: [runner/API.md](../runner/API.md#resuming-a-thread).

### Worker logs

Each thread's pane has a **Worker log** tab next to Dialog and Tools: the last
300 lines its runner wrote to stdout and stderr, with a Refresh button. The same
text is served as plain text at `/api/threads/<id>/worker-logs?lines=N`.
The control plane reads them itself: from `/.sprite/logs/services/runner.log`
on a sprite (Sprites `GET /v1/sprites/<name>/fs/read`), or from
`~/.orchestrate/workers/<worker>/runner.log` for a local worker.

Every worker's runner log is also printed to the control plane's stdout as it
is written (`pixi run desktop` or `pixi run serve`), each line prefixed with
its worker, e.g. `orch-p_ab12-w0 | INFO: runner.harness: ...`. Attaching a
worker prints its last 50 lines first. The control plane follows it itself:
the Sprites log stream (`GET /v1/sprites/<name>/services/runner/logs?duration=…`,
NDJSON) for a sprite, the log file for a local worker. Runner failures land
there in full: tracebacks, prime-agent's stderr, failed model calls (the whole
model message but its content) and failed tool calls.

The Worker log tab starts with where the thread stands: its binding and why,
and its worker's state and why. When a worker is lost, the control plane keeps
why and the last 300 lines it followed, since destroying the worker takes its
log; a thread whose worker was lost shows that log's end.

To read a live sprite's log without the UI:

```sh
curl -sS -H "Authorization: Bearer $SPRITES_TOKEN" \
  "https://api.sprites.dev/v1/sprites/orch-w<n>/fs/read?path=/.sprite/logs/services/runner.log&workingDir=/" | tail -n 300
```

The spawn path has been exercised against the live API: create, bootstrap,
`/health`, an agent turn, and destroy.

### Files

Each thread's pane has a **Files** tab: the thread's worktree, one directory at
a time. Click a directory to open it (Up goes back); click a markdown file
(`.md` or `.markdown`) to read it rendered in the pane (Back returns to the
directory, and Download stays available there). Any other file downloads, in
the same window. The control plane reads them from the worker that holds
the thread (runner `GET /threads/<id>/files` and `/files/raw`; the worker never
connects in) and serves them at `/api/threads/<id>/files?path=<dir>` (JSON) and
`/api/threads/<id>/files/raw?path=<file>` (a download). `.git` is not shown.
A thread on no worker is bound to one first (see Worker and thread states).

### Pasted images

An image the browser sends is a `data:` URL (`png`, `jpeg`, `webp` or `gif`; at
most 4 per message, 8 MB each). The control plane stores its bytes in an
`images` table (SQLite or Postgres, created the first time it opens an existing
database) and logs only a reference (`{id, media_type, name, size}`), so
transcripts and SSE streams stay small. `/api/images/<id>` serves the bytes,
cacheable for good. A worker cannot reach the control plane, so the images are
inlined as base64 when a message is delivered; the runner turns them into
image parts of that user message, and from then on they are in the thread's
session ([runner/API.md](../runner/API.md)).

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
| `control/router.py` | `ModelRouter` (DeepSeek) |
| `control/store.py` | `Store` interface, `SqliteStore`, `PostgresStore` |
| `control/providers.py` | `WorkerProvider`, `LocalProvider`, `SpritesProvider`, `Places` (where threads' workers can run, and why not) |
| `control/runner_client.py` | HTTP client for `runner/API.md` |
| `tests/devrunner.py` | echo runner speaking the same API, for the tests only |
| `control/secrets.py` | env-then-keyring lookup (shared convention with the runner) |
| `ui/app.py` | the ultra app (LPython → wasm) |
| `ui/orch.py`, `ui/imports.js` | extra `@jscall` imports: POST, SSE, named windows |
| `control/server.py` | uvicorn server that ends the SSE streams on shutdown (`python -m control` and the desktop window) |
| `desktop/window.py` | the desktop window: a web view on the UI, links, downloads, the status page |
| `desktop/server.py` | the control plane on a thread in the window's process |

## HTTP API

| Method | Path | |
| --- | --- | --- |
| GET/POST | `/api/projects` | list, create `{name, model?}`; a project has `repo_path` (its hub) and `repo_url` (its remote, `""` for none) |
| POST | `/api/projects/{id}/settings` | change any of `{name, model, repo_url}`; a remote that cannot be joined is a 400 with git's reason, and nothing changes |
| POST | `/api/projects/{id}/requests` | `{text?, images?, mode: auto\|thread\|new, target?}`; returns at once. `auto` → 202 `{queued}`, the plan follows as a `route` event; `thread`/`new` → the plan. A request that is only `images` skips the router and starts one new thread |
| GET/POST | `/api/projects/{id}/threads` | directory of live threads, `?archived=1` for archived ones; force a new thread `{request?, images?, title?}` |
| GET | `/api/projects/{id}/log` | the newest events in the project (JSON), oldest first |
| GET | `/api/projects/{id}/events` | SSE, resumable with `?after=seq` or `Last-Event-ID` |
| GET | `/api/threads/{id}` | one thread |
| GET | `/api/threads/{id}/transcript` | its newest events (JSON) |
| GET | `/api/threads/{id}/worker-logs` | its worker's runner log, `?lines=300` (text) |
| GET | `/api/threads/{id}/events` | SSE |
| POST | `/api/threads/{id}/messages` | `{body, images?}` |
| POST | `/api/threads/{id}/stop` | |
| POST | `/api/threads/{id}/archive` | stops it if running, then archives it; the thread |
| POST | `/api/threads/{id}/unarchive` | the thread |
| GET | `/api/images/{id}` | a pasted image's bytes (`image/png`, `jpeg`, `webp` or `gif`), cacheable for good |

A stream sends one frame per event, except for a reply still streaming: its
`delta` events fold into the row they end up as — one per part (the reasoning,
the reply, each tool call), under the `seq` the part started at, with
`live: 1` and its `text` grown so far, and `began: 1` on the frame that started
the part — so the window grows the row it shows instead of fetching the whole
pane again. The event that ends the model call replaces them with the call's
own events.
