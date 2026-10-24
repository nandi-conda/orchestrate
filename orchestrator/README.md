# orchestrator

The control plane and the ultra UI. Every project gets one main window. A
request typed there goes to a router, which spawns new threads or messages
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
git repo. Its main window opens at `/p/<project id>`; opening it again
focuses the same window. Type a request and press Enter:

- **Router decides** (default): the router spawns threads or messages
  existing ones, leaning toward spawning. A bulleted or numbered list fans out
  into one thread per item.
- **This thread**: sends the message straight to the selected thread.
- **New thread**: always starts a fresh thread.

The router's decision shows above the request box. **Project activity** in the
thread list shows every event in the project, and each thread shows its own
transcript, including messages from other threads.

With `DEEPSEEK_API_KEY` set (env or keyring, service `orchestrate`), the
orchestrator starts the real runner from `../runner` as a local subprocess,
one worker per project, and the router uses `deepseek-flash`. Without the key it
falls back to an echo runner (`control.devrunner`) and a rule-based router. That
way you can click through the whole UI with no credentials. Set
`ORCH_DEVRUNNER=1` to force the echo runner even when the key is present.

The echo runner understands two lines, so you can test thread-to-thread
traffic without a model: `spawn: <task>` starts a child thread, which reports
`Finished: …` back to its parent, and `tell t_xxx: <text>` messages another
thread.

## Tests

```sh
pixi run test             # store, router, the whole loop against the echo runner, Postgres if installed
```

The Postgres test starts a throwaway server with `initdb`, which refuses to run
as root, so it is skipped there.

## Configuration

| Env var | Default | Meaning |
| --- | --- | --- |
| `DEEPSEEK_API_KEY` | env, then keyring | router model, and passed to local runners. Sent as `Authorization: Bearer <key>` to `https://api.deepseek.com` |
| `ROUTER_MODEL` | `deepseek-flash` | model the router uses |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com` | any OpenAI-compatible endpoint |
| `DATABASE_URL` | `sqlite:///orchestrator.db` | `postgresql://…` selects the Postgres store |
| `WORKER_PROVIDER` | `local` | `local` or `sprites` |
| `RUNNER_DIR` | `../runner` | runner checkout for the local provider |
| `RUNNER_CAPACITY` | `16` | running threads per worker before another worker starts |
| `ORCHESTRATOR_PUBLIC_URL` | `http://127.0.0.1:<port>` | where runners reach the peer API |
| `SPRITES_TOKEN` | env, then keyring | needed with `WORKER_PROVIDER=sprites`. Sent as `Authorization: Bearer <token>` to `https://api.sprites.dev/v1` |
| `RUNNER_SOURCE` / `RUNNER_REF` | this repo / `main` | what a sprite checks out |
| `SPRITES_PREFIX` | `orch-` | sprite name prefix |

Missing secrets are printed to the console at startup with the exact name, the header
they're sent as, and the keyring backend that was searched. Store any of them with
`pixi run set-key NAME`. A password added by hand in a keyring GUI (for example
`DEEPSEEK_API_KEY` in the Login keyring) also works: an item labelled exactly like
the variable is accepted, as long as its collection is unlocked.

### Storage

`control/store.py` defines a `Store` interface with two backends:

- **SQLite** is the default. It needs no setup and fits a single control plane process.
- **Postgres** (`DATABASE_URL=postgresql://…`) lets several control plane
  instances share state. Appends are announced with `LISTEN/NOTIFY`, so an
  event written by one instance wakes SSE streams on all of them.

Runners never touch the database; they talk to the control plane over HTTP.

### Sprites

With `WORKER_PROVIDER=sprites`, each worker is a sprite named
`orch-<project>-w<n>`. The provider clones this repo to `/opt/orchestrate`, runs
`runner/deploy/bootstrap.sh`, and registers `runner/deploy/start.sh` as the
sprite service on port 8080. It also sets a network policy that allows only
DeepSeek, GitHub, the package mirrors and the orchestrator. Runners call the
peer API back on `ORCHESTRATOR_PUBLIC_URL`, so that URL has to be reachable from
the sprite: deploy the control plane somewhere public, or tunnel it. This
provider hasn't been run against the live API yet.

## Layout

| Path | What |
| --- | --- |
| `control/app.py` | Starlette app: UI API, SSE streams, runner peer API, static UI |
| `control/service.py` | `Orchestrator`: workers, spawning, messaging, event relay, request handling |
| `control/router.py` | `ModelRouter` (DeepSeek), `HeuristicRouter` |
| `control/store.py` | `Store` interface, `SqliteStore`, `PostgresStore` |
| `control/providers.py` | `WorkerProvider`, `LocalProvider`, `SpritesProvider` |
| `control/runner_client.py` | HTTP client for `runner/API.md` |
| `control/devrunner.py` | echo runner speaking the same API |
| `control/secrets.py` | env-then-keyring lookup (shared convention with the runner) |
| `ui/app.py` | the ultra app (LPython → wasm) |
| `ui/orch.py`, `ui/imports.js` | extra `@jscall` imports: POST, SSE, named windows |

## HTTP API

| Method | Path | |
| --- | --- | --- |
| GET/POST | `/api/projects` | list, create `{name, repo_url?, model?}` |
| POST | `/api/projects/{id}/requests` | `{text, mode: auto\|thread\|new, target?}` → router plan |
| GET/POST | `/api/projects/{id}/threads` | directory; force a new thread `{request, title?}` |
| GET | `/api/projects/{id}/log` | every event in the project (JSON) |
| GET | `/api/projects/{id}/events` | SSE, resumable with `?after=seq` or `Last-Event-ID` |
| GET | `/api/threads/{id}` | one thread |
| GET | `/api/threads/{id}/transcript` | its events (JSON) |
| GET | `/api/threads/{id}/events` | SSE |
| POST | `/api/threads/{id}/messages` | `{body}` |
| POST | `/api/threads/{id}/stop` | |
| | `/api/peer/...` | the runner peer API from `runner/API.md`, authenticated with the worker's token |
