# Code layout

## orchestrator/

| Path | What |
| --- | --- |
| `control/app.py` | Starlette app: the UI API, SSE streams, the static UI |
| `control/service.py` | `Orchestrator`: workers, bindings, spawning, messaging, event relay, peer calls, requests and edits |
| `control/states.py` | the worker and binding state machines |
| `control/router.py` | `ModelRouter` (DeepSeek) |
| `control/store.py` | the `Store` interface, `SqliteStore`, `PostgresStore` |
| `control/hub.py` | the per-project hub repository: join a remote, take a thread's refs, land |
| `control/sync.py` | sharing projects through the sync server: `SyncedStore`, the Automerge replica, leases (which computer drives a project), hubs as bundles |
| `control/gitio.py` | git subprocess and bundle helpers |
| `control/providers.py` | `WorkerProvider`, `LocalProvider`, `SpritesProvider`, `Places` (where threads' workers can run, and why not) |
| `control/runner_client.py` | HTTP client for the runner API |
| `tests/devrunner.py` | the tests' echo runner, speaking the same API with no model |
| `control/paths.py` | where the data lives (`~/.orchestrate`), and the one-time move from the checkout |
| `control/secrets.py` | environment-then-keyring secret lookup |
| `control/server.py` | uvicorn server that ends SSE streams on shutdown |
| `ui/app.py` | the ultra app (LPython compiled to wasm) |
| `ui/orch.py`, `ui/imports.js` | extra `@jscall` imports: POST, SSE, history, clipboard, paste, files |
| `ui/markdown.js` | Markdown to render ops for the UI |
| `desktop/window.py` | the desktop window: a web view on the UI, links, downloads, the status page |
| `desktop/server.py` | the control plane on a thread in the window's process |
| `tests/`, `tests_desktop/` | offline test suites |

## runner/

| Path | What |
| --- | --- |
| `src/runner/server.py` | the Starlette app (the runner API) |
| `src/runner/thread.py` | `AgentThread` (prime-agent events to runner events, turns) and `ThreadHost` (workspaces, capacity) |
| `src/runner/harness.py` | `PrimeAgent` (how a thread's prime-agent starts, its kernel wrapper) and `Harness` (its RPC) |
| `src/runner/sandbox.py` | `View` (what a thread's kernel sees), bubblewrap arguments, the kernel's environment |
| `src/runner/team.py` | the runner's side of the team skill: each thread's socket for peer and web calls |
| `src/runner/peers.py` | `OrchestratorPeers` (queued for the control plane to collect) and `LocalPeers` (same process, for the CLI) |
| `src/runner/repo.py` | the worker's repositories (one per project, `Repos`), worktrees, snapshots, sessions, bundles |
| `src/runner/files.py` | the Files API, confined to the worktree |
| `src/runner/web.py` | DeepSeek Search and Fetch |
| `src/runner/cli.py` | a one-shot local run |
| `skills/team/` | the team skill: its `SKILL.md` and the `team` module |
| `models.json` | the models prime-agent knows (it runs `--offline`) |
| `deploy/bootstrap.sh`, `deploy/start.sh`, `deploy/install-harness.sh` | prepare a Linux worker, start the service, install the pinned prime-agent |

## sync/

| Path | What |
| --- | --- |
| `src/index.js` | the sync server: a Cloudflare Worker and its Durable Object (`SyncHub`) |
| `wrangler.toml` | the Worker, its Durable Object and R2 bucket |
| `deploy.mjs` | `pixi run deploy-sync`: wrangler login if needed, the bucket, the Worker |

## Root

| Path | What |
| --- | --- |
| `pixi.toml` | front-door tasks delegating to both subprojects, and the `docs` environment |
| `mkdocs.yml`, `docs/` | this site |
| `.agents/skills/land/` | the landing procedure agents follow to merge into `main` |
