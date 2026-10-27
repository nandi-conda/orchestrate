# Credentials and configuration

There are no environment variables you have to set. The control plane needs at
most three secrets, and works everything else out at runtime.

## Secrets

Each secret is looked up as an **environment variable first**, then in the
**system keyring** under service `orchestrate` with the variable name as the
username. The runner uses the same convention, so one keyring entry serves
both subprojects.

```sh
pixi run set-key DEEPSEEK_API_KEY     # runs: keyring set orchestrate DEEPSEEK_API_KEY
```

A password added by hand in a keyring GUI (Seahorse, KWallet, the macOS
Keychain) works too when its **label is exactly the variable name** and its
collection is unlocked.

| Name | What it is | Sent as | Without it |
| --- | --- | --- | --- |
| `DEEPSEEK_API_KEY` | A DeepSeek API key. The router uses it, and the control plane passes it to every worker, where prime-agent and the web tools use it. | `Authorization: Bearer <key>` to `https://api.deepseek.com` | Required: the control plane does not start, and says how to store it. |
| `SPRITES_TOKEN` | A Fly Sprites API token for the org your sprites go in. | `Authorization: Bearer <token>` to `https://api.sprites.dev/v1` | Sprites is off: the router is told why and puts every thread on this computer. |
| `GITHUB_TOKEN` | A GitHub **fine-grained** personal access token with **Contents: Read and write** on the repositories you attach to projects. Used by the control plane only; workers never get it. | git `http.<repo url>.extraHeader`: `Authorization: Basic base64("x-access-token:<token>")` | Projects without a remote work as usual; attaching a private remote, or pushing to any remote, fails with git's error. |
| `ORCHESTRATE_SYNC_URL` | The URL of your sync server, which `npx wrangler deploy` in `sync/` prints ([Projects on several computers](sync.md)). Set it together with `ORCHESTRATE_SYNC_TOKEN`, or neither. | The base of every request to the sync server | Projects stay on this computer. One of the two without the other stops the control plane, saying which is missing. |
| `ORCHESTRATE_SYNC_TOKEN` | The sync server's token, the one you gave the Worker with `wrangler secret put SYNC_TOKEN`. | `Authorization: Bearer <token>` to the sync server | As above. |

Missing secrets are printed at startup (`pixi run serve` or `pixi run desktop`)
with the exact name, the header it is sent as, and the keyring backend that was
searched.

### What reaches a worker

The control plane gives each worker `DEEPSEEK_API_KEY` (and `DEEPSEEK_MODEL`,
`DEEPSEEK_BASE_URL` when set), plus a fresh random `RUNNER_TOKEN` that the
control plane then uses as `Authorization: Bearer <token>` on every call to that
runner. Inside the worker, the key stays with the prime-agent binary; the
sandboxed kernel where the model's code runs gets no token, key or `RUNNER_*`
variable (see [The runner and prime-agent](../architecture/runner.md#the-sandbox)).

## What the control plane works out itself

| | How |
| --- | --- |
| Where workers run | The router decides per thread: Sprites, unless the request asks to run locally ("run it locally"), then this computer. This computer needs Linux and `../runner`; Sprites needs `SPRITES_TOKEN`; the router never picks a place the control plane cannot run workers in. Each place has its own pool of 3. Threads from before this get the place their worker was in. |
| What a sprite runs | This checkout's `origin` remote at its current branch, cloned to `/opt/orchestrate` on the sprite. A warning is printed when that is not your exact `HEAD`. |
| Which router | `ModelRouter` with `deepseek-flash`. A request it cannot plan (the model unreachable, an answer that is no plan) shows as an error in the project. |
| Which model threads use | prime-agent's `deepseek` provider with `deepseek-flash` by default; a project's model is set in its **Settings**. Image turns on a text-only model go to `deepseek/deepseek-flash`. |
| Where its data lives | `~/.orchestrate/`, one directory per user, whichever checkout or directory the control plane runs from (below). |
| Storage | SQLite in `~/.orchestrate/orchestrator.db`, or `--db postgresql://…`. |
| Project repos | Bare git repositories in `~/.orchestrate/repos/<project id>.git`. |
| Local workers | `~/.orchestrate/workers/<worker id>/`, with the runner log in `runner.log`. |

### Data from before `~/.orchestrate`

Control planes used to keep their data in the checkout: `orchestrator/orchestrator.db`
and `orchestrator/.orchestrator/{repos,workers}`. `pixi run serve` and
`pixi run desktop` move it to `~/.orchestrate/` when they start, once, and log
each move. Worktrees that pointed at the old place are made again from the hub
(a worker's threads, a hub's landings). If both places hold data, the control
plane refuses to start and names both: keep one and remove the other.

## Flags

| Flag | Default | Applies to |
| --- | --- | --- |
| `--host` | `127.0.0.1` | `pixi run serve` |
| `--port` | `8000` | `serve` and `desktop` |
| `--db` | `~/.orchestrate/orchestrator.db` | `serve` and `desktop`: a SQLite path or a `postgresql://` URL |

!!! danger "The UI API has no authentication"
    Keep `--host` on loopback unless the network in front of it is trusted.
    Workers never need to reach it, so there is no reason to expose it for
    them.

## Storage

`orchestrator/control/store.py` defines a `Store` interface with two backends:

- **SQLite** (the default) needs no setup and fits one control plane process.
- **Postgres** (`--db postgresql://…`) lets several control plane instances
  share state. Appends are announced with `LISTEN/NOTIFY`, so an event written
  by one instance wakes the SSE streams of all of them.

Columns added since a database was created are added the first time a newer
control plane opens it. Runners never touch the database.
