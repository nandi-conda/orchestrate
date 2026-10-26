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
| `DEEPSEEK_API_KEY` | A DeepSeek API key. The router uses it, and the control plane passes it to every worker, where prime-agent and the web tools use it. | `Authorization: Bearer <key>` to `https://api.deepseek.com` | Workers run the echo runner locally and the router is rule-based. Sprites cannot start (a runner needs the key). |
| `SPRITES_TOKEN` | A Fly Sprites API token for the org your sprites go in. | `Authorization: Bearer <token>` to `https://api.sprites.dev/v1` | Workers run on this machine as subprocesses. |
| `GITHUB_TOKEN` | A GitHub **fine-grained** personal access token with **Contents: Read and write** on the repositories you attach to projects. Used by the control plane only; workers never get it. | git `http.<repo url>.extraHeader`: `Authorization: Basic base64("x-access-token:<token>")` | Projects without a remote work as usual; attaching a private remote, or pushing to any remote, fails with git's error. |

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
| Where workers run | On Sprites whenever `SPRITES_TOKEN` is stored, even when the browser is on this machine (`AutoProvider`). Locally only when no token is stored. |
| What a sprite runs | This checkout's `origin` remote at its current branch, cloned to `/opt/orchestrate` on the sprite. A warning is printed when that is not your exact `HEAD`. |
| Which router | `ModelRouter` with `deepseek-flash` when `DEEPSEEK_API_KEY` is stored, `HeuristicRouter` otherwise. |
| Which model threads use | prime-agent's `deepseek` provider with `deepseek-flash` by default; a project's model is set in its **Settings**. Image turns on a text-only model go to `deepseek/deepseek-flash`. |
| Storage | SQLite in `orchestrator.db` in the working directory, or `--db postgresql://…`. |
| Project repos | Bare git repositories in `.orchestrator/repos/<project id>.git`. |
| Local workers | `.orchestrator/workers/<worker id>/`, with the runner log in `runner.log`. |

## Flags

| Flag | Default | Applies to |
| --- | --- | --- |
| `--host` | `127.0.0.1` | `pixi run serve` |
| `--port` | `8000` | `serve` and `desktop` |
| `--db` | `orchestrator.db` | `serve` and `desktop`: a SQLite path or a `postgresql://` URL |

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
