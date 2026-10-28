# Getting started

## What you need

- [pixi](https://pixi.sh). Every dependency, Python included, comes from the
  pixi environments; nothing needs installing system-wide.
- A machine the orchestrator supports: linux-64, osx-64, osx-arm64 or win-64.
  ultra's compiler (lpython 0.22) has no linux-aarch64 build.
- For the desktop window: linux-64, osx-arm64 or win-64 (conda-forge has no
  Qt WebEngine for osx-64).
- For the real runner on **this** machine: Linux with unprivileged user
  namespaces, since threads are sandboxed with bubblewrap. Workers on Fly
  Sprites are Linux whatever your machine is, so a Mac or Windows control plane
  with `SPRITES_TOKEN` stored runs real agents fine, for threads the router puts
  on Sprites (all of them, unless a request says to run locally).

## Install

### As a package

Every push to main publishes the `orchestrate` conda package to
[prefix.dev/nandi-testing](https://prefix.dev/channels/nandi-testing):

```sh
pixi global install -c conda-forge -c https://prefix.dev/nandi-testing orchestrate
orchestrate           # the desktop window
orchestrate-serve     # the control plane alone, for a browser on http://127.0.0.1:8000
```

The two commands are `pixi run desktop` and `pixi run serve` below, with the
same options. The package carries the runner too, so a local worker runs from
the install with `pixi run`, which keeps the runner's environments inside it.
The package is noarch but needs Qt WebEngine, so it installs where the desktop
window runs: linux-64, osx-arm64 and win-64. Store keys with
`keyring set orchestrate NAME` (keyring comes with it). Keep conda-forge first:
nandi-testing mirrors only part of conda-forge (no python 3.12, an older Qt),
so with it first the strict channel priority finds no solution.

The install also adds **Orchestrator** to the system's application menu (Start
menu on Windows, Applications on macOS), with the app icon; it opens the
desktop window. `recipe/menu/` defines the shortcut (menuinst), and
`pixi global install --no-shortcuts` leaves it out.

`pixi run package` in a checkout builds the same package into `output/`
(`recipe/recipe.yaml`; `.github/workflows/package.yml` publishes it).

### From a checkout

```sh
git clone https://github.com/nandi-conda/orchestrate && cd orchestrate
pixi run setup
```

`setup` installs the runner's environments and every orchestrator environment
(default, `test`, `desktop`, `desktop-test`).

## Store your keys

Secrets live in the system keyring under the service `orchestrate`, with the
variable name as the username. An environment variable of the same name is
read first, so `export DEEPSEEK_API_KEY=...` works too.

```sh
pixi run set-key DEEPSEEK_API_KEY     # DeepSeek API key: router + agents
pixi run set-key SPRITES_TOKEN        # Fly Sprites API token: where workers run
pixi run set-key GITHUB_TOKEN         # optional: only for projects with a GitHub remote
```

What each one is, which header it is sent in, and what happens without it is
in [Credentials and configuration](configuration.md).

## Run it

Pick one front end. Both run the same control plane and UI.

```sh
pixi run desktop      # a desktop window; the control plane runs in the same process
pixi run serve        # the control plane alone; open http://127.0.0.1:8000 in a browser
```

Both build the UI first (`build-ui` compiles `orchestrator/ui/app.py` to
WebAssembly with ultra), then start the control plane on port 8000. They take
`--port N` and `--db <sqlite path or postgresql:// URL>`; `serve` also takes
`--host`. With `pixi run desktop`, the UI is also reachable in a browser at
`http://127.0.0.1:8000` while the window is open.

At startup the control plane prints any missing secret by its exact name, the
header it would be sent as, and the keyring backend it searched.

Then create a project, type a request, and press Enter. The
[UI guide](ui.md) walks through the rest.

### What runs where, by which keys you stored

| Stored | Workers | Router |
| --- | --- | --- |
| nothing | the control plane does not start: `DEEPSEEK_API_KEY` is required | |
| `DEEPSEEK_API_KEY` | every thread on this computer: the real runner as a local subprocess (Linux), a pool of 3 shared by every project | `deepseek-flash` (`ModelRouter`) |
| `DEEPSEEK_API_KEY` + `SPRITES_TOKEN` | threads on Fly Sprites, a pool of 3: `orch-<pool>-w1`..`orch-<pool>-w3`; a request that says "run locally" puts its threads on this computer instead | `deepseek-flash` |

!!! warning "Sprites cost money"
    With `SPRITES_TOKEN` stored, the first thread creates a billable sprite,
    even when the control plane runs on your laptop, unless its request says
    to run locally. There are never more than 3 (the pool, shared by every
    project); they pause while idle and are **not** destroyed when the control
    plane stops (the next one attaches to them again). At startup the control
    plane destroys every other `orch-` sprite of ours, such as each project's
    own workers from before the pool. See
    [The worker pool](../worker-lifecycle.md#the-worker-pool).

!!! note "Sprites run pushed code"
    A sprite clones this checkout's `origin` remote at its current branch, not
    your working tree. Push before expecting local commits on workers; the
    control plane warns at startup when the sprite would not get your exact
    commit. The clone uses no credentials, so the repository must be publicly
    readable.

## The echo runner (tests only)

The orchestrator's tests run threads on `tests/devrunner.py`, which speaks the
runner API with no model; the control plane never uses it. It understands a
few lines, so thread-to-thread traffic and the git path can be exercised
offline:

| Line | Effect |
| --- | --- |
| `spawn: <task>` | starts a child thread, which reports `Finished: …` back to its parent |
| `tell t_xxx: <text>` | messages another thread |
| `write <path>: <text>` | writes a file into the thread's worktree |

## A standalone worker

```sh
DEEPSEEK_API_KEY=... RUNNER_TOKEN=... pixi run worker    # runner service on :8080
```

The control plane starts its own workers, so this is only for running a runner
by hand. Its environment variables are in the
[runner API reference](../reference/runner-api.md#environment). A one-shot
local run with no orchestrator:

```sh
cd runner
DEEPSEEK_API_KEY=... pixi run run "add a README" --repo /path/to/repo
```

## Tests

```sh
pixi run test            # runner, orchestrator and desktop window suites, all offline
```

Per subproject: `cd runner && pixi run test`, `cd orchestrator && pixi run test`
and `pixi run test-desktop`. The tests use prime-agent's scripted model and the
echo runner; none of them calls DeepSeek or Sprites.
