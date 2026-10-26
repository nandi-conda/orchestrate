# Development

## Layout and environments

`runner/` and `orchestrator/` are separate pixi projects with their own
`pixi.toml` and lock file, and deploy separately. The root `pixi.toml` only
delegates (a delegated task runs with the subproject as its working directory,
like `cd` then `pixi run`) and owns the `docs` environment.

| Project | Environments |
| --- | --- |
| `runner/` | `default` (with tests), `prod` (what a worker installs), `kernel` (the Python threads' kernels run on, Python 3.13) |
| `orchestrator/` | `default`, `test`, `desktop`, `desktop-test` |
| root | `docs` |

## Tests

```sh
pixi run test                                  # everything, from the root
cd runner && pixi run test                     # runner: prime-agent's scripted model
cd orchestrator && pixi run test               # store, router, the whole loop on the echo runner, markdown, Postgres
cd orchestrator && pixi run test-desktop       # the desktop window, offscreen
```

Every suite is offline: the runner's tests drive prime-agent with its scripted
model (`PRIME_AGENT_FAUX_SCRIPT`), and the orchestrator's run the echo runner.
Nothing calls DeepSeek or Sprites; live runs are done by hand. Tests run side
by side (`-n auto`), each with its own directory, ports and workers. The
Postgres test starts a throwaway server with `initdb`, which refuses to run as
root, so it is skipped there.

The orchestrator's tests depend on `build-ui`, which compiles the ultra UI into
`orchestrator/ui/dist/`. That directory is build output; never commit it.

## Conventions

- **One designed path.** No fallbacks, degraded modes or "if X fails, try Y"
  branches. When something cannot work, fail with the reason: which secret by
  its exact name, which header, which worker's log.
- **Invariants** in the [architecture overview](architecture/overview.md#invariants)
  are not negotiable: no worker-to-control-plane connection, no API handler
  awaiting a model, provider or runner, every state change through the state
  machines.
- **One window.** New UI goes in panes, tabs or ultra dialogs; never new
  windows or `target=_blank`. Keep pane structure fixed (ultra diffs unkeyed
  children by position).
- **Commit subjects** read `<area>: <lowercase imperative summary>`, where the
  area is `runner`, `orchestrator`, `docs` or `agents`.
- **Landing.** Changes go straight to `main`, no pull requests, after the test
  suites pass. `.agents/skills/land/SKILL.md` is the procedure.

## Writing docs

The site is [MkDocs](https://www.mkdocs.org) with the
[Material](https://squidfunk.github.io/mkdocs-material/) theme, configured in
`mkdocs.yml`, with pages in `docs/`.

```sh
pixi run docs          # live-reloading preview on http://127.0.0.1:8001
pixi run docs-build    # strict build into site/ (ignored by git)
```

- The build is strict: a broken link or a page missing from `nav` fails it.
  Add every new page to `nav` in `mkdocs.yml`.
- Diagrams are Mermaid, in ` ```mermaid ` fences.
- `runner/API.md` is the runner contract and stays next to the code; the
  [Runner HTTP API](reference/runner-api.md) page includes it with
  `--8<-- "runner/API.md"` instead of copying it. Do the same for any other
  file that must live beside its code.
- Link to code by path in backticks (`control/service.py`), or to GitHub for a
  file you want clickable; links outside `docs/` other than snippets are not
  resolved by MkDocs.
