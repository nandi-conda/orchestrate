---
name: land
description: >-
  Land the current thread's changes into `main`: commit outstanding work, rebase
  onto the latest `local/main` and `origin/main` so conflicts surface first, run
  the project's pixi test suites, then push to `origin` (GitHub) and to the
  `local` remote (the user's checkout at ~/Projects/orchestrate). Invoke only
  when the user has explicitly requested landing, such as via Land Changes or a
  request to merge the change. Not for review, preparation, or while checks are
  still running; passing checks alone is not a landing request.
disable-model-invocation: true
metadata:
  delta-action: land
---

# Land thread changes into main

This project's landing destination is `main`, on both remotes: `origin`
(GitHub, `https://github.com/nandi-conda/orchestrate`) and `local` (the user's
working checkout of this repository, at ~/Projects/orchestrate). Push to GitHub
first, then to the checkout. Do not open pull requests.

The request that brought you here is the landing request. Do not ask whether to
merge, and do not stop to reconfirm permission; proceed until the change is on
both `origin/main` and `local/main`, or until a genuine blocker (below) means it
is not.

There are no git hooks in this repository and commits are not signed. The
only workflow, `.github/workflows/docs.yml`, publishes the MkDocs site; it
runs no tests. The pixi test suites are the whole verification gate.

## Workflow

### 1. Preflight

```sh
git rev-parse --show-toplevel
git remote get-url origin
git remote get-url local
git status --porcelain
```

Stop and report if either remote is not configured. Use `HEAD` rather than a
branch name below: the thread worktree may be on `main` or on a detached head,
and the push target is explicit.

If nothing is staged and nothing is unstaged, skip to step 3: the rebase tells
you whether there is anything to land.

### 2. Commit outstanding work

Commit the work in progress until the tree has no tracked changes left. Stage
only what the change is about:

```sh
git add -u                      # modified and deleted tracked files
git add <new source files>      # new files, named explicitly
GIT_EDITOR=true git commit -m "<area>: <lowercase imperative summary>"
```

Subjects in this repository read like `runner: never emit an empty turn
summary; fall back to the last reply` or `orchestrator: create ui/dist before
building the wasm UI` (`git log --oneline -10`): the area (`runner`,
`orchestrator`, or both when the change spans them; `agents` for the skill files
under `.agents/`), a colon, then a lowercase summary. Add a body only when the
reason is not obvious from the subject.

Never commit generated build output such as `orchestrator/ui/dist/` (the
`build-ui` task writes it) or `.pixi/`. If a commit fails, stop and report the
error.

### 3. Rebase onto the latest main

```sh
git fetch origin
git fetch local
git rebase local/main
git rebase origin/main
```

The point is to find out *now* whether the work conflicts with main. The second
rebase is a no-op unless `origin/main` has commits the worktree does not have —
someone pushed to GitHub, so its history comes under this change. If either
rebase conflicts:

```sh
git diff --name-only --diff-filter=U
git rebase --abort
```

Report which files conflicted, say the change has not landed, and stop. Do not
resolve conflicts, do not force anything, and do not push a partially rebased
history. If a rebase stops for any other reason, abort it (`git rebase
--abort`, or `git rebase --quit` if only the replay state is left) and report.

With a clean tree, `git rev-parse HEAD origin/main local/main` printing the same
commit three times means there is nothing to land: say so and stop.

### 4. Verify with the project's test tasks

Run the suite of each subproject whose non-documentation files changed in
`git diff --name-only origin/main..HEAD` — what both pushes carry, since
`local/main` is folded in before it (both if both changed; if only `*.md`
changed, say tests were skipped because nothing executable changed):

```sh
# runner/
pixi install            # only when runner/.pixi/envs is missing
pixi run test           # runner/pixi.toml, [tasks] test = "pytest -q"

# orchestrator/
pixi install -e test    # only when orchestrator/.pixi/envs/test is missing
pixi run -e test test   # orchestrator/pixi.toml, [feature.test.tasks] test
```

`orchestrator`'s test task depends on `build-ui`, which compiles the ultra UI
(`orchestrator/pixi.toml`, `[tasks] build-ui`) and leaves
`orchestrator/ui/dist/` untracked; leave that output out of every commit.

The `postgres` test in `orchestrator/tests/test_orchestrator.py` starts a
throwaway server and skips itself when `initdb` is missing or the tests run as
root. A skip is not a failure; read the summary line.

Any failing check means the change has not landed: report the failing command
and the relevant output, and stop. Do not push, and do not paper over a failure
by narrowing the test command. The suites must pass on the exact commit being
pushed.

### 5. Push to GitHub, then to the local checkout

```sh
git merge-base --is-ancestor origin/main HEAD  # must succeed
git merge-base --is-ancestor local/main HEAD   # must succeed
git push origin HEAD:main
git push local HEAD:main
```

Both are plain pushes, never `--force`. Guards before them:

- If either `main` moved since the rebase (an ancestry check fails), fetch and
  rebase onto it again rather than pushing or forcing.
- The `local` remote is the user's working checkout, so it has to be clean:
  git refuses to update a checkout with uncommitted changes, and a push would
  otherwise run over the user's edits. When `git remote get-url local` is a
  local path, check it first and stop, naming the paths, if it is dirty:

  ```sh
  git --no-optional-locks -C "$(dirname "$(git remote get-url local)")" status --porcelain
  ```

  Skip this check when the remote URL is not a local path.
- A non-bare repository refuses a push into its checked-out branch unless
  `receive.denyCurrentBranch=updateInstead`. Check the destination and set it
  when it is missing — once per checkout — then say so in the report:

  ```sh
  repo="$(dirname "$(git remote get-url local)")"
  if [ "$(git -C "$repo" config --get receive.denyCurrentBranch)" != updateInstead ]; then
    git -C "$repo" config receive.denyCurrentBranch updateInstead
  fi
  ```

  Skip this too when the remote URL is not a local path.

A credential or network error on the `origin` push stops here: nothing is
landed yet, so report it and the change as not landed.

### 6. Confirm and report

```sh
git ls-remote origin refs/heads/main
git ls-remote local refs/heads/main
git rev-parse HEAD
```

When the three commits match, the change is landed. Report the landed commit
sha(s) and subject(s), the checks you ran and their results, and that both
GitHub (`origin/main`) and the user's checkout at `~/Projects/orchestrate` now
have it.

If they do not match, the change has **not** landed: report what each push
actually did and any error, and stop. Name which of the two has it — landing on
GitHub and then failing on the checkout (or the reverse) is a state the next run
fixes with the same commands, not with a force-push.

## Blockers

Stop and report, with the change explicitly not landed, when: a rebase
conflicts, a required test fails, the user's checkout is dirty, either push is
not a fast-forward, a credential or network error stops either push, or the
request would need a force-push or history rewrite. State the blocker and the
concrete next step instead of choosing for the user.
