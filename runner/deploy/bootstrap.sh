#!/usr/bin/env bash
# Prepare a fresh Linux worker (a Sprite, a VM, a container) to run the runner.
# Idempotent: run it again to update. Then start the service with deploy/start.sh.
#   ORCHESTRATE_REPO   git URL of this monorepo (default: github.com/nandi-conda/orchestrate)
#   ORCHESTRATE_REF    branch, tag or commit (default: main)
#   ORCHESTRATE_HOME   where to check it out (default: /opt/orchestrate)
set -euo pipefail
REPO="${ORCHESTRATE_REPO:-https://github.com/nandi-conda/orchestrate}"
REF="${ORCHESTRATE_REF:-main}"
HOME_DIR="${ORCHESTRATE_HOME:-/opt/orchestrate}"

if ! command -v pixi >/dev/null 2>&1 && [ ! -x "$HOME/.pixi/bin/pixi" ]; then
  curl -fsSL https://pixi.sh/install.sh | bash
fi
export PATH="$HOME/.pixi/bin:$PATH"

[ -d "$HOME_DIR/.git" ] || git init -q "$HOME_DIR"
git -C "$HOME_DIR" remote remove origin 2>/dev/null || true
git -C "$HOME_DIR" remote add origin "$REPO"
git -C "$HOME_DIR" fetch -q --depth 1 origin "$REF"
git -C "$HOME_DIR" checkout -q --force FETCH_HEAD

cd "$HOME_DIR/runner"
pixi install -e prod
pixi install -e kernel        # the Python threads' prime-agent kernels run on
# The team skill is this checkout's own code under a version that never changes,
# so pixi keeps the build it first installed; rebuild it from the checkout each run.
pixi reinstall -e kernel team
bash deploy/install-harness.sh  # the prime-agent binary, pinned
echo "runner ready at $HOME_DIR/runner"
