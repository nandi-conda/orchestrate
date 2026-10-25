#!/usr/bin/env bash
# Prepare a fresh Linux worker (a Sprite, a VM, a container) to run the runner.
# Idempotent: run it again to update. Then start the service with deploy/start.sh.
#   ORCHESTRATE_REPO   git URL of this monorepo (default: github.com/nandi-conda/orchestrate)
#   ORCHESTRATE_REF    branch or tag (default: main)
#   ORCHESTRATE_HOME   where to check it out (default: /opt/orchestrate)
set -euo pipefail
REPO="${ORCHESTRATE_REPO:-https://github.com/nandi-conda/orchestrate}"
REF="${ORCHESTRATE_REF:-main}"
HOME_DIR="${ORCHESTRATE_HOME:-/opt/orchestrate}"

if ! command -v pixi >/dev/null 2>&1 && [ ! -x "$HOME/.pixi/bin/pixi" ]; then
  curl -fsSL https://pixi.sh/install.sh | bash
fi
export PATH="$HOME/.pixi/bin:$PATH"

if [ -d "$HOME_DIR/.git" ]; then
  git -C "$HOME_DIR" fetch --depth 1 origin "$REF"
  git -C "$HOME_DIR" checkout -q FETCH_HEAD
else
  git clone --depth 1 --branch "$REF" "$REPO" "$HOME_DIR"
fi

cd "$HOME_DIR/runner"
pixi install -e prod
pixi install -e kernel        # the Python threads' prime-agent kernels run on
bash deploy/install-harness.sh  # the prime-agent binary, pinned
echo "runner ready at $HOME_DIR/runner"
