#!/usr/bin/env bash
# Start the runner service (foreground, for a process supervisor or a Sprite service).
# Needs DEEPSEEK_API_KEY and RUNNER_TOKEN; see API.md for the rest.
set -euo pipefail
export PATH="$HOME/.pixi/bin:$PATH"
cd "${ORCHESTRATE_HOME:-/opt/orchestrate}/runner"
exec pixi run -e prod serve
