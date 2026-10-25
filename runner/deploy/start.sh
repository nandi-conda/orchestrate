#!/usr/bin/env bash
# Start the runner service (foreground, for a process supervisor or a Sprite service).
# Needs DEEPSEEK_API_KEY and RUNNER_TOKEN; see API.md for the rest.
set -euo pipefail
export PATH="$HOME/.pixi/bin:$PATH"

# Threads' commands get a /proc of their own (bubblewrap, src/runner/sandbox.py).
# The kernel lets a user namespace mount one only while this machine's /proc is
# fully visible, and container runtimes mask parts of it with mounts on top
# (crun on Sprites: /proc/kcore, keys, timer_list, scsi). Take those off; the
# worker's user has root through sudo there. The masks come back with the
# container, so this runs at every start.
for m in $(awk '$5 ~ "^/proc/" {print $5}' /proc/self/mountinfo | sort -r); do
  sudo -n umount "$m"
done
cd "${ORCHESTRATE_HOME:-/opt/orchestrate}/runner"
exec pixi run -e prod serve
