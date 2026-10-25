#!/usr/bin/env bash
# Install the prime-agent harness binary a thread runs (src/runner/harness.py)
# into runner/.harness/, pinned and checksummed. prime-agent ships its Linux
# builds from Prime Intellect's release bucket, not conda-forge or GitHub
# releases. Idempotent: a present install of this version is kept.
set -euo pipefail
VERSION=0.10.1
BASE=https://pub-728493de92a943e2a9b2d17b4719f318.r2.dev/releases/v$VERSION
case "$(uname -s):$(uname -m)" in
  Linux:x86_64) PLATFORM=linux-x64; SHA256=21ec219183f4134dd23dce9572102e69456066fe374846525561c776d4d1945c ;;
  Linux:aarch64) PLATFORM=linux-arm64; SHA256=5af68c10a088538fd4990fd18778d8c7fe28c0491083dc069c5177f733770e91 ;;
  *) echo "prime-agent $VERSION: no build for $(uname -s) $(uname -m); the runner is Linux only" >&2; exit 1 ;;
esac
RUNNER="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$RUNNER/.harness/prime-agent-$VERSION"
[ -x "$DEST/prime-agent" ] && exit 0
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
FILE="prime-agent-$VERSION-$PLATFORM.tar.gz"
curl -fsSL -o "$TMP/$FILE" "$BASE/$FILE"
echo "$SHA256  $TMP/$FILE" | sha256sum -c --quiet
mkdir -p "$TMP/x"
tar -xzf "$TMP/$FILE" -C "$TMP/x"
mkdir -p "$RUNNER/.harness"
rm -rf "$DEST"
mv "$TMP/x" "$DEST"
echo "prime-agent $VERSION installed at $DEST"
