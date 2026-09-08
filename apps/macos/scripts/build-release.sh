#!/usr/bin/env bash
# Build a universal app and its CLI from this checkout. No signing credentials needed.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
REPO=$(cd "$ROOT/../.." && pwd)
cd "$REPO"
export MACOSX_DEPLOYMENT_TARGET=14.0
CARGO=(cargo)
if command -v mbx >/dev/null 2>&1; then
  CARGO=(mbx)
else
  echo "mbx unavailable; using uncached cargo builds." >&2
fi
for target in aarch64-apple-darwin x86_64-apple-darwin; do
  "${CARGO[@]}" build --locked --release --target "$target" --target-dir "$ROOT/build/cli"
done
lipo -create "$ROOT/build/cli/aarch64-apple-darwin/release/memex" \
  "$ROOT/build/cli/x86_64-apple-darwin/release/memex" \
  -output "$ROOT/build/memex-universal"
ARCHES="arm64 x86_64" MEMEX_CLI="$ROOT/build/memex-universal" "$ROOT/scripts/build.sh" release
