#!/usr/bin/env bash
# Build an Apple Silicon app and its CLI from this checkout. No signing credentials needed.
# MEMEX_RELEASE_RUNTIME must select the runtime explicitly:
#   history-only  public build; ignores any configured local runtime checkout
#   locked        includes the pinned runtime, which must be reachable from its main
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
REPO=$(cd "$ROOT/../.." && pwd)
case "${MEMEX_RELEASE_RUNTIME:-}" in
  history-only)
    export MEMEX_HISTORY_ONLY=1
    unset MEMEX_AGENT_RUNTIME_ROOT MEMEX_AGENT_RUNTIME_LIBRARY MEMEX_CLAUDE_HELPER \
      MEMEX_RUNTIME_DEVELOPMENT MEMEX_RUNTIME_CANDIDATE_REVISION
    ;;
  locked)
    if [[ "${MEMEX_HISTORY_ONLY:-0}" == 1 || "${MEMEX_RUNTIME_DEVELOPMENT:-0}" == 1 ||
          -n "${MEMEX_RUNTIME_CANDIDATE_REVISION:-}" ]]; then
      echo "MEMEX_RELEASE_RUNTIME=locked cannot be combined with history-only, development, or candidate runtime settings." >&2
      exit 1
    fi
    export MEMEX_REQUIRE_RUNTIME=1 MEMEX_RUNTIME_REQUIRE_PUBLISHED=1
    ;;
  *)
    echo "Set MEMEX_RELEASE_RUNTIME=history-only (public releases) or MEMEX_RELEASE_RUNTIME=locked (pinned private runtime)." >&2
    exit 1
    ;;
esac
cd "$REPO"
export MACOSX_DEPLOYMENT_TARGET=14.0
CARGO=(cargo)
if command -v mbx >/dev/null 2>&1; then
  CARGO=(mbx)
else
  echo "mbx unavailable; using uncached cargo builds." >&2
fi
"${CARGO[@]}" build --locked --release --target aarch64-apple-darwin --target-dir "$ROOT/build/cli"
ARCHES=arm64 MEMEX_CLI="$ROOT/build/cli/aarch64-apple-darwin/release/memex" "$ROOT/scripts/build.sh" release
bash "$ROOT/scripts/verify-runtime-mode.sh" "$ROOT/build/Memex.app" "$MEMEX_RELEASE_RUNTIME"
echo "Release runtime mode: $MEMEX_RELEASE_RUNTIME"
