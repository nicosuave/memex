#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
"$ROOT/scripts/build.sh" "${1:-release}"
open "$ROOT/build/Memex.app"
