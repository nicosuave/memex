#!/usr/bin/env bash
# Assert that an assembled Memex.app contains runtime artifacts exactly when its
# runtime mode says it should. Usage: verify-runtime-mode.sh APP MODE
set -euo pipefail
app=${1:?Usage: verify-runtime-mode.sh APP MODE}
mode=${2:?Usage: verify-runtime-mode.sh APP MODE}
runtime_files=(
  "Contents/Resources/runtime-provenance.json"
  "Contents/Helpers/claude-agent-sdk-host"
  "Contents/Helpers/MemexExecutionHost"
)
if [[ "$mode" == history-only ]]; then
  for file in "${runtime_files[@]}"; do
    [[ ! -e "$app/$file" ]] || { echo "History-only bundle unexpectedly contains $file" >&2; exit 1; }
  done
else
  for file in "${runtime_files[@]}"; do
    [[ -e "$app/$file" ]] || { echo "Runtime bundle ($mode) is missing $file" >&2; exit 1; }
  done
  jq -e --arg mode "$mode" '.mode == $mode' "$app/Contents/Resources/runtime-provenance.json" >/dev/null ||
    { echo "Bundled runtime provenance does not record mode $mode" >&2; exit 1; }
fi
echo "Verified $mode bundle contents"
