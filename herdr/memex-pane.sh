#!/usr/bin/env bash
# The command behind both plugin panes ("desk" and "sidebar"): resolve the memex binary and hand
# the pane over to the TUI.
#
# Invoked by herdr as: sh -c 'exec "$HERDR_PLUGIN_ROOT/herdr/memex-pane.sh"'
set -uo pipefail

mode="pane"
# shellcheck source=herdr/lib.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

if MEMEX=$(resolve_memex); then
  # Optional pane parameters, passed via `herdr plugin pane open --env`:
  #   MEMEX_ROOT         alternate data directory
  #   MEMEX_TUI_QUERY    open with this search query
  #   MEMEX_TUI_PROJECT  open filtered to this project (the "recent here" action)
  args=(tui)
  [ -n "${MEMEX_ROOT:-}" ] && args+=(--root "$MEMEX_ROOT")
  [ -n "${MEMEX_TUI_QUERY:-}" ] && args+=(--query "$MEMEX_TUI_QUERY")
  [ -n "${MEMEX_TUI_PROJECT:-}" ] && args+=(--project "$MEMEX_TUI_PROJECT")
  exec "$MEMEX" "${args[@]}"
fi

# A pane whose command exits immediately just vanishes, taking the reason with it. Print the fix
# with a builtin (the message must survive even a broken PATH) and hold the pane open long enough
# to read it.
printf '%s\n' \
  "memex: could not find the memex binary." \
  "" \
  "Looked for:" \
  "  $MEMEX_PLUGIN_ROOT/bin/memex" \
  "  $MEMEX_PLUGIN_ROOT/target/release/memex" \
  "  memex on PATH" \
  "" \
  "Fix it with one of:" \
  "  herdr plugin install nicosuave/memex   # downloads a release build into the plugin" \
  "  cargo build --release                  # from a linked checkout of nicosuave/memex" \
  "  brew install memex                     # puts memex on PATH"

# `read -t` is the builtin fallback for a PATH so broken that even sleep is missing.
command -v sleep >/dev/null 2>&1 && exec sleep 5
read -r -t 5 _ || true
