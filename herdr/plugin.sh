#!/usr/bin/env bash
# Every memex herdr action, plus the session-start hook.
#
#   plugin.sh desk          open the zoomed session desk over the focused pane
#   plugin.sh palette       open the session palette (overlay; focus returns on quit)
#   plugin.sh recent-here   the palette pre-filtered to the focused workspace's repo
#   plugin.sh toggle        open the sidebar, or close it if one is already open
#   plugin.sh open          open the sidebar, no-op if one is open
#   plugin.sh close         close every memex pane in the workspace, no-op if none
#   plugin.sh resume-last   resume the most recent session for the focused pane's cwd
#   plugin.sh index         incremental reindex, logged to the plugin state dir
#   plugin.sh web           open the local memex web UI, starting it if needed
#   plugin.sh startup       session start: background an index (gated by index_on_startup)
#
# There is no state file: the workspace's memex panes are whatever the live pane list says they
# are. Actions refuse loudly (exit 1, exactly one stderr line) and report success on one stdout
# line; startup refuses silently so it can never block a herdr session start.
#
# Helpers below set globals rather than printing into `$(...)`: refuse() inside a command
# substitution would kill only the subshell and let the caller carry on with an empty result.
set -uo pipefail

mode="${1:-desk}"
# shellcheck source=herdr/lib.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

ws="${HERDR_WORKSPACE_ID:-}"
pane="${HERDR_PANE_ID:-}"
PANES_JSON=""
EXISTING=""
OPENED=""
# Extra `--env KEY=VALUE` pairs for open_plugin_pane; modes fill it before opening.
# (The guarded expansion below keeps an empty array safe under `set -u` on bash 3.2.)
PANE_ENV=()

# The workspace directory to scope a resume to. ctx_state records whether the context was
# actually readable: "ok" (parsed; ctx_cwd may still be legitimately empty), "nojq", or
# "unparsable". Modes that scope by directory must refuse on the failure states rather than
# treat them as "no context" — silently dropping the scope would resume some other project's
# session (resume-last) or misreport why scoping failed (recent-here).
ctx_cwd=""
ctx_state="ok"
if [ -n "${HERDR_PLUGIN_CONTEXT_JSON:-}" ]; then
  if ! command -v jq >/dev/null 2>&1; then
    ctx_state="nojq"
  elif ctx_cwd=$(printf '%s' "$HERDR_PLUGIN_CONTEXT_JSON" |
    jq -r '.focused_pane_cwd // .workspace_cwd // empty' 2>/dev/null); then
    :
  else
    ctx_cwd=""
    ctx_state="unparsable"
  fi
fi

# Config, re-read every run; an unknown value falls back to the default beside it.
placement="split"
direction="right"
index_on_startup="true"
web_listen="127.0.0.1:6363"
conf_placement=$(config_str toggle_placement)
conf_direction=$(config_str toggle_direction)
conf_startup=$(config_bool index_on_startup)
conf_listen=$(config_str web_listen)
case "$conf_placement" in split | overlay | zoomed | tab) placement="$conf_placement" ;; esac
case "$conf_direction" in right | down) direction="$conf_direction" ;; esac
case "$conf_startup" in true | false) index_on_startup="$conf_startup" ;; esac
case "$conf_listen" in *:*) web_listen="$conf_listen" ;; esac

require_workspace() {
  [ -n "$ws" ] || refuse "no workspace context (invoke from inside herdr)"
}

# One pane-list snapshot serves the whole run. A failed listing must never read as "no memex
# pane": that would stack a duplicate on toggle and false-succeed a close.
load_panes() {
  [ -n "$PANES_JSON" ] && return 0
  PANES_JSON=$("$H" pane list --workspace "$ws" 2>/dev/null) && [ -n "$PANES_JSON" ] ||
    refuse "herdr pane list failed for $ws"
}

# EXISTING := every memex-labeled pane in the workspace, any tab, any placement.
find_existing() {
  load_panes
  EXISTING=$(printf '%s' "$PANES_JSON" | jq -r '.result.panes[] | select(.label == "memex") | .pane_id' 2>/dev/null)
}

# Split and zoomed placements attach to a pane: the focused one, else the workspace's first.
attach_pane() {
  if [ -z "$pane" ]; then
    load_panes
    pane=$(printf '%s' "$PANES_JSON" | jq -r '.result.panes[0].pane_id // empty' 2>/dev/null)
  fi
  [ -n "$pane" ] || refuse "no pane to attach to in $ws"
}

# Plain `pane close`, not `plugin pane close`: the plugin-pane registry does not survive a herdr
# restart, and a stale registry would strand the pane instead of closing it.
close_all() {
  local closed="" failed="" p
  while IFS= read -r p; do
    [ -n "$p" ] || continue
    if "$H" pane close "$p" >/dev/null 2>&1; then closed="$closed $p"; else failed="$failed $p"; fi
  done <<EOF
$EXISTING
EOF
  [ -z "$failed" ] || refuse "failed to close$failed in $ws"
  printf 'closed%s in %s\n' "$closed" "$ws"
}

# OPENED := the new pane's id. $1 entrypoint, $2 placement, $3 focus flag.
open_plugin_pane() {
  local entrypoint="$1" want="$2" focus="$3" out
  local -a args
  case "$want" in
  split | zoomed)
    # split and zoomed require --target-pane; tab requires --workspace; overlay takes neither.
    attach_pane
    args=(--placement "$want" --target-pane "$pane")
    [ "$want" = "split" ] && args+=(--direction "$direction")
    ;;
  tab)
    args=(--placement tab --workspace "$ws")
    ;;
  overlay)
    args=(--placement overlay)
    ;;
  *)
    refuse "unreachable placement '$want'"
    ;;
  esac

  out=$("$H" plugin pane open --plugin "$PLUGIN_ID" --entrypoint "$entrypoint" \
    "${args[@]}" ${PANE_ENV[@]+"${PANE_ENV[@]}"} "$focus" 2>/dev/null) ||
    refuse "herdr plugin pane open failed ($entrypoint)"
  # The pane id is a nicety for the log line; without jq the open still counted.
  OPENED=$(printf '%s' "$out" | jq -r '.result.plugin_pane.pane.pane_id // empty' 2>/dev/null)
  OPENED="${OPENED:-pane}"
}

case "$mode" in
desk)
  require_workspace
  # The desk is always zoomed and always takes focus: it is the thing you just asked for.
  # No --cwd — the pane command is absolute, so the pane's directory does not matter.
  open_plugin_pane desk zoomed --focus
  printf 'opened session desk %s in %s\n' "$OPENED" "$ws"
  ;;

palette)
  require_workspace
  # An overlay always takes focus and hands it back when memex quits.
  open_plugin_pane palette overlay --focus
  printf 'opened session palette %s in %s\n' "$OPENED" "$ws"
  ;;

recent-here)
  require_workspace
  # The palette scoped to this repo: memex projects are directory basenames, so scope to the
  # git root's name when there is one, else the focused directory's own name.
  [ "$ctx_state" = "ok" ] || refuse "cannot read pane context ($ctx_state)"
  [ -n "$ctx_cwd" ] || refuse "no directory context (invoke from inside herdr)"
  repo=$(git -C "$ctx_cwd" rev-parse --show-toplevel 2>/dev/null) || repo="$ctx_cwd"
  project=$(basename "$repo")
  [ -n "$project" ] || refuse "cannot derive a project name from $repo"
  PANE_ENV=(--env "MEMEX_TUI_PROJECT=$project")
  open_plugin_pane palette overlay --focus
  printf 'opened recent sessions for %s (%s) in %s\n' "$project" "$OPENED" "$ws"
  ;;

close | toggle | open)
  require_workspace
  find_existing
  if [ "$mode" = "close" ]; then
    [ -n "$EXISTING" ] || {
      printf 'close: no memex pane open in %s\n' "$ws"
      exit 0
    }
    close_all
    exit 0
  fi
  if [ -n "$EXISTING" ]; then
    if [ "$mode" = "toggle" ]; then
      close_all
      exit 0
    fi
    printf 'open: already open (%s) in %s\n' "$(printf '%s' "$EXISTING" | tr '\n' ' ' | sed 's/ $//')" "$ws"
    exit 0
  fi
  # Focus follows the placement: a split sits beside your work, anything that covers it is
  # somewhere you meant to go.
  focus="--no-focus"
  [ "$placement" = "split" ] || focus="--focus"
  open_plugin_pane sidebar "$placement" "$focus"
  printf 'opened %s (%s) in %s\n' "$OPENED" "$placement" "$ws"
  ;;

resume-last)
  MEMEX=$(resolve_memex) ||
    refuse "memex binary not found (run 'herdr plugin install nicosuave/memex', or 'cargo build --release' in a linked checkout)"
  # A context we could not read is not the same as no context: proceeding without the cwd
  # scope would resume the most recent session of some other project.
  [ "$ctx_state" = "ok" ] ||
    refuse "cannot read pane context ($ctx_state); refusing to resume without directory scoping"
  # memex resolves the most recent resumable session and opens it in a new herdr tab itself,
  # driving the herdr CLI with the HERDR_* env this action already carries.
  args=(herdr resume-last --strict-cwd)
  [ -n "$ctx_cwd" ] && args+=(--cwd "$ctx_cwd")
  # MEMEX_ROOT (matching memex-pane.sh) points every memex call at an alternate data directory.
  [ -n "${MEMEX_ROOT:-}" ] && args+=(--root "$MEMEX_ROOT")
  # stdout and stderr are kept apart so a refusal quotes memex's own failure line rather than
  # whatever it happened to print last.
  errfile=$(mktemp "${TMPDIR:-/tmp}/memex-herdr-resume.XXXXXX" 2>/dev/null) || errfile="/dev/null"
  out=$("$MEMEX" "${args[@]}" 2>"$errfile")
  status=$?
  err=$(grep -v '^[[:space:]]*$' "$errfile" 2>/dev/null | head -n1)
  [ "$errfile" = "/dev/null" ] || rm -f "$errfile"
  [ "$status" -eq 0 ] || refuse "resume-last failed: ${err:-exit $status}"
  line=$(printf '%s' "$out" | grep -v '^[[:space:]]*$' | tail -n1)
  printf '%s\n' "${line:-resumed the last session}"
  ;;

index)
  MEMEX=$(resolve_memex) ||
    refuse "memex binary not found (run 'herdr plugin install nicosuave/memex', or 'cargo build --release' in a linked checkout)"
  dir=$(state_dir) || refuse "cannot create state dir ${HERDR_PLUGIN_STATE_DIR:-<unset>}"
  log="$dir/index.log"
  if "$MEMEX" index ${MEMEX_ROOT:+--root "$MEMEX_ROOT"} >>"$log" 2>&1; then
    # Best effort: a herdr without `notification show` must not fail a successful index.
    "$H" notification show "memex" --body "index refreshed" >/dev/null 2>&1 || true
    printf 'index refreshed (log: %s)\n' "$log"
  else
    refuse "index failed, see $log"
  fi
  ;;

web)
  MEMEX=$(resolve_memex) ||
    refuse "memex binary not found (run 'herdr plugin install nicosuave/memex', or 'cargo build --release' in a linked checkout)"
  dir=$(state_dir) || refuse "cannot create state dir ${HERDR_PLUGIN_STATE_DIR:-<unset>}"
  log="$dir/web.log"
  url="http://$web_listen"
  # Reuse a server that is already up — the background index service may well be serving it.
  if ! curl -fsS -m 1 "$url/api/stats" >/dev/null 2>&1; then
    nohup "$MEMEX" web --listen "$web_listen" ${MEMEX_ROOT:+--root "$MEMEX_ROOT"} >>"$log" 2>&1 </dev/null &
    # The assets are embedded in the binary, so it comes up fast; give it ~3s anyway.
    up=""
    for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
      sleep 0.2
      if curl -fsS -m 1 "$url/api/stats" >/dev/null 2>&1; then
        up=1
        break
      fi
    done
    [ -n "$up" ] || refuse "memex web did not come up on $web_listen, see $log"
  fi
  case "$(uname -s)" in
  Darwin) opener="open" ;;
  *) opener="xdg-open" ;;
  esac
  command -v "$opener" >/dev/null 2>&1 || refuse "$opener not found; memex web is running at $url"
  "$opener" "$url" >/dev/null 2>&1 || refuse "failed to open $url with $opener"
  printf 'opened %s\n' "$url"
  ;;

startup)
  # Session start. Silent throughout, and never blocking: the index runs detached and this exits
  # immediately whether or not it succeeds.
  [ "$index_on_startup" = "true" ] || exit 0
  MEMEX=$(resolve_memex) || exit 0
  dir=$(state_dir) || exit 0
  nohup "$MEMEX" index ${MEMEX_ROOT:+--root "$MEMEX_ROOT"} >>"$dir/startup-index.log" 2>&1 </dev/null &
  exit 0
  ;;

*)
  refuse "unknown mode '$mode' (desk | palette | recent-here | toggle | open | close | resume-last | index | web | startup)"
  ;;
esac
