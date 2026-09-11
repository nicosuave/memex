#!/usr/bin/env bash
set -euo pipefail

systemctl=$1
mode=$2
runtime_dir=${XDG_RUNTIME_DIR:-/run/user/$(id -u)}
ownership_file="$runtime_dir/memex-nixos-service"
timer_ownership_file="$runtime_dir/memex-nixos-timer"
service=memex-index.service
timer=memex-index.timer

# A removed unit is expected when the module is disabled on this switch.
fragment=$("$systemctl" --user show "$service" --property=FragmentPath --value 2>/dev/null || true)
case "$fragment" in
  /etc/systemd/user/memex-index.service)
    # Remember ownership across a switch that removes the unit. A running
    # obsolete unit may no longer expose its FragmentPath after daemon-reload.
    touch "$ownership_file"
    ;;
  "")
    if [[ ! -f "$ownership_file" ]]; then
      exit 0
    fi
    ;;
  *)
    # A Home Manager or CLI unit shadows the NixOS unit. Leave it alone.
    rm -f "$ownership_file" "$timer_ownership_file"
    exit 0
    ;;
esac

service_active=false
timer_active=false
if "$systemctl" --user is-active --quiet "$service"; then
  service_active=true
fi
timer_fragment=$("$systemctl" --user show "$timer" --property=FragmentPath --value 2>/dev/null || true)
timer_owned=false
case "$timer_fragment" in
  /etc/systemd/user/memex-index.timer)
    touch "$timer_ownership_file"
    timer_owned=true
    ;;
  "")
    if [[ -f "$timer_ownership_file" ]]; then timer_owned=true; fi
    ;;
  *) rm -f "$timer_ownership_file" ;;
esac
if $timer_owned; then
  if "$systemctl" --user is-active --quiet "$timer"; then
    timer_active=true
  fi
fi

# An interval transition requires this module's current timer. Do not stop the
# working daemon and then start a shadowing or unidentifiable timer.
if [[ "$mode" == interval && "$timer_fragment" != /etc/systemd/user/memex-index.timer ]]; then
  exit 0
fi

"$systemctl" --user daemon-reload
case "$mode" in
  disabled)
    if $timer_active; then "$systemctl" --user stop "$timer"; fi
    if $service_active; then "$systemctl" --user stop "$service"; fi
    rm -f "$ownership_file" "$timer_ownership_file"
    ;;
  continuous)
    if $timer_active; then "$systemctl" --user stop "$timer"; fi
    if $service_active; then
      "$systemctl" --user restart "$service"
    elif $timer_active; then
      "$systemctl" --user start "$service"
    fi
    ;;
  interval)
    if $service_active; then "$systemctl" --user stop "$service"; fi
    if $timer_active; then
      "$systemctl" --user restart "$timer"
    elif $service_active; then
      "$systemctl" --user start "$timer"
    fi
    ;;
  *)
    printf 'Unknown memex service mode: %s\n' "$mode" >&2
    exit 1
    ;;
esac
