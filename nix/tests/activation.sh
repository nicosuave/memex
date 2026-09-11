#!/usr/bin/env bash
set -euo pipefail

test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT
script_dir=$(cd "$(dirname "$0")/.." && pwd)
export XDG_RUNTIME_DIR="$test_dir/runtime"
mkdir -p "$XDG_RUNTIME_DIR"
export trace="$test_dir/trace"

cat > "$test_dir/systemctl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
shift # --user
case "$1" in
  show)
    if [[ "$2" == memex-index.service ]]; then
      printf '%s\n' "$MOCK_SERVICE_FRAGMENT"
    else
      printf '%s\n' "$MOCK_TIMER_FRAGMENT"
    fi
    ;;
  is-active)
    if [[ "$3" == memex-index.service ]]; then
      "$MOCK_SERVICE_ACTIVE"
    else
      "$MOCK_TIMER_ACTIVE"
    fi
    ;;
  *) printf '%s\n' "$*" >> "$trace" ;;
esac
MOCK
chmod +x "$test_dir/systemctl"

check() {
  local mode=$1 expected=$2
  : > "$trace"
  bash "$script_dir/activate-user-service.sh" "$test_dir/systemctl" "$mode"
  local actual
  actual=$(cat "$trace")
  if [[ "$actual" != "$expected" ]]; then
    printf 'mode=%s: expected <%s>, got <%s>\n' "$mode" "$expected" "$actual" >&2
    exit 1
  fi
}

export MOCK_SERVICE_FRAGMENT=/etc/systemd/user/memex-index.service
export MOCK_TIMER_FRAGMENT=/etc/systemd/user/memex-index.timer
export MOCK_SERVICE_ACTIVE=false MOCK_TIMER_ACTIVE=false
check continuous 'daemon-reload'
check interval 'daemon-reload'
export MOCK_SERVICE_ACTIVE=true
check continuous $'daemon-reload\nrestart memex-index.service'
check interval $'daemon-reload\nstop memex-index.service\nstart memex-index.timer'
export MOCK_SERVICE_ACTIVE=false MOCK_TIMER_ACTIVE=true
check interval $'daemon-reload\nrestart memex-index.timer'
check continuous $'daemon-reload\nstop memex-index.timer\nstart memex-index.service'
export MOCK_SERVICE_ACTIVE=true
check disabled $'daemon-reload\nstop memex-index.timer\nstop memex-index.service'

# The unit can disappear on disable, while its previous process is still alive.
touch "$XDG_RUNTIME_DIR/memex-nixos-service"
export MOCK_SERVICE_FRAGMENT= MOCK_TIMER_FRAGMENT= MOCK_TIMER_ACTIVE=false
check disabled $'daemon-reload\nstop memex-index.service'
check disabled ''

# A foreign timer must not be started, even when our own daemon is active.
export MOCK_SERVICE_FRAGMENT=/etc/systemd/user/memex-index.service
export MOCK_TIMER_FRAGMENT=/home/test/.config/systemd/user/memex-index.timer
export MOCK_SERVICE_ACTIVE=true MOCK_TIMER_ACTIVE=false
check interval ''
export MOCK_TIMER_ACTIVE=true
check interval ''

# Service ownership alone does not establish ownership of an unknown timer.
export MOCK_TIMER_FRAGMENT=
check interval ''
check disabled $'daemon-reload\nstop memex-index.service'

# A previously observed owned timer can disappear while it is still active.
touch "$XDG_RUNTIME_DIR/memex-nixos-service" "$XDG_RUNTIME_DIR/memex-nixos-timer"
export MOCK_SERVICE_FRAGMENT= MOCK_SERVICE_ACTIVE=false
check disabled $'daemon-reload\nstop memex-index.timer'

# A user or Home Manager unit with the same label takes precedence.
export MOCK_SERVICE_FRAGMENT=/home/test/.config/systemd/user/memex-index.service
touch "$XDG_RUNTIME_DIR/memex-nixos-service"
check continuous ''
[[ ! -f "$XDG_RUNTIME_DIR/memex-nixos-service" ]]
printf 'NixOS activation checks passed\n'
