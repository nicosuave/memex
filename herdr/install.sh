#!/usr/bin/env bash
# herdr `[[build]]` step: make sure a working memex binary is available to the plugin.
#
# Runs on `herdr plugin install nicosuave/memex` (a managed checkout). `herdr plugin link` skips
# the build step entirely — from a local checkout, run `cargo build --release` and the plugin
# scripts pick up target/release/memex.
#
# Build commands run with the plugin checkout as the working directory and may not receive the
# runtime HERDR_* env, so the plugin root is resolved from this script's own location.
#
# Order of preference:
#   1. a memex already on PATH that speaks the herdr surface (brew, setup.sh, nix) — use it
#   2. the release tarball matching this manifest's version -> $ROOT/bin/memex
#   3. cargo build --release from this checkout -> $ROOT/bin/memex
set -euo pipefail

NAME="memex"
REPO="nicosuave/memex"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="$ROOT/bin"

# herdr build commands inherit the same minimal PATH as actions; curl, tar, and cargo need help.
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$HOME/.cargo/bin:${PATH:-}"

# Install a candidate binary at $1 into $BIN_DIR and prove it runs.
#
# Overwriting a Mach-O in place invalidates its code signature, and macOS then SIGKILLs it on
# launch — so remove the old file first (fresh inode) and re-sign ad-hoc afterwards.
install_bin() {
  mkdir -p "$BIN_DIR"
  rm -f "$BIN_DIR/$NAME"
  install -m 0755 "$1" "$BIN_DIR/$NAME"
  if [ "$(uname -s)" = "Darwin" ] && command -v codesign >/dev/null 2>&1; then
    codesign --force --sign - "$BIN_DIR/$NAME" >/dev/null 2>&1 || true
  fi
  "$BIN_DIR/$NAME" --version >/dev/null 2>&1
}

# 1. An existing install is the best outcome: no download, and it stays updated by brew/setup.sh.
#    The probe is the herdr surface itself, not --version, because an older memex on PATH would
#    answer --version happily and then fail every action.
if command -v "$NAME" >/dev/null 2>&1 && "$NAME" sessions --limit 0 >/dev/null 2>&1; then
  echo "$NAME: using PATH $NAME ($(command -v "$NAME"))"
  exit 0
fi

# 2. The release tarball for this exact manifest version, so a checkout always pulls its own build.
VERSION="$(grep -m1 '^version' "$ROOT/herdr-plugin.toml" | sed -E 's/.*"([^"]+)".*/\1/')"

# Same OS/arch tokens the release workflow packages and scripts/setup.sh downloads.
os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
Darwin) os="macos" ;;
Linux) os="linux" ;;
*) os="" ;;
esac
case "$arch" in
x86_64 | amd64) arch="x86_64" ;;
arm64 | aarch64) arch="arm64" ;;
*) arch="" ;;
esac

tmp=""
cleanup() {
  [ -n "$tmp" ] && rm -rf "$tmp"
  return 0
}
trap cleanup EXIT

download_release() {
  local archive url expected actual
  archive="${NAME}-${VERSION}-${os}-${arch}.tar.gz"
  url="https://github.com/${REPO}/releases/download/v${VERSION}/${archive}"

  tmp="$(mktemp -d)"

  # Release assets are eventually consistent: GitHub's CDN can 404 for a few minutes after a
  # release publishes. Retry on 404 too, so installing right after a release is not a coin flip.
  curl -fsSL --retry 5 --retry-delay 3 --retry-all-errors --retry-connrefused "$url" -o "$tmp/$archive" || return 1

  # The checksum sidecar is best effort (older releases may not have one), but a mismatch is fatal.
  if curl -fsSL --retry 3 --retry-delay 2 --retry-all-errors "$url.sha256" -o "$tmp/$archive.sha256" 2>/dev/null; then
    expected="$(awk '{print $1}' "$tmp/$archive.sha256")"
    if command -v sha256sum >/dev/null 2>&1; then
      actual="$(sha256sum "$tmp/$archive" | awk '{print $1}')"
    else
      actual="$(shasum -a 256 "$tmp/$archive" | awk '{print $1}')"
    fi
    if [ -n "$expected" ] && [ "$expected" != "$actual" ]; then
      echo "$NAME: checksum mismatch for $archive (expected $expected, got $actual)" >&2
      return 1
    fi
  fi

  tar -xzf "$tmp/$archive" -C "$tmp" || return 1
  [ -f "$tmp/$NAME" ] || return 1
  install_bin "$tmp/$NAME"
}

if [ -n "$os" ] && [ -n "$arch" ]; then
  echo "$NAME: downloading ${NAME}-${VERSION}-${os}-${arch}.tar.gz"
  if download_release; then
    echo "$NAME: installed $BIN_DIR/$NAME (v$VERSION release build)"
    exit 0
  fi
  echo "$NAME: release download failed, falling back to a source build" >&2
else
  echo "$NAME: no release build for $(uname -s)-$(uname -m), falling back to a source build" >&2
fi

# 3. Build from this checkout. Slower, but it is the only option on an unreleased version or an
#    unpackaged platform.
if command -v cargo >/dev/null 2>&1; then
  echo "$NAME: building from source (this takes a few minutes)"
  if (cd "$ROOT" && cargo build --release) && install_bin "$ROOT/target/release/$NAME"; then
    echo "$NAME: installed $BIN_DIR/$NAME (source build)"
    exit 0
  fi
  echo "$NAME: source build failed" >&2
fi

cat >&2 <<EOF
$NAME: could not obtain a memex binary.

Install memex yourself, then reinstall the plugin:
  brew install memex
  curl -fsSL https://raw.githubusercontent.com/${REPO}/main/scripts/setup.sh | sh
Or install a Rust toolchain (https://rustup.rs) so this step can build from source.
EOF
exit 1
