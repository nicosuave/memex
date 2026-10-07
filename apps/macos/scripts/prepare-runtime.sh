#!/usr/bin/env bash
# Build or verify the optional runtime. Never downloads private source or signs/publishes artifacts.
set -euo pipefail

APP_ROOT=$(cd "$(dirname "$0")/.." && pwd)
SOURCE_LOCK="$APP_ROOT/runtime-source.json"
RUNTIME_ROOT=${MEMEX_AGENT_RUNTIME_ROOT:-}
OUTPUT=${MEMEX_RUNTIME_OUTPUT:-}
MODE=locked
CANDIDATE_REVISION=""
VERIFY_ONLY=0

fail() { printf '%s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'USAGE'
Usage: prepare-runtime.sh [--source PATH] [--output PATH] [--verify]
                          [--development | --candidate-revision FULL_SHA]

ARCHES="arm64 x86_64" selects output architectures (default: this Mac).
The normal build requires the clean revision in runtime-source.json.
--development explicitly records local uncommitted work; it is not release input.
--candidate-revision tests a clean immutable candidate in private CI; it is not release input.
--verify checks provenance, source identity, architectures and artifact hashes without compiling.
USAGE
}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) [[ $# -ge 2 ]] || fail "--source requires a directory"; RUNTIME_ROOT=$2; shift 2 ;;
    --output) [[ $# -ge 2 ]] || fail "--output requires a directory"; OUTPUT=$2; shift 2 ;;
    --verify) VERIFY_ONLY=1; shift ;;
    --development) [[ "$MODE" == locked ]] || fail "Choose one runtime mode"; MODE=development; shift ;;
    --candidate-revision)
      [[ $# -ge 2 && "$MODE" == locked ]] || fail "--candidate-revision requires a SHA and cannot be combined with --development"
      MODE=candidate; CANDIDATE_REVISION=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) fail "Unknown argument: $1" ;;
  esac
done

for tool in git jq shasum; do command -v "$tool" >/dev/null || fail "Missing required tool: $tool"; done
if [[ -z "$RUNTIME_ROOT" && -f "$APP_ROOT/.local-runtime-root" ]]; then
  RUNTIME_ROOT=$(cat "$APP_ROOT/.local-runtime-root")
fi
[[ -n "$RUNTIME_ROOT" && -d "$RUNTIME_ROOT" ]] || fail "Set MEMEX_AGENT_RUNTIME_ROOT or --source to your authorized runtime checkout. Private source is not downloaded automatically."
RUNTIME_ROOT=$(cd "$RUNTIME_ROOT" && pwd -P)
[[ "$(git -C "$RUNTIME_ROOT" rev-parse --show-toplevel)" == "$RUNTIME_ROOT" ]] || fail "Runtime source must be its repository root"
LOCK_REVISION=$(jq -er 'select(.version == 1) | .revision | select(test("^[0-9a-f]{40}$"))' "$SOURCE_LOCK")
LOCK_REPOSITORY=$(jq -er '.repository | select(. == "sidequery/mono")' "$SOURCE_LOCK")
SOURCE_REVISION=$(git -C "$RUNTIME_ROOT" rev-parse HEAD)
SOURCE_DIRTY=false
[[ -z "$(git -C "$RUNTIME_ROOT" status --porcelain --untracked-files=all)" ]] || SOURCE_DIRTY=true
case "$MODE" in
  locked)
    [[ "$SOURCE_REVISION" == "$LOCK_REVISION" ]] || fail "Runtime HEAD does not match runtime-source.json. Prepare the pinned checkout, or explicitly use --development for local work."
    [[ "$SOURCE_DIRTY" == false ]] || fail "Locked runtime builds require a clean source checkout. Use --development for local work."
    ;;
  candidate)
    [[ "$CANDIDATE_REVISION" =~ ^[0-9a-f]{40}$ && "$SOURCE_REVISION" == "$CANDIDATE_REVISION" ]] || fail "Candidate runtime HEAD must equal the supplied full immutable SHA"
    [[ "$SOURCE_DIRTY" == false ]] || fail "Candidate CI builds require a clean source checkout"
    ;;
esac
# Consumed packages currently have no submodule dependency. Do not pretend a
# dirty nested worktree can be represented by its parent gitlink alone.
git -C "$RUNTIME_ROOT" submodule foreach --quiet --recursive \
  'test -z "$(git status --porcelain --untracked-files=all)"' || fail "Dirty submodules are not supported runtime inputs"

ARCH_TEXT=$(jq -nr --arg value "${ARCHES:-$(uname -m)}" '$value | split(" ") | map(select(length > 0)) | unique | sort | join(" ")')
read -r -a ARCH_LIST <<< "$ARCH_TEXT"
[[ ${#ARCH_LIST[@]} -gt 0 ]] || fail "Select at least one architecture"
for arch in "${ARCH_LIST[@]}"; do
  case "$arch" in arm64|x86_64) ;; *) fail "Unsupported runtime architecture: $arch" ;; esac
done
ARCH_KEY=${ARCH_TEXT// /-}
OUTPUT=${OUTPUT:-$APP_ROOT/.build/runtime/$ARCH_KEY}
if [[ "$VERIFY_ONLY" == 1 ]]; then
  [[ -d "$OUTPUT" ]] || fail "Runtime output is missing. Run prepare-runtime.sh first."
else
  mkdir -p "$OUTPUT"
fi
OUTPUT=$(cd "$OUTPUT" && pwd -P)
PROVENANCE="$OUTPUT/runtime-provenance.json"
LIBRARY="$OUTPUT/libsq_acp_runtime.a"
CLAUDE_HELPER="$OUTPUT/claude-agent-sdk-host"
hash_file() { shasum -a 256 < "$1" | awk '{print $1}'; }
DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET:-14.0}
verify_deployment_target() {
  # Inspect every static-library object, not just the final executable's load
  # command: setting MACOSX_DEPLOYMENT_TARGET cannot rebuild a prebuilt Rust std.
  otool -l "$1" | awk -v expected="$DEPLOYMENT_TARGET" '
    function newer(value, minimum, a, b, i) {
      split(value, a, "."); split(minimum, b, ".")
      for (i = 1; i <= 3; i++) {
        if (a[i] + 0 > b[i] + 0) return 1
        if (a[i] + 0 < b[i] + 0) return 0
      }
      return 0
    }
    /:$/ { object = $0 }
    $1 == "cmd" { kind = $2 }
    (kind == "LC_BUILD_VERSION" && $1 == "minos") ||
    (kind == "LC_VERSION_MIN_MACOSX" && $1 == "version") {
      if (newer($2, expected)) {
        failures++
        if (failures <= 5) print object " requires macOS " $2 ", expected <= " expected > "/dev/stderr"
      }
    }
    END { if (failures) exit 1 }
  ' || fail "Runtime artifact exceeds the deployment target. Select a compatible Rust toolchain with RUSTUP_TOOLCHAIN and prepare again."
}

source_fingerprint() {
  # Includes staged/unstaged tracked changes and untracked nonignored input
  # contents, not merely `git status`. Ignored build outputs do not invalidate it.
  {
    printf 'revision:%s\0' "$SOURCE_REVISION"
    git -C "$RUNTIME_ROOT" diff --binary --no-ext-diff HEAD --
    while IFS= read -r -d '' file; do
      printf 'untracked:%s\0' "$file"
      if [[ -L "$RUNTIME_ROOT/$file" ]]; then
        printf 'symlink:'
        readlink "$RUNTIME_ROOT/$file"
        printf '\0'
      elif [[ -f "$RUNTIME_ROOT/$file" ]]; then
        hash_file "$RUNTIME_ROOT/$file"
      else
        fail "Untracked runtime input is not a regular file: $file"
      fi
    done < <(git -C "$RUNTIME_ROOT" ls-files --others --exclude-standard -z)
  } | shasum -a 256 | awk '{print $1}'
}
SOURCE_FINGERPRINT=$(source_fingerprint)
LOCK_HASH=$(hash_file "$SOURCE_LOCK")

if [[ "$VERIFY_ONLY" == 1 ]]; then
  [[ -f "$PROVENANCE" && -f "$LIBRARY" && -x "$CLAUDE_HELPER" ]] || fail "Prepared runtime outputs are incomplete; run prepare-runtime.sh again."
  jq -e --arg mode "$MODE" --arg revision "$SOURCE_REVISION" \
    --arg fingerprint "$SOURCE_FINGERPRINT" --arg lockHash "$LOCK_HASH" \
    --arg root "$RUNTIME_ROOT" --arg arches "$ARCH_TEXT" --arg library "$LIBRARY" --arg helper "$CLAUDE_HELPER" \
    --arg libraryHash "$(hash_file "$LIBRARY")" --arg helperHash "$(hash_file "$CLAUDE_HELPER")" '
      .version == 1 and .mode == $mode and .source.revision == $revision and
      .source.root == $root and .source.fingerprint == $fingerprint and .source.lockSHA256 == $lockHash and
      (.architectures | join(" ")) == $arches and
      .artifacts.library.path == $library and .artifacts.library.sha256 == $libraryHash and
      .artifacts.claudeHelper.path == $helper and .artifacts.claudeHelper.sha256 == $helperHash
    ' "$PROVENANCE" >/dev/null || fail "Runtime provenance mismatch: source, lock, mode, architectures, or compiled artifacts changed. Run prepare-runtime.sh again."
  lipo "$LIBRARY" -verify_arch "${ARCH_LIST[@]}"
  lipo "$CLAUDE_HELPER" -verify_arch "${ARCH_LIST[@]}"
  verify_deployment_target "$LIBRARY"
  verify_deployment_target "$CLAUDE_HELPER"
  printf 'Verified %s runtime %s (%s)\n' "$MODE" "$SOURCE_REVISION" "$ARCH_TEXT"
  exit 0
fi

[[ "$(uname -s)" == Darwin ]] || fail "The native runtime must be prepared on macOS"
for tool in cargo rustc bun xcrun lipo otool; do command -v "$tool" >/dev/null || fail "Missing required build tool: $tool"; done
RUNTIME_MANIFEST="$RUNTIME_ROOT/packages/sq-acp/rust/sq-acp-ffi-runtime/Cargo.toml"
HELPER_ROOT="$RUNTIME_ROOT/packages/sq-acp/Tools/ClaudeAgentSDKHost"
[[ -f "$RUNTIME_MANIFEST" && -f "${RUNTIME_MANIFEST%/*}/Cargo.lock" && -f "$HELPER_ROOT/bun.lock" ]] || fail "The selected source lacks locked runtime/helper inputs"
export MACOSX_DEPLOYMENT_TARGET="$DEPLOYMENT_TARGET"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$OUTPUT/cargo}
mkdir -p "$CARGO_TARGET_DIR"
CARGO_TARGET_DIR=$(cd "$CARGO_TARGET_DIR" && pwd -P)
CARGO=(cargo)
uses_configured_wrapper() {
  [[ -z "${RUSTC_WRAPPER:-}${CARGO_BUILD_RUSTC_WRAPPER:-}" ]] || return 0
  local directory="$RUNTIME_ROOT" config
  while :; do
    for config in "$directory/.cargo/config" "$directory/.cargo/config.toml"; do
      if [[ -f "$config" ]] && grep -Eq '^[[:space:]]*rustc-wrapper[[:space:]]*=' "$config"; then return 0; fi
    done
    [[ "$directory" != / ]] || break
    directory=$(dirname "$directory")
  done
  for config in "${CARGO_HOME:-$HOME/.cargo}/config" "${CARGO_HOME:-$HOME/.cargo}/config.toml"; do
    if [[ -f "$config" ]] && grep -Eq '^[[:space:]]*rustc-wrapper[[:space:]]*=' "$config"; then return 0; fi
  done
  return 1
}
if uses_configured_wrapper; then
  printf 'Keeping the configured Rust compiler wrapper; invoking cargo directly.\n' >&2
elif command -v mbx >/dev/null 2>&1; then
  CARGO=(mbx)
else
  printf 'mbx unavailable; using uncached cargo builds.\n' >&2
fi
RUSTC_COMMAND=(rustc)
if [[ -n "${RUSTUP_TOOLCHAIN:-}" ]]; then
  command -v rustup >/dev/null || fail "RUSTUP_TOOLCHAIN requires an installed rustup"
  TOOLCHAIN_CARGO=$(rustup which --toolchain "$RUSTUP_TOOLCHAIN" cargo)
  TOOLCHAIN_RUSTC=$(rustup which --toolchain "$RUSTUP_TOOLCHAIN" rustc)
  [[ -x "$TOOLCHAIN_CARGO" && -x "$TOOLCHAIN_RUSTC" ]] || fail "Selected Rust toolchain executables are unavailable"
  # mbx launches Cargo through PATH. A +toolchain argument only works with the
  # rustup proxy, and Homebrew Cargo may precede that proxy on the host PATH.
  # Resolve actual installed binaries and scope selection to this process.
  export PATH="$(dirname "$TOOLCHAIN_CARGO"):$PATH"
  export RUSTC="$TOOLCHAIN_RUSTC"
  RUSTC_COMMAND=("$TOOLCHAIN_RUSTC")
  if [[ "${CARGO[0]}" != mbx ]]; then CARGO=("$TOOLCHAIN_CARGO"); fi
fi
mkdir -p "$OUTPUT/slices"
LIBRARY_SLICES=()
HELPER_SLICES=()
(cd "$HELPER_ROOT" && bun install --frozen-lockfile)
for arch in "${ARCH_LIST[@]}"; do
  case "$arch" in
    arm64) rust_target=aarch64-apple-darwin; bun_target=bun-darwin-arm64 ;;
    x86_64) rust_target=x86_64-apple-darwin; bun_target=bun-darwin-x64 ;;
  esac
  if [[ "${CARGO[0]}" == mbx && ( "${MBX_VERIFY:-0}" == 1 || "${MBX_VERIFY:-0}" == true ) ]]; then
    # mbx returns the compiler status even when shadow verification diverges.
    # A unique report prevents an earlier successful run from qualifying this
    # invocation. Keep each architecture's report for diagnosis.
    verification_report=$(mktemp "$OUTPUT/slices/mbx-verification-$arch.XXXXXX")
    printf 'Boxington verification report: %s\n' "$verification_report"
    (cd "$RUNTIME_ROOT" && MBX_STATS_REPORT="$verification_report" \
      "${CARGO[@]}" build --locked --manifest-path "$RUNTIME_MANIFEST" --lib --release --target "$rust_target")
    if [[ -n "${MBX_STATS_REPORT:-}" && -s "$verification_report" ]]; then
      # Preserve the caller's requested report as well. As with sequential mbx
      # invocations, it contains the most recently completed architecture.
      requested_report=$MBX_STATS_REPORT
      [[ "$requested_report" == /* ]] || requested_report="$RUNTIME_ROOT/$requested_report"
      cp "$verification_report" "$requested_report"
    fi
    jq -e '.verifications | type == "number"' "$verification_report" >/dev/null 2>&1 &&
      jq -e '.divergences == 0' "$verification_report" >/dev/null 2>&1 ||
      fail "Boxington verification failed or its report is missing/invalid: $verification_report. Runtime outputs are not qualified."
  else
    (cd "$RUNTIME_ROOT" && "${CARGO[@]}" build --locked --manifest-path "$RUNTIME_MANIFEST" --lib --release --target "$rust_target")
  fi
  library_slice="$CARGO_TARGET_DIR/$rust_target/release/libsq_acp_runtime.a"
  helper_slice="$OUTPUT/slices/claude-agent-sdk-host-$arch"
  (cd "$HELPER_ROOT" && bun build --compile --minify --target="$bun_target" ./src/index.ts --outfile "$helper_slice")
  LIBRARY_SLICES+=("$library_slice")
  HELPER_SLICES+=("$helper_slice")
done
if [[ ${#ARCH_LIST[@]} == 1 ]]; then
  cp "${LIBRARY_SLICES[0]}" "$LIBRARY"
  cp "${HELPER_SLICES[0]}" "$CLAUDE_HELPER"
else
  lipo -create "${LIBRARY_SLICES[@]}" -output "$LIBRARY"
  lipo -create "${HELPER_SLICES[@]}" -output "$CLAUDE_HELPER"
fi
chmod u+x "$CLAUDE_HELPER"
[[ "$SOURCE_FINGERPRINT" == "$(source_fingerprint)" && "$SOURCE_REVISION" == "$(git -C "$RUNTIME_ROOT" rev-parse HEAD)" &&
   "$LOCK_HASH" == "$(hash_file "$SOURCE_LOCK")" ]] || fail "Runtime source changed during compilation; outputs are not qualified. Prepare again once edits are complete."
lipo "$LIBRARY" -verify_arch "${ARCH_LIST[@]}"
lipo "$CLAUDE_HELPER" -verify_arch "${ARCH_LIST[@]}"
verify_deployment_target "$LIBRARY"
verify_deployment_target "$CLAUDE_HELPER"
temporary=$(mktemp "$OUTPUT/provenance.XXXXXX")
trap 'rm -f "$temporary"' EXIT
jq -n --arg mode "$MODE" --arg repository "$LOCK_REPOSITORY" --arg revision "$SOURCE_REVISION" \
  --arg pinnedRevision "$LOCK_REVISION" --arg root "$RUNTIME_ROOT" --arg fingerprint "$SOURCE_FINGERPRINT" \
  --arg lockHash "$LOCK_HASH" --argjson dirty "$SOURCE_DIRTY" --arg arches "$ARCH_TEXT" \
  --arg library "$LIBRARY" --arg libraryHash "$(hash_file "$LIBRARY")" \
  --arg helper "$CLAUDE_HELPER" --arg helperHash "$(hash_file "$CLAUDE_HELPER")" \
  --arg cargoLockHash "$(hash_file "${RUNTIME_MANIFEST%/*}/Cargo.lock")" --arg bunLockHash "$(hash_file "$HELPER_ROOT/bun.lock")" \
  --arg rustc "$("${RUSTC_COMMAND[@]}" --version)" --arg bun "$(bun --version)" --arg sdk "$(xcrun --sdk macosx --show-sdk-version)" \
  --arg cargoDriver "$(basename "${CARGO[0]}")" \
  --arg deploymentTarget "$MACOSX_DEPLOYMENT_TARGET" --arg builtAt "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" '
  {version: 1, mode: $mode, builtAt: $builtAt, architectures: ($arches | split(" ")),
   source: {repository: $repository, revision: $revision, pinnedRevision: $pinnedRevision, root: $root,
            fingerprint: $fingerprint, dirty: $dirty, lockSHA256: $lockHash},
   inputs: {cargoLockSHA256: $cargoLockHash, bunLockSHA256: $bunLockHash},
   toolchain: {rustc: $rustc, cargoDriver: $cargoDriver, bun: $bun, macOSSDK: $sdk, deploymentTarget: $deploymentTarget},
   artifacts: {library: {path: $library, sha256: $libraryHash}, claudeHelper: {path: $helper, sha256: $helperHash}}}
' > "$temporary"
mv "$temporary" "$PROVENANCE"
printf 'Prepared %s runtime %s (%s)\nProvenance: %s\n' "$MODE" "$SOURCE_REVISION" "$ARCH_TEXT" "$PROVENANCE"
