#!/usr/bin/env bash
# Adapted from the macos-spm-app-packaging SwiftPM bundle template.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
CONFIGURATION=${1:-release}
REPO=$(cd "$ROOT/../.." && pwd)
# The Rust package owns the release version. Keep local and release bundles aligned.
MARKETING_VERSION=$(sed -n '/^\[package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' "$REPO/Cargo.toml")
BUILD_NUMBER=$(git -C "$REPO" rev-list --count HEAD)
SIGNING_MODE=${SIGNING_MODE:-adhoc}
case "$SIGNING_MODE" in
  adhoc) SIGN_ARGS=(--force --sign -) ;;
  developer-id)
    : "${CODESIGN_IDENTITY:?Set CODESIGN_IDENTITY to a local Developer ID Application identity}"
    SIGN_ARGS=(--force --options runtime --timestamp --sign "$CODESIGN_IDENTITY")
    ;;
  *) echo "Unknown SIGNING_MODE: $SIGNING_MODE" >&2; exit 1 ;;
esac
read -r -a ARCH_LIST <<< "${ARCHES:-$(uname -m)}"
SDK_PATH=$(xcrun --sdk macosx --show-sdk-path)
# Swift 6.4's Clang linker invocation can record the deployment target as the
# SDK version. Pass Clang's Darwin SDK option explicitly so AppKit uses the
# current SDK's toolbar/sidebar layout instead of legacy compatibility drawing.
SWIFT_ARGS=(--sdk "$SDK_PATH"
  -Xswiftc -Xclang-linker -Xswiftc -isysroot
  -Xswiftc -Xclang-linker -Xswiftc "$SDK_PATH")
for arch in "${ARCH_LIST[@]}"; do
  case "$arch" in arm64|x86_64) ;; *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; esac
  SWIFT_ARGS+=(--arch "$arch")
done
APP="$ROOT/build/Memex.app"
RUNTIME_ROOT=${MEMEX_AGENT_RUNTIME_ROOT:-}
if [[ "${MEMEX_HISTORY_ONLY:-0}" == 1 ]]; then
  RUNTIME_ROOT=""
  export MEMEX_AGENT_RUNTIME_ROOT=""
elif [[ -z "$RUNTIME_ROOT" && -f "$ROOT/.local-runtime-root" ]]; then
  RUNTIME_ROOT=$(cat "$ROOT/.local-runtime-root")
fi
CLAUDE_HELPER=""
RUNTIME_PROVENANCE=""
RUNTIME_VERIFY_ARGS=()
if [[ -n "$RUNTIME_ROOT" ]]; then
  RUNTIME_ROOT=$(cd "$RUNTIME_ROOT" && pwd -P)
  export MEMEX_AGENT_RUNTIME_ROOT="$RUNTIME_ROOT"
  if [[ "${MEMEX_RUNTIME_DEVELOPMENT:-0}" == 1 && -n "${MEMEX_RUNTIME_CANDIDATE_REVISION:-}" ]]; then
    echo "Choose development or candidate runtime mode, not both." >&2
    exit 1
  fi
  if [[ "$SIGNING_MODE" == developer-id && ( "${MEMEX_RUNTIME_DEVELOPMENT:-0}" == 1 || -n "${MEMEX_RUNTIME_CANDIDATE_REVISION:-}" ) ]]; then
    echo "Developer ID packaging requires the clean, pinned runtime; development/candidate artifacts cannot be released." >&2
    exit 1
  fi
  ARCH_KEY=$(printf '%s\n' "${ARCH_LIST[@]}" | sort -u | paste -sd '-' -)
  RUNTIME_OUTPUT=${MEMEX_RUNTIME_OUTPUT:-$ROOT/.build/runtime/$ARCH_KEY}
  RUNTIME_VERIFY_ARGS=(--source "$RUNTIME_ROOT" --output "$RUNTIME_OUTPUT" --verify)
  if [[ "${MEMEX_RUNTIME_DEVELOPMENT:-0}" == 1 ]]; then RUNTIME_VERIFY_ARGS+=(--development); fi
  if [[ -n "${MEMEX_RUNTIME_CANDIDATE_REVISION:-}" ]]; then
    RUNTIME_VERIFY_ARGS+=(--candidate-revision "$MEMEX_RUNTIME_CANDIDATE_REVISION")
  fi
  ARCHES="${ARCH_LIST[*]}" bash "$ROOT/scripts/prepare-runtime.sh" "${RUNTIME_VERIFY_ARGS[@]}"
  RUNTIME_PROVENANCE="$RUNTIME_OUTPUT/runtime-provenance.json"
  RUNTIME_LIBRARY=$(jq -er '.artifacts.library.path' "$RUNTIME_PROVENANCE")
  CLAUDE_HELPER=$(jq -er '.artifacts.claudeHelper.path' "$RUNTIME_PROVENANCE")
  if [[ -n "${MEMEX_AGENT_RUNTIME_LIBRARY:-}" && "$MEMEX_AGENT_RUNTIME_LIBRARY" != "$RUNTIME_LIBRARY" ]] ||
     [[ -n "${MEMEX_CLAUDE_HELPER:-}" && "$MEMEX_CLAUDE_HELPER" != "$CLAUDE_HELPER" ]]; then
    echo "Runtime artifact overrides must match the verified prepared outputs. Unset old overrides or prepare into MEMEX_RUNTIME_OUTPUT." >&2
    exit 1
  fi
  export MEMEX_AGENT_RUNTIME_LIBRARY="$RUNTIME_LIBRARY"
fi
CLI=${MEMEX_CLI:-$(command -v memex || true)}
if [[ ! -f "$CLI" || ! -x "$CLI" ]]; then
  echo "Set MEMEX_CLI to an executable memex CLI, or install memex on PATH." >&2
  exit 1
fi
CLI=$(cd "$(dirname "$CLI")" && pwd)/$(basename "$CLI")
"$CLI" --version
if ! "$CLI" projects --help >/dev/null 2>&1 || ! "$CLI" machines --help >/dev/null 2>&1 || ! "$CLI" activity --help >/dev/null 2>&1; then
  echo "This app requires a Memex CLI with projects, machines, and activity commands. Build this checkout's CLI and set MEMEX_CLI to it." >&2
  exit 1
fi
swift build --package-path "$ROOT" -c "$CONFIGURATION" "${SWIFT_ARGS[@]}" --product Memex --force-resolved-versions
swift build --package-path "$ROOT" -c "$CONFIGURATION" "${SWIFT_ARGS[@]}" --product MemexExecutionHost --force-resolved-versions
BIN_DIR=$(swift build --package-path "$ROOT" -c "$CONFIGURATION" "${SWIFT_ARGS[@]}" --show-bin-path)
if [[ -n "$RUNTIME_PROVENANCE" ]]; then
  # Swift source is part of the pinned runtime too. Do not package a build
  # assembled while another process edited that checkout or replaced artifacts.
  ARCHES="${ARCH_LIST[*]}" bash "$ROOT/scripts/prepare-runtime.sh" "${RUNTIME_VERIFY_ARGS[@]}"
fi

# This directory contains only generated bundle output.
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Helpers" \
  "$APP/Contents/Resources" "$APP/Contents/Frameworks"
cp "$BIN_DIR/Memex" "$APP/Contents/MacOS/Memex"
cp "$CLI" "$APP/Contents/Helpers/memex"
cp "$BIN_DIR/MemexExecutionHost" "$APP/Contents/Helpers/MemexExecutionHost"
chmod u+w "$APP/Contents/Helpers/MemexExecutionHost"
if [[ -n "$CLAUDE_HELPER" ]]; then
  cp "$CLAUDE_HELPER" "$APP/Contents/Helpers/claude-agent-sdk-host"
  chmod u+w "$APP/Contents/Helpers/claude-agent-sdk-host"
  lipo "$CLAUDE_HELPER" -verify_arch "${ARCH_LIST[@]}"
  # Keep build identity in the signed bundle without embedding workstation paths.
  jq 'del(.source.root, .artifacts.library.path, .artifacts.claudeHelper.path)' \
    "$RUNTIME_PROVENANCE" > "$APP/Contents/Resources/runtime-provenance.json"
fi
cp "$ROOT/bundle/Memex.icns" "$APP/Contents/Resources/Memex.icns"
cp "$ROOT/bundle/TerminalNotices.txt" "$APP/Contents/Resources/TerminalNotices.txt"
chmod u+w "$APP/Contents/Helpers/memex"
shopt -s nullglob
for resource in "$BIN_DIR/"*.bundle; do
  cp -R "$resource" "$APP/Contents/Resources/"
done

# The installed CLI is self-contained apart from Apple's system libraries.
# Fail explicitly for custom builds that would rely on unbundled dependencies.
verify_dependencies() {
  local binary="$1" dependency
  while IFS= read -r dependency; do
    case "$dependency" in
      /usr/lib/*|/System/Library/*) ;;
      *)
        echo "Unsupported dependency $dependency in $binary. Use a CLI linked only to system libraries." >&2
        exit 1
        ;;
    esac
  done < <(otool -L "$binary" | awk '/^\t/ {sub(/^[[:space:]]+/, ""); sub(/ \(compatibility.*$/, ""); print}')
}
verify_dependencies "$CLI"
verify_dependencies "$BIN_DIR/Memex"
verify_dependencies "$BIN_DIR/MemexExecutionHost"
if [[ -n "$CLAUDE_HELPER" ]]; then verify_dependencies "$CLAUDE_HELPER"; fi
for binary in "$APP/Contents/MacOS/Memex" "$APP/Contents/Helpers/memex" "$APP/Contents/Helpers/MemexExecutionHost"; do
  lipo "$binary" -verify_arch "${ARCH_LIST[@]}"
done

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleName</key><string>Memex</string>
<key>CFBundleDisplayName</key><string>Memex</string>
<key>CFBundleIdentifier</key><string>dev.memex.app</string>
<key>CFBundleExecutable</key><string>Memex</string>
<key>CFBundleIconFile</key><string>Memex</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>CFBundleShortVersionString</key><string>${MARKETING_VERSION}</string>
<key>CFBundleVersion</key><string>${BUILD_NUMBER}</string>
<key>LSMinimumSystemVersion</key><string>14.0</string>
<key>NSHighResolutionCapable</key><true/>
<key>NSAppleEventsUsageDescription</key><string>Memex opens your selected terminal to resume the conversation you choose.</string>
<key>NSMicrophoneUsageDescription</key><string>Memex records audio when you start dictation and keeps it locally for transcription and recovery.</string>
<key>NSSpeechRecognitionUsageDescription</key><string>Memex transcribes your dictation on this Mac so you can review and insert it into a draft.</string>
</dict></plist>
PLIST
plutil -lint "$APP/Contents/Info.plist"
xattr -cr "$APP"
codesign "${SIGN_ARGS[@]}" "$APP/Contents/Helpers/memex"
codesign "${SIGN_ARGS[@]}" --entitlements "$ROOT/bundle/Release.entitlements" "$APP/Contents/Helpers/MemexExecutionHost"
if [[ -n "$CLAUDE_HELPER" ]]; then
  codesign "${SIGN_ARGS[@]}" --entitlements "$ROOT/bundle/Release.entitlements" "$APP/Contents/Helpers/claude-agent-sdk-host"
  # Signing changes Mach-O bytes. Preserve both the qualified build hashes and
  # the hashes of the signed helpers users will actually run.
  provenance="$APP/Contents/Resources/runtime-provenance.json"
  jq --arg executionHost "$(shasum -a 256 "$APP/Contents/Helpers/MemexExecutionHost" | awk '{print $1}')" \
    --arg claudeHelper "$(shasum -a 256 "$APP/Contents/Helpers/claude-agent-sdk-host" | awk '{print $1}')" \
    '.bundledHelpers = {executionHostSHA256: $executionHost, claudeHelperSHA256: $claudeHelper}' \
    "$provenance" > "$provenance.tmp"
  mv "$provenance.tmp" "$provenance"
fi
codesign "${SIGN_ARGS[@]}" --entitlements "$ROOT/bundle/Release.entitlements" "$APP"
codesign --verify --deep --strict --verbose=2 "$APP"
"$APP/Contents/Helpers/memex" --version
echo "Built $APP"
