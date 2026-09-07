#!/usr/bin/env bash
# Adapted from the macos-spm-app-packaging SwiftPM bundle template.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
CONFIGURATION=${1:-release}
source "$ROOT/version.env"
APP="$ROOT/build/Memex.app"
CLI=${MEMEX_CLI:-$(command -v memex || true)}
if [[ ! -f "$CLI" || ! -x "$CLI" ]]; then
  echo "Set MEMEX_CLI to an executable memex CLI, or install memex on PATH." >&2
  exit 1
fi
CLI=$(cd "$(dirname "$CLI")" && pwd)/$(basename "$CLI")
"$CLI" --version
if ! "$CLI" projects --help >/dev/null 2>&1 || ! "$CLI" machines --help >/dev/null 2>&1; then
  echo "This app requires a Memex CLI with projects and machines commands. Build this checkout's CLI and set MEMEX_CLI to it." >&2
  exit 1
fi
swift build --package-path "$ROOT" -c "$CONFIGURATION" --product Memex
BIN_DIR=$(swift build --package-path "$ROOT" -c "$CONFIGURATION" --show-bin-path)

# This directory contains only generated bundle output.
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Helpers" \
  "$APP/Contents/Resources" "$APP/Contents/Frameworks"
cp "$BIN_DIR/Memex" "$APP/Contents/MacOS/Memex"
cp "$CLI" "$APP/Contents/Helpers/memex"
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
  done < <(otool -L "$binary" | awk 'NR>1 {sub(/^[[:space:]]+/, ""); sub(/ \(compatibility.*$/, ""); print}')
}
verify_dependencies "$CLI"
verify_dependencies "$BIN_DIR/Memex"

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleName</key><string>Memex</string>
<key>CFBundleDisplayName</key><string>Memex</string>
<key>CFBundleIdentifier</key><string>dev.memex.app</string>
<key>CFBundleExecutable</key><string>Memex</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>CFBundleShortVersionString</key><string>${MARKETING_VERSION}</string>
<key>CFBundleVersion</key><string>${BUILD_NUMBER}</string>
<key>LSMinimumSystemVersion</key><string>14.0</string>
<key>NSHighResolutionCapable</key><true/>
</dict></plist>
PLIST
plutil -lint "$APP/Contents/Info.plist"
xattr -cr "$APP"
codesign --force --sign - "$APP/Contents/Helpers/memex"
codesign --force --sign - "$APP"
codesign --verify --deep --strict --verbose=2 "$APP"
"$APP/Contents/Helpers/memex" --version
echo "Built $APP"
