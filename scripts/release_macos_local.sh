#!/usr/bin/env bash
# Publish only the desktop app; the existing CI workflow still owns CLI archives.
set -euo pipefail
if [[ "${1:-}" == --help ]]; then
  echo "Usage: scripts/release_macos_local.sh [--publish-only] VERSION"
  echo "Build, sign, notarize, upload the Apple Silicon Memex.app, and update the Homebrew cask from a clean vVERSION tag."
  echo "Requires local CODESIGN_IDENTITY and NOTARY_PROFILE (default: sidequery-notarization)."
  echo "Public Homebrew releases are always history-only; the private agent runtime is never bundled."
  exit 0
fi
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
publish_only=false
if [[ "${1:-}" == --publish-only ]]; then publish_only=true; shift; fi
version=${1:?Usage: scripts/release_macos_local.sh VERSION}
# The cask is public. Never let a configured local runtime checkout leak into it.
if [[ -n "${MEMEX_RELEASE_RUNTIME:-}" && "$MEMEX_RELEASE_RUNTIME" != history-only ]]; then
  echo "Public Homebrew releases are history-only; unset MEMEX_RELEASE_RUNTIME=$MEMEX_RELEASE_RUNTIME." >&2
  exit 1
fi
runtime_mode=history-only
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Expected a stable semantic version" >&2; exit 1; }
tag="v$version"
repo=nicosuave/memex
manifest=$(sed -n '/^\[package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' Cargo.toml)
[[ "$manifest" == "$version" ]] || { echo "Cargo.toml is $manifest, requested $version" >&2; exit 1; }
assert_clean() {
  [[ -z "$(git status --porcelain --untracked-files=normal)" ]] || { echo "Release requires a clean checkout" >&2; exit 1; }
}
assert_clean
commit=$(git rev-parse HEAD)
[[ "$commit" == "$(git rev-parse "$tag^{commit}")" ]] || { echo "HEAD must be tagged $tag" >&2; exit 1; }
remote=$(git ls-remote "https://github.com/$repo.git" "refs/tags/$tag" "refs/tags/$tag^{}" |
  awk '/\^\{\}$/ {peeled=$1} !/\^\{\}$/ {direct=$1} END {print peeled ? peeled : direct}')
[[ "$commit" == "$remote" ]] || { echo "Published tag $tag does not match HEAD" >&2; exit 1; }
gh release view "$tag" --repo "$repo" --json isDraft,isPrerelease |
  jq -e '.isDraft == false and .isPrerelease == false' >/dev/null

artifact="memex-app-${version}-macos-arm64.zip"
artifacts="$ROOT/apps/macos/build/releases/$tag"
assets=$(gh release view "$tag" --repo "$repo" --json assets --jq '.assets[].name')
if ! "$publish_only" && grep -Fxq -e "$artifact" -e "$artifact.sha256" <<< "$assets"; then
  echo "App assets already exist. Retry the saved artifacts with: scripts/release_macos_local.sh --publish-only $version" >&2
  exit 1
fi
if ! "$publish_only"; then
  : "${CODESIGN_IDENTITY:?Set CODESIGN_IDENTITY to your local Developer ID Application identity}"
  NOTARY_PROFILE=${NOTARY_PROFILE:-sidequery-notarization}
  xcrun notarytool history --keychain-profile "$NOTARY_PROFILE" --output-format json >/dev/null
  SIGNING_MODE=developer-id MEMEX_RELEASE_RUNTIME="$runtime_mode" "$ROOT/apps/macos/scripts/build-release.sh"
  app="$ROOT/apps/macos/build/Memex.app"
  bash "$ROOT/apps/macos/scripts/verify-runtime-mode.sh" "$app" "$runtime_mode"
  [[ "$("$app/Contents/Helpers/memex" --version)" == "memex $version" ]]
  [[ "$(plutil -extract CFBundleShortVersionString raw -o - "$app/Contents/Info.plist")" == "$version" ]]
  codesign --verify --deep --strict "$app"

  mkdir -p "$artifacts"
  submission="$artifacts/notarization.zip"
  ditto --norsrc -c -k --keepParent "$app" "$submission"
  xcrun notarytool submit "$submission" --keychain-profile "$NOTARY_PROFILE" \
    --wait --output-format json > "$artifacts/notarization.json"
  if ! jq -e '.status == "Accepted"' "$artifacts/notarization.json" >/dev/null; then
    echo "Notarization was not accepted; inspect $artifacts/notarization.json and retrieve its notarytool log." >&2
    exit 1
  fi
  xcrun stapler staple "$app"
  xcrun stapler validate "$app"
  spctl --assess --type execute --verbose=2 "$app"
  ditto --norsrc -c -k --keepParent "$app" "$artifacts/$artifact"
  (cd "$artifacts" && shasum -a 256 "$artifact" > "$artifact.sha256")
  printf '%s\n' "$commit" > "$artifacts/commit.txt"
  printf '%s\n' "$runtime_mode" > "$artifacts/runtime-mode.txt"
fi

# Validate the saved distribution itself, including on upload-only retries.
assert_clean
[[ "$(git rev-parse HEAD)" == "$commit" ]] || { echo "HEAD changed during the build" >&2; exit 1; }
[[ "$(cat "$artifacts/commit.txt")" == "$commit" ]]
[[ "$(cat "$artifacts/runtime-mode.txt" 2>/dev/null)" == "$runtime_mode" ]] ||
  { echo "Saved artifacts do not record runtime mode $runtime_mode; rebuild them." >&2; exit 1; }
(cd "$artifacts" && shasum -a 256 -c "$artifact.sha256")
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
ditto -x -k "$artifacts/$artifact" "$scratch"
app="$scratch/Memex.app"
codesign --verify --deep --strict "$app"
xcrun stapler validate "$app"
spctl --assess --type execute --verbose=2 "$app"
lipo "$app/Contents/MacOS/Memex" -verify_arch arm64
lipo "$app/Contents/Helpers/memex" -verify_arch arm64
bash "$ROOT/apps/macos/scripts/verify-runtime-mode.sh" "$app" "$runtime_mode"
[[ "$(plutil -extract CFBundleShortVersionString raw -o - "$app/Contents/Info.plist")" == "$version" ]]
[[ "$("$app/Contents/Helpers/memex" --version)" == "memex $version" ]]
gh release view "$tag" --repo "$repo" --json assets --jq '.assets[].name' > "$scratch/assets"
mkdir "$scratch/remote"
for file in "$artifact" "$artifact.sha256"; do
  if grep -Fxq "$file" "$scratch/assets"; then
    gh release download "$tag" --repo "$repo" --pattern "$file" --dir "$scratch/remote"
    cmp "$artifacts/$file" "$scratch/remote/$file" || { echo "Published $file differs; refusing to overwrite" >&2; exit 1; }
  fi
done
for file in "$artifact" "$artifact.sha256"; do
  if ! grep -Fxq "$file" "$scratch/assets"; then
    gh release upload "$tag" --repo "$repo" "$artifacts/$file"
  fi
done
"$ROOT/apps/macos/scripts/publish-cask.sh" "$version"
echo "Published signed and notarized Memex.app $version"
