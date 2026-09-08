#!/usr/bin/env bash
# Contract tests for local release preflight and cask publishing; no Apple/GitHub writes.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/bin" "$scratch/repo/scripts"
cp "$ROOT/scripts/release_macos_local.sh" "$scratch/repo/scripts/"
cat > "$scratch/repo/Cargo.toml" <<'EOF'
[package]
name = "memex"
version = "1.2.3"
EOF
git -C "$scratch/repo" init -q
git -C "$scratch/repo" add .
git -C "$scratch/repo" -c user.name=Test -c user.email=test@example.invalid commit -qm fixture
git -C "$scratch/repo" tag v1.2.3
export TEST_COMMIT
TEST_COMMIT=$(git -C "$scratch/repo" rev-parse HEAD)
export REAL_GIT
REAL_GIT=$(command -v git)
export TEST_SCRATCH="$scratch"
cat > "$scratch/bin/git" <<'EOF'
#!/usr/bin/env bash
if [[ "$1" == ls-remote ]]; then
  printf '%s\trefs/tags/v1.2.3\n' "${REMOTE_COMMIT:-$TEST_COMMIT}"
else
  exec "$REAL_GIT" "$@"
fi
EOF
cat > "$scratch/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1 $2" == 'release view' ]]; then
  if [[ "$*" == *'--json assets'* ]]; then
    if [[ "${UPLOAD_TEST:-no}" == yes ]]; then
      for file in "$TEST_SCRATCH/remote/"*; do [[ ! -f "$file" ]] || basename "$file"; done
    else
      [[ "${ASSET_EXISTS:-no}" != yes ]] || echo memex-app-1.2.3-macos-universal.zip
    fi
  else
    echo '{"isDraft":false,"isPrerelease":false}'
  fi
elif [[ "$1 $2" == 'release download' ]]; then
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == --dir ]]; then dest=$2; shift; fi
    if [[ "$1" == --pattern ]]; then pattern=$2; shift; fi
    shift
  done
  if [[ "${UPLOAD_TEST:-no}" == yes ]]; then
    cp "$TEST_SCRATCH/remote/$pattern" "$dest/"
  else
    cp "$TEST_SCRATCH/asset.zip" "$dest/memex-app-1.2.3-macos-universal.zip"
    cp "$TEST_SCRATCH/asset.sha256" "$dest/memex-app-1.2.3-macos-universal.zip.sha256"
  fi
elif [[ "$1 $2" == 'release upload' ]]; then
  file=${!#}
  if [[ "${FAIL_UPLOAD_CHECKSUM:-no}" == yes && "$file" == *.sha256 ]]; then
    echo 'Simulated checksum upload failure' >&2; exit 1
  fi
  cp "$file" "$TEST_SCRATCH/remote/"
  echo "$file" >> "$TEST_SCRATCH/uploads"
elif [[ "$1" == api ]]; then
  case "$*" in
    *'--method PUT'*)
      while [[ $# -gt 0 ]]; do
        if [[ "$1" == --input ]]; then cp "$2" "$TEST_SCRATCH/put.json"; break; fi
        shift
      done
      ;;
    *'/git/trees/'*)
      [[ "${TREE_FAIL:-no}" != yes ]] || exit 1
      if [[ -f "$TEST_SCRATCH/current.rb" ]]; then
        echo '{"truncated":false,"tree":[{"path":"Casks/memex-app.rb","sha":"old-sha"}]}'
      else
        echo '{"truncated":false,"tree":[]}'
      fi
      ;;
    *'/git/blobs/'*) base64 < "$TEST_SCRATCH/current.rb" ;;
    *) echo main ;;
  esac
else
  echo "Unexpected gh invocation: $*" >&2
  exit 1
fi
EOF
chmod +x "$scratch/bin/"*
export PATH="$scratch/bin:$PATH"

expect_failure() {
  local message=$1
  shift
  if "$@" > "$scratch/output" 2>&1; then
    echo "Unexpected success: $*" >&2; exit 1
  fi
  [[ -z "$message" ]] || grep -Fq "$message" "$scratch/output" || { cat "$scratch/output"; exit 1; }
}
release="$scratch/repo/scripts/release_macos_local.sh"
expect_failure 'stable semantic version' "$release" '../bad'
expect_failure 'Cargo.toml is 1.2.3' "$release" 1.2.4
touch "$scratch/repo/untracked"
expect_failure 'clean checkout' "$release" 1.2.3
rm "$scratch/repo/untracked"
REMOTE_COMMIT=wrong expect_failure 'does not match HEAD' "$release" 1.2.3
ASSET_EXISTS=yes expect_failure 'assets already exist' "$release" 1.2.3
expect_failure 'Set CODESIGN_IDENTITY' env -u CODESIGN_IDENTITY "$release" 1.2.3

renderer="$ROOT/apps/macos/scripts/render-cask.sh"
publisher="$ROOT/apps/macos/scripts/publish-cask.sh"
printf 'archive fixture\n' > "$scratch/asset.zip"
checksum=$(shasum -a 256 "$scratch/asset.zip" | awk '{print $1}')
printf '%s  memex-app-1.2.3-macos-universal.zip\n' "$checksum" > "$scratch/asset.sha256"
expect_failure 'SHA256 checksum' "$renderer" 1.2.3 garbage
expect_failure 'stable semantic version' "$renderer" '1.2.3";bad' "$checksum"
"$publisher" 1.2.3
jq -e '.branch == "main" and (has("sha") | not)' "$scratch/put.json" >/dev/null
jq -r .content "$scratch/put.json" | base64 --decode > "$scratch/published.rb"
"$renderer" 1.2.3 "$checksum" > "$scratch/expected.rb"
cmp "$scratch/published.rb" "$scratch/expected.rb"
rm "$scratch/put.json"
cp "$scratch/expected.rb" "$scratch/current.rb"
"$publisher" 1.2.3
[[ ! -f "$scratch/put.json" ]]
"$renderer" 1.2.4 "$checksum" > "$scratch/current.rb"
expect_failure 'Refusing to replace' "$publisher" 1.2.3
[[ ! -f "$scratch/put.json" ]]
"$renderer" 1.2.2 "$checksum" > "$scratch/current.rb"
"$publisher" 1.2.3
jq -e '.sha == "old-sha"' "$scratch/put.json" >/dev/null
rm "$scratch/put.json"
TREE_FAIL=yes expect_failure '' "$publisher" 1.2.3
[[ ! -f "$scratch/put.json" ]]
printf 'corruption\n' >> "$scratch/asset.zip"
expect_failure 'checksum mismatch' "$publisher" 1.2.3
[[ ! -f "$scratch/put.json" ]]

# Exercise partial-upload recovery through the actual release command. Only OS
# validation and external services are faked; control flow and file comparisons run.
mkdir -p "$scratch/repo/apps/macos/scripts" "$scratch/remote"
cat > "$scratch/repo/apps/macos/scripts/publish-cask.sh" <<'EOF'
#!/usr/bin/env bash
touch "$TEST_SCRATCH/cask-invoked"
EOF
chmod +x "$scratch/repo/apps/macos/scripts/publish-cask.sh"
git -C "$scratch/repo" add apps
git -C "$scratch/repo" -c user.name=Test -c user.email=test@example.invalid commit -qm publisher
git -C "$scratch/repo" tag -f v1.2.3 >/dev/null
TEST_COMMIT=$(git -C "$scratch/repo" rev-parse HEAD)
echo 'apps/macos/build/' >> "$scratch/repo/.git/info/exclude"
artifacts="$scratch/repo/apps/macos/build/releases/v1.2.3"
mkdir -p "$artifacts"
asset=memex-app-1.2.3-macos-universal.zip
cp "$scratch/asset.zip" "$artifacts/$asset"
(cd "$artifacts" && shasum -a 256 "$asset" > "$asset.sha256")
echo "$TEST_COMMIT" > "$artifacts/commit.txt"
cat > "$scratch/bin/ditto" <<'EOF'
#!/usr/bin/env bash
dest=${!#}
mkdir -p "$dest/Memex.app/Contents/Helpers"
printf '#!/usr/bin/env bash\necho "memex 1.2.3"\n' > "$dest/Memex.app/Contents/Helpers/memex"
chmod +x "$dest/Memex.app/Contents/Helpers/memex"
EOF
cat > "$scratch/bin/plutil" <<'EOF'
#!/usr/bin/env bash
echo 1.2.3
EOF
for tool in codesign lipo xcrun; do
  printf '#!/usr/bin/env bash\nexit 0\n' > "$scratch/bin/$tool"
done
cat > "$scratch/bin/spctl" <<'EOF'
#!/usr/bin/env bash
[[ "${GATEKEEPER_FAIL:-no}" != yes ]] || { echo 'Gatekeeper rejected app' >&2; exit 1; }
EOF
chmod +x "$scratch/bin/"*
export UPLOAD_TEST=yes
GATEKEEPER_FAIL=yes expect_failure 'Gatekeeper rejected' "$release" --publish-only 1.2.3
[[ ! -f "$scratch/uploads" ]]
FAIL_UPLOAD_CHECKSUM=yes expect_failure 'checksum upload failure' "$release" --publish-only 1.2.3
[[ -f "$scratch/remote/$asset" && ! -f "$scratch/remote/$asset.sha256" ]]
[[ ! -f "$scratch/cask-invoked" ]]
"$release" --publish-only 1.2.3
[[ -f "$scratch/remote/$asset.sha256" && -f "$scratch/cask-invoked" ]]
[[ $(wc -l < "$scratch/uploads" | tr -d ' ') == 2 ]]
"$release" --publish-only 1.2.3
[[ $(wc -l < "$scratch/uploads" | tr -d ' ') == 2 ]]
printf 'different' >> "$scratch/remote/$asset"
expect_failure 'refusing to overwrite' "$release" --publish-only 1.2.3
rm "$scratch/remote/$asset"
printf 'different' >> "$scratch/remote/$asset.sha256"
expect_failure 'refusing to overwrite' "$release" --publish-only 1.2.3
[[ ! -f "$scratch/remote/$asset" ]]

# A completed notarytool command can still report rejection in its JSON response.
# It must not result in a release upload or cask publication.
cat > "$scratch/repo/apps/macos/scripts/build-release.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$SIGNING_MODE" == developer-id ]]
root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$root/build/Memex.app/Contents/Helpers"
printf '#!/usr/bin/env bash\necho "memex 1.2.3"\n' > "$root/build/Memex.app/Contents/Helpers/memex"
chmod +x "$root/build/Memex.app/Contents/Helpers/memex"
EOF
chmod +x "$scratch/repo/apps/macos/scripts/build-release.sh"
git -C "$scratch/repo" add apps
git -C "$scratch/repo" -c user.name=Test -c user.email=test@example.invalid commit -qm builder
git -C "$scratch/repo" tag -f v1.2.3 >/dev/null
TEST_COMMIT=$(git -C "$scratch/repo" rev-parse HEAD)
cat > "$scratch/bin/ditto" <<'EOF'
#!/usr/bin/env bash
dest=${!#}
if [[ "$1" != -x ]]; then
  printf 'signed app fixture\n' > "$dest"
else
  mkdir -p "$dest/Memex.app/Contents/Helpers"
  printf '#!/usr/bin/env bash\necho "memex 1.2.3"\n' > "$dest/Memex.app/Contents/Helpers/memex"
  chmod +x "$dest/Memex.app/Contents/Helpers/memex"
fi
EOF
cat > "$scratch/bin/xcrun" <<'EOF'
#!/usr/bin/env bash
if [[ "$1 $2" == 'notarytool submit' ]]; then
  printf '{"status":"%s","id":"fixture"}\n' "${NOTARY_STATUS:-Accepted}"
fi
EOF
rm "$scratch/remote/$asset.sha256" "$scratch/uploads" "$scratch/cask-invoked"
export CODESIGN_IDENTITY='Developer ID Application: fixture'
NOTARY_STATUS=Invalid expect_failure 'Notarization was not accepted' "$release" 1.2.3
[[ ! -f "$scratch/uploads" && ! -f "$scratch/cask-invoked" ]]
"$release" 1.2.3
[[ -f "$scratch/cask-invoked" && $(wc -l < "$scratch/uploads" | tr -d ' ') == 2 ]]
echo "Release script contract tests passed"
