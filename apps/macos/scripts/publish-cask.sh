#!/usr/bin/env bash
# Also usable independently to retry cask publication after a successful release upload.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
version=${1:?Usage: publish-cask.sh VERSION}
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Expected a stable semantic version" >&2; exit 1; }
repo=nicosuave/memex
tap=nicosuave/homebrew-tap
artifact="memex-app-${version}-macos-universal.zip"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
gh release download "v$version" --repo "$repo" --dir "$scratch" \
  --pattern "$artifact" --pattern "$artifact.sha256"
checksum=$(awk 'NR == 1 {print $1}' "$scratch/$artifact.sha256")
"$ROOT/scripts/render-cask.sh" "$version" "$checksum" > "$scratch/memex-app.rb"
actual=$(shasum -a 256 "$scratch/$artifact" | awk '{print $1}')
[[ "$actual" == "$checksum" ]] || { echo "Published app checksum mismatch" >&2; exit 1; }

# Read the default branch explicitly, then update only this cask with optimistic locking.
branch=$(gh api "repos/$tap" --jq .default_branch)
gh api --method GET "repos/$tap/git/trees/$branch" -f recursive=1 > "$scratch/tree.json"
jq -e '.truncated == false' "$scratch/tree.json" >/dev/null
sha=$(jq -r '.tree[] | select(.path == "Casks/memex-app.rb") | .sha' "$scratch/tree.json")
if [[ -n "$sha" ]]; then
  gh api "repos/$tap/git/blobs/$sha" --jq .content | base64 --decode > "$scratch/current.rb"
  if cmp -s "$scratch/current.rb" "$scratch/memex-app.rb"; then
    echo "Homebrew cask already matches $version"
    exit 0
  fi
  current=$(sed -n 's/^  version "\([^"]*\)"/\1/p' "$scratch/current.rb")
  # Never let retrying an older release downgrade the shared cask.
  if ! awk -v old="$current" -v new="$version" 'BEGIN {
    if (split(old,a,".") != 3) exit 1;
    split(new,b,".");
    for (i=1;i<=3;i++) { if (b[i]+0>a[i]+0) exit 0; if (b[i]+0<a[i]+0) exit 1 }
  }'; then
    echo "Refusing to replace cask version $current with $version" >&2
    exit 1
  fi
fi
jq -n --arg message "Update Memex app to $version" --arg branch "$branch" \
  --arg content "$(base64 < "$scratch/memex-app.rb" | tr -d '\n')" --arg sha "$sha" \
  '{message: $message, branch: $branch, content: $content} + if $sha == "" then {} else {sha: $sha} end' \
  > "$scratch/request.json"
gh api --method PUT "repos/$tap/contents/Casks/memex-app.rb" --input "$scratch/request.json" --jq .commit.html_url
echo "Install with: brew install --cask nicosuave/tap/memex-app"
