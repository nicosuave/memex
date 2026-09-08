#!/usr/bin/env bash
set -euo pipefail
version=${1:?Usage: render-cask.sh VERSION SHA256}
checksum=${2:?Usage: render-cask.sh VERSION SHA256}
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Expected a stable semantic version" >&2; exit 1; }
[[ "$checksum" =~ ^[a-f0-9]{64}$ ]] || { echo "Expected a SHA256 checksum" >&2; exit 1; }
cat <<CASK
cask "memex-app" do
  version "$version"
  sha256 "$checksum"

  url "https://github.com/nicosuave/memex/releases/download/v#{version}/memex-app-#{version}-macos-universal.zip"
  name "Memex"
  desc "Browse and search local agent conversation history"
  homepage "https://github.com/nicosuave/memex"

  depends_on macos: :sonoma

  app "Memex.app"
end
CASK
