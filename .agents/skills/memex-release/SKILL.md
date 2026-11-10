---
name: memex-release
description: Publish and verify a full Memex release (CLI archives, Qt AppImage, signed macOS app, Homebrew). Use when asked to do, cut, or recover a memex patch, minor, or major release.
argument-hint: "[patch|minor|major]"
---

# Memex release

A full release is: version bump and tag via `cargo release`, the tag-triggered
GitHub release workflow, the locally signed macOS app plus its Homebrew cask,
then verification. The repo owns the mechanics; when this file disagrees with
them, trust `release.toml`, `.github/workflows/release.yml`,
`scripts/release_macos_local.sh`, and `apps/macos/scripts/publish-cask.sh`.

## Before starting

- Release from the maintainer's main checkout on `main`, not from a worktree:
  `cargo release` refuses a detached HEAD.
- Preserve unrelated working-tree changes; the macOS script requires a clean tree.
- Pick the bump from the commits since the latest tag
  (`git log --oneline $(git describe --tags --abbrev=0)..origin/main`): fixes only
  -> patch, new user-visible capability -> minor, breaking change -> major. A
  requested bump does not override contrary evidence; say which you chose and why.
- CI on the latest `main` commit must be green. If it is still running, wait for
  that exact commit.

## Procedure

1. Sync: `git fetch origin --prune --tags && git pull --ff-only`.
2. Check: `cargo fmt --check`, `mbx clippy -- -D warnings`, and `mbx test` once.
   If the full suite stalls or a known timing-sensitive test fails, rerun that test
   alone and report the full suite as unverified rather than passed.
3. Bump and tag: `cargo release <bump> --execute --no-confirm`. This updates
   `Cargo.toml`, `Cargo.lock`, and `herdr-plugin.toml`, commits
   "Release memex version X.Y.Z", tags `vX.Y.Z`, and pushes both. It does not
   publish to crates.io. Run it once without `--execute` first if unsure.
4. Release workflow (`release.yml`, triggered by the tag): builds
   `memex-X.Y.Z-macos-arm64.tar.gz` and `memex-X.Y.Z-linux-{x86_64,arm64}.tar.gz`
   (each with `.sha256`) plus the Qt AppImage, and creates the GitHub release.
   Watch it with `gh run list --workflow release.yml` and one bounded wait.
5. macOS app, after the GitHub release exists (needs the maintainer's local
   Developer ID and notarization setup):
   `CODESIGN_IDENTITY='<Developer ID Application identity>' scripts/release_macos_local.sh X.Y.Z`
   (`NOTARY_PROFILE` has a default in the script). It builds, signs, notarizes,
   staples, uploads `memex-app-X.Y.Z-macos-arm64.zip`, and updates
   `Casks/memex-app.rb` in `nicosuave/homebrew-tap`. Run it in the background; it
   takes minutes. On an upload or cask failure, retry with `--publish-only X.Y.Z`.
6. Homebrew formula: `memex` lives in homebrew-core (the tap's
   `tap_migrations.json` points there). Nothing to push; verify with
   `gh api repos/Homebrew/homebrew-core/contents/Formula/m/memex.rb --jq .content | base64 --decode`.
   It usually lags the tag; report it as pending rather than failed.

## Verify

- `gh release view vX.Y.Z --repo nicosuave/memex --json isDraft,isPrerelease,assets`:
  not draft or prerelease, and has the CLI archives, AppImage, and app zip, each
  with `.sha256`.
- Download one archive, check it with `shasum -a 256 -c`, extract it, and confirm
  `memex --version` prints `memex X.Y.Z`.
- The tap cask shows the new version; the homebrew-core formula shows it or is
  reported as pending.
- The main checkout ends clean and in sync with `origin/main`.

## Pitfalls

- `gh release view` does not support `isLatest`; use `isDraft` and `isPrerelease`.
- If Homebrew needs a newer OpenSSL, update the locked `openssl` and `openssl-sys`
  crates together before releasing (`openssl-sys 0.9.111` rejects OpenSSL >= 4).
- `brew lgtm <formula>` can fail; run
  `HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_INSTALL_FROM_API=1 brew lgtm --online`
  without a formula argument from a homebrew-core checkout.
