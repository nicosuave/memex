# macOS app development

[Back to the app overview](README.md)

## Build and launch

Requires macOS 14 or later, Swift 6.2 or later (including the terminal dependency),
Apple command-line developer tools, and an installed Memex CLI. No Xcode project
or Xcode GUI is needed. The developer tools supply `swift`, `codesign`, `otool`,
and `plutil`.

From the repository root:

```sh
apps/macos/scripts/launch.sh
```

This builds the SwiftPM executable, assembles `apps/macos/build/Memex.app`, embeds
the CLI found on `PATH`, signs the bundle locally, verifies its signature, and
opens the app (or focuses the existing instance). It does not terminate existing instances. Quit an older
instance normally when switching builds.

To select a particular CLI build:

```sh
MEMEX_CLI=/absolute/path/to/memex apps/macos/scripts/launch.sh
```

The CLI must support `memex projects` and `memex machines`. Remote peers need the
projects/sessions metadata RPC operations supplied by this checkout. For this checkout,
build the Rust CLI first and set `MEMEX_CLI` to that binary when packaging.

Packaging uses an optimized release build by default. To package without launching, or select a debug build:

```sh
apps/macos/scripts/build.sh
apps/macos/scripts/build.sh debug
```

Run the native tests with:

```sh
swift test --package-path apps/macos
```

Check the packaged app's SDK metadata with `bash apps/macos/Tests/build-sdk.sh`.
This builds a debug bundle and verifies that it records the selected SDK while
retaining macOS 14 support. Incorrect SDK metadata enables legacy AppKit drawing
and can misalign the toolbar and sidebar on newer macOS versions.

To also test the Swift client against a real Rust daemon, build the CLI and run:

```sh
MEMEX_DAEMON_TEST_CLI="$PWD/target/debug/memex" swift test --package-path apps/macos --filter isolatedRustDaemonServesSwiftClientAndReconnects
```

This test starts and stops its own foreground daemon with a temporary data root
and one synthetic transcript; it does not use your sources or service settings.

## Workspace terminals

The native terminal uses the pinned public `libghostty-spm` package and its
checksummed Ghostty XCFramework. SwiftPM downloads the framework; the app build
script includes the package's shell-integration and terminfo resource bundle.
No local Ghostty installation, Zig toolchain, or private agent runtime is required.

The terminal registry is keyed by the canonical Git worktree root, or the local
working folder for a non-Git workspace. Each shell starts lazily when displayed.
The registry retains its native view and surface when the user hides it, changes
chats, or moves it between the inspector and the Command-J drawer. Distinct Git
worktrees have distinct shells. Remote paths never create local terminals.
Closing a terminal or quitting the app ends its shell; terminal processes do not
survive an app quit or restart.

Run the real-shell and presentation regressions with:

```sh
swift test --package-path apps/macos --filter WorkspaceTerminal
```

## Local in-app agent runtime

An optional local Sidequery checkout supplies `SQACP`, `SQACPHost`, `SQACPUI`, the standalone
`sq_acp_runtime` static library, and the compiled Claude Agent SDK helper. Set
`MEMEX_AGENT_RUNTIME_ROOT` to that checkout, or put its absolute path in the
ignored `apps/macos/.local-runtime-root` file. Ordinary builds without this
configuration retain external resume only; no runtime source or binary is fetched.

Use [the runtime preparation and provenance workflow](RUNTIME.md) to build the
locked source archive and Claude helper before packaging. `build.sh` rejects stale
or mismatched artifacts, and embeds/signs the execution-host and Claude helpers.
Local dirty source requires an explicit development mode; Developer ID packaging
requires the clean pinned revision. `MEMEX_HISTORY_ONLY=1` keeps the public
history-only graph available even when a local runtime checkout is configured.

Selecting a local main Codex or Claude session shows Sidequery's shared composer,
send/stop controls, and approval/question panels. The first send loads the original
session and submits the prompt once it is ready; browsing history alone does not
launch a provider. The toolbar's **Open in** menu opens external applications.
Continuing a chat requires the original native transcript and working directory. Each provider
child receives the source installation's `CODEX_HOME` or `CLAUDE_CONFIG_DIR`.
The provider's normal sign-in remains required. Optional executable overrides
are `MEMEX_CODEX_EXECUTABLE`, `MEMEX_CLAUDE_EXECUTABLE`, and `MEMEX_CLAUDE_HELPER`.

The provider writes its original transcript; existing Memex indexing continues
to pick it up. Private runtime sidecars live under
`~/Library/Application Support/dev.memex.app/Resume/`. They retain delivery
receipts and a source archive, separate from the search index. Reconnect reloads
native history and never automatically retries an uncertain prompt.

The `nativeRuntimeResumesDisposableProviderSessions` test is opt-in. Set
`MEMEX_LIVE_TEST_SESSIONS` to a JSON array of Memex session metadata for disposable
sessions whose initial assistant response contains `MEMEX_SEED_OK`. It sends one
test prompt, checks persistence and duplicate-free reconnect, and closes only its
own providers. Direct Swift tests also need `MEMEX_CLAUDE_HELPER` for Claude.
Never point this test at a working conversation.

## App releases

The app is distributed separately from the CLI archives. GitHub Actions continues
to publish the CLI; app builds, Developer ID signing, and notarization run on a
maintainer's Mac. Apple credentials stay in the local Keychain.

User installation instructions are in the [app README](README.md#install).

The release Mac needs Xcode developer tools, Rust with the Apple Silicon macOS target, `gh`,
and `jq`, plus a Developer ID Application certificate with its private key in
Keychain. Authenticate `gh` with write access to `nicosuave/memex` and
`nicosuave/homebrew-tap`. Set up Rust targets once:

```sh
rustup target add aarch64-apple-darwin
```

Use the existing `sidequery-notarization` notarytool Keychain profile, or create a
profile interactively with `xcrun notarytool store-credentials PROFILE` and set
`NOTARY_PROFILE`. Do not put passwords or certificate exports in the repository
or GitHub secrets. Select the local signing identity explicitly:

```sh
export CODESIGN_IDENTITY='Developer ID Application: YOUR NAME (TEAMID)'
```

After the normal CLI release has created and published `vVERSION`, use a clean
checkout at that exact tag and run:

```sh
scripts/release_macos_local.sh VERSION
```

The command checks the package version, clean checkout, local/published tag, and
GitHub release. It builds both the Swift app and Rust helper from that checkout,
verifies the arm64 architecture and system-library dependencies, signs with hardened
runtime, submits to Apple, requires an Accepted result, staples the ticket, and
checks Gatekeeper. It then verifies the extracted ZIP, uploads it and its SHA256,
and creates or updates `Casks/memex-app.rb` in the tap using your local GitHub
authentication. It never overwrites published assets or downgrades the cask.

Outputs and notarization results remain under
`apps/macos/build/releases/vVERSION/`. If uploading is interrupted, retry the
saved, verified artifacts without rebuilding or notarizing again:

```sh
scripts/release_macos_local.sh --publish-only VERSION
```

If only the cask update failed, it can be retried independently; the command
downloads the published app and verifies its checksum before updating the tap:

```sh
apps/macos/scripts/publish-cask.sh VERSION
```

To validate Apple Silicon packaging without Apple credentials or publishing anything:

```sh
apps/macos/scripts/build-release.sh
```

This defaults to ad hoc signing. Both normal and release app builds derive their
marketing version from `Cargo.toml` and build number from the commit count.
`SIGNING_MODE=developer-id` enables distribution signing; it requires
`CODESIGN_IDENTITY` and does not fall back to ad hoc signing on failure.

Run the release contract tests with `bash apps/macos/Tests/release-scripts.sh`.
These use temporary fixtures for external services and need no Apple credentials.

## Local data and packaging

The app uses your existing Memex configuration and index. When a compatible
continuous daemon is running, it connects through the private Unix socket at
`<data-root>/state/native/app.sock`. A small pool of persistent connections lets
lists, counts, search, and transcript reads proceed independently. This avoids
launching a CLI process for each request; query collectors still open their
readers per operation so published index generations remain visible.

Fresh browsing uses the session list's `message_count` to request the latest page
with its current total in one call (`session_page` over the socket, or
`session --full --page-info --limit 60` through the CLI). The list count is a hint;
if the returned total changes the final page offset, the reader fetches that page.
This uses the existing remote page RPC, so remote peers do not need an upgrade.
Older local CLI overrides fall back to separate record and metadata reads.

If the daemon is absent, older, or unavailable, requests use the bundled CLI.
The app does not enable or restart the daemon or change service settings. An older running daemon needs an updated binary and a normal restart
before it provides the socket. Local socket reads do not trigger auto-indexing;
remote machines and CLI fallback retain their existing indexing policies.
A runtime `MEMEX_CLI` override selects CLI-only operation for development.

The socket is private to the current user and data root. Its versioned protocol
exposes only native-app reads, without an HTTP port or browser authentication.
Cancellation closes the request's connection; completed requests return their
connections to the pool. An operation error is shown normally rather than being
silently retried through the CLI.

Project summaries are cached under `~/Library/Caches/dev.memex.app`, separately
for each data root and machine. Reading, decoding, sorting, and saving this cache happen away
from the main actor. Failed refreshes retain cached projects and expose a retry.
It does not require a separately running server. Index your sources with the CLI
before browsing them in the app. The embedded CLI is refreshed each time the app
is packaged; updating your installed CLI alone does not change an existing bundle.

The packaging script checks that both executables depend only on Apple's system
libraries. Custom CLI builds with external libraries fail with the dependency name
rather than producing a bundle that depends on your Homebrew installation.
The normal build targets the local machine and signs ad hoc for development;
the app release commands above build arm64 bundles for distribution.

The SwiftUI shell embeds an AppKit NSTableView transcript with native text selection.
The transcript has no LazyVStack; row measurements retain TextKit glyph layout across resizes. Tool bodies are only
constructed when opened. Bounded displays preserve full source and selection;
Find expands the matching content automatically. The app never launches itself after you quit it.
