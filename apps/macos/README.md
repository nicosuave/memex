# Memex for macOS

A native macOS companion to the Memex CLI: a project sidebar, a compact session
list, and a paged conversation reader. It supports sessions and lexical search
across configured machines, project and provider filters, and local conversation resuming.
The toolbar has one Filters button: its popover combines timeframe
(all time, last 24 hours, 7 days, or 30 days), provider, and type
(Chats and subagents, Chats only, or Subagents only).
Permission reviews are hidden by default; enable Show permission reviews with Chats and subagents to include them.
Filters combine with the selected project and machine, persist across launches,
and can be reset together. Project counts remain full-index, all-history totals excluding permission reviews.
Press Command-F in the reader for literal find across the complete conversation,
including earlier pages and tool contents. Command-G and Shift-Command-G navigate
matches; Escape closes find. Results appear incrementally while scanning.
Source-only matches hidden by Markdown reveal and select the original text.
Tool activity and session context start collapsed. Recognized injected plugin,
AGENTS.md, environment, permissions, and harness sections are separated from the
actual request, including mixed records. Quoted examples and arbitrary HTML stay visible.
Activity rows use tool icons and compact summaries derived from recognized arguments;
groups count logical operations rather than counting calls and results separately.
Explicitly failed, cancelled, or interrupted operations stay visible between compact
routine groups. Missing results do not imply a running or successful operation.
Completed work collapses around the final answer only when the same source turn
has explicit completion and final-answer metadata. Older or incomplete records
retain their visible assistant messages.
Expanded tools show labeled JSON fields, native command/file/diff cards, literal code, and decoded output lines.
Long encoded payloads stay compact; Show raw content reveals the complete source.
Find automatically uses raw tool content to retain exact matches. Hover or focus a
message to reveal its copy icon. Source access stays in the toolbar; full raw
transcript access stays in the header context menu.
The header's context menu offers Raw transcript, exposing every normalized CLI record, including
unrecognized fields, with normal bidirectional paging. Reveal source opens the original local log.
Each tool call and its result share
one expandable row; single operations and instructions have no extra outer disclosure. Both the session list and
transcript load additional pages as you scroll, without load buttons. Browsing
opens at the newest messages; scrolling up loads earlier context. Search opens at
the matched message, with paging in both directions. Returning to a conversation
restores its reading position during the current app session (up to 20 recent views).
Messages render Markdown headings, emphasis, lists, links, quotes, and code blocks
as native content. Top-level fenced code gets syntax colors, a language label,
copy control, and horizontal scrolling. Long messages and outputs start bounded;
Show all reveals their full content, and copying preserves the full source.
Typed provider images and documents, Markdown images, and local file cards retain
their original references. Local images and embedded image data have thumbnails
and enlargement; remote URLs open explicitly. Paths from another machine never
resolve against this machine. Local file links with line numbers open a native
read-only source preview at that line.
Expanded/raw states and viewport anchors survive paging, resizing, and switching conversations.
The reader has no secondary navigation bar.

New Codex turn/phase/lifecycle and Codex/Claude typed attachment metadata is stored
in optional record fields. Old indexes remain readable; their next ingestion
rebuilds a private generation before publishing the refreshed metadata. Remote
machines need the updated backend and indexing before those fields are available.
The Projects sidebar uses full-index session counts and latest activity from
`memex projects`. Its ellipsis menu sorts by recent activity, conversation count,
or name. Cached projects and the selected sort survive restarts; an immediate
background refresh updates them without waiting for conversation pagination.
The fixed sidebar chin selects All Machines (default), This Mac, or an enabled
machine from Memex configuration. Projects and conversations load independently
from each machine, so a slow peer does not hold back local results. Project counts
combine the selected machines; failed peers retain their last cached totals and
show a retry warning. Remote sessions carry their machine identity through search
and transcript reads.
The toolbar Resume split button opens a fresh window in an installed Ghostree, Ghostty,
Terminal, Alacritty, kitty, WezTerm, or cmux
using the CLI's configured resume command and the conversation's working directory.
For local Codex conversations, ChatGPT is also available through its conversation deep link.
Its menu remembers your chosen app. Older Ghostty versions without
the scripting API are omitted. macOS may request Automation permission on first use.
Remote conversations must be resumed on their own machine; their Resume action is disabled.
cmux requires socket access and opens the resumed session in a fresh workspace.

## Build and launch

Requires macOS 14 or later, a Swift toolchain compatible with `Package.swift`,
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

To also test the Swift client against a real Rust daemon, build the CLI and run:

```sh
MEMEX_DAEMON_TEST_CLI="$PWD/target/debug/memex" swift test --package-path apps/macos --filter isolatedRustDaemonServesSwiftClientAndReconnects
```

This test starts and stops its own foreground daemon with a temporary data root
and one synthetic transcript; it does not use your sources or service settings.

## App releases

The app is distributed separately from the CLI archives. GitHub Actions continues
to publish the CLI; app builds, Developer ID signing, and notarization run on a
maintainer's Mac. Apple credentials stay in the local Keychain.

Once the first app release is published, install or upgrade it with:

```sh
brew install --cask nicosuave/tap/memex-app
brew upgrade --cask nicosuave/tap/memex-app
```

Alternatively, download `memex-app-VERSION-macos-universal.zip` from the matching
GitHub release, unzip it, and move `Memex.app` to Applications. The app includes
its CLI and supports Apple Silicon and Intel on macOS 14+. The separate `memex`
formula remains available for terminal use. There is no in-app updater.

The release Mac needs Xcode developer tools, Rust with both macOS targets, `gh`,
and `jq`, plus a Developer ID Application certificate with its private key in
Keychain. Authenticate `gh` with write access to `nicosuave/memex` and
`nicosuave/homebrew-tap`. Set up Rust targets once:

```sh
rustup target add aarch64-apple-darwin x86_64-apple-darwin
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
verifies both architectures and system-library dependencies, signs with hardened
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

To validate universal packaging without Apple credentials or publishing anything:

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
the app release commands above build universal bundles for distribution.

The SwiftUI shell embeds an AppKit NSTableView transcript with native text selection.
The transcript has no LazyVStack; row measurements retain TextKit glyph layout across resizes. Tool bodies are only
constructed when opened. Bounded displays preserve full source and selection;
Find expands the matching content automatically. The app never launches itself after you quit it.
