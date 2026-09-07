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
Source-only matches hidden by Markdown still reveal the containing message.
Tool activity and session instructions start collapsed. Each tool call and its result share
one expandable row; single operations and instructions have no extra outer disclosure. Both the session list and
transcript load additional pages as you scroll, without load buttons. Browsing
opens at the newest messages; scrolling up loads earlier context. Search opens at
the matched message, with paging in both directions. Returning to a conversation
restores its reading position during the current app session (up to 20 recent views).
Messages render Markdown headings, emphasis, lists, links, quotes, and code blocks
as native attributed text. XML-style prompt wrappers become labeled sections with
subtle borders and formatted content; fenced code stays literal.
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
The toolbar Resume split button opens a fresh window in Ghostree, Ghostty, or Terminal
using the CLI's configured resume command and the conversation's working directory.
Its menu remembers your chosen installed terminal. Older Ghostty versions without
the scripting API are omitted. macOS may request Automation permission on first use.
Remote conversations must be resumed on their own machine; their Resume control is disabled.

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

## Local data and packaging

The app uses the bundled CLI and your existing Memex configuration and index.
Project summaries are cached under `~/Library/Caches/dev.memex.app`, separately
for each data root and machine. Reading, decoding, sorting, and saving this cache happen away
from the main actor. Failed refreshes retain cached projects and expose a retry.
It does not require a separately running server. Index your sources with the CLI
before browsing them in the app. The embedded CLI is refreshed each time the app
is packaged; updating your installed CLI alone does not change an existing bundle.

The packaging script checks that both executables depend only on Apple's system
libraries. Custom CLI builds with external libraries fail with the dependency name
rather than producing a bundle that depends on your Homebrew installation.
The bundle is built for the local machine and signed ad hoc for development;
distribution signing and notarization are not configured.

The SwiftUI shell embeds an AppKit NSTableView transcript with native text selection.
The transcript has no LazyVStack; row measurements retain TextKit glyph layout across resizes. Messages display their full text; tool bodies are only
constructed when opened and then display their full input and output. The app never launches itself after you quit it.
