# Memex for macOS

A native app for browsing and searching your agent conversations. Find a project,
read its history, and resume a local session in your preferred terminal or open a
Codex conversation in ChatGPT.

<img width="1876" height="1146" alt="Memex native macOS app" src="https://github.com/user-attachments/assets/6e112c96-5b1c-4de3-80bc-c06939dac18a" />

## Install

Requires Apple Silicon and macOS 14 or later.

```sh
brew install --cask nicosuave/tap/memex-app
```

The app includes the Memex CLI and uses your existing Memex configuration and
index. If you have not set up an index yet, install the terminal CLI and index
your sources:

```sh
brew install nicosuave/tap/memex
memex index
```

See the [main README](../../README.md) for setup and supported engines.

Update the app with `brew upgrade --cask nicosuave/tap/memex-app`. There is no
in-app updater; upgrading the separate CLI does not update the app's bundled copy.

## Browse and search

Choose a project in the sidebar, then select a conversation. Sessions open at the
latest messages; scroll up to read earlier history. Search uses lexical matching
and opens the matching message.

The **Filters** button narrows conversations by timeframe, provider, and type
(chats, subagents, or both). Permission reviews are hidden by default. Filters
persist across launches and can be reset together.

The project menu sorts by recent activity, conversation count, or name. Project
counts cover all indexed history, excluding permission reviews; they do not
shrink with the conversation filters.

## Read conversations

Messages include formatted Markdown, code blocks, tool activity, and attachment
previews. Tool activity and session context start collapsed. Expand a tool to see
its inputs and results, or use **Show raw content** to inspect the source.

Long content has a **Show all** action. Copying preserves the full content. Local
images can be enlarged, and local file links open read-only previews. Remote URLs
open only when you choose them.

| Action | Shortcut or control |
| --- | --- |
| Find text throughout the conversation, including tool contents | Command-F |
| Next / previous match | Command-G / Shift-Command-G |
| Close find | Escape |
| Copy a message | Hover or focus the message to reveal Copy |
| Read normalized transcript records | **Raw transcript** in the header context menu |
| Open the original local log | **Reveal source** |

Find includes earlier pages and reveals matching raw content when needed.
Returning to a recently viewed conversation restores your reading position.

## Resume a session

Use **Resume** to open a local session in Ghostree, Ghostty, Terminal, Alacritty,
kitty, WezTerm, or cmux. The menu remembers your chosen app. Local Codex sessions
can also open in ChatGPT.

Resuming requires the original agent and its session data. Available actions
depend on the engine; see the [engine support table](../../README.md#engine-support).
macOS may request Automation permission on first use. Ghostty needs its scripting
API, and cmux needs socket access.

## Multiple machines

The machine selector offers **All Machines**, **This Mac**, and your configured
machines. Project and conversation results load independently from each machine,
so a slow peer does not hold back local results. Failed peers show an error and
retain cached project counts.

You can search and read remote conversations, but **remote resume is disabled**
in the macOS app. Resume those sessions on their own machine. Remote file paths
are never opened against this computer's filesystem.

See [SSH configuration](../../docs/machines.md) to connect another machine.

## Background indexing

The app uses a compatible running daemon when available and falls back to its
bundled CLI. It does not start, restart, or reconfigure the daemon.

To keep the index updated in the background:

```sh
memex daemon enable --continuous
```

See the [daemon guide](../../docs/daemon.md) for service management, and
[development documentation](DEVELOPMENT.md) for building, testing, packaging,
release procedures, and implementation details.
