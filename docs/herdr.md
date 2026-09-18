# herdr integration

[Back to Memex](../README.md)

## herdr plugin

This repo is also a herdr plugin: it turns the memex TUI into a herdr-native session desk. Browse
and search every past agent session from a herdr pane, then resume one into a new herdr tab.

```bash
herdr plugin install nicosuave/memex
```

Or from a checkout:

```bash
cargo build --release
herdr plugin link .
```

`install` reuses a `memex` already on your PATH when it is current, otherwise it downloads the
release build matching the plugin version. `link` uses `target/release/memex` from the checkout.

| Action | What it does |
| --- | --- |
| `memex: session palette` | Recent sessions as an overlay: Enter resumes into a new tab, quitting returns focus where it was |
| `memex: recent sessions here` | The palette pre-filtered to the focused workspace's repo |
| `memex: session desk` | Opens the TUI zoomed over the focused pane |
| `memex: toggle sidebar` | Opens the TUI as a split beside your work, or closes it |
| `memex: resume last session` | Resumes the most recent session for the focused pane's directory, without opening the TUI |
| `memex: refresh index` | Runs an incremental `memex index` now |
| `memex: open web UI` | Starts `memex web` if nothing is listening, then opens it in the browser |

Resuming inside herdr opens the session in a new herdr tab rather than taking over the current
pane, so the desk stays where it is and you can resume several sessions in a row. Set
`herdr_resume = "split"` (or `"off"`) in memex's own `config.toml` to change that. Each herdr
session start also kicks off a background incremental index.

The plugin is backed by two new CLI surfaces that work anywhere:

```bash
memex sessions --cwd . --limit 5     # JSONL: session_id, cwd, git_root, resume_cmd, ...
memex herdr resume-last --cwd .      # resume the newest session for this repo into a herdr tab
```

Plugin config lives at `config.toml` in the plugin's herdr config directory and is re-read on
every action:

```toml
toggle_placement = "split"     # split, overlay, zoomed, tab
toggle_direction = "right"     # right, down
index_on_startup = true        # background index at herdr session start
web_listen = "127.0.0.1:6363"
```

Bind the desk to a key in your herdr config:

```toml
[[keys.command]]
key = "cmd+m"
type = "plugin_action"
command = "nicosuave.memex.palette"
description = "memex session palette"
```

The plugin is listed in the herdr marketplace through the `herdr-plugin` GitHub topic on this
repo.
