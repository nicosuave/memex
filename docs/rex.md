# Rex integration

[Back to Memex](../README.md)

## Rex module

memex ships a [Rex](https://www.superlogical.com/rex/docs) module that turns the memex TUI into a
Rex-native session desk. Browse and search every past agent session from a Rex pane, then resume
one into a new Rex tab, or jump to the pane where it is already running.

Rex loads modules from beside its `init.lua`, so install by writing the module there:

```bash
memex rex install
```

Then add it to `~/.config/rex/init.lua` and reload:

```lua
require("memex").setup{}
rex.bind("cmd+shift+m", "memex_palette")
```

```bash
rex config reload
```

`memex rex install` writes `memex.lua` beside the `init.lua` Rex reads (`REX_CONFIG`, else
`$XDG_CONFIG_HOME/rex`, else `~/.config/rex`), or into `--dir`. It never edits `init.lua`. Run it
again after upgrading memex. From a checkout, link `rex/memex.lua` into that directory instead.

| Action | What it does |
| --- | --- |
| `memex_palette` | The TUI zoomed over the focused pane: Enter resumes into a new tab (or focuses the pane already running it), quitting restores the layout |
| `memex_here` | The palette pre-filtered to the focused terminal's repo |
| `memex_desk` | The TUI in its own `memex` tab, reused when one is open |
| `memex_sidebar` | Opens the TUI as a split beside the focused pane, or closes it |
| `memex_resume_last` | Resumes the most recent session for the focused terminal's directory, without opening the TUI |
| `memex_index` | Runs an incremental `memex index` in the background |
| `memex_web` | Starts `memex web` if nothing is listening, then opens it in the browser |

The actions are in Rex's command palette under "memex", and `rex do memex_palette` runs one from a
shell.

Whenever memex runs in a Rex terminal, resuming a session opens it in a new Rex tab at the
session's directory, with the resume command typed into the tab's shell, so the TUI stays where it
is and the shell stays open when the agent exits. When a terminal in the same Rex session is
already running that session, memex focuses it instead of opening a duplicate; it matches the
session id against the command lines of the processes in each terminal. Set `rex_resume =
"split"` (or `"off"`) in memex's own `config.toml` to change where resumed sessions open.

The palette is a zoomed split rather than a floating layer because Rex draws floating layers
without a backdrop, and the TUI keeps the terminal's default background.

## Options

```lua
require("memex").setup{
  bin           = "memex",           -- the memex binary; a bare name is looked up on your login PATH
  root          = nil,               -- an alternate memex data directory (memex --root)
  sidebar_focus = false,             -- whether the sidebar takes focus when it opens
  web_listen    = "127.0.0.1:6363",  -- where memex web listens
}
```

`memex_index` and `memex_web` log to `$XDG_STATE_HOME/memex-rex` (default
`~/.local/state/memex-rex`).

The module is backed by a CLI command that also works from any Rex terminal:

```bash
memex rex resume-last --cwd .   # resume the newest session for this repo into a Rex tab,
                                # or focus its terminal when it is already open
```
