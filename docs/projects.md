# Projects and tags

[Back to Memex](../README.md)

Memex groups sessions by repository. Sessions without repository metadata, such as
work started in a scratch directory, appear under `Unfiled`. You can assign any
session to a project and add free-form tags. Both apply everywhere sessions are
grouped or filtered: `memex sessions`, `memex projects`, `memex search`, MCP, the TUI,
and the web UI.

```sh
memex session set-project SESSION_ID ProjectA   # group under ProjectA
memex session clear-project SESSION_ID          # back to the derived grouping
memex session tag add SESSION_ID tooling infra
memex session tag rm SESSION_ID infra
memex session tag ls                            # tags with session counts
memex session inspect SESSION_ID                # assigned, inherited, and derived values

memex sessions --project ProjectA
memex sessions --tag tooling
memex projects --tag tooling
memex search "editor keymap" --tag tooling
```

In the TUI, type `#tooling` in the project filter, or pick a `#tag` entry from the
project list. The web UI has a tag picker beside the project picker and a Tag field in
the filter panel.

## Behavior

- An assignment replaces only the grouping. The session's derived facts (`repo_project`,
  `cwd`, `git_root`) are kept and shown by `inspect`, and `clear-project` restores them.
- Tags are additive and never change a session's project. Tags are case-insensitive;
  a leading `#` is ignored. Repeating `--tag` requires every tag.
- Assignments are keyed by machine, provider, and session ID, not transcript path. The
  same session ID from two providers is two sessions; pass `--source` when it is
  ambiguous. Continuation files that share a session ID share its assignments, and a
  moved transcript keeps them.
- Sub-agent and forked sessions inherit their parent session's project unless they have
  their own, and also carry their parent's tags. Unrelated sessions never change.
- Only indexed sessions can be assigned, so a mistyped ID is an error, not an orphan.

## Storage

Assignments live in `~/.memex/state/session-metadata.sqlite`, separate from the
rebuildable analytics cache. `memex index rebuild`, schema migrations, analytics
backfills, and restarts leave them alone, and transcripts and provider metadata are
never modified. Back up that file to keep assignments across reinstalls.

## Multiple machines

Each machine stores assignments for the sessions it indexes, so its own project and
tag filters apply them. Pass `--machine ID` to `session set-project`, `clear-project`,
`tag add`, `tag rm`, or `inspect` to change a remote session; the write runs on that
machine. Remote peers need this Memex version for assignment commands and tag
filters. Session lists drop an older peer's rows when filtering by tag, but an older
peer ignores the tag filter in search.
