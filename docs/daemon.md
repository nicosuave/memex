# Background indexing and the Web UI

[Back to Memex](../README.md)

## Memex daemon

Works on macOS (launchd) and Linux (systemd).

Run indexing and any configured Web UI or MCP server in the foreground:

```
memex daemon run
```

Enable:
```
memex daemon enable
memex daemon enable --continuous
memex daemon enable --web-ui
```

Regenerate the daemon from current config and restart it:
```
memex daemon restart
```

Inspect the registered daemon and whether it is serving the Web UI or MCP:
```
memex daemon status
```

Open an authenticated browser session:
```
memex web open
```

Disable:
```
memex daemon disable
```

The daemon reads config defaults for its mode, interval, listeners, and log paths.
Flags override those defaults.

### Resumable embeddings

When embeddings are enabled, continuous indexing runs background backfills in a supervised
`memex embed` child. New lexical records can be indexed during backfill; replacements and
deletions coordinate with the embedding writer. Existing parser-version migrations and
interrupted synchronous embedding operations retain their recovery behavior.

Completed batches are checkpointed in
`state/embed-backfill.sqlite3`; restarting the daemon or rerunning `memex embed` resumes
those batches. The active vector generation stays searchable until its replacement is ready.
`memex stats` reports checkpoint progress, the worker PID, and an estimated remaining time.

The daemon stops and reaps its child on shutdown, embedding-configuration changes, and
executable handoff. `--no-embeddings` disables its worker. Explicit index rebuilds discard
embedding checkpoints along with the derived indexes. Changing embedding models discards
incompatible checkpoint vectors; embeddings cannot be reused between models.

### Reclaiming obsolete index generations

After an upgrade, normal indexing automatically migrates a legacy index and removes obsolete
pre-lease generations. It preserves the committed Tantivy segments without rebuilding or reparsing
conversation history. No user action is required.

For diagnostics or to reclaim space immediately without waiting for the next index run, stop the
background daemon and close TUI/Web readers, then preview and run GC:

```bash
memex daemon disable
memex index gc --dry-run
memex index gc --offline
memex daemon enable --web-ui # or restore the mode you previously used
```

`memex index gc` validates the committed index, hard-links only its live Tantivy segments into a clean
generation, atomically switches `CURRENT`, validates the document count again, and then removes
unreachable generations. It does not rebuild the index and does not rewrite live segment data.
The explicit command retains an `--offline` acknowledgement because it performs cleanup without a
normal index publication.

On Linux, creates systemd user units in `~/.config/systemd/user/`. On macOS, creates a launchd plist in `~/.memex/`.
On successful enable, memex writes `auto_index_on_search = false` to config when that setting is absent, so searches do not duplicate daemon work. Explicit user config is preserved.

`--web-ui` implies continuous mode and serves a local search and transcript browser at
`http://127.0.0.1:6363`. It mirrors the TUI's core workflow with search-as-you-type,
source and project filters, a persistent session list, and Matches/History transcript
previews. The server binds to loopback by default because the index
contains private conversation history. Memex refuses non-loopback HTTP listeners. If
remote access is required, use an authenticated TLS reverse proxy to `127.0.0.1` that
injects the installation bearer token into upstream requests. To use a different local port:

```
memex daemon enable --web-listen 127.0.0.1:8080
memex web open --listen 127.0.0.1:8080
```

The first Web UI start creates `~/.memex/web-auth-token` with mode `0600`. Private API
routes require that token as `Authorization: Bearer ...` or a browser session established
by `web open`. Browser links carry a signed, one-time credential in the URL
fragment, remove it before navigation continues, and exchange it for an `HttpOnly`,
same-origin cookie. JavaScript cannot read the cookie, and Memex does not store browser
credentials in `localStorage` or session storage. Browser sessions survive refreshes and
reopened tabs, expire after 12 hours, and are invalidated whenever the daemon restarts.

To run the same UI in the foreground without changing the background daemon:

```
memex web serve
```

`memex web` is shorthand for `memex web serve`; foreground startup prints a one-time
login URL. A background `memex daemon` never writes that credential to its service logs.
Run `memex web open` from a terminal to open an authenticated browser session.

The browser frontend lives in `web/`, uses React and shadcn components, and is
built with `cd web && bun install && bun run build`. The generated static assets
are embedded in the memex binary, so serving the UI does not add a JavaScript
runtime to the daemon.
