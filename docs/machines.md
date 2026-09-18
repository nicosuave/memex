# Multiple machines

[Back to Memex](../README.md)

## Multiple machines over SSH

Each machine keeps and updates its own index. The coordinating memex queries configured
machines concurrently over SSH, merges their rankings, and keeps the originating machine
attached to every result. The TUI uses the same backend for search, history previews,
sharing, token charts, and interactive resume.

Install a protocol-compatible memex binary on each machine and configure SSH normally in
`~/.ssh/config`. Then add machines to `~/.memex/config.toml`:

```toml
[multi_machine]
default = ["local", "mini"]
timeout_seconds = 10

[[machines]]
id = "mini"
label = "Mac mini"

[machines.control]
type = "ssh"
host = "mini" # SSH config alias

[machines.index]
type = "remote"
```

The `ssh = "mini"` field is a shorthand for the `machines.control` table. Set
`command = "/path/to/memex"` when `memex` is not on the non-interactive SSH `PATH`.
SSH keys, users, ports, jump hosts, and host-key policy remain in `~/.ssh/config`.

```sh
memex search "tantivy corruption"             # configured defaults
memex search "tantivy corruption" --machine mini
memex usage --machine local --machine mini
```

Unavailable machines produce partial results with a warning. Remote token usage requires
`token_usage = true` in that machine's memex config. The index backend is intentionally
separate from the control transport so an immutable S3 split backend can replace
`type = "remote"` later while SSH continues to handle indexing and resume.

In the TUI, use the `machines` dropdown (or press `m` while the session list is focused)
to select the configured default set, `local`, or one remote machine. The machine, source,
project, and query filters are shared by the session results and token chart; the range
dropdown bounds the chart. Repository project mode groups linked checkouts and worktrees
under their repository name. Sessions without repository metadata appear under `Unfiled`
instead of turning arbitrary working-directory or standalone-task names into projects.

### Opening federated results

Search results include the originating machine. Use it when opening a document or
session from another machine:

~~~sh
memex show 123 --machine mini
memex show --record-id rid1_example --machine mini
memex session SESSION_ID --machine mini --source-path /path/on/mini/session.jsonl
memex session SESSION_ID --machine mini --offset 50
memex context --record-id rid1_example --machine mini --before 5 --after 5
~~~

`memex context` selects a source/session/path neighborhood around a stable record ID,
document ID, or native event ID. It accepts `--offset` into that neighborhood and returns
`offset`, `total`, and `next_offset`, while applying the same shared content budget and
per-record continuation metadata as `session`. Bounded pages use `order: "anchor_first"`
so the requested record cannot be crowded out by preceding content; remaining records
stay chronological. `--full` uses `order: "chronological"`. Keep the same mode when
following `next_offset`. `--expand-interactions` adds only directly
owned tool calls/results; it does not follow conversation ancestry. Expansion is capped at
100 additional records and reports an actionable error when the cap is exceeded.

New indexes provide exact canonical record-ID lookup plus indexed document/event lookup.
Bounded remote reads require a peer with the new read RPC operations. If a peer is older,
update it. Legacy document-ID `show` and `session` reads can explicitly use
`--full` for complete-content reads; remote context and stable-ID reads need an updated
peer in either mode. Memex does not silently fetch unbounded bodies as a fallback. Character limits apply before the peer
serializes content. They are not limits on JSON metadata or total network bytes.

Existing indexes remain readable: canonical record IDs fall back to a stored-record scan
until the index is rebuilt. Neighborhood reads use indexed session, source, and source-path
scope.

Session and batch pages are limited to 500 records. To fetch several sessions in one
bounded request, provide JSONL on stdin or as a file:

~~~json
{"machine":"mini","session_id":"SESSION_ID","source_path":"/path/on/mini/session.jsonl","offset":0,"limit":500}
~~~

~~~sh
memex session batch requests.jsonl
cat requests.jsonl | memex session batch
~~~

The batch input accepts at most 32 requests and returns one JSONL response per request in
input order, including machine provenance, `offset`, `total`, `next_offset`, stable
`record_id` values, and per-record content metadata. One 16,000-character budget is shared
across all requests in input order. Use `--max-chars N` to change it or `--full` for full
record content; `--full` conflicts with `--max-chars`. The request envelope's
`next_offset` resumes later records.
