# memex

Fast local history search for Claude, Codex CLI, Cursor, OpenCode, Pi, Oh My Pi, OpenClaw, GitHub Copilot CLI, Grok, Jcode, and Muse. Also supports Hermes usage records. Uses BM-25 and optionally embeds your transcripts locally for hybrid search.

Agents can use Memex through its MCP server or CLI skill. Ask about a previous session, then narrow the search and retrieve source records as needed.

Includes a TUI for browsing, finding and resuming agent CLI sessions, with optional [token usage](#token-usage) tracking.

![memex tui](docs/tui.png?raw=1&v=4)

## Install
```bash
brew install nicosuave/tap/memex
```

Or

```bash
curl -fsSL https://raw.githubusercontent.com/nicosuave/memex/main/scripts/setup.sh | sh
```

Or (from the [AUR](https://aur.archlinux.org/packages/memex) on Arch Linux):

```bash
paru -S memex
```

Or (with [Nix](https://nixos.org/)):

```bash
nix run github:nicosuave/memex
```

<details>
<summary>Nix development and advanced configuration</summary>

**Development shell:**

```bash
nix develop
```

> **Note:** No binary cache is configured, so first builds compile from source.

**NixOS service:**

Enable background indexing with the provided module:

```nix
{
  inputs.memex.url = "github:nicosuave/memex";

  outputs = { nixpkgs, memex, ... }: {
    nixosConfigurations.default = nixpkgs.lib.nixosSystem {
      modules = [
        memex.nixosModules.default
        {
          services.memex = {
            enable = true;
            continuous = true; # Run as a daemon (optional)
          };
        }
      ];
    };
  };
}
```

**Home Manager:**

Configure memex declaratively (generates `~/.memex/config.toml`):

```nix
{
  inputs.memex.url = "github:nicosuave/memex";

  outputs = { memex, ... }: {
    # Inside your Home Manager configuration
    modules = [
      memex.homeManagerModules.default
      {
        programs.memex = {
          enable = true;
          settings = {
            embeddings = true;
            include_reasoning = false;
            model = "minilm";
            execution_provider = "auto"; # coreml on macOS, cpu elsewhere
            cuda_device_id = 0; # optional when execution_provider = "cuda"
            cuda_library_paths = ["/usr/local/cuda/lib64"]; # optional override
            cudnn_library_paths = ["/usr/lib/x86_64-linux-gnu"]; # optional override
            compute_units = "ane"; # CoreML only: ane, gpu, cpu, all
            auto_index_on_search = true;
            token_usage = false; # opt in to local token and cost tracking
            index_service_interval = 3600;
          };
        };
      }
    ];
  };
}
```

</details>

Then install the shared skill used by Codex, OpenCode, Pi, and Oh My Pi:

```bash
memex skill install --target shared
```

The shared `memex-search` skill is installed once at
`~/.agents/skills/memex-search/SKILL.md` for Codex, OpenCode, Pi, and Oh My Pi.
For Claude Code, use `memex skill install --target claude`; its copy lives at
`~/.claude/skills/memex-search/SKILL.md`.

Launching `memex` in a human terminal offers an update when a newer release is
available. Press Enter to update Memex and refresh its installed skill copies, or
choose no to continue into the TUI. Homebrew installs run `brew update` followed by
`brew upgrade nicosuave/tap/memex`; skills are refreshed by the newly installed binary.
After updating, run `memex` again to start that version.

Agents and scripts still receive update notices but never a startup prompt. Memex
recognizes CI, Codex, and Claude Code environment markers; use `--non-interactive`
explicitly for other agents, including those using a PTY. Bare noninteractive
`memex` prints help, while `search` and other data commands run normally. To update:

```bash
memex update --yes
```

Without `--yes`, explicit `memex update` requires a human terminal and confirmation.
Both update paths replace existing shared/Claude skill copies, including local edits;
they do not install missing copies. Restart your agent after its skill is updated.

Searches warn on stderr when an installed skill differs from the running binary
(outdated or locally modified). `memex skill status` shows which copies differ;
`memex skill update` refreshes skills without upgrading Memex. Searches never update
anything or prompt, and JSONL/TOON output on stdout stays clean. Use
`memex skill cleanup` to explicitly remove obsolete paths from older releases.

Release checks have a two-second network timeout and are cached for six hours
(failed checks retry after five minutes). `--no-update-check` skips release checks
without hiding stale-skill warnings. Install never overwrites a differing skill file.

## Quickstart

Index (incremental):
```
memex index
```

Modern OpenCode sessions stored in `opencode*.db` under
`~/.local/share/opencode` are discovered automatically, alongside OpenCode's
legacy JSON storage. To use one or more alternate data roots, set
`OPENCODE_DATA_DIR` to a comma-separated list of directories before running
the installed `memex` binary.

The default scan indexes Pi sessions from `~/.pi/agent/sessions` and Oh My Pi sessions
separately from `~/.omp/agent/sessions` plus named profile session directories.

Plaintext reasoning is excluded by default because it is usually low-value search noise. Opt
in with `memex index --include-reasoning`; reasoning records remain BM25-only. Encrypted and
redacted payloads, along with reasoning signature fields, are always excluded.

Search (JSONL default):
```
memex search "your query" --limit 20
```

Search output is compact by default. Each hit contains `machine`, `score`, `ts`,
`doc_id`, `record_id`, `project`, `role`, `session_id`, `source`, `source_path`,
`snippet`, and `matches`. Lexical snippets center the earliest literal match;
semantic-only hits use a compact prefix. Use `--fields` for an explicit projection or
`--full` to restore every legacy search field, including full record text and linkage
metadata.

Agent-facing TOON output is available for search:

```sh
memex search "your query" --format toon
```

It returns one TOON document with a `results` array and preserves the same values as
JSON output, including custom `--fields` and `--full`. `--format jsonl` is the default;
`--format json` returns one JSON array, like `--json-array`. Explicit `--format`
conflicts with `--json-array` and verbose output (`-v`).

TUI:
```
memex tui
```

Notes:
- Embeddings are disabled by default. Pass `--embeddings` to generate them during indexing.
- Searches run an incremental reindex by default (configurable).
- Index updates are copy-on-write generations. A writer builds a private generation and atomically
  publishes it when complete; searches keep using the previous immutable generation until then.
- Concurrent searches coalesce stale auto-index work: one process refreshes while other lexical
  searches query the last committed index. Semantic and hybrid searches wait for vector writes to
  finish. Explicit `index`, `reindex`, `embed`, and analytics backfill commands wait up to 30
  seconds for another index mutation to finish and report its holder on timeout.

Bounded transcript page:
```
memex session <session_id>
```

Single bounded record:
```
memex show <doc_id>
memex show --record-id <record_id> --machine <machine_id>
```

Read commands return at most 16,000 Unicode content characters by default. The budget
counts `text`, `tool_input`, and `tool_output` together; JSON metadata and wire bytes do
not count. Each record includes content metadata shaped like:

```json
{"returned_chars":16000,"total_chars":24000,"truncated":true,"continuations":[{"field":"tool_output","offset_chars":12000,"total_chars":20000}]}
```

Continue a truncated field from its reported Unicode character offset:

```sh
memex show --record-id <record_id> --machine <machine_id> \
  --field tool-output --offset-chars 12000
```

`--field` accepts `text`, `tool-input`, or `tool-output`. Use `--max-chars N` to
choose another shared budget. `--full` disables the content budget and conflicts with
`--max-chars`.

`memex session` returns 50 records by default and shares the 16,000-character budget
across them. Its JSONL stream ends with a `{"type":"page",...}` object containing
`offset`, `total`, and `next_offset`. Use `next_offset` with `--offset` to read later
records, and use `memex show` with a record's continuation metadata to finish a field
that was truncated within a record. `memex session --full` preserves the unbounded
transcript behavior; adding `--limit` bounds its record count.

Human output:
```
memex search "your query" -v
```

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
dropdown bounds the chart.

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
update it. Legacy document-ID `show`, `session`, and `hydrate` can explicitly use
`--full` for complete-content reads; remote context and stable-ID reads need an updated
peer in either mode. Memex does not silently fetch unbounded bodies as a fallback. Character limits apply before the peer
serializes content. They are not limits on JSON metadata or total network bytes.

Existing indexes remain readable: canonical record IDs fall back to a stored-record scan
until the index is rebuilt. Neighborhood reads use indexed session, source, and source-path
scope.

Session and hydrate pages are limited to 500 records. To fetch several sessions in one
bounded request, provide JSONL on stdin or as a file:

~~~json
{"machine":"mini","session_id":"SESSION_ID","source_path":"/path/on/mini/session.jsonl","offset":0,"limit":500}
~~~

~~~sh
memex hydrate requests.jsonl
cat requests.jsonl | memex hydrate
~~~

The batch input accepts at most 32 requests and returns one JSONL response per request in
input order, including machine provenance, `offset`, `total`, `next_offset`, stable
`record_id` values, and per-record content metadata. One 16,000-character budget is shared
across all requests in input order. Use `--max-chars N` to change it or `--full` for full
record content; `--full` conflicts with `--max-chars`. The request envelope's
`next_offset` resumes later records.

## Token usage

Token tracking is disabled by default because it scans and caches local agent logs. Enable it in `~/.memex/config.toml`:

```toml
token_usage = true
```

Then reconstruct historical token usage from local Claude Code, Codex, Cursor, OpenCode, Pi, Oh My Pi, OpenClaw, Copilot, Grok, Hermes, Jcode, and Muse records:

```
memex usage
memex usage --source codex --since 2026-07-01
memex usage --source grok --since 2026-07-01
memex usage --source hermes --since 2026-07-01
memex usage --json --events
```

`--cost auto` prefers a provider-stored request cost and otherwise applies the versioned built-in API price catalog. `--cost source` uses only stored costs; `--cost reprice` always applies the catalog. Calculated costs are API-equivalent estimates, not subscription charges. Events with unknown models or prices remain in token totals and are reported as unpriced.

Each source also reports prompt-cache efficiency: the cache hit rate, plus an estimate of cache waste — prompt tokens that were in the previous request's prompt but were re-billed at input rates instead of read from cache, priced at catalog rates and attributed to idle gaps past the cache TTL or model switches where those apply. Waste is estimated per transcript file chain and errs toward undercounting: subagent sidechains, ambiguous dedupe deltas, and prompts that shrink past compaction are not counted.

Local token history is reconstructed usage. It is deliberately kept separate from authoritative subscription quota percentages and reset windows. Hermes usage is read from `state.db` in the Hermes root and immediate profile directories (`HERMES_PROFILE_ROOTS`, `HERMES_HOME`, or `HERMES_STATE_DIR`, with safe local defaults), opened read-only and WAL-compatible. The `sessions` aggregate is used for legacy databases; newer `session_model_usage` delta rows are emitted by model/task and reconciled against the session aggregate so historical seeded rows count once and positive residuals are retained. Snapshots, backups, arbitrary nested databases, JSON/JSONL transcripts, and auth, config, memory, skills, plugins, and cron paths are excluded. Hermes queries never read message, system-prompt, tool, reasoning-text, or credential tables. Usage output contains counters and metadata only. Any API-equivalent cost estimate is analytical and is not a Hermes subscription quota measurement; source-stored API costs are not quota percentages. Hermes parser-version changes invalidate only Hermes usage cache rows.

When token tracking is enabled, press `Ctrl+T` on the TUI home screen to toggle the 30-day activity chart between session count and token volume. Token activity is loaded lazily and cached when first shown.

## Build from source

```
cargo build --release
```

Linux with NVIDIA CUDA support:

```
cargo build --release --features cuda
```

Binary:
```
./target/release/memex
```

## Setup (manual)

If you built from source, install the skill embedded in that build:

```bash
memex skill install --target shared
```

Omit `--target` for an interactive menu of detected Claude/Codex/OpenCode/Pi/Oh My Pi installations.
## MCP server

Run `memex mcp` to serve the Model Context Protocol over **Streamable HTTP** at
`http://127.0.0.1:5363/mcp` using the official Rust SDK (`rmcp`). Use
`--listen 127.0.0.1:5364` to change the socket and `--root /path/to/memex-data`
for a custom data directory. This is separate from Memex's internal `rpc` protocol.

For self-hosted ChatGPT or Claude access, enable the built-in single-owner OAuth
flow with the public origin of your instance:

```bash
memex mcp --public-url https://memex.example.com
```

Point your existing HTTPS reverse proxy at `http://127.0.0.1:5363`, forwarding
both `/mcp` and the OAuth/discovery routes at the root. The public URL must be an
origin, without a path prefix, query, or fragment. Plain HTTP is accepted only
for loopback development. Memex automatically allows the configured public Host.

Add `https://memex.example.com/mcp` as a custom connector in ChatGPT or Claude,
select OAuth where prompted, and leave client credentials empty to use automatic
registration. Memex presents one approval page showing the requesting client and
its redirect destination. Enter your instance's owner key on that page and
approve access. There is no separate login, signup, gateway, or identity provider.
Client names are supplied by the requesting app; inspect the redirect destination
before approving. Approval grants access to the history this instance can read,
including its configured remote machines.

The owner key is in `<root>/web-auth-token` (normally `~/.memex/web-auth-token`),
the same restricted key file used by Memex's web server. Startup prints its path,
never the key. Enter it only on your Memex instance's approval page; the chat
client receives its own access and refresh tokens, including when it requests only
`memex:read` or omits the scope. Access tokens last one hour; rotating refresh tokens
last 30 days from issuance. Grants survive server restarts.
To disconnect all OAuth clients, run:

```bash
memex mcp --revoke-all
```

Use the same `--root` as the server if customized. Revocation invalidates issued
OAuth grants without rotating the owner key. Existing direct bearer clients can
still use that key, so revoke-all does not disconnect those clients.

Without `--public-url`, HTTP uses the existing static bearer configuration:
`Authorization: Bearer <token>` from `web-auth-token`. For a browser that calls
Memex directly, allow its exact origin:

```bash
memex mcp --allowed-origin https://chat.example.com
```

Repeat `--allowed-origin` for multiple origins. Browser origins are denied by
default; clients without an Origin header still require bearer authentication.
CORS preflights do not require authentication. Remote clients need a reachable
HTTPS URL, typically through a TLS reverse proxy to the loopback listener; add
`--allowed-host memex.example.com` when the proxy preserves that Host header.
Binding to loopback alone does not make the server reachable by hosted chat apps.
For private ChatGPT setups, [Secure MCP Tunnel](https://developers.openai.com/api/docs/guides/secure-mcp-tunnels)
can instead connect to `memex mcp --transport stdio`; this requires OpenAI tunnel
access and a running tunnel client. It is an alternative connection path, separate
from the public HTTPS/OAuth setup above.

The transport follows the [2026-07-28 Streamable HTTP specification](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http):
one POST endpoint with request-scoped SSE responses and no protocol sessions or
standalone GET stream. The SDK also accepts older clients' initialize flow without
creating a session. HTTP disconnects cancel the request's wait for retrieval.

For local process clients, stdio remains available with `memex mcp --transport stdio`.
For clients using an `mcpServers` configuration:

```json
{
  "mcpServers": {
    "memex": { "command": "memex", "args": ["mcp", "--transport", "stdio"] }
  }
}
```

Use the absolute path to your built or installed `memex` binary if it is not on
the client's PATH. For a local build, run `mbx build` (or `cargo build` when
Boxington is unavailable) and use the resulting binary. Existing installed
versions need this change before they can run `mcp`.

| Tool | Behavior |
| --- | --- |
| `search` | Compact hits with provenance, match snippets, selected machines, and failures. Shares CLI ranking, multi-query fusion, filters and projection. |
| `sessions` | Recent local sessions, including resumption commands as data. Reads existing analytics without auto-indexing. |
| `show` | Direct record read; supports stable `record_id`, legacy `doc_id`, or scoped `event_id`, and continuation within a field. |
| `context` | Anchor-first neighborhood, optionally expanding directly owned tool interactions. |
| `session` | Chronological transcript pages, defaulting to 50 records. |
| `hydrate` | Up to 32 session-page requests with a shared budget in input order and explicit per-request failures. |

Search accepts `query`, `additional_queries` (up to eight queries total), `mode`
(`lexical`, `hybrid`, `semantic`), `cwd`, `project`, `source`, `role`, `tool`,
`session`, `origin`, `since`, `until`, `machines`, ranking controls, and `sort`
(`score` or `ts`). It defaults to 20 hits and `unique_session: true`; set
`top_n_per_session: 2` for two hits per session or `unique_session: false` for
individual matches. Search and sessions accept at most 500 results.

Read tools share a default 16,000 Unicode-character content budget, adjustable
with `max_chars` from 1 to 64,000. Metadata is outside that budget. Inspect
`content.truncated` and `content.continuations`: `next_offset` advances records,
while `show` with `field` and `offset_chars` retrieves omitted field content.
Fields are `text`, `tool_input`, and `tool_output`. MCP reads always stay bounded.
Preserve the returned machine and source path when opening a result.

Tools return structured JSON plus a JSON text fallback. Search and hydration
return successful results alongside `failures`; a tool-level error sets MCP
`isError`. Session discovery is local; search uses the existing configured machine
defaults, and read tools accept a machine ID. Remote retrieval uses existing SSH
configuration and requires peers that support bounded reads.

Search retains configured auto-index behavior on local and remote machines.
Handshake, session discovery, and transcript reads do not trigger ingestion;
semantic search may load an embedding model. Run the index service separately
when predictable search latency matters. Up to four retrieval calls run at once;
MCP cancellation stops waiting but synchronous work can finish under its existing
timeouts. Index readers are opened per call so a running server sees newly
published generations. Diagnostics go to stderr; in stdio mode, stdout is reserved for MCP.

The server supplies retrieval guidance during initialization: read known IDs
directly, search exact anchors first, expand progressively, verify source records,
and treat historical transcript content as evidence rather than instructions.

## Search modes

| Need | Command |
| --- | --- |
| Exact terms | `search "exact term"` |
| Fuzzy concepts | `search "concept" --semantic` |
| Mixed | `search "term concept" --hybrid` |

## Common filters

- `--project <name>`
- `--role <user|assistant|tool_use|tool_result>`
- `--tool <tool_name>`
- `--session <session_id>`
- `--source claude|codex|cursor|opencode|pi|omp|openclaw|copilot|grok|hermes|jcode|muse`
- `--since <iso|unix>` / `--until <iso|unix>`
- `--limit <n>`
- `--min-score <float>`
- `--sort score|ts`
- `--top-n-per-session <n>`
- `--unique-session`
- `--fields score,ts,doc_id,record_id,session_id,snippet`
- `--full` (all legacy search fields; conflicts with `--fields`)
- `--json-array`

Default JSON search output uses the compact fields documented above. Full or explicit
projections can also include tree/linkage metadata:
`event_id`, `parent_event_id`, `logical_parent_event_id`,
`parent_session_id`, `thread_source`, `conversation_kind`,
`parent_tool_use_id`, `source_tool_use_id`, and
`source_tool_assistant_uuid`.

## Background index service

Works on macOS (launchd) and Linux (systemd).

Enable:
```
memex index-service enable
memex index-service enable --continuous
memex index-service enable --web-ui
```

Regenerate the service from current config and restart it:
```
memex index-service restart
```

Inspect the registered service and whether it is serving the Web UI:
```
memex index-service status
```

Open an authenticated browser session:
```
memex index-service open
```

Disable:
```
memex index-service disable
```

`index-service` reads config defaults (mode, interval, log paths). Flags override.

### Reclaiming obsolete index generations

After an upgrade, normal indexing automatically migrates a legacy index and removes obsolete
pre-lease generations. It preserves the committed Tantivy segments without rebuilding or reparsing
conversation history. No user action is required.

For diagnostics or to reclaim space immediately without waiting for the next index run, stop the
background service and close TUI/Web readers, then preview and run GC:

```bash
memex index-service disable
memex index-gc --dry-run
memex index-gc --offline
memex index-service enable --web-ui # or restore the mode you previously used
```

`index-gc` validates the committed index, hard-links only its live Tantivy segments into a clean
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
memex index-service enable --web-listen 127.0.0.1:8080
memex index-service open --listen 127.0.0.1:8080
```

The first Web UI start creates `~/.memex/web-auth-token` with mode `0600`. Private API
routes require that token as `Authorization: Bearer ...` or a browser session established
by `index-service open`. Browser links carry a signed, one-time credential in the URL
fragment, remove it before navigation continues, and exchange it for an ephemeral bearer
token held only in page memory. The browser token is never stored in a cookie,
`localStorage`, or session storage. Browser sessions expire after 12 hours, disappear when
the page closes, and are invalidated whenever the daemon restarts.

To run the same UI in the foreground without changing the background service:

```
memex web
```

Then run `memex index-service open` from another terminal.

The browser frontend lives in `web/`, uses React and shadcn components, and is
built with `cd web && bun install && bun run build`. The generated static assets
are embedded in the memex binary, so serving the UI does not add a JavaScript
runtime to the daemon.

## Embeddings

Enable during indexing:
```
memex index --embeddings
```

Recommended when embeddings are on (especially non-`potion` models): run the background
index service or `index --watch`, and consider setting `auto_index_on_search = false`
to keep searches fast.

## Embedding model

Select via `--model` flag or `MEMEX_MODEL` env var:

| Model | Dims | Speed | Quality |
|-------|------|-------|---------|
| minilm | 384 | Fastest | Good |
| bge | 384 | Fast | Better |
| nomic | 768 | Moderate | Good |
| gemma | 768 | Slowest | Best |
| potion | 256 | Fastest (tiny) | Lowest |

```
memex index --model minilm
# or
MEMEX_MODEL=minilm memex index
```

## Execution provider

Select via `execution_provider` in config or `MEMEX_EXECUTION_PROVIDER`:

| Provider | Platforms | Notes |
|----------|-----------|-------|
| auto | all | Default. Uses CoreML on macOS, CPU elsewhere |
| cpu | all | Force CPU execution |
| coreml | macOS | Uses CoreML; `compute_units` controls ane/gpu/cpu/all |
| cuda | Linux/NVIDIA | Requires a binary built with `--features cuda` and CUDA 12/cuDNN runtime libraries |

When `execution_provider = "cuda"`, you can optionally select a GPU with
`cuda_device_id` or `MEMEX_CUDA_DEVICE_ID`.

When loading CUDA, memex first tries the system loader paths, then any
configured `cuda_library_paths` / `cudnn_library_paths`, then common CUDA install
locations and active `venv` / `conda` `site-packages/nvidia/*/lib` directories.
If your system keeps CUDA or cuDNN in a nonstandard location, set
`MEMEX_CUDA_LIBRARY_PATHS` and `MEMEX_CUDNN_LIBRARY_PATHS` or the matching config
keys.

## Config (optional)

Create `~/.memex/config.toml` (or `<root>/config.toml` if you use `--root`):

```toml
embeddings = true
auto_index_on_search = true
include_reasoning = false  # opt in to plaintext reasoning; encrypted/redacted payloads stay excluded
token_usage = false  # opt in to local token and cost tracking
model = "minilm"  # minilm, bge, nomic, gemma, potion
execution_provider = "auto"  # auto, cpu, coreml, cuda
cuda_device_id = 0  # optional, when execution_provider = "cuda"
cuda_library_paths = ["/usr/local/cuda/lib64"]  # optional list of CUDA library dirs
cudnn_library_paths = ["/usr/lib/x86_64-linux-gnu"]  # optional list of cuDNN library dirs
compute_units = "ane"  # CoreML only: ane, gpu, cpu, all
scan_cache_ttl = 3600  # seconds (default 1 hour)
max_indexed_tool_input_bytes = 65536  # 64 KiB default
max_indexed_tool_output_bytes = 262144  # 256 KiB default
exclude_paths = ["~/.claude/projects/*-client-*", "~/work/**"]  # never index matched transcripts
index_service_mode = "interval"  # interval or continuous
index_service_interval = 3600  # seconds (ignored when mode = "continuous")
index_service_poll_interval = 30  # seconds
index_service_web_ui = false  # serve local browser; forces continuous mode when true
index_service_web_listen = "127.0.0.1:6363"
index_service_label = "memex-index"  # service name (default: com.memex.index on macOS)
index_service_systemd_dir = "~/.config/systemd/user"  # Linux only
claude_resume_cmd = "claude --resume {session_id}"
codex_resume_cmd = "codex resume {session_id}"
cursor_resume_cmd = "cursor-agent --resume {session_id}"
opencode_resume_cmd = "opencode resume {session_id}"
pi_resume_cmd = "pi --session {source_path_shell}"
# copilot_resume_cmd = "your-copilot-resume-command {session_id}"
grok_resume_cmd = "cd {cwd_shell} && grok --resume {session_id}"
jcode_resume_cmd = "cd {cwd_shell} && jcode --resume {session_id}"
muse_resume_cmd = "cd {cwd_shell} && muse resume {session_id}"
herdr_resume = "tab"  # inside a herdr pane: "tab" (default), "split", or "off"
```

Service logs and the plist live under `~/.memex` by default (macOS). On Linux, systemd units are created in `~/.config/systemd/user/`.

`scan_cache_ttl` controls how long auto-indexing considers scans fresh.
`include_reasoning` defaults to false. Set it to true (or pass `memex index
--include-reasoning`) to add plaintext reasoning as BM25-only records. Encrypted
and redacted reasoning payloads are always excluded.
`max_indexed_tool_*_bytes` limits oversized tool payloads while leaving user and assistant text
unchanged. memex keeps roughly the first three quarters and final quarter, with a marker reporting
the omitted middle. Each value must be at least 1024 bytes. Run `memex index --reindex` to apply
new limits to records that are already indexed.
`exclude_paths` takes glob patterns matched against transcript source paths at index time, so
matched transcripts never enter the index (a leading `~/` is expanded to your home directory).
Adding a pattern also removes records previously indexed from matched paths — no `--reindex`
required. For one-off runs, pass `--exclude GLOB` (repeatable) to `memex index`.
`execution_provider` applies to ONNX-backed models; `potion` uses the model2vec backend.
`cuda_library_paths` and `cudnn_library_paths` accept path lists and are only used
when `execution_provider = "cuda"`.

Resume command templates accept `{session_id}`, `{project}`, `{source}`, `{source_path}`, `{source_dir}`, `{cwd}`, plus shell-quoted `{source_path_shell}`, `{source_dir_shell}`, and `{cwd_shell}`.

The skill definitions are bundled in `skills/`.

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
