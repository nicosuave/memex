# Token usage and cost estimates

[Back to Memex](../README.md)

## Token usage

Token tracking is disabled by default because it scans and caches local agent logs. Enable it in `~/.memex/config.toml`:

```toml
token_usage = true
```

Then reconstruct historical token usage from local Claude Code, Codex, Cursor, OpenCode, Pi, Oh My Pi, OpenClaw, Copilot, Grok, Hermes, Jcode, Muse, and IBM Bob records:

```
memex usage
memex usage --source codex --since 2026-07-01
memex usage --source grok --since 2026-07-01
memex usage --source hermes --since 2026-07-01
memex usage --format json --events
```

Coverage depends on the counters saved by each engine. Cursor usage is read from
local databases, separately from its searchable agent transcripts. Copilot usage
is read from local OpenTelemetry JSONL exports under `~/.copilot/otel` (or
`COPILOT_HOME/otel`) and `COPILOT_OTEL_FILE_EXPORTER_PATH`; its session transcripts
alone are insufficient. Antigravity history is searchable, but its token buckets
are not yet validated and Memex does not report usage for it.

`--cost auto` prefers a provider-stored request cost and otherwise applies the versioned built-in API price catalog. `--cost source` uses only stored costs; `--cost reprice` always applies the catalog. Calculated costs are API-equivalent estimates, not subscription charges. Events with unknown models or prices remain in token totals and are reported as unpriced.

Each source also reports prompt-cache efficiency: the cache hit rate, plus an estimate of cache waste — prompt tokens that were in the previous request's prompt but were re-billed at input rates instead of read from cache, priced at catalog rates and attributed to idle gaps past the cache TTL or model switches where those apply. Waste is estimated per transcript file chain and errs toward undercounting: subagent sidechains, ambiguous dedupe deltas, and prompts that shrink past compaction are not counted.

Local token history is reconstructed usage. It is deliberately kept separate from authoritative subscription quota percentages and reset windows. Hermes usage is read from `state.db` in the Hermes root and immediate profile directories (`HERMES_PROFILE_ROOTS`, `HERMES_HOME`, or `HERMES_STATE_DIR`, with safe local defaults), opened read-only and WAL-compatible. The `sessions` aggregate is used for legacy databases; newer `session_model_usage` delta rows are emitted by model/task and reconciled against the session aggregate so historical seeded rows count once and positive residuals are retained. Snapshots, backups, arbitrary nested databases, JSON/JSONL transcripts, and auth, config, memory, skills, plugins, and cron paths are excluded. Hermes queries never read message, system-prompt, tool, reasoning-text, or credential tables. Usage output contains counters and metadata only. Any API-equivalent cost estimate is analytical and is not a Hermes subscription quota measurement; source-stored API costs are not quota percentages. Hermes parser-version changes invalidate only Hermes usage cache rows.

When token tracking is enabled, press `Ctrl+T` on the TUI home screen to toggle the 30-day activity chart between session count and token volume. Token activity is loaded lazily and cached when first shown.
