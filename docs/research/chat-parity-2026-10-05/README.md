# Memex capability inventory

Open [index.html](index.html) for the local filterable report; it embeds its data and needs no CDN or network request. [report.md](report.md) is the readable companion; [findings.json](findings.json) contains every mapped source finding and evidence record.

The inventory describes its pinned research baseline. [Implementation follow-through](implementation.md) maps the subsequent changes, validation, and capability boundaries for all 24 actionable Memex findings.

Serve using the root-owned server: `bun serve.ts`, then open http://127.0.0.1:4782. The HTML also works directly from disk. Use Print for a print-friendly rendering of the current filtered findings; choose All first for the complete inventory.

Default view: actionable Memex missing/partial/defect rows. All also includes present capabilities, comparator defects, validation unknowns and scope exclusions. Search covers workflows, comparator details, recommendations, source paths, anchors and raw IDs; category, comparator, priority and status filters combine.

Local Codex/runtime evidence paths are selectable and copyable, not broken HTTP links. Installed Codex evidence includes exact version, asset hash and archive hash; public source links use immutable commits. No proprietary source is embedded.

72 raw findings map to 54 unified workflows with no dropped IDs. Product behavior was source-inspected; no new build/test/runtime campaign was performed. See the report for limits and editorial priority policy.
