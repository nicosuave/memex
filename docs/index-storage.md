# Shared immutable index files

New publishing generations store metadata and flat references to immutable segment files. Unchanged segment payloads are not linked or copied into each new generation.

```text
index/
  CURRENT
  generations/<generation>/
    meta.json
    .managed.json
    .storage-format
    .segments.json
    .lease
  segments/
    .lock
    <creating-generation>/<segment-file>
```

`.segments.json` is versioned and maps logical Tantivy filenames to their creating generation. Owner IDs namespace files so independent staging branches cannot collide when they generate the same logical deletion filename. References never form chains.

## Publication

A writable directory reads inherited files from the shared store and writes new files locally. Deleting an inherited logical file does not delete its shared physical file. Published views reject writes.

After the index writer finishes, new committed files enter the shared store once. Their local links can remain in the creating generation until it is pruned. The file data and reference/format metadata are made durable before the generation pointer changes. Metadata already durably written by Tantivy is not synchronized a second time. The store guard serializes reference publication and reclamation, not parsing or index construction.

A failed publication leaves the previous `CURRENT` intact until the new reference set is ready. A crash after the pointer changes uses the existing ingestion recovery protocol.

### macOS staging durability

Private ingest generations on eligible local APFS/HFS layouts synchronize Tantivy file writes and interim directory updates with `fsync`. Eligibility requires matching device identities for the index root, generations directory, staging directory, segment store, and retained lease descriptor, plus successful `F_FULLFSYNC` support. Unsupported or cross-device layouts use the original synchronization path; non-macOS behavior is unchanged.

The initial lease-file synchronization establishes eligibility. Publication preparation includes owner/store directory entries, generation metadata, and the manifest/format files in the same checked `fsync` batch. The pre-rename lease-file synchronization then issues a strict `F_FULLFSYNC`. macOS guarantees that this drains earlier `fsync` operations on the same device. Output descriptors and adoption destinations are checked against that device; changed directory or lease identities abort publication. Copy-fallback synchronization and preparation without a capability retain their original full-strength behavior.

The generation rename and `CURRENT` replacement retain their original strong directory synchronization before reclamation. Pending-intent persistence, analytics, vectors, post-publication checkpoints, and pending-intent removal keep their existing durability boundaries. Intermediate private Tantivy commits are not independently power-loss durable; the publication boundary is.

Failure-injection tests check synchronization order, unchanged `CURRENT` after a failed barrier, and safe retry. They do not simulate physical power loss.

## Readers and collection

The current generation, leased readers, and live staging generations retain their shared references. Normal pruning removes unleased generations before collecting unreachable shared files. Offline GC also sweeps orphaned files and reports `shared_files_removed`.

Each cleanup pass requests its existing trailing directory synchronization only after a removal attempt. Current/leased generations and other no-candidate passes skip that synchronization. Attempts count conservatively because recursive deletion can partially succeed before a permission error. Empty shared-owner directory removal also triggers synchronization, even when the file-removal count is zero; dry runs never synchronize. Publication and recovery barriers are unchanged. A crash after failed cleanup can leave unreachable garbage for a later scan to reclaim without invalidating the durable `CURRENT`.

Malformed references, unsupported versions, traversal paths, missing referenced files, and symlinked shared data fail closed. Shared files are reclaimed only when no retained generation or staging manifest references them.

## Compatibility

Old flat and full-directory indexes remain readable. Their committed files are adopted once when migration is published; the first migration can be slower than a steady-state update. Explicit indexing may publish this format upgrade even when no records changed.

Older binaries cannot read the shared-reference layout. Upgrade every process using an index before migrating it. Do not roll back to an older binary against a migrated index; restore a pre-migration snapshot or rebuild using the older version.

See [shared-segment measurements](shared-segments-benchmark.md) for migration-excluded latency and paired trace/flamegraph results.
