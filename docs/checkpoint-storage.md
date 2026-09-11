# Checkpoint storage

Ingest checkpoints bind source offsets, parser state, OpenCode ownership, the document-ID allocator, pending recovery intent, and scan freshness to published records. Analytics remains independently rebuildable.

## Schema and access

`state/checkpoints.sqlite` has two tables:

| Table | Columns |
| --- | --- |
| `metadata` | Singleton key `singleton=1`; `format_version=2`; opaque `store_id`; migration/bootstrap `origin`; canonical unsigned decimal TEXT `next_doc_id`; complete JSON object TEXT `opencode_databases` and `legacy_extras`; nullable JSON object TEXT `pending_json` and `scancache_json`. |
| `files` | Exact, unnormalized TEXT primary key `path`; complete JSON TEXT `payload`; validated, generated, stored signed INTEGER `mtime`, indexed by `files_mtime`. |

Unsigned payload values remain JSON integers, including `u64::MAX`. Only `mtime` is extracted into SQLite INTEGER. Private codecs preserve unknown top-level, file, identity, pending-tool-call, OpenCode database, pending-intent, and scan-cache fields. Pending scope extensions follow `(source_path, session_id)`, never array position. Updates replace known fields without restoring removed optional fields; deleting an entity deletes its extensions. Conflicting extensions for duplicate scope identities fail rather than silently discard data.

`CheckpointReader` opens read-only and never creates the database, authority marker, or lifecycle lock. Its v2 header reads allocator, OpenCode map, pending intent, and scan cache in one row query. It also exposes requested file rows with explicit absent results, key-only queries, indexed recent rows, and administrative exports. Statements and read transactions end before callers perform filesystem probes. Missing intent is `None`; invalid pending JSON is fatal. Missing or malformed scan-cache values produce an expired default cache. A valid cache JSON object retains opaque fields even when known fields require defaulting.

Every writer configures and reads back WAL mode, `synchronous=FULL`, `fullfsync=OFF`, `checkpoint_fullfsync=OFF`, `wal_autocheckpoint=1000`, and a 16 MiB `journal_size_limit`. Checkpoint commits are therefore `fsync`-grade, the same as the analytics store; the index publication flush that follows them drains those writes to the drive. SQLite `NO_CKPT_ON_CLOSE` disables implicit close-time checkpoints. These thresholds do not impose a hard WAL-size bound while readers pin frames. Explicit maintenance runs `wal_checkpoint(TRUNCATE)` and requires zero busy, log, and remaining checkpoint counts; an incomplete truncate is an error.

## Publication

`commit_intent(&PendingIngest)` durably updates only pending intent in its own transaction before `WriterDecision::Commit` permits any analytics or index mutation. It cannot publish staged file deltas, allocator changes, OpenCode state, or cache updates. Failure leaves the previously committed checkpoint intact.

After lexical/vector publication and the existing analytics completion step, `commit_delta` atomically applies explicit file changes, allocator/OpenCode updates, optional scan-cache replacement, and `PendingChange::{Keep, Replace, Clear}`. Deferred recovery replaces intent; completed recovery clears it in this final transaction. Pending-only finalization supports vector-only recovery. Full scans may commit cache-only changes; targeted runs leave cache unchanged. A completely empty delta opens no write transaction. Failed finalization rolls back every field and leaves early intent available for recovery.

## Authority and migration

`state/ingest.json` is either a legacy state object or a JSON marker string:

```json
"memex-checkpoints:2:<64-lowercase-hex-store-id>"
```

The marker identity must equal `metadata.store_id`. Its string shape makes old struct deserializers reject migrated state instead of treating it as empty.

| Marker/database | Pending and cache authority |
| --- | --- |
| Missing or legacy JSON | Legacy sidecars; read-only access does not initialize storage. |
| v1/v1 | Legacy sidecars until a writer upgrades. |
| v1/v2, matching identity and complete valid v2 metadata | Database columns only; readers do not mutate and writers finish activation without reimport. |
| v2/v2 | Database columns only; stale sidecars never override them. |
| v2/v1, unsupported version, mismatched identity, corrupt required metadata, or missing database/lock | Fail closed. |

Under the caller's ingest lease and an exclusive lifecycle lease:

1. Reread authority. Validate pending sidecar contents strictly and treat malformed cache contents as expired. Preserve exact existing sidecar bytes in content-addressed, no-clobber archives. Direct legacy migration also archives the ingest state.
2. For v1, transactionally add both nullable columns, import pending/cache documents, and set database version 2. The actual v1 schema permits the version update. Preserve identity, origin, file rows, allocator, OpenCode state, and extensions. Direct legacy migration imports all three documents in one transaction without an intermediate v1 activation.
3. Complete checked WAL truncation and synchronize the database and containing directory. Atomically publish and synchronize the v2 marker.
4. Only after activation, remove obsolete active pending/cache sidecars. Synchronize the directory whenever removal is attempted, including interrupted cleanup. Retain archives. Release the exclusive lifecycle lease before opening the ordinary shared-leased writer.

A crash after the v1 upgrade transaction but before marker replacement leaves the certified v1/v2 transition. Retrying finishes synchronization and marker activation without reimporting stale sidecars. Old v1 writers reject database version 2 before configuring writes, and reject marker version 2. Interrupted legacy imports still retry from the current authoritative legacy documents rather than archived backups. Backups alone never authorize recovery or rollback.

Missing authority permits initialization at document ID 1 only after the caller validates an empty index or pending-intent coverage of all indexed records and document IDs. Existing vector-recovery requirements still apply. Missing-authority first-ingest recovery can read legacy intent before writer bootstrap. Interrupted empty initialization requires a matching durable `bootstrap:<store-id>` receipt in the lifecycle lock, empty metadata/files, and NULL pending/cache columns. Unexplained intent/cache, populated state, or an unidentified database requires a consistent restore or explicit rebuild.

## Lifecycle and compatibility

The persistent `state/.checkpoints.lock` protects database-handle lifetime. Connections hold shared leases. Upgrade, initialization, stale-sidecar cleanup, and reset take exclusive leases after the ingest lease; shared leases are never upgraded. Lock and SQLite contention produce bounded errors. Reset removes active checkpoint artifacts and legacy sidecars while retaining archives and the lifecycle lock inode. Live readers must close before reset can finish.

`IngestState::load` remains a read-only full-snapshot adapter. `save` writes legacy JSON only and refuses marker replacement. `save_with_lease` updates the ingest snapshot without changing pending/cache state. Ordinary ingestion uses sparse deltas.

`PendingIngest::load` and `ScanCache::load` route by authority/version. Their legacy setters support legacy/v1 sidecars and refuse SQL-v2 roots. `save_with_lease` and `PendingIngest::clear_with_lease` accept the existing sidecar path and caller's ingest lease for legitimate fixtures and administration. They do not reacquire the ingest lease. Generic `atomic_write` remains available for legacy files, markers, and backups.

The canonical generated-artifact filter is `state::checkpoint::is_checkpoint_artifact_name`; its artifact definitions also govern reset and archive recognition. Watch consumers query current checkpoint rows rather than infer commits from database, WAL, or marker modification times. Rollback requires a consistent root snapshot or rebuild; restoring a stale legacy backup over a marker is not a safe rollback.

## Diagnostics

Profiling builds record fixed spans `state.checkpoint.open_reader`, `state.checkpoint.open_writer`, `state.checkpoint.validate`, `state.checkpoint.lock_wait`, `state.checkpoint.commit_intent`, `state.checkpoint.commit_delta`, and `state.checkpoint.upgrade`, alongside existing `state.checkpoint.load_files` and `state.checkpoint.maintenance` spans. `state.checkpoint.early_intent_writes` counts successful early transactions; `state.checkpoint.transactions` counts successful final delta transactions, excluding reads and migration. `state.checkpoint.lock_contention` counts actual `WouldBlock` attempts. Existing row/key counters remain unchanged. Instrumentation compiles out without the profiling feature and records no paths.

Administrative checks use `CheckpointReader::export_json()` for the unchanged complete ingest-state shape, `export_pending_json()` and `export_scan_cache_json()` for full opaque logical sidecar documents, and `CheckpointWriter::checkpoint()` for charged terminal maintenance. Exports distinguish absent documents from JSON objects; typed `snapshot()` remains unchanged. Content-addressed archives preserve original bytes rather than current logical state.
