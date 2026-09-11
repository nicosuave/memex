# Directory inventory reuse

The directory inventory caches complete immediate child names and types, not file contents or source freshness. TTL-0 requests still reconcile sources without a daemon. Claude, Codex rollout, Pi and Omp discovery share a request-owned inventory; other discovery paths retain their existing traversal and database checks.

Every traversed directory is checked independently. An unchanged ancestor does not certify its descendants. Candidate files still pass through the existing parallel metadata classifier on every request, including ctime-based detection of equal-size rewrites with restored mtime. Directory reuse does not bypass parser, database or WAL checks.

## Eligibility

Reuse is supported only on local APFS volumes on macOS with the required native attributes available. Other filesystems and platforms enumerate normally; there is no timestamp-only fallback.

A cached listing must match the current discovery projection and all of these observations:

- Boot-session UUID and system mount/unmount operation counter (`kern.bootsessionuuid`, `vfs.nummntops`). A changed epoch invalidates reuse, including after an unrelated mount operation.
- Volume UUID and filesystem ID, with the volume identified as local APFS.
- Directory device, inode, birthtime, modification time and change time.
- A returned, nonzero `ATTR_CMN_GEN_COUNT`, requested through `FSOPT_ATTR_CMN_EXTENDED` and checked against the returned-attributes bitmap.

Timestamps retain separate seconds and nanoseconds without saturation. Birthtime must be valid and nonfuture. Modification and change times must have valid, nonzero fractional components and satisfy `now.seconds - timestamp.seconds > 1`. Recent, coarse-looking, invalid or future timestamps prevent reuse. This age restriction is not a claim about APFS clock precision. Nanosecond storage does not make timestamps unique generations.

Cached observation times and request boundary observations reject detected wall-clock regression. Epoch availability and equality are checked before and after traversal and before producing a replacement inventory.

## Traversal and fallback

Directories are opened without following a final symlink. Native enumeration uses a duplicate of the open directory descriptor. Eligible listings require matching fingerprints before and after observation, and reopening the pathname must identify the same directory. Cached child types are accepted only under the matching parent fingerprint; each child directory receives its own validation before descent.

Changed and newly discovered directories are enumerated recursively, including empty directories and files without indexed checkpoints. Missing roots are reconsidered on later requests. Source-specific filtering, sorting and deduplication remain outside the inventory.

Unavailable eligibility information causes enumeration rather than a source omission. A listing error or unstable eligible observation discards the accelerated root result and uses the original `WalkDir` traversal. Partial enumerations are never stored as complete listings. Surviving traversal errors remain errors, not cached empty directories.

Relative roots and symlink roots use the original walker. Root symlinks retain its default traversal behavior; interior symlinks are not followed. Paths presented to source filters retain their original spelling.

## Storage and bounds

`ScanCache.directory_inventory` is optional and versioned. Its discovery projection identifies the effective roots and options. A missing, malformed, incompatible or invalid inventory cannot remove source coverage. Listings are looked up only for directories reached from the current requested roots.

Unix paths and child names are serialized as native byte arrays, without lossy UTF-8 conversion. Child names must be nonempty immediate components: no NUL, slash, absolute path, `.` or `..`. Relative directory paths cannot contain root or parent components. Duplicate directory keys, duplicate child names and invalid child ranges reject the inventory.

The admitted inventory limits in `src/directory_inventory.rs` are:

| Item | Limit |
|---|---:|
| Directories | 4,096 |
| Immediate children across all directories | 65,536 |
| Each encoded root or relative directory path | 4,096 bytes |
| Each child name | 1,024 bytes |
| Discovery projection | 65,536 bytes |
| Combined root, relative-path, child-name and projection bytes | 16 MiB |

Scan-cache loading accepts at most 80 MiB of JSON, reading at most one additional byte to detect overflow before parsing. Oversized files load as an empty optional cache. Sequence limits are enforced during typed deserialization; combined byte counts and structural validity are checked before reuse. The 16 MiB limit describes admitted native bytes, not JSON size or total allocation. Exceeding inventory limits disables storage, not filesystem traversal.

Only complete listings from successful discovery are staged for the existing scan-cache finalization. Storage uses the existing atomic `scan_cache.json` save under the ingestion lease; see [refresh boundaries](refresh.md). Invalidation is serialized explicitly as `"directory_inventory": null`, never omitted or merged with a previous inventory. The inventory adds no independent publication authority.

## Correctness boundary

The content-generation attribute is distinct from `stat.st_gen` and from directory entry count. Darwin documents an unchanged nonzero content generation as indicating unchanged data for the same filesystem object. Identity and epoch checks preserve that comparison's object and mount context; ctime retains the existing file fast path's protection against restored mtime.

The guarantee uses the existing metadata fast path's ordinary-filesystem-mutation model, not an immutable snapshot or adversarial replay model. APFS permits its 32-bit write-generation counter to wrap. Matching generation alone is therefore insufficient: the identity and timestamp checks remain mandatory. Counter wrap combined with replayed or colliding identity/timestamps, or clock rollback that reverses entirely between observations, is outside this model. Concurrent changes after a directory observation remain possible, as with ordinary traversal.

Directory reuse neither requires nor substitutes for [event-driven indexing](event-driven-indexing-spec.md).

Native contracts: [Darwin `getattrlist`](https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/man/man2/getattrlist.2), [APFS reference, inode timestamps and write-generation counter](https://developer.apple.com/support/downloads/Apple-File-System-Reference.pdf), and [XNU mount-operation counter](https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/vfs/vfs_syscalls.c).
