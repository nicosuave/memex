# Preserve embeddings across lexical index upgrades

An index format or parser upgrade may change record IDs without changing the
text sent to the embedding model. `vector_transfer` exports existing vectors by
the SHA-256 of their exact embedding input: the first 8192 UTF-8 bytes, shortened
to a character boundary. Import associates those inputs with the new IDs.

The helper performs no inference. Its SQLite cache records the model, dimensions,
source snapshot, and completion state. Interrupted exports can resume against the
same stopped snapshot. Import rejects incomplete or incompatible caches, checks
every reused f32 value before publishing, and checks again after reopening.
Duplicate inputs share one cached vector. Use the same model and model files on
both sides; a matching model name does not verify changed model weights.

This is an offline operator workflow. Stop the daemon, embedding workers, and
other writers before exporting; keep them stopped until the staged root is ready.
Use a cache outside both roots. Never run `index rebuild` on the original root
when preserving its vectors. Automatic parser-version rebuilds are unchanged by
this helper; complete the staged migration before starting the new daemon.
Stale-schema startup now requires operator action even when embeddings are disabled.
If there are no vectors to preserve, explicitly rebuild instead of using this export.

## Build readers for both storage versions

The following example upgrades an upstream v0.19.0 index. From a checkout that
contains this helper, create an isolated v0.19.0 exporter. That release needs one
read-only vector accessor, supplied by the compatibility patch below. Other old
versions must be checked against their own APIs and input truncation semantics.
The source vector store must have a committed `vectors/current.json` generation.

```sh
MIGRATION_REF=$(git rev-parse HEAD)
OLD_TOOLS=$(mktemp -d)/memex-old-tools
git worktree add --detach "$OLD_TOOLS" v0.19.0
git show "$MIGRATION_REF:src/vector_transfer.rs" > "$OLD_TOOLS/src/vector_transfer.rs"
git show "$MIGRATION_REF:examples/vector_transfer.rs" > "$OLD_TOOLS/examples/vector_transfer.rs"
printf '\npub mod vector_transfer;\n' >> "$OLD_TOOLS/src/lib.rs"
git show "$MIGRATION_REF:docs/vector-transfer-v019.patch" |
  git -C "$OLD_TOOLS" apply -
cargo build --release --manifest-path "$OLD_TOOLS/Cargo.toml" --example vector_transfer
cargo build --release --bin memex --example vector_transfer
OLD_EXPORTER="$OLD_TOOLS/target/release/examples/vector_transfer"
NEW_HELPER="$PWD/target/release/examples/vector_transfer"
NEW_MEMEX="$PWD/target/release/memex"
```

## Export, rebuild separately, and verify

Set the model and dimensions to the original store's values (`memex stats`), and
use the existing model cache. This example uses BGE, not a model conversion.
The original raw transcript files must still be available for lexical rebuilding.

```sh
umask 077
OLD_ROOT="$HOME/.memex"
STAGE="$HOME/.memex-migration-stage"
CACHE="$HOME/memex-vector-migration.sqlite3"
test ! -e "$STAGE" && mkdir -m 700 "$STAGE"
cp -p "$OLD_ROOT/config.toml" "$STAGE/config.toml"
"$OLD_EXPORTER" export --root "$OLD_ROOT" --cache "$CACHE"
"$NEW_MEMEX" index rebuild --root "$STAGE" --no-embeddings --non-interactive
"$NEW_HELPER" import --root "$STAGE" --cache "$CACHE" --model bge --dimensions 384
"$NEW_HELPER" verify --root "$STAGE" --cache "$CACHE" --model bge --dimensions 384
```

Check `matched`, `missing`, and `unmatched_exported`. Investigate every unmatched
exported input before switching; absent sources or parser exclusions can reduce
coverage. On import, `existing` counts vectors already present in the destination,
so a retry does not duplicate them. A new lexical record with no matching cache entry stays
missing; the helper never invents a replacement or silently changes models.

Preserve the original `memory` directory and `web-auth-token`, if present, in the
staged root while writers remain stopped. Replace only the generated staged
memory directory; keep the original untouched. For compatible memory formats this
retains documents, vectors, and the section embedding cache. Validate memory
search and counts before switching. The helper does not migrate memory formats.

Run `memex embed --root "$STAGE" --model bge` with the existing model cache to
compute only genuinely missing conversation inputs. Then run `verify` again and
test lexical, semantic, memory, context, and session queries with `--machine local`
and `--root "$STAGE"`. Keep the original database until the new binary and staged
root have been installed and service restart checks pass. Directory renames must
stay on the same filesystem; preserve permissions and configuration.

After successful validation, remove the old root, SQLite export, and temporary
exporter worktree/build artifacts. Do not delete the model cache used by the
running service. `git worktree remove --force "$OLD_TOOLS"` removes the temporary
exporter checkout and its generated files.
