use super::{FileIdentity, FileState, OpencodeDatabaseOutcome, SourceKind};
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FileChange {
    New,
    Unchanged,
    Append,
    Replaced,
    ParserChanged,
}

impl FileChange {
    pub fn replaces_records(self) -> bool {
        matches!(self, Self::Replaced | Self::ParserChanged)
    }
}

pub(super) fn classify_file(
    source: SourceKind,
    size: u64,
    mtime: i64,
    identity: &FileIdentity,
    parser_version: u32,
    previous: Option<&FileState>,
) -> FileChange {
    let Some(previous) = previous else {
        return FileChange::New;
    };
    // Claude and Codex number their records with a legacy ordinal. State written before
    // that ordinal was tracked cannot resume without renumbering, so it reparses once.
    let tracks_legacy_ordinal = matches!(source, SourceKind::Claude | SourceKind::Codex);
    if previous.parser_version != parser_version
        || (tracks_legacy_ordinal && previous.offset > 0 && previous.legacy_turn_id.is_none())
    {
        return FileChange::ParserChanged;
    }
    if size < previous.size
        || previous.offset > size
        || mtime < previous.mtime
        || file_was_replaced(&previous.identity, identity)
        || previous.identity.sqlite_wal != identity.sqlite_wal
        || (size == previous.size
            && (previous
                .identity
                .modified_ns
                .zip(identity.modified_ns)
                .is_some_and(|(old, new)| old != new)
                || previous
                    .identity
                    .changed_ns
                    .zip(identity.changed_ns)
                    .is_some_and(|(old, new)| old != new)
                || mtime != previous.mtime))
    {
        return FileChange::Replaced;
    }
    if size == previous.size && mtime == previous.mtime {
        return FileChange::Unchanged;
    }
    // A jcode session is one JSON object and an antigravity store is rewritten wholesale
    // as the conversation grows, so a byte offset cannot resume mid-file for either.
    if matches!(source, SourceKind::Jcode | SourceKind::Antigravity) {
        FileChange::Replaced
    } else {
        FileChange::Append
    }
}

pub(super) fn file_was_replaced(previous: &FileIdentity, current: &FileIdentity) -> bool {
    let prefix_matches = previous
        .prefix_sha256
        .as_ref()
        .zip(current.prefix_sha256.as_ref())
        .is_some_and(|(old, new)| {
            previous.prefix_bytes > 0 && previous.prefix_bytes == current.prefix_bytes && old == new
        });
    let prefix_changed = previous
        .prefix_sha256
        .as_ref()
        .zip(current.prefix_sha256.as_ref())
        .is_some_and(|(old, new)| previous.prefix_bytes == current.prefix_bytes && old != new);
    let identity_changed = previous
        .device
        .zip(previous.inode)
        .zip(current.device.zip(current.inode))
        .is_some_and(|((old_device, old_inode), (new_device, new_inode))| {
            old_inode != new_inode || (old_device != new_device && !prefix_matches)
        });
    identity_changed || prefix_changed
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RefreshDecision {
    Unchanged,
    CheckpointsOnly,
    Publish,
}

pub(super) struct RefreshFacts {
    pub source_changes: bool,
    pub vector_work: bool,
    pub checkpoint_changes: bool,
}

pub(super) fn decide_refresh(facts: RefreshFacts) -> RefreshDecision {
    if facts.source_changes || facts.vector_work {
        RefreshDecision::Publish
    } else if facts.checkpoint_changes {
        RefreshDecision::CheckpointsOnly
    } else {
        RefreshDecision::Unchanged
    }
}

pub(super) fn classify_opencode_database_outcome(
    outcome: Option<OpencodeDatabaseOutcome>,
    discovered: bool,
    inventory_completed: bool,
) -> OpencodeDatabaseOutcome {
    outcome.unwrap_or(if inventory_completed && !discovered {
        OpencodeDatabaseOutcome::ConfirmedAbsent
    } else {
        OpencodeDatabaseOutcome::Failed
    })
}

pub(super) fn claim_opencode_session_owner(
    owners: &mut HashMap<String, String>,
    session_id: String,
    database_path: &str,
) {
    owners
        .entry(session_id)
        .or_insert_with(|| database_path.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn refresh_decision_distinguishes_data_from_checkpoint_work() {
        assert_eq!(
            decide_refresh(RefreshFacts {
                source_changes: false,
                vector_work: false,
                checkpoint_changes: false
            }),
            RefreshDecision::Unchanged
        );
        assert_eq!(
            decide_refresh(RefreshFacts {
                source_changes: false,
                vector_work: false,
                checkpoint_changes: true
            }),
            RefreshDecision::CheckpointsOnly
        );
        assert_eq!(
            decide_refresh(RefreshFacts {
                source_changes: true,
                vector_work: false,
                checkpoint_changes: false
            }),
            RefreshDecision::Publish
        );
        assert_eq!(
            decide_refresh(RefreshFacts {
                source_changes: false,
                vector_work: true,
                checkpoint_changes: false
            }),
            RefreshDecision::Publish
        );
    }
}
