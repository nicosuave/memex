//! Human-readable views of a generation. `dreams.json` is the record; these files are
//! rendered from it and never read back.

use super::apply::Change;
use super::gather::format_day;
use super::model::{Citation, CitationTarget, Dream, DreamKind, DreamStatus, Generation};
use std::collections::BTreeMap;

fn citation_line(citation: &Citation) -> String {
    let state = if citation.state.supports() {
        String::new()
    } else {
        format!(" [{}]", citation.state.label())
    };
    match &citation.target {
        CitationTarget::Record {
            rid, source, role, ..
        } => format!(
            "  - {} {} {} on {} · `memex show --record-id {} --machine {}`{}\n    > {}\n",
            format_day(citation.ts),
            source.label(),
            role,
            citation.machine,
            rid,
            citation.machine,
            state,
            citation.excerpt,
        ),
        CitationTarget::Memory {
            memory_id, source, ..
        } => format!(
            "  - {} {} memory · `memex show --memory-id {} --machine {}`{}\n    > {}\n",
            format_day(citation.ts),
            source.label(),
            memory_id,
            citation.machine,
            state,
            citation.excerpt,
        ),
    }
}

fn dream_block(dream: &Dream) -> String {
    let mut out = format!(
        "- {} `{}` (evidence {}{})\n",
        dream.text,
        dream.id,
        dream.evidence_health().label(),
        if dream.reinforced > 0 {
            format!(", reinforced {}x", dream.reinforced)
        } else {
            String::new()
        },
    );
    for citation in &dream.citations {
        out.push_str(&citation_line(citation));
    }
    out
}

/// The current dreams as a document: active ones by project and kind, then what was
/// replaced or withdrawn.
pub fn render_markdown(generation: &Generation) -> String {
    let mut out = format!(
        "# Dreams\n\nGeneration `{}` · {} · {} active dreams\n\n\
         Dreams are derived from agent history by a model. Each cites the records it rests\n\
         on; read a citation before relying on a dream.\n",
        generation.id,
        format_day(generation.created_at_ms),
        generation.active().count(),
    );
    let mut by_project: BTreeMap<&str, BTreeMap<DreamKind, Vec<&Dream>>> = BTreeMap::new();
    for dream in generation.active() {
        by_project
            .entry(dream.project.as_deref().unwrap_or(""))
            .or_default()
            .entry(dream.kind)
            .or_default()
            .push(dream);
    }
    for (project, kinds) in by_project {
        out.push_str(&format!(
            "\n## {}\n",
            if project.is_empty() {
                "All projects"
            } else {
                project
            }
        ));
        for (kind, dreams) in kinds {
            out.push_str(&format!("\n### {}\n\n", kind.heading()));
            for dream in dreams {
                out.push_str(&dream_block(dream));
            }
        }
    }
    let inactive = generation
        .dreams
        .iter()
        .filter(|dream| dream.status != DreamStatus::Active)
        .collect::<Vec<_>>();
    if !inactive.is_empty() {
        out.push_str("\n## No longer current\n\n");
        for dream in inactive {
            let why = match (&dream.superseded_by, &dream.retraction) {
                (Some(replacement), _) => format!("superseded by `{replacement}`"),
                (None, Some(retraction)) => format!("retracted: {}", retraction.reason),
                (None, None) => dream.status.label().to_string(),
            };
            out.push_str(&format!("- ~~{}~~ `{}` ({why})\n", dream.text, dream.id));
        }
    }
    out
}

/// What this generation changed relative to its parent.
pub fn render_diff(generation: &Generation, changes: &[Change]) -> String {
    let mut out = format!(
        "# Dream diff\n\nGeneration `{}` from {}\n",
        generation.id,
        generation
            .parent
            .as_deref()
            .map(|parent| format!("`{parent}`"))
            .unwrap_or_else(|| "an empty store".to_string()),
    );
    if changes.is_empty() && generation.run.rejected.is_empty() {
        out.push_str("\nNo changes.\n");
        return out;
    }
    let text = |id: &str| {
        generation
            .dream(id)
            .map(|dream| dream.text.as_str())
            .unwrap_or("(unknown dream)")
    };
    for change in changes {
        match change {
            Change::Added { dream_id } => {
                out.push_str(&format!("\n+ added `{dream_id}`: {}\n", text(dream_id)));
                if let Some(dream) = generation.dream(dream_id) {
                    for citation in &dream.citations {
                        out.push_str(&citation_line(citation));
                    }
                }
            }
            Change::Reinforced {
                dream_id,
                citations,
            } => out.push_str(&format!(
                "\n^ reinforced `{dream_id}` with {citations} new citation(s): {}\n",
                text(dream_id)
            )),
            Change::Revised { old_id, new_id } => out.push_str(&format!(
                "\n~ revised `{old_id}` -> `{new_id}`\n  - was: {}\n  - now: {}\n",
                text(old_id),
                text(new_id)
            )),
            Change::Retracted { dream_id, reason } => out.push_str(&format!(
                "\n- retracted `{dream_id}`: {}\n  - reason: {reason}\n",
                text(dream_id)
            )),
            Change::CitationChanged {
                dream_id,
                citation,
                from,
                to,
                detail,
            } => out.push_str(&format!(
                "\n* citation {citation} of `{dream_id}`: {} -> {} ({detail})\n",
                from.label(),
                to.label()
            )),
        }
    }
    if !generation.run.rejected.is_empty() {
        out.push_str("\n## Rejected operations\n\n");
        for rejected in &generation.run.rejected {
            out.push_str(&format!(
                "- {}: {}{}\n",
                rejected.op,
                rejected.reason,
                if rejected.text.is_empty() {
                    String::new()
                } else {
                    format!(" · \"{}\"", rejected.text)
                }
            ));
        }
    }
    out
}
