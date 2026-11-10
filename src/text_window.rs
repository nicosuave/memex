//! Choosing the part of a long text that best shows why it matched a query.
//!
//! Search results carry long transcript fields, and callers show only a window of each.
//! The window at the earliest query hit often shows a common word such as "error" while the
//! identifier the query is about sits further in, so windows are ranked by how specific
//! their hits are instead.

use regex::Regex;

/// Query terms considered, bounding the cost per text.
const MAX_TERMS: usize = 16;
/// Occurrences kept per term for each match kind, whole word or inside a word.
const MAX_HITS_PER_KIND: usize = 64;
/// Characters of a hit that add weight; longer hits weigh the same.
const MAX_HIT_WEIGHT: usize = 32;

/// The first query hit of the most specific window of a text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Anchor {
    /// Byte offset of the hit.
    pub byte: usize,
    /// Character offset of the hit.
    pub chars: usize,
    /// The head of the text, its first `window` characters, shows query hits at least as
    /// specific, so a caller that can show the head may keep it.
    pub head_suffices: bool,
}

/// One occurrence of a query term, with offsets in characters of the text.
struct Hit {
    byte: usize,
    start: usize,
    end: usize,
    term: usize,
    weight: u32,
    identifier: bool,
}

/// Weight of the identifier hits in a window, then of all its hits; compared in that order.
type Score = (u32, u32);

/// The first hit of the most specific `window`-character span of `text`, for spans that
/// begin `lead` characters before their first hit; `None` when no matcher matches.
///
/// A hit weighs the length of its matched text in characters, up to 32, and an ASCII hit
/// shorter than three characters weighs 1; a whole-word hit counts double. A hit of at least
/// four characters with a letter and a digit, `_`, `::`, or a lower-to-upper case change is
/// an identifier, unless its term fills its hit cap in this text and so cannot locate a
/// passage. A window with identifier hits beats any window without. Each term counts its
/// best hit in a window, and ties go to the earliest window.
pub(crate) fn best_anchor(
    text: &str,
    matchers: &[Regex],
    window: usize,
    lead: usize,
) -> Option<Anchor> {
    let hits = hits(text, matchers);
    let span = window.saturating_sub(lead).max(1);
    let mut best_by_term = vec![(0, false); matchers.len().min(MAX_TERMS)];
    let head = score(&hits, 0, window, &mut best_by_term);
    let mut best: Option<(Score, &Hit)> = None;
    for (index, anchor) in hits.iter().enumerate() {
        // The anchor counts even when its hit is longer than the span.
        let end = anchor.start.saturating_add(span).max(anchor.end);
        let candidate = score(&hits[index..], anchor.start, end, &mut best_by_term);
        if best.is_none_or(|(best, _)| candidate > best) {
            best = Some((candidate, anchor));
        }
    }
    best.map(|(score, hit)| Anchor {
        byte: hit.byte,
        chars: hit.start,
        head_suffices: head >= score,
    })
}

/// Score of the distinct terms with a hit inside characters `start..end`, given `hits`
/// sorted by start.
fn score(hits: &[Hit], start: usize, end: usize, best_by_term: &mut [(u32, bool)]) -> Score {
    best_by_term.fill((0, false));
    for hit in hits {
        if hit.start >= end {
            break;
        }
        if hit.start < start || hit.end > end {
            continue;
        }
        if let Some(best) = best_by_term.get_mut(hit.term)
            && hit.weight > best.0
        {
            *best = (hit.weight, hit.identifier);
        }
    }
    best_by_term
        .iter()
        .fold((0, 0), |(identifiers, all), &(weight, identifier)| {
            let identifiers = if identifier {
                identifiers + weight
            } else {
                identifiers
            };
            (identifiers, all + weight)
        })
}

/// Hits of the first [`MAX_TERMS`] distinct matchers in `text`, sorted by start.
fn hits(text: &str, matchers: &[Regex]) -> Vec<Hit> {
    let mut hits = Vec::new();
    let mut seen = Vec::new();
    for (term, matcher) in matchers.iter().take(MAX_TERMS).enumerate() {
        if seen.contains(&matcher.as_str()) {
            continue;
        }
        seen.push(matcher.as_str());
        let first = hits.len();
        let (mut whole, mut partial) = (0, 0);
        for found in matcher.find_iter(text) {
            if found.is_empty() {
                continue;
            }
            let is_whole = !text[..found.start()]
                .chars()
                .next_back()
                .is_some_and(is_word)
                && !text[found.end()..].chars().next().is_some_and(is_word);
            let count = if is_whole { whole } else { partial };
            if count >= MAX_HITS_PER_KIND {
                if whole >= MAX_HITS_PER_KIND && partial >= MAX_HITS_PER_KIND {
                    break;
                }
                continue;
            }
            if is_whole {
                whole += 1;
            } else {
                partial += 1;
            }
            let (weight, identifier) = hit_weight(found.as_str());
            hits.push(Hit {
                byte: found.start(),
                start: found.start(),
                end: found.end(),
                term,
                weight: if is_whole { 2 * weight } else { weight },
                identifier,
            });
        }
        // A term this common in the text cannot locate a passage in it.
        if whole >= MAX_HITS_PER_KIND || partial >= MAX_HITS_PER_KIND {
            for hit in &mut hits[first..] {
                hit.identifier = false;
            }
        }
    }
    hits.sort_by_key(|hit| (hit.byte, hit.term));
    if !text.is_ascii() {
        to_char_offsets(text, &mut hits);
    }
    hits
}

/// Convert the byte offsets in `start` and `end` of `hits`, sorted by start, to character
/// offsets in one pass over `text`.
fn to_char_offsets(text: &str, hits: &mut [Hit]) {
    let mut bytes: Vec<usize> = hits.iter().flat_map(|hit| [hit.start, hit.end]).collect();
    bytes.sort_unstable();
    bytes.dedup();
    let mut chars = Vec::with_capacity(bytes.len());
    for (index, (byte, _)) in text.char_indices().enumerate() {
        while bytes.get(chars.len()).is_some_and(|next| *next <= byte) {
            chars.push(index);
        }
        if chars.len() == bytes.len() {
            break;
        }
    }
    // Only offsets at the end of the text remain.
    if chars.len() < bytes.len() {
        chars.resize(bytes.len(), text.chars().count());
    }
    let position = |byte: usize| {
        bytes
            .binary_search(&byte)
            .ok()
            .and_then(|slot| chars.get(slot).copied())
            .unwrap_or_default()
    };
    for hit in hits {
        hit.start = position(hit.start);
        hit.end = position(hit.end);
    }
}

fn is_word(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

/// Weight of one matched text, and whether it is an identifier; see [`best_anchor`].
fn hit_weight(matched: &str) -> (u32, bool) {
    let length = matched.chars().count();
    if length < 3 && matched.is_ascii() {
        return (1, false);
    }
    let weight = u32::try_from(length.min(MAX_HIT_WEIGHT)).unwrap_or(u32::MAX);
    let camel_case = matched
        .chars()
        .zip(matched.chars().skip(1))
        .any(|(before, after)| before.is_lowercase() && after.is_uppercase());
    let identifier = length >= 4
        && matched.chars().any(char::is_alphabetic)
        && (matched
            .chars()
            .any(|character| character.is_ascii_digit() || character == '_')
            || matched.contains("::")
            || camel_case);
    (weight, identifier)
}

#[cfg(test)]
mod tests {
    use super::best_anchor;
    use crate::cli::build_matchers;

    #[test]
    fn an_identifier_beats_an_earlier_common_word() {
        let text = format!(
            "error: build failed {} the parse_retry_budget_v2 limit was exceeded",
            "filler ".repeat(500)
        );
        let matchers = build_matchers("error parse_retry_budget_v2").unwrap();
        let anchor = best_anchor(&text, &matchers, 400, 100).unwrap();
        assert!(text[anchor.byte..].starts_with("parse_retry_budget_v2"));
        assert!(!anchor.head_suffices);
    }

    #[test]
    fn the_head_suffices_when_it_already_shows_the_specific_terms() {
        let text = format!(
            "error in parse_retry_budget_v2 {} another error",
            "filler ".repeat(500)
        );
        let matchers = build_matchers("error parse_retry_budget_v2").unwrap();
        let anchor = best_anchor(&text, &matchers, 400, 100).unwrap();
        assert_eq!(anchor.byte, 0);
        assert!(anchor.head_suffices);
    }

    #[test]
    fn a_term_too_common_to_locate_a_passage_is_not_an_identifier() {
        let text = format!(
            "{}{} baz_qux",
            "foo_bar ".repeat(100),
            "filler ".repeat(500)
        );
        let matchers = build_matchers("foo_bar baz_qux").unwrap();
        let anchor = best_anchor(&text, &matchers, 400, 100).unwrap();
        assert!(text[anchor.byte..].starts_with("baz_qux"));
    }

    #[test]
    fn a_whole_word_outranks_an_earlier_hit_inside_a_longer_word() {
        let text = format!("needles {} needle", "filler ".repeat(100));
        let matchers = build_matchers("needle").unwrap();
        let anchor = best_anchor(&text, &matchers, 40, 10).unwrap();
        assert_eq!(&text[anchor.byte..], "needle");
    }

    #[test]
    fn offsets_count_characters_in_multibyte_text() {
        let text = format!("{} needle", "界".repeat(300));
        let matchers = build_matchers("needle").unwrap();
        let anchor = best_anchor(&text, &matchers, 40, 10).unwrap();
        assert_eq!(anchor.chars, 301);
        assert_eq!(anchor.byte, 300 * '界'.len_utf8() + 1);
        assert!(!anchor.head_suffices);
    }

    #[test]
    fn short_terms_still_anchor_and_no_match_has_no_anchor() {
        let matchers = build_matchers("ok").unwrap();
        let text = format!("{} ok", "x".repeat(500));
        assert_eq!(best_anchor(&text, &matchers, 40, 10).unwrap().chars, 501);
        assert_eq!(best_anchor("nothing here", &matchers, 40, 10), None);
    }
}
