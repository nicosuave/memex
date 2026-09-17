//! Retained usage events use dictionary IDs instead of separately allocated strings.
//! Parsing and reconciliation still operate on owned events. Reports borrow text from
//! this assembly; only callers requesting detailed events allocate owned strings again.

use super::{UsageActivityPoint, UsageEvent, UsageEventData};
use hashbrown::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

pub(crate) type UsageEventView<'a> = UsageEventData<&'a str, &'a str>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct StringId(NonZeroUsize);

/// Retained usage events: every assembly is compacted (dictionary-encoded
/// text), so filtering and reporting borrow instead of cloning per event.
pub(crate) struct UsageAssembly(CompactUsageAssembly);

/// Compact borrowed rows while their backing storage is still available.
#[derive(Default)]
pub(crate) struct UsageAssemblyBuilder {
    assembly: CompactUsageAssembly,
    dictionary: HashMap<String, StringId>,
}

impl UsageAssemblyBuilder {
    pub(crate) fn push(&mut self, event: UsageEventView<'_>) {
        let intern_text = |text: &str, dictionary: &mut HashMap<String, StringId>| {
            dictionary
                .get(text)
                .copied()
                .unwrap_or_else(|| intern(dictionary, text.to_owned()))
        };
        let path = intern_text(event.source_path, &mut self.dictionary);
        self.assembly
            .events
            .push(event.map_text(path, |text| intern_text(text, &mut self.dictionary)));
    }

    pub(crate) fn finish(mut self) -> UsageAssembly {
        self.assembly.events.shrink_to_fit();
        self.assembly.pack_dictionary(self.dictionary);
        UsageAssembly(self.assembly)
    }
}

pub(crate) struct FilterFields<'a> {
    pub source: &'static str,
    pub permission_review: bool,
    pub project: Option<&'a str>,
    pub session_id: Option<&'a str>,
}

impl UsageAssembly {
    pub fn new(events: Vec<UsageEvent>, previous: Option<Self>) -> Self {
        Self(CompactUsageAssembly::new(
            events,
            previous.map(|assembly| assembly.0),
        ))
    }

    pub fn len(&self) -> usize {
        self.0.events.len()
    }

    pub fn timestamp_ms(&self, index: usize) -> u64 {
        self.0.events[index].timestamp_ms
    }

    /// Global sort key: timestamp, source path, then source order. Merging
    /// partitions by this key reproduces the combined assembly's sort exactly.
    pub fn sort_key(&self, index: usize) -> (u64, &str, u64) {
        let event = &self.0.events[index];
        (
            event.timestamp_ms,
            self.0.text(event.source_path),
            event.source_order,
        )
    }

    /// Lower bound of `since` in the timestamp-sorted assembly.
    pub fn lower_bound(&self, since: u64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.timestamp_ms(mid) < since {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Upper bound of `until` (first index with `timestamp >= until`) starting from `start`.
    pub fn upper_bound(&self, until: u64, start: usize) -> usize {
        let mut lo = start;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.timestamp_ms(mid) < until {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    pub fn filter_fields(&self, index: usize) -> FilterFields<'_> {
        let event = &self.0.events[index];
        FilterFields {
            source: event.source,
            permission_review: event.permission_review,
            project: event.project.map(|id| self.0.text(id)),
            session_id: event.session_id.map(|id| self.0.text(id)),
        }
    }

    pub fn activity_point(&self, index: usize) -> UsageActivityPoint {
        self.0.events[index].activity_point()
    }

    pub fn view(&self, index: usize) -> UsageEventView<'_> {
        self.0.view(index)
    }

    pub fn details(&self, indices: impl Iterator<Item = usize>) -> Vec<UsageEvent> {
        self.0.details(indices)
    }
}

#[derive(Default)]
pub(super) struct CompactUsageAssembly {
    events: Vec<UsageEventData<StringId, StringId>>,
    text: String,
    ends: Vec<usize>,
}

impl CompactUsageAssembly {
    pub fn new(events: Vec<UsageEvent>, previous: Option<Self>) -> Self {
        // Reuse retained buffers across refreshes to avoid repeated large allocation
        // and free operations, which can inflate the allocator's cached memory.
        let mut assembly = previous.unwrap_or_default();
        assembly.events.clear();
        assembly.events.reserve(events.len());
        assembly.text.clear();
        assembly.ends.clear();
        let mut dictionary = HashMap::<String, StringId>::new();
        assembly.events.extend(events.into_iter().map(|event| {
            let path = dictionary
                .get(event.source_path.as_ref())
                .copied()
                .unwrap_or_else(|| intern(&mut dictionary, event.source_path.to_string()));
            event.map_text(path, |text| intern(&mut dictionary, text))
        }));

        assembly.pack_dictionary(dictionary);
        assembly
    }

    fn pack_dictionary(&mut self, dictionary: HashMap<String, StringId>) {
        // Pack the dictionary into one allocation; keep neither a lookup hash table
        // nor one allocation per unique string alive between requests.
        let mut ordered = vec![String::new(); dictionary.len()];
        let mut bytes = 0;
        for (text, id) in dictionary {
            bytes += text.len();
            ordered[id.0.get() - 1] = text;
        }
        self.text.reserve(bytes);
        self.ends.reserve(ordered.len());
        for value in ordered {
            self.text.push_str(&value);
            self.ends.push(self.text.len());
        }
    }

    pub fn text(&self, id: StringId) -> &str {
        let index = id.0.get() - 1;
        let start = if index == 0 { 0 } else { self.ends[index - 1] };
        &self.text[start..self.ends[index]]
    }

    pub fn view(&self, index: usize) -> UsageEventView<'_> {
        let event = &self.events[index];
        event
            .clone()
            .map_text(self.text(event.source_path), |id| self.text(id))
    }

    pub fn details(&self, indices: impl Iterator<Item = usize>) -> Vec<UsageEvent> {
        // Preserve the parser's per-file path sharing in detailed responses too.
        let mut paths = HashMap::<StringId, Arc<str>>::new();
        indices
            .map(|index| {
                let event = self.view(index);
                let path = paths
                    .entry(self.events[index].source_path)
                    .or_insert_with(|| Arc::from(event.source_path))
                    .clone();
                event.map_text(path, str::to_owned)
            })
            .collect()
    }

    #[cfg(test)]
    fn event(&self, index: usize) -> UsageEvent {
        self.details(std::iter::once(index)).pop().unwrap()
    }
}

fn intern(dictionary: &mut HashMap<String, StringId>, text: String) -> StringId {
    let next = StringId(NonZeroUsize::new(dictionary.len() + 1).unwrap());
    *dictionary.entry(text).or_insert(next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::usage::TokenBuckets;

    #[test]
    fn compaction_preserves_details_and_accounting_flags() {
        let event = UsageEvent {
            source: "codex",
            source_path: Arc::from("/logs/会話.jsonl"),
            source_record_id: Some("record-1".into()),
            session_id: Some("session-1".into()),
            request_id: Some("request-1".into()),
            message_id: Some("message-1".into()),
            timestamp_ms: u64::MAX,
            project: Some("/projects/é".into()),
            provider: Some("openai".into()),
            model: Some("gpt-5.4".into()),
            tokens: TokenBuckets {
                raw_input: 1,
                uncached_input: 2,
                cache_read: 3,
                cache_write: 4,
                cache_write_1h: 5,
                output: 6,
                reasoning: 7,
            },
            source_cost_usd: Some(0.123456),
            cost_authoritative: true,
            dedupe_confidence: "strong",
            conservative_undercount: true,
            cache_chain_excluded: true,
            sidechain: true,
            permission_review: true,
            source_order: u64::MAX,
        };
        let mut empty = event.clone();
        empty.source_path = Arc::from("");
        empty.session_id = Some(String::new());
        empty.source_record_id = None;
        empty.request_id = None;
        empty.message_id = None;
        empty.project = None;
        empty.provider = None;
        empty.model = None;
        empty.source_cost_usd = None;
        empty.cost_authoritative = false;
        empty.conservative_undercount = false;
        empty.cache_chain_excluded = false;
        empty.sidechain = false;
        empty.permission_review = false;
        empty.source_order = 0;
        let original = vec![event, empty];
        let assembly = CompactUsageAssembly::new(original.clone(), None);
        for (index, expected) in original.iter().enumerate() {
            let actual = assembly.event(index);
            assert_eq!(
                serde_json::to_value(&actual).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
            assert_eq!(actual.cost_authoritative, expected.cost_authoritative);
            assert_eq!(actual.cache_chain_excluded, expected.cache_chain_excluded);
            assert_eq!(actual.sidechain, expected.sidechain);
            assert_eq!(actual.permission_review, expected.permission_review);
            assert_eq!(actual.source_order, expected.source_order);
            assert_eq!(
                serde_json::to_value(assembly.view(index)).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
    }

    #[test]
    fn repeated_text_is_stored_once_across_fields_and_events() {
        let event = super::super::cache_event("same", 1, "same", 10, 0, 0);
        let assembly = CompactUsageAssembly::new(vec![event; 100], None);
        assert_eq!(assembly.ends.len(), 3); // path, "same", provider
        assert_eq!(
            assembly.text.len(),
            "log.jsonl".len() + "same".len() + "anthropic".len()
        );
        assert!(std::mem::size_of_val(&assembly.events[0]) < std::mem::size_of::<UsageEvent>());
        assert_eq!(assembly.event(99).session_id.as_deref(), Some("same"));
        let details = assembly.details(0..100);
        assert!(Arc::ptr_eq(
            &details[0].source_path,
            &details[99].source_path
        ));
    }

    #[test]
    fn refresh_reuses_storage_and_replaces_events_without_stale_text() {
        let event = super::super::cache_event("old", 1, "same", 10, 0, 0);
        let mut assembly = CompactUsageAssembly::new(vec![event; 100], None);
        let event_buffer = assembly.events.as_ptr();
        let text_buffer = assembly.text.as_ptr();
        let offset_buffer = assembly.ends.as_ptr();
        for timestamp in 2..22 {
            let mut updated = super::super::cache_event("new", timestamp, "diff", 20, 0, 0);
            updated.permission_review = true;
            assembly = CompactUsageAssembly::new(vec![updated.clone(); 100], Some(assembly));
            assert_eq!(assembly.events.as_ptr(), event_buffer);
            assert_eq!(assembly.text.as_ptr(), text_buffer);
            assert_eq!(assembly.ends.as_ptr(), offset_buffer);
            assert_eq!(assembly.events.len(), 100);
            assert_eq!(
                serde_json::to_value(assembly.event(99)).unwrap(),
                serde_json::to_value(&updated).unwrap()
            );
            assert!(assembly.view(99).permission_review);
        }

        let grown = super::super::cache_event("bigger", 22, "same", 30, 0, 0);
        assembly = CompactUsageAssembly::new(vec![grown.clone(); 101], Some(assembly));
        assert_eq!(assembly.events.len(), 101);
        assert_eq!(
            serde_json::to_value(assembly.event(100)).unwrap(),
            serde_json::to_value(grown).unwrap()
        );

        assembly = CompactUsageAssembly::new(Vec::new(), Some(assembly));
        assert!(assembly.events.is_empty());
        assert!(assembly.text.is_empty());
        assert!(assembly.ends.is_empty());
        assert!(assembly.details(0..0).is_empty());
    }

    #[test]
    fn openai_cached_input_is_a_subset() {
        let tokens = TokenBuckets::codex(100, 80, 10, 4);
        assert_eq!(tokens.uncached_input, 20);
        assert_eq!(tokens.cache_read, 80);
        assert_eq!(tokens.additive_total(), 110);
    }
}

impl<S, P> UsageEventData<S, P> {
    fn activity_point(&self) -> UsageActivityPoint {
        UsageActivityPoint {
            source: self.source,
            timestamp_ms: self.timestamp_ms,
            total_tokens: self.tokens.total(),
        }
    }
    fn map_text<T, Q>(self, source_path: Q, mut map: impl FnMut(S) -> T) -> UsageEventData<T, Q> {
        UsageEventData {
            source: self.source,
            source_path,
            source_record_id: self.source_record_id.map(&mut map),
            session_id: self.session_id.map(&mut map),
            request_id: self.request_id.map(&mut map),
            message_id: self.message_id.map(&mut map),
            timestamp_ms: self.timestamp_ms,
            project: self.project.map(&mut map),
            provider: self.provider.map(&mut map),
            model: self.model.map(&mut map),
            tokens: self.tokens,
            source_cost_usd: self.source_cost_usd,
            cost_authoritative: self.cost_authoritative,
            dedupe_confidence: self.dedupe_confidence,
            conservative_undercount: self.conservative_undercount,
            cache_chain_excluded: self.cache_chain_excluded,
            sidechain: self.sidechain,
            permission_review: self.permission_review,
            source_order: self.source_order,
        }
    }
}
