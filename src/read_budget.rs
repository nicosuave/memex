use anyhow::{Result, anyhow};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::types::Record;

pub const DEFAULT_MAX_CHARS: usize = 16_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "kebab-case")]
pub enum ReadField {
    Text,
    #[value(alias = "tool_input")]
    ToolInput,
    #[value(alias = "tool_output")]
    ToolOutput,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentContinuation {
    pub field: ReadField,
    pub offset_chars: usize,
    pub total_chars: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentPage {
    pub returned_chars: usize,
    pub total_chars: usize,
    pub truncated: bool,
    pub continuations: Vec<ContentContinuation>,
}

#[derive(Clone, Debug)]
pub struct ReadBudget {
    remaining: Option<usize>,
}

impl ReadBudget {
    pub fn new(max_chars: Option<usize>) -> Result<Self> {
        if max_chars == Some(0) {
            return Err(anyhow!("max_chars must be greater than zero"));
        }
        Ok(Self {
            remaining: max_chars,
        })
    }

    /// Internal continuation budgets may already be exhausted. User-facing limits
    /// must go through `new` so an explicit zero remains an error.
    pub(crate) fn from_remaining(remaining: Option<usize>) -> Self {
        Self { remaining }
    }

    pub fn remaining(&self) -> Option<usize> {
        self.remaining
    }

    pub fn apply(
        &mut self,
        record: &mut Record,
        field: Option<ReadField>,
        offset_chars: usize,
    ) -> Result<ContentPage> {
        if field.is_none() && offset_chars != 0 {
            return Err(anyhow!(
                "offset_chars requires an explicitly selected field"
            ));
        }

        match field {
            Some(field) => self.apply_field(record, field, offset_chars),
            None => Ok(self.apply_all(record)),
        }
    }

    fn apply_field(
        &mut self,
        record: &mut Record,
        field: ReadField,
        offset_chars: usize,
    ) -> Result<ContentPage> {
        let original = match field {
            ReadField::Text => Some(record.text.as_str()),
            ReadField::ToolInput => record.tool_input.as_deref(),
            ReadField::ToolOutput => record.tool_output.as_deref(),
        };
        let total_chars = original.map_or(0, char_count);
        if offset_chars > total_chars {
            return Err(anyhow!(
                "offset_chars {offset_chars} is past the end of {field:?} ({total_chars} chars)"
            ));
        }

        let available = self.remaining.unwrap_or(usize::MAX);
        let returned_chars = available.min(total_chars - offset_chars);
        let selected = original.map(|value| char_range(value, offset_chars, returned_chars));

        record.text.clear();
        record.tool_input = None;
        record.tool_output = None;
        match field {
            ReadField::Text => record.text = selected.unwrap_or_default(),
            ReadField::ToolInput => record.tool_input = selected,
            ReadField::ToolOutput => record.tool_output = selected,
        }
        self.consume(returned_chars);

        let next_offset = offset_chars + returned_chars;
        let continuations = if next_offset < total_chars {
            vec![ContentContinuation {
                field,
                offset_chars: next_offset,
                total_chars,
            }]
        } else {
            Vec::new()
        };
        Ok(ContentPage {
            returned_chars,
            total_chars,
            truncated: !continuations.is_empty(),
            continuations,
        })
    }

    fn apply_all(&mut self, record: &mut Record) -> ContentPage {
        let text = std::mem::take(&mut record.text);
        let tool_input = record.tool_input.take();
        let tool_output = record.tool_output.take();

        let fields = [
            (ReadField::Text, Some(text)),
            (ReadField::ToolInput, tool_input),
            (ReadField::ToolOutput, tool_output),
        ];
        let total_chars = fields
            .iter()
            .filter_map(|(_, value)| value.as_deref())
            .map(char_count)
            .sum();
        let mut available = self.remaining.unwrap_or(usize::MAX);
        let mut returned_chars = 0;
        let mut continuations = Vec::new();

        for (field, original) in fields {
            let page = original.map(|value| {
                let field_total = char_count(&value);
                let take = available.min(field_total);
                available -= take;
                returned_chars += take;
                if take < field_total {
                    continuations.push(ContentContinuation {
                        field,
                        offset_chars: take,
                        total_chars: field_total,
                    });
                }
                char_range(&value, 0, take)
            });

            match field {
                ReadField::Text => record.text = page.unwrap_or_default(),
                ReadField::ToolInput => record.tool_input = page,
                ReadField::ToolOutput => record.tool_output = page,
            }
        }
        self.consume(returned_chars);

        ContentPage {
            returned_chars,
            total_chars,
            truncated: !continuations.is_empty(),
            continuations,
        }
    }

    fn consume(&mut self, chars: usize) {
        if let Some(remaining) = &mut self.remaining {
            *remaining -= chars;
        }
    }
}

fn char_count(value: &str) -> usize {
    value.chars().count()
}

fn char_range(value: &str, offset_chars: usize, length_chars: usize) -> String {
    value
        .chars()
        .skip(offset_chars)
        .take(length_chars)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RecordLinks, SourceKind};

    fn record(text: &str, tool_input: Option<&str>, tool_output: Option<&str>) -> Record {
        Record {
            source: SourceKind::Codex,
            doc_id: 1,
            ts: 2,
            project: "project".into(),
            session_id: "session".into(),
            turn_id: 3,
            role: "assistant".into(),
            text: text.into(),
            tool_name: None,
            tool_input: tool_input.map(str::to_owned),
            tool_output: tool_output.map(str::to_owned),
            links: RecordLinks::default(),
            source_path: "source".into(),
        }
    }

    #[test]
    fn defaults_to_a_sixteen_thousand_character_limit() {
        let mut value = record(&"x".repeat(DEFAULT_MAX_CHARS + 1), None, None);
        let mut budget = ReadBudget::new(Some(DEFAULT_MAX_CHARS)).unwrap();

        let page = budget.apply(&mut value, None, 0).unwrap();

        assert_eq!(value.text.chars().count(), DEFAULT_MAX_CHARS);
        assert_eq!(page.returned_chars, DEFAULT_MAX_CHARS);
        assert_eq!(page.total_chars, DEFAULT_MAX_CHARS + 1);
        assert!(page.truncated);
        assert_eq!(
            page.continuations,
            [ContentContinuation {
                field: ReadField::Text,
                offset_chars: DEFAULT_MAX_CHARS,
                total_chars: DEFAULT_MAX_CHARS + 1,
            }]
        );
        assert_eq!(budget.remaining(), Some(0));
    }

    #[test]
    fn unicode_offsets_are_scalar_character_offsets() {
        let mut value = record("aé🦀z", Some("ignored"), Some("ignored"));
        let mut budget = ReadBudget::new(Some(2)).unwrap();

        let page = budget.apply(&mut value, Some(ReadField::Text), 1).unwrap();

        assert_eq!(value.text, "é🦀");
        assert_eq!(value.tool_input, None);
        assert_eq!(value.tool_output, None);
        assert_eq!(page.returned_chars, 2);
        assert_eq!(page.total_chars, 4);
        assert_eq!(page.continuations[0].offset_chars, 3);
    }

    #[test]
    fn one_budget_crosses_fields_and_marks_unreached_fields() {
        let mut value = record("abc", Some("de"), Some("fghi"));
        let mut budget = ReadBudget::new(Some(6)).unwrap();

        let page = budget.apply(&mut value, None, 0).unwrap();

        assert_eq!(value.text, "abc");
        assert_eq!(value.tool_input.as_deref(), Some("de"));
        assert_eq!(value.tool_output.as_deref(), Some("f"));
        assert_eq!(page.returned_chars, 6);
        assert_eq!(page.total_chars, 9);
        assert_eq!(
            page.continuations,
            [ContentContinuation {
                field: ReadField::ToolOutput,
                offset_chars: 1,
                total_chars: 4,
            }]
        );
    }

    #[test]
    fn zero_remaining_emits_empty_values_and_continuations_from_zero() {
        let mut first = record("a", None, None);
        let mut second = record("bc", Some("de"), Some(""));
        let mut budget = ReadBudget::new(Some(1)).unwrap();
        budget.apply(&mut first, None, 0).unwrap();

        let page = budget.apply(&mut second, None, 0).unwrap();

        assert_eq!(second.text, "");
        assert_eq!(second.tool_input.as_deref(), Some(""));
        assert_eq!(second.tool_output.as_deref(), Some(""));
        assert_eq!(page.returned_chars, 0);
        assert_eq!(page.total_chars, 4);
        assert_eq!(
            page.continuations,
            [
                ContentContinuation {
                    field: ReadField::Text,
                    offset_chars: 0,
                    total_chars: 2,
                },
                ContentContinuation {
                    field: ReadField::ToolInput,
                    offset_chars: 0,
                    total_chars: 2,
                },
            ]
        );
    }

    #[test]
    fn absent_and_empty_optional_fields_remain_distinct() {
        let mut absent = record("", None, None);
        let mut empty = record("", Some(""), Some(""));
        let mut budget = ReadBudget::new(Some(1)).unwrap();

        budget.apply(&mut absent, None, 0).unwrap();
        budget.apply(&mut empty, None, 0).unwrap();

        assert_eq!(absent.tool_input, None);
        assert_eq!(absent.tool_output, None);
        assert_eq!(empty.tool_input.as_deref(), Some(""));
        assert_eq!(empty.tool_output.as_deref(), Some(""));
    }

    #[test]
    fn budget_is_shared_across_records() {
        let mut first = record("abc", None, None);
        let mut second = record("def", None, None);
        let mut budget = ReadBudget::new(Some(5)).unwrap();

        let first_page = budget.apply(&mut first, None, 0).unwrap();
        let second_page = budget.apply(&mut second, None, 0).unwrap();

        assert_eq!(first.text, "abc");
        assert!(!first_page.truncated);
        assert_eq!(second.text, "de");
        assert!(second_page.truncated);
        assert_eq!(budget.remaining(), Some(0));
    }

    #[test]
    fn exact_limit_empty_and_full_modes_are_complete() {
        let mut exact = record("abc", Some("de"), None);
        let mut exact_budget = ReadBudget::new(Some(5)).unwrap();
        let exact_page = exact_budget.apply(&mut exact, None, 0).unwrap();
        assert!(!exact_page.truncated);
        assert_eq!(exact_budget.remaining(), Some(0));

        let mut empty = record("", Some(""), None);
        let empty_page = exact_budget.apply(&mut empty, None, 0).unwrap();
        assert_eq!(empty_page.returned_chars, 0);
        assert!(!empty_page.truncated);

        let mut full = record("αβ", Some("γ"), Some("δ"));
        let original = full.clone();
        let mut full_budget = ReadBudget::new(None).unwrap();
        let full_page = full_budget.apply(&mut full, None, 0).unwrap();
        assert_eq!(full.text, original.text);
        assert_eq!(full.tool_input, original.tool_input);
        assert_eq!(full.tool_output, original.tool_output);
        assert_eq!(full_page.returned_chars, 4);
        assert!(!full_page.truncated);
        assert_eq!(full_budget.remaining(), None);
    }

    #[test]
    fn continuations_reconstruct_each_field_without_loss_or_duplication() {
        let original = record("ab🦀", Some("déf"), Some("ghij"));
        for field in [ReadField::Text, ReadField::ToolInput, ReadField::ToolOutput] {
            let expected = match field {
                ReadField::Text => original.text.as_str(),
                ReadField::ToolInput => original.tool_input.as_deref().unwrap(),
                ReadField::ToolOutput => original.tool_output.as_deref().unwrap(),
            };
            let mut offset = 0;
            let mut reconstructed = String::new();

            loop {
                let mut value = original.clone();
                let mut budget = ReadBudget::new(Some(2)).unwrap();
                let page = budget.apply(&mut value, Some(field), offset).unwrap();
                reconstructed.push_str(match field {
                    ReadField::Text => &value.text,
                    ReadField::ToolInput => value.tool_input.as_deref().unwrap(),
                    ReadField::ToolOutput => value.tool_output.as_deref().unwrap(),
                });
                let Some(continuation) = page.continuations.first() else {
                    break;
                };
                offset = continuation.offset_chars;
            }

            assert_eq!(reconstructed, expected);
        }
    }

    #[test]
    fn rejects_zero_limit_and_invalid_offsets() {
        assert!(ReadBudget::new(Some(0)).is_err());

        let mut value = record("abc", None, None);
        let mut budget = ReadBudget::new(Some(1)).unwrap();
        assert!(budget.apply(&mut value, None, 1).is_err());
        assert!(budget.apply(&mut value, Some(ReadField::Text), 4).is_err());

        let page = budget.apply(&mut value, Some(ReadField::Text), 3).unwrap();
        assert_eq!(value.text, "");
        assert_eq!(page.returned_chars, 0);
        assert!(!page.truncated);
    }

    #[test]
    fn field_names_serialize_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&ReadField::ToolInput).unwrap(),
            "\"tool_input\""
        );
        assert_eq!(
            ReadField::ToolOutput
                .to_possible_value()
                .unwrap()
                .get_name(),
            "tool-output"
        );
    }
}
