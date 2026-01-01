use crate::types::SourceKind;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const SOURCE_COUNT: usize = 3;

pub struct Progress {
    #[allow(dead_code)] // Kept alive to coordinate progress bars
    multi: MultiProgress,

    // Headers
    claude_header: ProgressBar,
    codex_header: ProgressBar,

    // All spinners
    claude_parse: ProgressBar,
    codex_parse: ProgressBar,
    claude_index: ProgressBar,
    codex_index: ProgressBar,
    claude_embed: ProgressBar,
    codex_embed: ProgressBar,

    // Totals for display
    claude_files_total: u64,
    codex_files_total: u64,

    // Tracking
    files_done: [AtomicU64; SOURCE_COUNT],
    produced: [AtomicU64; SOURCE_COUNT],
    embed_total: [AtomicU64; SOURCE_COUNT],
    embed_pending: [AtomicU64; SOURCE_COUNT],

    embeddings_enabled: bool,
}

impl Progress {
    pub fn new(
        _totals_bytes: [u64; SOURCE_COUNT],
        files_total: [u64; SOURCE_COUNT],
        embeddings: bool,
    ) -> Self {
        let multi = MultiProgress::new();

        let claude_files = files_total[SourceKind::Claude.idx()];
        let codex_files = files_total[SourceKind::CodexSession.idx()]
            + files_total[SourceKind::CodexHistory.idx()];

        // Header style (just text)
        let header_style = ProgressStyle::with_template("{msg}").unwrap();

        // Spinner style for all phases
        let spinner_style = ProgressStyle::with_template("  {spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏");

        // Claude header
        let claude_header = multi.add(ProgressBar::new_spinner());
        claude_header.set_style(header_style.clone());
        claude_header.set_message("claude");
        claude_header.tick();

        // Claude spinners
        let claude_parse = multi.add(ProgressBar::new_spinner());
        claude_parse.set_style(spinner_style.clone());
        claude_parse.set_message(format!("parsed 0 B / {claude_files} files"));
        claude_parse.enable_steady_tick(Duration::from_millis(80));

        let claude_index = multi.add(ProgressBar::new_spinner());
        claude_index.set_style(spinner_style.clone());
        claude_index.set_message("indexed 0 rec");
        claude_index.enable_steady_tick(Duration::from_millis(80));

        let claude_embed = multi.add(ProgressBar::new_spinner());
        claude_embed.set_style(spinner_style.clone());
        if embeddings {
            claude_embed.set_message("embedded 0");
        } else {
            claude_embed.set_message("embeddings off");
        }
        claude_embed.enable_steady_tick(Duration::from_millis(80));

        // Codex header
        let codex_header = multi.add(ProgressBar::new_spinner());
        codex_header.set_style(header_style);
        codex_header.set_message("codex");
        codex_header.tick();

        // Codex spinners
        let codex_parse = multi.add(ProgressBar::new_spinner());
        codex_parse.set_style(spinner_style.clone());
        codex_parse.set_message(format!("parsed 0 B / {codex_files} files"));
        codex_parse.enable_steady_tick(Duration::from_millis(80));

        let codex_index = multi.add(ProgressBar::new_spinner());
        codex_index.set_style(spinner_style.clone());
        codex_index.set_message("indexed 0 rec");
        codex_index.enable_steady_tick(Duration::from_millis(80));

        let codex_embed = multi.add(ProgressBar::new_spinner());
        codex_embed.set_style(spinner_style);
        if embeddings {
            codex_embed.set_message("embedded 0");
        } else {
            codex_embed.set_message("embeddings off");
        }
        codex_embed.enable_steady_tick(Duration::from_millis(80));

        Self {
            multi,
            claude_header,
            codex_header,
            claude_parse,
            codex_parse,
            claude_index,
            codex_index,
            claude_embed,
            codex_embed,
            claude_files_total: claude_files,
            codex_files_total: codex_files,
            files_done: std::array::from_fn(|_| AtomicU64::new(0)),
            produced: std::array::from_fn(|_| AtomicU64::new(0)),
            embed_total: std::array::from_fn(|_| AtomicU64::new(0)),
            embed_pending: std::array::from_fn(|_| AtomicU64::new(0)),
            embeddings_enabled: embeddings,
        }
    }

    pub fn add_parsed_bytes(&self, source: SourceKind, bytes: u64) {
        match source {
            SourceKind::Claude => {
                self.claude_parse.inc(bytes);
                let total = self.claude_parse.position();
                let files_done = self.files_done[SourceKind::Claude.idx()].load(Ordering::Relaxed);
                self.claude_parse.set_message(format!(
                    "parsed {} {}/{} files",
                    format_bytes(total),
                    files_done,
                    self.claude_files_total
                ));
            }
            SourceKind::CodexSession | SourceKind::CodexHistory => {
                self.codex_parse.inc(bytes);
                let total = self.codex_parse.position();
                let files_done = self.files_done[SourceKind::CodexSession.idx()]
                    .load(Ordering::Relaxed)
                    + self.files_done[SourceKind::CodexHistory.idx()].load(Ordering::Relaxed);
                self.codex_parse.set_message(format!(
                    "parsed {} {}/{} files",
                    format_bytes(total),
                    files_done,
                    self.codex_files_total
                ));
            }
        }
    }

    pub fn add_files_done(&self, source: SourceKind, count: u64) {
        let idx = source.idx();
        let done = self.files_done[idx].fetch_add(count, Ordering::Relaxed) + count;

        // Check if parsing is complete for this source
        match source {
            SourceKind::Claude => {
                if done >= self.claude_files_total {
                    let bytes = self.claude_parse.position();
                    self.claude_parse.finish_with_message(format!(
                        "parsed {} {} files done",
                        format_bytes(bytes),
                        self.claude_files_total
                    ));
                }
            }
            SourceKind::CodexSession | SourceKind::CodexHistory => {
                let codex_done = self.files_done[SourceKind::CodexSession.idx()]
                    .load(Ordering::Relaxed)
                    + self.files_done[SourceKind::CodexHistory.idx()].load(Ordering::Relaxed);
                if codex_done >= self.codex_files_total {
                    let bytes = self.codex_parse.position();
                    self.codex_parse.finish_with_message(format!(
                        "parsed {} {} files done",
                        format_bytes(bytes),
                        self.codex_files_total
                    ));
                }
            }
        }
    }

    pub fn add_produced(&self, source: SourceKind, count: u64) {
        self.produced[source.idx()].fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_indexed(&self, source: SourceKind, count: u64) {
        match source {
            SourceKind::Claude => {
                self.claude_index.inc(count);
                let indexed = self.claude_index.position();
                let produced = self.produced[SourceKind::Claude.idx()].load(Ordering::Relaxed);
                let files_done = self.files_done[SourceKind::Claude.idx()].load(Ordering::Relaxed);
                // If parsing done and all produced are indexed, finish
                if files_done >= self.claude_files_total && indexed >= produced && produced > 0 {
                    self.claude_index
                        .finish_with_message(format!("indexed {} rec done", format_count(indexed)));
                } else {
                    self.claude_index
                        .set_message(format!("indexed {} rec", format_count(indexed)));
                }
            }
            SourceKind::CodexSession | SourceKind::CodexHistory => {
                self.codex_index.inc(count);
                let indexed = self.codex_index.position();
                let produced = self.produced[SourceKind::CodexSession.idx()]
                    .load(Ordering::Relaxed)
                    + self.produced[SourceKind::CodexHistory.idx()].load(Ordering::Relaxed);
                let files_done = self.files_done[SourceKind::CodexSession.idx()]
                    .load(Ordering::Relaxed)
                    + self.files_done[SourceKind::CodexHistory.idx()].load(Ordering::Relaxed);
                // If parsing done and all produced are indexed, finish
                if files_done >= self.codex_files_total && indexed >= produced && produced > 0 {
                    self.codex_index
                        .finish_with_message(format!("indexed {} rec done", format_count(indexed)));
                } else {
                    self.codex_index
                        .set_message(format!("indexed {} rec", format_count(indexed)));
                }
            }
        }
    }

    pub fn add_embed_total(&self, source: SourceKind, count: u64) {
        // Just track the total - embed stays as spinner since we don't know final count
        self.embed_total[source.idx()].fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_embed_pending(&self, source: SourceKind, count: u64) {
        self.embed_pending[source.idx()].fetch_add(count, Ordering::Relaxed);
        self.update_embed_message(source);
    }

    pub fn sub_embed_pending(&self, source: SourceKind, count: u64) {
        self.embed_pending[source.idx()].fetch_sub(count, Ordering::Relaxed);
        self.update_embed_message(source);
    }

    fn update_embed_message(&self, source: SourceKind) {
        if !self.embeddings_enabled {
            return;
        }
        let embedded = match source {
            SourceKind::Claude => self.claude_embed.position(),
            SourceKind::CodexSession | SourceKind::CodexHistory => self.codex_embed.position(),
        };

        let msg = format!("embedded {}", format_count(embedded));

        match source {
            SourceKind::Claude => self.claude_embed.set_message(msg),
            SourceKind::CodexSession | SourceKind::CodexHistory => {
                self.codex_embed.set_message(msg)
            }
        }
    }

    pub fn add_embedded(&self, source: SourceKind, count: u64) {
        match source {
            SourceKind::Claude => {
                self.claude_embed.inc(count);
                let embedded = self.claude_embed.position();
                let total = self.embed_total[SourceKind::Claude.idx()].load(Ordering::Relaxed);
                let pending = self.embed_pending[SourceKind::Claude.idx()].load(Ordering::Relaxed);
                let indexed = self.claude_index.position();
                let produced = self.produced[SourceKind::Claude.idx()].load(Ordering::Relaxed);
                // If indexing done and all embeddings done, finish
                if indexed >= produced && pending == 0 && embedded >= total && total > 0 {
                    self.claude_embed
                        .finish_with_message(format!("embedded {} done", format_count(embedded)));
                    return;
                }
            }
            SourceKind::CodexSession | SourceKind::CodexHistory => {
                self.codex_embed.inc(count);
                let embedded = self.codex_embed.position();
                let total = self.embed_total[SourceKind::CodexSession.idx()]
                    .load(Ordering::Relaxed)
                    + self.embed_total[SourceKind::CodexHistory.idx()].load(Ordering::Relaxed);
                let pending = self.embed_pending[SourceKind::CodexSession.idx()]
                    .load(Ordering::Relaxed)
                    + self.embed_pending[SourceKind::CodexHistory.idx()].load(Ordering::Relaxed);
                let indexed = self.codex_index.position();
                let produced = self.produced[SourceKind::CodexSession.idx()]
                    .load(Ordering::Relaxed)
                    + self.produced[SourceKind::CodexHistory.idx()].load(Ordering::Relaxed);
                // If indexing done and all embeddings done, finish
                if indexed >= produced && pending == 0 && embedded >= total && total > 0 {
                    self.codex_embed
                        .finish_with_message(format!("embedded {} done", format_count(embedded)));
                    return;
                }
            }
        }
        self.update_embed_message(source);
    }

    pub fn set_embed_ready(&self) {
        // Model loaded, update messages if still in spinner mode
        if self.embeddings_enabled {
            if self.embed_total[SourceKind::Claude.idx()].load(Ordering::Relaxed) == 0 {
                self.claude_embed.set_message("embedded 0 ready");
            }
            let codex_total = self.embed_total[SourceKind::CodexSession.idx()]
                .load(Ordering::Relaxed)
                + self.embed_total[SourceKind::CodexHistory.idx()].load(Ordering::Relaxed);
            if codex_total == 0 {
                self.codex_embed.set_message("embedded 0 ready");
            }
        }
    }

    pub fn finish(&self) {
        // Finish headers
        self.claude_header.finish();
        self.codex_header.finish();

        // Finish parse spinners
        let claude_parsed = self.claude_parse.position();
        if claude_parsed > 0 {
            self.claude_parse.finish_with_message(format!(
                "parsed {} {} files",
                format_bytes(claude_parsed),
                self.claude_files_total
            ));
        } else {
            self.claude_parse.finish_and_clear();
        }
        let codex_parsed = self.codex_parse.position();
        if codex_parsed > 0 {
            self.codex_parse.finish_with_message(format!(
                "parsed {} {} files",
                format_bytes(codex_parsed),
                self.codex_files_total
            ));
        } else {
            self.codex_parse.finish_and_clear();
        }

        // Finish index bars
        let claude_indexed = self.claude_index.position();
        if claude_indexed > 0 {
            self.claude_index
                .finish_with_message(format!("indexed {} rec", format_count(claude_indexed)));
        } else {
            self.claude_index.finish_and_clear();
        }
        let codex_indexed = self.codex_index.position();
        if codex_indexed > 0 {
            self.codex_index
                .finish_with_message(format!("indexed {} rec", format_count(codex_indexed)));
        } else {
            self.codex_index.finish_and_clear();
        }

        // Finish embed bars
        let claude_embedded = self.claude_embed.position();
        if self.embeddings_enabled && claude_embedded > 0 {
            self.claude_embed
                .finish_with_message(format!("embedded {}", format_count(claude_embedded)));
        } else {
            self.claude_embed.finish_and_clear();
        }
        let codex_embedded = self.codex_embed.position();
        if self.embeddings_enabled && codex_embedded > 0 {
            self.codex_embed
                .finish_with_message(format!("embedded {}", format_count(codex_embedded)));
        } else {
            self.codex_embed.finish_and_clear();
        }
    }
}

fn format_count(value: u64) -> String {
    if value < 1000 {
        return value.to_string();
    }
    let s = value.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GiB", b / GB)
    } else if b >= MB {
        format!("{:.1} MiB", b / MB)
    } else if b >= KB {
        format!("{:.1} KiB", b / KB)
    } else {
        format!("{bytes} B")
    }
}
