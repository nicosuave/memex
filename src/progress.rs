use crate::types::SourceKind;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const SOURCE_COUNT: usize = 6;
const SOURCES: [SourceKind; SOURCE_COUNT] = [
    SourceKind::Claude,
    SourceKind::CodexSession,
    SourceKind::CodexHistory,
    SourceKind::Opencode,
    SourceKind::Cursor,
    SourceKind::Pi,
];

pub struct Progress {
    #[allow(dead_code)] // Kept alive to coordinate progress bars.
    multi: MultiProgress,
    headers: Vec<ProgressBar>,
    parse: Vec<ProgressBar>,
    index: Vec<ProgressBar>,
    embed: Vec<ProgressBar>,
    files_total: [u64; SOURCE_COUNT],
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
        let header_style = ProgressStyle::with_template("{msg}").unwrap();
        let spinner_style = ProgressStyle::with_template("  {spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏");

        let mut headers = Vec::with_capacity(SOURCE_COUNT);
        let mut parse = Vec::with_capacity(SOURCE_COUNT);
        let mut index = Vec::with_capacity(SOURCE_COUNT);
        let mut embed = Vec::with_capacity(SOURCE_COUNT);

        for source in SOURCES {
            let idx = source.idx();
            let header = multi.add(ProgressBar::new_spinner());
            header.set_style(header_style.clone());
            header.set_message(progress_label(source));
            header.tick();
            headers.push(header);

            let parse_bar = multi.add(ProgressBar::new_spinner());
            parse_bar.set_style(spinner_style.clone());
            parse_bar.set_message(format!("parsed 0 B / {} files", files_total[idx]));
            parse_bar.enable_steady_tick(Duration::from_millis(80));
            parse.push(parse_bar);

            let index_bar = multi.add(ProgressBar::new_spinner());
            index_bar.set_style(spinner_style.clone());
            index_bar.set_message("indexed 0 rec");
            index_bar.enable_steady_tick(Duration::from_millis(80));
            index.push(index_bar);

            let embed_bar = if embeddings {
                let bar = multi.add(ProgressBar::new_spinner());
                bar.set_style(spinner_style.clone());
                bar.set_message("embedded 0");
                bar.enable_steady_tick(Duration::from_millis(80));
                bar
            } else {
                ProgressBar::hidden()
            };
            embed.push(embed_bar);
        }

        Self {
            multi,
            headers,
            parse,
            index,
            embed,
            files_total,
            files_done: std::array::from_fn(|_| AtomicU64::new(0)),
            produced: std::array::from_fn(|_| AtomicU64::new(0)),
            embed_total: std::array::from_fn(|_| AtomicU64::new(0)),
            embed_pending: std::array::from_fn(|_| AtomicU64::new(0)),
            embeddings_enabled: embeddings,
        }
    }

    pub fn add_parsed_bytes(&self, source: SourceKind, bytes: u64) {
        let idx = source.idx();
        self.parse[idx].inc(bytes);
        let total = self.parse[idx].position();
        let files_done = self.files_done[idx].load(Ordering::Relaxed);
        self.parse[idx].set_message(format!(
            "parsed {} {}/{} files",
            format_bytes(total),
            files_done,
            self.files_total[idx]
        ));
    }

    pub fn add_files_done(&self, source: SourceKind, count: u64) {
        let idx = source.idx();
        let done = self.files_done[idx].fetch_add(count, Ordering::Relaxed) + count;
        if done >= self.files_total[idx] {
            let bytes = self.parse[idx].position();
            self.parse[idx].finish_with_message(format!(
                "parsed {} {} files done",
                format_bytes(bytes),
                self.files_total[idx]
            ));
        }
    }

    pub fn add_produced(&self, source: SourceKind, count: u64) {
        self.produced[source.idx()].fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_indexed(&self, source: SourceKind, count: u64) {
        let idx = source.idx();
        self.index[idx].inc(count);
        let indexed = self.index[idx].position();
        let produced = self.produced[idx].load(Ordering::Relaxed);
        let files_done = self.files_done[idx].load(Ordering::Relaxed);
        if files_done >= self.files_total[idx] && indexed >= produced && produced > 0 {
            self.index[idx]
                .finish_with_message(format!("indexed {} rec done", format_count(indexed)));
        } else {
            self.index[idx].set_message(format!("indexed {} rec", format_count(indexed)));
        }
    }

    pub fn add_embed_total(&self, source: SourceKind, count: u64) {
        self.embed_total[source.idx()].fetch_add(count, Ordering::Relaxed);
        self.update_embed_message(source);
    }

    pub fn add_embed_pending(&self, source: SourceKind, count: u64) {
        self.embed_pending[source.idx()].fetch_add(count, Ordering::Relaxed);
        self.update_embed_message(source);
    }

    #[allow(dead_code)]
    pub fn sub_embed_pending(&self, source: SourceKind, count: u64) {
        self.embed_pending[source.idx()].fetch_sub(count, Ordering::Relaxed);
        self.update_embed_message(source);
    }

    fn update_embed_message(&self, source: SourceKind) {
        if !self.embeddings_enabled {
            return;
        }
        let idx = source.idx();
        let embedded = self.embed[idx].position();
        let total = self.embed_total[idx].load(Ordering::Relaxed);
        let pending = self.embed_pending[idx].load(Ordering::Relaxed);
        let msg = if total > 0 {
            if pending > 0 {
                format!(
                    "embedded {} / {} ({} queued)",
                    format_count(embedded),
                    format_count(total),
                    format_count(pending)
                )
            } else {
                format!(
                    "embedded {} / {}",
                    format_count(embedded),
                    format_count(total)
                )
            }
        } else {
            format!("embedded {}", format_count(embedded))
        };
        self.embed[idx].set_message(msg);
    }

    pub fn add_embedded(&self, source: SourceKind, count: u64) {
        let idx = source.idx();
        self.embed[idx].inc(count);
        let embedded = self.embed[idx].position();
        let total = self.embed_total[idx].load(Ordering::Relaxed);
        let pending = self.embed_pending[idx].load(Ordering::Relaxed);
        let indexed = self.index[idx].position();
        let produced = self.produced[idx].load(Ordering::Relaxed);
        if indexed >= produced && pending == 0 && embedded >= total && total > 0 {
            self.embed[idx]
                .finish_with_message(format!("embedded {} done", format_count(embedded)));
            return;
        }
        self.update_embed_message(source);
    }

    pub fn set_embed_ready(&self) {
        if !self.embeddings_enabled {
            return;
        }
        for source in SOURCES {
            let idx = source.idx();
            if self.embed_total[idx].load(Ordering::Relaxed) == 0 {
                self.embed[idx].set_message("embedded 0 ready");
            }
        }
    }

    pub fn finish(&self) {
        for source in SOURCES {
            let idx = source.idx();
            self.headers[idx].finish();

            let parsed = self.parse[idx].position();
            if parsed > 0 {
                self.parse[idx].finish_with_message(format!(
                    "parsed {} {} files",
                    format_bytes(parsed),
                    self.files_total[idx]
                ));
            } else {
                self.parse[idx].finish_and_clear();
            }

            let indexed = self.index[idx].position();
            if indexed > 0 {
                self.index[idx]
                    .finish_with_message(format!("indexed {} rec", format_count(indexed)));
            } else {
                self.index[idx].finish_and_clear();
            }

            let embedded = self.embed[idx].position();
            if self.embeddings_enabled && embedded > 0 {
                self.embed[idx].finish_with_message(format!("embedded {}", format_count(embedded)));
            } else {
                self.embed[idx].finish_and_clear();
            }
        }
    }
}

fn progress_label(source: SourceKind) -> &'static str {
    match source {
        SourceKind::Claude => "claude",
        SourceKind::CodexSession => "codex",
        SourceKind::CodexHistory => "codex-history",
        SourceKind::Opencode => "opencode",
        SourceKind::Cursor => "cursor",
        SourceKind::Pi => "pi",
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
