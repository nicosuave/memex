use crate::types::SourceKind;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub const SOURCE_COUNT: usize = SourceKind::COUNT;
const SOURCES: [SourceKind; SOURCE_COUNT] = SourceKind::ALL;

pub struct Progress {
    #[allow(dead_code)] // Kept alive to coordinate progress bars.
    multi: MultiProgress,
    header_style: ProgressStyle,
    spinner_style: ProgressStyle,
    headers: Vec<OnceLock<ProgressBar>>,
    parse: Vec<OnceLock<ProgressBar>>,
    index: Vec<OnceLock<ProgressBar>>,
    embed: Vec<OnceLock<ProgressBar>>,
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

        let mut headers: Vec<OnceLock<ProgressBar>> = Vec::with_capacity(SOURCE_COUNT);
        let mut parse: Vec<OnceLock<ProgressBar>> = Vec::with_capacity(SOURCE_COUNT);
        let mut index: Vec<OnceLock<ProgressBar>> = Vec::with_capacity(SOURCE_COUNT);
        let mut embed: Vec<OnceLock<ProgressBar>> = Vec::with_capacity(SOURCE_COUNT);
        for _ in SOURCES {
            headers.push(OnceLock::new());
            parse.push(OnceLock::new());
            index.push(OnceLock::new());
            embed.push(OnceLock::new());
        }

        let progress = Self {
            multi,
            header_style,
            spinner_style,
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
        };
        // Only show sources with file work upfront. Sources with no files stay
        // hidden until real work arrives (e.g. OpenCode database records or
        // embedding backfill), so agents that aren't installed don't spin.
        for source in SOURCES {
            if files_total[source.idx()] > 0 {
                progress.ensure_header(source);
                progress.ensure_parse(source);
                progress.ensure_index(source);
                if embeddings {
                    progress.ensure_embed(source);
                }
            }
        }
        progress
    }

    fn ensure_header(&self, source: SourceKind) -> ProgressBar {
        let idx = source.idx();
        self.headers[idx]
            .get_or_init(|| {
                let header = self.multi.add(ProgressBar::new_spinner());
                header.set_style(self.header_style.clone());
                header.set_message(progress_label(source));
                header.tick();
                header
            })
            .clone()
    }

    fn ensure_parse(&self, source: SourceKind) -> ProgressBar {
        self.ensure_header(source);
        let idx = source.idx();
        self.parse[idx]
            .get_or_init(|| {
                let bar = self.multi.add(ProgressBar::new_spinner());
                bar.set_style(self.spinner_style.clone());
                bar.set_message(format!("parsed 0 B / {} files", self.files_total[idx]));
                bar.enable_steady_tick(Duration::from_millis(80));
                bar
            })
            .clone()
    }

    fn ensure_index(&self, source: SourceKind) -> ProgressBar {
        self.ensure_header(source);
        let idx = source.idx();
        self.index[idx]
            .get_or_init(|| {
                let bar = self.multi.add(ProgressBar::new_spinner());
                bar.set_style(self.spinner_style.clone());
                bar.set_message("indexed 0 rec");
                bar.enable_steady_tick(Duration::from_millis(80));
                bar
            })
            .clone()
    }

    /// A standalone spinner line appended below the per-source bars, for phases
    /// that aren't per-source work (commit/merge/publish, analytics backfill).
    pub fn tail_spinner(&self, msg: &'static str) -> ProgressBar {
        let bar = self.multi.add(ProgressBar::new_spinner());
        bar.set_style(self.spinner_style.clone());
        bar.set_message(msg);
        bar.enable_steady_tick(Duration::from_millis(80));
        bar
    }

    fn ensure_embed(&self, source: SourceKind) -> ProgressBar {
        self.ensure_header(source);
        let idx = source.idx();
        self.embed[idx]
            .get_or_init(|| {
                if self.embeddings_enabled {
                    let bar = self.multi.add(ProgressBar::new_spinner());
                    bar.set_style(self.spinner_style.clone());
                    bar.set_message("embedded 0");
                    bar.enable_steady_tick(Duration::from_millis(80));
                    bar
                } else {
                    ProgressBar::hidden()
                }
            })
            .clone()
    }

    pub fn add_parsed_bytes(&self, source: SourceKind, bytes: u64) {
        let idx = source.idx();
        let bar = self.ensure_parse(source);
        bar.inc(bytes);
        let total = bar.position();
        let files_done = self.files_done[idx].load(Ordering::Relaxed);
        bar.set_message(format!(
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
            if let Some(bar) = self.parse[idx].get() {
                let bytes = bar.position();
                bar.finish_with_message(format!(
                    "parsed {} {} files done",
                    format_bytes(bytes),
                    self.files_total[idx]
                ));
            }
            // Files with no records leave no index work behind; clear a
            // possibly-visible index line promptly instead of spinning until
            // the end of the run.
            if self.produced[idx].load(Ordering::Relaxed) == 0
                && let Some(bar) = self.index[idx].get()
                && !bar.is_finished()
            {
                bar.finish_and_clear();
            }
        }
    }

    pub fn add_produced(&self, source: SourceKind, count: u64) {
        self.produced[source.idx()].fetch_add(count, Ordering::Relaxed);
        // Reveal lazily-hidden sources (e.g. OpenCode database records with no
        // file tasks) as soon as real work arrives.
        self.ensure_index(source);
    }

    pub fn add_indexed(&self, source: SourceKind, count: u64) {
        let idx = source.idx();
        let bar = self.ensure_index(source);
        bar.inc(count);
        let indexed = bar.position();
        let produced = self.produced[idx].load(Ordering::Relaxed);
        let files_done = self.files_done[idx].load(Ordering::Relaxed);
        if files_done < self.files_total[idx] || indexed < produced {
            bar.set_message(format!("indexed {} rec", format_count(indexed)));
        } else if produced == 0 {
            bar.finish_and_clear();
        } else {
            bar.finish_with_message(format!("indexed {} rec done", format_count(indexed)));
        }
    }

    pub fn add_embed_total(&self, source: SourceKind, count: u64) {
        self.embed_total[source.idx()].fetch_add(count, Ordering::Relaxed);
        if self.embeddings_enabled {
            self.ensure_embed(source);
        }
        self.update_embed_message(source);
    }

    pub fn add_embed_pending(&self, source: SourceKind, count: u64) {
        self.embed_pending[source.idx()].fetch_add(count, Ordering::Relaxed);
        if self.embeddings_enabled {
            self.ensure_embed(source);
        }
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
        let Some(bar) = self.embed[source.idx()].get() else {
            return;
        };
        let idx = source.idx();
        let embedded = bar.position();
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
        bar.set_message(msg);
    }

    pub fn add_embedded(&self, source: SourceKind, count: u64) {
        let idx = source.idx();
        let bar = self.ensure_embed(source);
        bar.inc(count);
        let embedded = bar.position();
        let total = self.embed_total[idx].load(Ordering::Relaxed);
        let pending = self.embed_pending[idx].load(Ordering::Relaxed);
        let indexed = self.index[idx].get().map(|b| b.position()).unwrap_or(0);
        let produced = self.produced[idx].load(Ordering::Relaxed);
        if indexed >= produced && pending == 0 && embedded >= total && total > 0 {
            bar.finish_with_message(format!("embedded {} done", format_count(embedded)));
            return;
        }
        self.update_embed_message(source);
    }

    pub fn set_embed_ready(&self) {
        if !self.embeddings_enabled {
            return;
        }
        // Only touch already-visible bars. Creating "ready" lines for idle
        // sources would reintroduce the spinner noise this hides.
        for source in SOURCES {
            let idx = source.idx();
            if self.embed_total[idx].load(Ordering::Relaxed) == 0
                && let Some(bar) = self.embed[idx].get()
            {
                bar.set_message("embedded 0 ready");
            }
        }
    }

    pub fn finish(&self) {
        for source in SOURCES {
            let idx = source.idx();
            if let Some(bar) = self.headers[idx].get() {
                bar.finish();
            }

            if let Some(bar) = self.parse[idx].get() {
                let parsed = bar.position();
                if parsed > 0 {
                    bar.finish_with_message(format!(
                        "parsed {} {} files",
                        format_bytes(parsed),
                        self.files_total[idx]
                    ));
                } else {
                    bar.finish_and_clear();
                }
            }

            if let Some(bar) = self.index[idx].get() {
                let indexed = bar.position();
                if indexed > 0 {
                    bar.finish_with_message(format!("indexed {} rec", format_count(indexed)));
                } else {
                    bar.finish_and_clear();
                }
            }

            if let Some(bar) = self.embed[idx].get() {
                let embedded = bar.position();
                if self.embeddings_enabled && embedded > 0 {
                    bar.finish_with_message(format!("embedded {}", format_count(embedded)));
                } else {
                    bar.finish_and_clear();
                }
            }
        }
    }
}

fn progress_label(source: SourceKind) -> &'static str {
    match source {
        SourceKind::Claude => "claude",
        SourceKind::Codex => "codex",
        SourceKind::Opencode => "opencode",
        SourceKind::Cursor => "cursor",
        SourceKind::Pi => "pi",
        SourceKind::OpenClaw => "openclaw",
        SourceKind::Copilot => "copilot",
        SourceKind::Omp => "omp",
        SourceKind::Grok => "grok",
        SourceKind::Hermes => "hermes",
        SourceKind::Jcode => "jcode",
        SourceKind::Muse => "muse",
        SourceKind::Antigravity => "antigravity",
    }
}

pub(crate) fn format_count(value: u64) -> String {
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
