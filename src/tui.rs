use crate::config::{Paths, UserConfig, default_claude_source};
use crate::index::{QueryOptions, SearchIndex};
use crate::ingest::{IngestOptions, ingest_if_stale};
use crate::types::{Record, SourceFilter, SourceKind};
use anyhow::Result;
use chrono::SecondsFormat;
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers, MouseButton,
    MouseEvent, MouseEventKind,
};
use crossterm::{execute, terminal};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, List, ListItem, ListState, Paragraph, Wrap};
use std::collections::{HashMap, HashSet};
use std::io::BufRead;
#[cfg(not(unix))]
use std::io::Stdout;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::ffi::CString;
#[cfg(unix)]
use std::fs::OpenOptions;

type TuiBackend = CrosstermBackend<TuiWriter>;
type TuiTerminal = Terminal<TuiBackend>;

#[cfg(unix)]
type TuiWriter = std::fs::File;
#[cfg(not(unix))]
type TuiWriter = Stdout;

enum IndexUpdate {
    Started,
    Skipped,
    Done { added: usize, embedded: usize },
    Error(String),
}

enum SearchUpdate {
    Started,
    Results(Vec<SessionSummary>),
    Projects {
        projects: Vec<String>,
        source: SourceChoice,
    },
    Error(String),
}

const RESULT_LIMIT: usize = 200;
const DETAIL_TAIL_LINES: usize = 10;
const MAX_MESSAGE_CHARS: usize = 4000;
const PREVIEW_LINE_MAX_CHARS: usize = 320;
const CONTEXT_AROUND_MATCH: usize = 1;
const RECENT_SESSIONS_LIMIT: usize = 200;
const RECENT_RECORDS_MULTIPLIER: usize = 50;

const OUTER_PAD_X: u16 = 0;
const OUTER_PAD_Y: u16 = 0;
const PANEL_PAD_X: u16 = 2;
const PANEL_PAD_Y: u16 = 1;
const PANEL_TITLE_HEIGHT: u16 = 1;
const HEADER_HEIGHT: u16 = 3;
const FOOTER_HEIGHT: u16 = 2;
const PROJECT_PANEL_HEIGHT: u16 = 6;
const SPLIT_GAP: u16 = 1;

const COLOR_BASE: Color = Color::Black;
const COLOR_PANEL: Color = Color::Black;
const COLOR_PANEL_ALT: Color = Color::Black;
const COLOR_TEXT: Color = Color::Rgb(220, 220, 220);
const COLOR_MUTED: Color = Color::Rgb(140, 140, 140);
const COLOR_ACCENT: Color = Color::Rgb(198, 150, 115);
const COLOR_SELECTION_BG: Color = Color::Rgb(214, 160, 120);
const COLOR_SELECTION_FG: Color = Color::Rgb(20, 20, 20);
const COLOR_DIVIDER: Color = Color::Rgb(36, 36, 36);

#[derive(Clone, Copy, Debug)]
enum Focus {
    Query,
    Project,
    List,
    Preview,
    Find,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PreviewMode {
    Matches,
    History,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum SourceChoice {
    All,
    Claude,
    Codex,
    Opencode,
}

impl SourceChoice {
    fn cycle(self) -> Self {
        match self {
            SourceChoice::All => SourceChoice::Claude,
            SourceChoice::Claude => SourceChoice::Codex,
            SourceChoice::Codex => SourceChoice::Opencode,
            SourceChoice::Opencode => SourceChoice::All,
        }
    }

    fn as_filter(self) -> Option<SourceFilter> {
        match self {
            SourceChoice::All => None,
            SourceChoice::Claude => Some(SourceFilter::Claude),
            SourceChoice::Codex => Some(SourceFilter::Codex),
            SourceChoice::Opencode => Some(SourceFilter::Opencode),
        }
    }

    fn label(self) -> &'static str {
        match self {
            SourceChoice::All => "all",
            SourceChoice::Claude => "claude",
            SourceChoice::Codex => "codex",
            SourceChoice::Opencode => "opencode",
        }
    }
}

#[derive(Clone, Debug)]
struct SessionSummary {
    session_id: String,
    project: String,
    source: SourceKind,
    last_ts: u64,
    hit_count: usize,
    top_score: f32,
    snippet: String,
    source_path: String,
    source_dir: String,
}

struct App {
    paths: Paths,
    config: UserConfig,
    index: SearchIndex,
    focus: Focus,
    query: String,
    project: String,
    source: SourceChoice,
    all_projects: Vec<String>,
    project_options: Vec<String>,
    project_selected: usize,
    project_source: SourceChoice,
    results: Vec<SessionSummary>,
    selected: ListState,
    preview_mode: PreviewMode,
    show_tools: bool,
    find_query: String,
    detail_lines: Vec<PreviewLine>,
    detail_scroll: usize,
    last_detail_session: Option<String>,
    last_detail_query: Option<String>,
    last_detail_mode: PreviewMode,
    last_detail_find: Option<String>,
    status: String,
    last_status_at: Option<Instant>,
    update_message: Option<String>,
    index_rx: std::sync::mpsc::Receiver<IndexUpdate>,
    index_tx: std::sync::mpsc::Sender<IndexUpdate>,
    search_rx: std::sync::mpsc::Receiver<SearchUpdate>,
    search_tx: std::sync::mpsc::Sender<SearchUpdate>,
    update_rx: Option<std::sync::mpsc::Receiver<String>>,
    header_area: Rect,
    body_area: Rect,
    list_area: Rect,
    preview_area: Rect,
    project_area: Option<Rect>,
    left_width: Option<u16>,
    dragging: bool,
    stdio_redirect: Option<StdIoRedirect>,
}

#[derive(Clone, Debug)]
enum PreviewLine {
    SessionHeader {
        project: String,
        source: String,
        session_id: String,
    },
    Meta {
        role: String,
        ts: String,
        highlight: bool,
    },
    Text(String),
    Empty,
}

struct Theme {
    base: Style,
    panel: Style,
    panel_alt: Style,
    text: Style,
    text_bold: Style,
    muted: Style,
    accent: Style,
    focus: Style,
    selection: Style,
}

impl Theme {
    fn new() -> Self {
        Self {
            base: Style::default().bg(COLOR_BASE).fg(COLOR_TEXT),
            panel: Style::default().bg(COLOR_PANEL).fg(COLOR_TEXT),
            panel_alt: Style::default().bg(COLOR_PANEL_ALT).fg(COLOR_TEXT),
            text: Style::default().fg(COLOR_TEXT),
            text_bold: Style::default().fg(COLOR_TEXT).add_modifier(Modifier::BOLD),
            muted: Style::default().fg(COLOR_MUTED),
            accent: Style::default().fg(COLOR_ACCENT),
            focus: Style::default()
                .fg(COLOR_ACCENT)
                .add_modifier(Modifier::BOLD),
            selection: Style::default()
                .fg(COLOR_SELECTION_FG)
                .bg(COLOR_SELECTION_BG)
                .add_modifier(Modifier::BOLD),
        }
    }
}

#[cfg(unix)]
struct StdIoRedirect {
    stdout_fd: i32,
    stderr_fd: i32,
    devnull_fd: i32,
    active: bool,
}

#[cfg(unix)]
impl StdIoRedirect {
    fn new() -> Result<Self> {
        let devnull = CString::new("/dev/null").unwrap();
        let devnull_fd = unsafe { libc::open(devnull.as_ptr(), libc::O_WRONLY) };
        if devnull_fd < 0 {
            return Err(anyhow::anyhow!("failed to open /dev/null"));
        }
        let stdout_fd = unsafe { libc::dup(libc::STDOUT_FILENO) };
        if stdout_fd < 0 {
            unsafe { libc::close(devnull_fd) };
            return Err(anyhow::anyhow!("failed to dup stdout"));
        }
        let stderr_fd = unsafe { libc::dup(libc::STDERR_FILENO) };
        if stderr_fd < 0 {
            unsafe {
                libc::close(devnull_fd);
                libc::close(stdout_fd);
            }
            return Err(anyhow::anyhow!("failed to dup stderr"));
        }
        Ok(Self {
            stdout_fd,
            stderr_fd,
            devnull_fd,
            active: false,
        })
    }

    fn enable(&mut self) -> Result<()> {
        if self.active {
            return Ok(());
        }
        let stdout_rc = unsafe { libc::dup2(self.devnull_fd, libc::STDOUT_FILENO) };
        if stdout_rc < 0 {
            return Err(anyhow::anyhow!("failed to redirect stdout"));
        }
        let stderr_rc = unsafe { libc::dup2(self.devnull_fd, libc::STDERR_FILENO) };
        if stderr_rc < 0 {
            return Err(anyhow::anyhow!("failed to redirect stderr"));
        }
        self.active = true;
        Ok(())
    }

    fn disable(&mut self) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        let stdout_rc = unsafe { libc::dup2(self.stdout_fd, libc::STDOUT_FILENO) };
        if stdout_rc < 0 {
            return Err(anyhow::anyhow!("failed to restore stdout"));
        }
        let stderr_rc = unsafe { libc::dup2(self.stderr_fd, libc::STDERR_FILENO) };
        if stderr_rc < 0 {
            return Err(anyhow::anyhow!("failed to restore stderr"));
        }
        self.active = false;
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for StdIoRedirect {
    fn drop(&mut self) {
        let _ = self.disable();
        unsafe {
            libc::close(self.devnull_fd);
            libc::close(self.stdout_fd);
            libc::close(self.stderr_fd);
        }
    }
}

#[cfg(not(unix))]
struct StdIoRedirect;

#[cfg(not(unix))]
impl StdIoRedirect {
    fn new() -> Result<Self> {
        Ok(Self)
    }
    fn enable(&mut self) -> Result<()> {
        Ok(())
    }
    fn disable(&mut self) -> Result<()> {
        Ok(())
    }
}

pub fn run(
    root: Option<PathBuf>,
    update_rx: Option<std::sync::mpsc::Receiver<String>>,
) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let (index_tx, index_rx) = std::sync::mpsc::channel();
    let (search_tx, search_rx) = std::sync::mpsc::channel();

    let mut app = App::new(
        paths, config, index, index_tx, index_rx, search_tx, search_rx,
    );
    app.stdio_redirect = Some(StdIoRedirect::new()?);
    app.update_rx = update_rx;
    app.kickoff_index_refresh();
    app.kickoff_search();

    let mut terminal = enter_terminal()?;
    app.suppress_stdio()?;
    let res = run_loop(&mut terminal, &mut app);
    app.restore_stdio()?;
    exit_terminal(&mut terminal)?;
    res
}

impl App {
    fn new(
        paths: Paths,
        config: UserConfig,
        index: SearchIndex,
        index_tx: std::sync::mpsc::Sender<IndexUpdate>,
        index_rx: std::sync::mpsc::Receiver<IndexUpdate>,
        search_tx: std::sync::mpsc::Sender<SearchUpdate>,
        search_rx: std::sync::mpsc::Receiver<SearchUpdate>,
    ) -> Self {
        Self {
            paths,
            config,
            index,
            focus: Focus::Query,
            query: String::new(),
            project: String::new(),
            source: SourceChoice::All,
            all_projects: Vec::new(),
            project_options: Vec::new(),
            project_selected: 0,
            project_source: SourceChoice::All,
            results: Vec::new(),
            selected: ListState::default(),
            preview_mode: PreviewMode::Matches,
            show_tools: false,
            find_query: String::new(),
            detail_lines: Vec::new(),
            detail_scroll: 0,
            last_detail_session: None,
            last_detail_query: None,
            last_detail_mode: PreviewMode::Matches,
            last_detail_find: None,
            status: String::new(),
            last_status_at: None,
            update_message: None,
            index_tx,
            index_rx,
            search_tx,
            search_rx,
            update_rx: None,
            header_area: Rect::default(),
            body_area: Rect::default(),
            list_area: Rect::default(),
            preview_area: Rect::default(),
            project_area: None,
            left_width: None,
            dragging: false,
            stdio_redirect: None,
        }
    }

    fn refresh_results(&mut self) {
        self.kickoff_search();
    }

    fn kickoff_index_refresh(&self) {
        if !self.config.auto_index_on_search_default() {
            return;
        }
        let paths = self.paths.clone();
        let config = self.config.clone();
        let tx = self.index_tx.clone();
        std::thread::spawn(move || {
            let _ = tx.send(IndexUpdate::Started);
            let result = (|| -> Result<Option<crate::ingest::IngestReport>> {
                let index = SearchIndex::open_or_create(&paths.index)?;
                let embeddings_default = config.embeddings_default();
                let model_choice = config.resolve_model(None)?;
                let vector_exists = paths.vectors.join("meta.json").exists()
                    && paths.vectors.join("vectors.f32").exists()
                    && paths.vectors.join("doc_ids.u64").exists();
                let backfill_embeddings =
                    embeddings_default && !vector_exists && index.doc_count()? > 0;
                let opts = IngestOptions {
                    claude_source: default_claude_source(),
                    include_agents: false,
                    include_codex: true,
                    include_opencode: true,
                    embeddings: embeddings_default,
                    backfill_embeddings,
                    model: model_choice,
                    compute_units: config.resolve_compute_units(),
                };
                ingest_if_stale(&paths, &index, &opts, config.scan_cache_ttl())
            })();
            match result {
                Ok(Some(report)) => {
                    let _ = tx.send(IndexUpdate::Done {
                        added: report.records_added,
                        embedded: report.records_embedded,
                    });
                }
                Ok(None) => {
                    let _ = tx.send(IndexUpdate::Skipped);
                }
                Err(err) => {
                    let _ = tx.send(IndexUpdate::Error(err.to_string()));
                }
            }
        });
    }

    fn update_detail(&mut self) {
        let Some(idx) = self.selected.selected() else {
            self.detail_lines = vec![PreviewLine::Text("no session selected".to_string())];
            self.detail_scroll = 0;
            return;
        };
        if idx >= self.results.len() {
            self.detail_lines = vec![PreviewLine::Text("no session selected".to_string())];
            self.detail_scroll = 0;
            return;
        }
        let session = &self.results[idx];
        let query_now = self.query.trim().to_string();
        let session_changed = self
            .last_detail_session
            .as_ref()
            .map(|s| s != &session.session_id)
            .unwrap_or(true);
        let query_changed = self
            .last_detail_query
            .as_ref()
            .map(|q| q != &query_now)
            .unwrap_or(true);
        let mode_changed = self.preview_mode != self.last_detail_mode;
        let find_now = self.find_query.trim().to_string();
        let find_changed = self
            .last_detail_find
            .as_ref()
            .map(|f| f != &find_now)
            .unwrap_or(true);
        if !session_changed && !query_changed && !mode_changed && !find_changed {
            return;
        }
        let active_query = if self.find_query.trim().is_empty() {
            query_now.as_str()
        } else {
            self.find_query.trim()
        };
        match build_detail_lines(
            &self.index,
            session,
            self.preview_mode,
            active_query,
            self.show_tools,
        ) {
            Ok(lines) => {
                self.detail_lines = lines;
                self.detail_scroll = 0;
                self.last_detail_session = Some(session.session_id.clone());
                self.last_detail_query = Some(query_now);
                self.last_detail_mode = self.preview_mode;
                self.last_detail_find = Some(find_now);
            }
            Err(err) => {
                self.detail_lines = vec![PreviewLine::Text(format!("detail error: {err}"))];
                self.detail_scroll = 0;
                self.last_detail_session = None;
                self.last_detail_query = None;
                self.last_detail_find = None;
            }
        }
    }

    fn kickoff_search(&mut self) {
        let query = self.query.trim().to_string();
        let project = self.project.trim().to_string();
        let project_opt = if project.is_empty() {
            None
        } else {
            Some(project)
        };
        let source = self.source;
        let paths = self.paths.clone();
        let tx = self.search_tx.clone();
        self.set_status("searching...");
        std::thread::spawn(move || {
            let _ = tx.send(SearchUpdate::Started);
            let result = (|| -> Result<(Vec<SessionSummary>, Option<Vec<String>>)> {
                let index = SearchIndex::open_or_create(&paths.index)?;
                let sessions = if query.is_empty() {
                    sessions_from_recent(&index, source.as_filter(), project_opt.as_deref())?
                } else {
                    sessions_from_query(
                        &index,
                        &query,
                        source.as_filter(),
                        project_opt.as_deref(),
                        RESULT_LIMIT,
                    )?
                };
                Ok((sessions, None))
            })();
            match result {
                Ok((sessions, projects)) => {
                    let _ = tx.send(SearchUpdate::Results(sessions));
                    if let Some(projects) = projects {
                        let _ = tx.send(SearchUpdate::Projects { projects, source });
                    }
                }
                Err(err) => {
                    let _ = tx.send(SearchUpdate::Error(err.to_string()));
                }
            }
        });
    }

    fn kickoff_project_load(&self) {
        let source = self.source;
        let paths = self.paths.clone();
        let tx = self.search_tx.clone();
        std::thread::spawn(move || {
            let result = (|| -> Result<Vec<String>> {
                let index = SearchIndex::open_or_create(&paths.index)?;
                collect_projects(&index, source.as_filter())
            })();
            match result {
                Ok(projects) => {
                    let _ = tx.send(SearchUpdate::Projects { projects, source });
                }
                Err(err) => {
                    let _ = tx.send(SearchUpdate::Error(err.to_string()));
                }
            }
        });
    }

    fn update_project_options(&mut self) {
        let filter = self.project.trim().to_lowercase();
        let mut options = Vec::new();
        for project in &self.all_projects {
            if filter.is_empty() || project.to_lowercase().contains(&filter) {
                options.push(project.clone());
            }
        }
        self.project_options = options;
        if self.project_options.is_empty() || self.project_selected >= self.project_options.len() {
            self.project_selected = 0;
        }
    }

    fn set_status(&mut self, msg: impl Into<String>) {
        self.status = msg.into();
        self.last_status_at = Some(Instant::now());
    }

    fn clear_status_if_old(&mut self) -> bool {
        if let Some(at) = self.last_status_at
            && at.elapsed() > Duration::from_secs(4)
        {
            self.status.clear();
            self.last_status_at = None;
            return true;
        }
        false
    }

    fn move_selection(&mut self, delta: isize) {
        if self.results.is_empty() {
            self.selected.select(None);
            return;
        }
        let idx = self.selected.selected().unwrap_or(0) as isize + delta;
        let next = idx.clamp(0, (self.results.len() - 1) as isize) as usize;
        self.selected.select(Some(next));
        self.update_detail();
    }

    fn move_project_selection(&mut self, delta: isize) {
        if self.project_options.is_empty() {
            self.project_selected = 0;
            return;
        }
        let idx = self.project_selected as isize + delta;
        let next = idx.clamp(0, (self.project_options.len() - 1) as isize) as usize;
        self.project_selected = next;
    }

    fn toggle_preview_mode(&mut self) {
        self.preview_mode = match self.preview_mode {
            PreviewMode::Matches => PreviewMode::History,
            PreviewMode::History => PreviewMode::Matches,
        };
        self.last_detail_session = None;
        self.update_detail();
    }

    fn toggle_tools(&mut self) {
        self.show_tools = !self.show_tools;
        self.last_detail_session = None;
        self.update_detail();
    }

    fn scroll_detail(&mut self, delta: isize) {
        if self.detail_lines.is_empty() {
            return;
        }
        let view_height = self.preview_area.height as usize;
        let max_scroll = if view_height == 0 {
            self.detail_lines.len().saturating_sub(1)
        } else {
            self.detail_lines.len().saturating_sub(view_height)
        };
        let next = (self.detail_scroll as isize + delta).clamp(0, max_scroll as isize) as usize;
        self.detail_scroll = next;
    }

    fn update_find(&mut self) {
        self.last_detail_session = None;
        self.update_detail();
    }

    fn resume_selected(&mut self, terminal: &mut TuiTerminal) -> Result<()> {
        let Some(idx) = self.selected.selected() else {
            self.set_status("no session selected");
            return Ok(());
        };
        let Some(session) = self.results.get(idx) else {
            self.set_status("no session selected");
            return Ok(());
        };
        let template = match session.source {
            SourceKind::Claude => self
                .config
                .claude_resume_cmd
                .clone()
                .or_else(|| default_resume_template("claude")),
            SourceKind::CodexSession | SourceKind::CodexHistory => self
                .config
                .codex_resume_cmd
                .clone()
                .or_else(|| default_resume_template("codex")),
            SourceKind::Opencode => self
                .config
                .opencode_resume_cmd
                .clone()
                .or_else(|| default_resume_template("opencode")),
        };
        let Some(template) = template else {
            self.set_status("resume command not configured in config.toml");
            return Ok(());
        };
        let cwd = resolve_session_cwd(session).unwrap_or_else(|| session.source_dir.clone());
        let command = expand_resume_template(&template, session, &cwd);
        run_external_command(self, terminal, &command)?;
        self.set_status(format!("ran: {command}"));
        Ok(())
    }

    fn share_selected(&mut self) -> Result<()> {
        let Some(idx) = self.selected.selected() else {
            self.set_status("no session selected");
            return Ok(());
        };
        let Some(session) = self.results.get(idx) else {
            self.set_status("no session selected");
            return Ok(());
        };

        // Check if agentexport is installed
        if find_in_path("agentexport").is_none() {
            self.set_status("agentexport not found (brew install nicosuave/tap/agentexport)");
            return Ok(());
        }

        let tool = match session.source {
            SourceKind::Claude => "claude",
            SourceKind::CodexSession | SourceKind::CodexHistory => "codex",
            SourceKind::Opencode => "opencode",
        };
        let source_path = session.source_path.clone();

        self.set_status("sharing...");

        // Run agentexport in background
        let output = std::process::Command::new("agentexport")
            .args(["publish", "--tool", tool, "--transcript", &source_path])
            .output();

        match output {
            Ok(output) if output.status.success() => {
                let url = String::from_utf8_lossy(&output.stdout);
                let url = url.trim();
                if url.is_empty() {
                    self.set_status("share failed: no URL returned");
                } else {
                    self.set_status(format!("shared: {url}"));
                }
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                self.set_status(format!(
                    "share failed: {}",
                    stderr.lines().next().unwrap_or("unknown error")
                ));
            }
            Err(err) => {
                self.set_status(format!("share failed: {err}"));
            }
        }
        Ok(())
    }

    fn suppress_stdio(&mut self) -> Result<()> {
        if let Some(redirect) = self.stdio_redirect.as_mut() {
            redirect.enable()?;
        }
        Ok(())
    }

    fn restore_stdio(&mut self) -> Result<()> {
        if let Some(redirect) = self.stdio_redirect.as_mut() {
            redirect.disable()?;
        }
        Ok(())
    }
}

fn run_loop(terminal: &mut TuiTerminal, app: &mut App) -> Result<()> {
    loop {
        let mut dirty = app.clear_status_if_old();
        if let Some(update_rx) = app.update_rx.as_ref() {
            while let Ok(message) = update_rx.try_recv() {
                app.update_message = Some(message);
                dirty = true;
            }
        }
        if let Ok(update) = app.index_rx.try_recv() {
            match update {
                IndexUpdate::Started => app.set_status("indexing..."),
                IndexUpdate::Skipped => app.set_status("index up to date"),
                IndexUpdate::Done { added, embedded } => {
                    app.set_status(format!("indexed {added} records, embedded {embedded}"))
                }
                IndexUpdate::Error(msg) => app.set_status(format!("index error: {msg}")),
            }
            dirty = true;
        }
        while let Ok(update) = app.search_rx.try_recv() {
            match update {
                SearchUpdate::Started => app.set_status("searching..."),
                SearchUpdate::Results(results) => {
                    app.results = results;
                    if app.results.is_empty() {
                        app.selected.select(None);
                    } else {
                        app.selected.select(Some(0));
                    }
                    app.last_detail_session = None;
                    app.detail_scroll = 0;
                    app.set_status(format!("{} sessions", app.results.len()));
                    app.update_detail();
                }
                SearchUpdate::Projects { projects, source } => {
                    app.all_projects = projects;
                    app.project_source = source;
                    app.update_project_options();
                }
                SearchUpdate::Error(msg) => app.set_status(format!("search error: {msg}")),
            }
            dirty = true;
        }
        let mut should_quit = false;
        if crossterm::event::poll(Duration::from_millis(16))? {
            loop {
                match crossterm::event::read()? {
                    Event::Key(key) => {
                        if handle_key(key, terminal, app)? {
                            should_quit = true;
                            break;
                        }
                    }
                    Event::Mouse(mouse) => {
                        handle_mouse(mouse, app);
                    }
                    _ => {}
                }
                dirty = true;
                if !crossterm::event::poll(Duration::from_millis(0))? {
                    break;
                }
            }
        }
        if should_quit {
            break;
        }
        if dirty {
            terminal.draw(|f| draw_ui(f, app))?;
        }
    }
    Ok(())
}

fn handle_key(key: KeyEvent, terminal: &mut TuiTerminal, app: &mut App) -> Result<bool> {
    if matches!(key.code, KeyCode::Esc) {
        if matches!(app.focus, Focus::List) {
            return Ok(true);
        }
        if matches!(app.focus, Focus::Find) {
            app.focus = Focus::Preview;
        } else {
            app.focus = Focus::List;
        }
        return Ok(false);
    }

    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('q')) {
        return Ok(true);
    }

    if matches!(app.focus, Focus::Query | Focus::Project) {
        match key.code {
            KeyCode::Tab => {
                app.focus = match app.focus {
                    Focus::Query => Focus::Project,
                    Focus::Project => Focus::List,
                    Focus::List => Focus::Preview,
                    Focus::Preview | Focus::Find => Focus::Query,
                };
            }
            KeyCode::BackTab => {
                app.focus = match app.focus {
                    Focus::Query => Focus::Preview,
                    Focus::Project => Focus::Query,
                    Focus::List => Focus::Project,
                    Focus::Preview | Focus::Find => Focus::List,
                };
            }
            KeyCode::Enter => {
                if matches!(app.focus, Focus::Project)
                    && let Some(project) = app.project_options.get(app.project_selected)
                {
                    app.project = project.clone();
                }
                app.set_status("searching...");
                terminal.draw(|f| draw_ui(f, app))?;
                app.refresh_results();
                app.focus = Focus::List;
            }
            KeyCode::Backspace => match app.focus {
                Focus::Query => {
                    app.query.pop();
                }
                Focus::Project => {
                    app.project.pop();
                    app.update_project_options();
                }
                Focus::List => {}
                Focus::Preview => {}
                Focus::Find => {}
            },
            KeyCode::Up => {
                if matches!(app.focus, Focus::Project) {
                    app.move_project_selection(-1);
                }
            }
            KeyCode::Down => {
                if matches!(app.focus, Focus::Project) {
                    app.move_project_selection(1);
                }
            }
            KeyCode::Char(ch) => {
                if !key.modifiers.contains(KeyModifiers::CONTROL) {
                    match app.focus {
                        Focus::Query => app.query.push(ch),
                        Focus::Project => {
                            app.project.push(ch);
                            app.update_project_options();
                        }
                        Focus::List => {}
                        Focus::Preview => {}
                        Focus::Find => {}
                    }
                }
            }
            _ => {}
        }
        return Ok(false);
    }

    if matches!(app.focus, Focus::Find) {
        match key.code {
            KeyCode::Enter => {
                app.update_find();
                app.focus = Focus::Preview;
            }
            KeyCode::Backspace => {
                app.find_query.pop();
                app.update_find();
            }
            KeyCode::Esc => {
                app.focus = Focus::Preview;
            }
            KeyCode::Char(ch) => {
                if !key.modifiers.contains(KeyModifiers::CONTROL) {
                    app.find_query.push(ch);
                    app.update_find();
                }
            }
            _ => {}
        }
        return Ok(false);
    }

    match key.code {
        KeyCode::Tab => {
            app.focus = match app.focus {
                Focus::Query => Focus::Project,
                Focus::Project => Focus::List,
                Focus::List => Focus::Preview,
                Focus::Preview | Focus::Find => Focus::Query,
            };
        }
        KeyCode::BackTab => {
            app.focus = match app.focus {
                Focus::Query => Focus::Preview,
                Focus::Project => Focus::Query,
                Focus::List => Focus::Project,
                Focus::Preview | Focus::Find => Focus::List,
            };
        }
        KeyCode::Up => {
            if matches!(app.focus, Focus::List) {
                app.move_selection(-1);
            }
        }
        KeyCode::Down => {
            if matches!(app.focus, Focus::List) {
                app.move_selection(1);
            }
        }
        KeyCode::Char('j') => {
            if matches!(app.focus, Focus::Preview) {
                app.scroll_detail(1);
            } else {
                app.move_selection(1);
            }
        }
        KeyCode::Char('k') => {
            if matches!(app.focus, Focus::Preview) {
                app.scroll_detail(-1);
            } else {
                app.move_selection(-1);
            }
        }
        KeyCode::Char('h') => {
            if matches!(app.focus, Focus::Preview) {
                app.focus = Focus::List;
            }
        }
        KeyCode::Char('l') => {
            if matches!(app.focus, Focus::List) {
                app.focus = Focus::Preview;
            }
        }
        KeyCode::PageDown => {
            if matches!(app.focus, Focus::Preview) {
                app.scroll_detail(8);
            }
        }
        KeyCode::PageUp => {
            if matches!(app.focus, Focus::Preview) {
                app.scroll_detail(-8);
            }
        }
        KeyCode::Char('s') => {
            app.source = app.source.cycle();
            app.set_status("searching...");
            terminal.draw(|f| draw_ui(f, app))?;
            app.refresh_results();
        }
        KeyCode::Char('m') => {
            app.toggle_preview_mode();
        }
        KeyCode::Char('t') => {
            app.toggle_tools();
        }
        KeyCode::Char('r') => {
            let _ = app.resume_selected(terminal);
        }
        KeyCode::Char('/') => {
            if matches!(app.focus, Focus::Preview) {
                app.focus = Focus::Find;
                app.find_query.clear();
                app.update_find();
            } else {
                app.focus = Focus::Query;
                app.query.clear();
            }
        }
        KeyCode::Char('p') => {
            app.focus = Focus::Project;
            if app.all_projects.is_empty() || app.project_source != app.source {
                app.kickoff_project_load();
            }
        }
        KeyCode::Char('f') => {
            app.focus = Focus::Find;
            app.find_query.clear();
            app.update_find();
        }
        KeyCode::Char('i') => {
            app.kickoff_index_refresh();
        }
        KeyCode::Char('S') => {
            let _ = app.share_selected();
        }
        _ => {}
    }
    Ok(false)
}

fn draw_ui(frame: &mut ratatui::Frame, app: &mut App) {
    let theme = Theme::new();
    frame.render_widget(Block::default().style(theme.base), frame.area());
    let area = inset(
        frame.area(),
        OUTER_PAD_X,
        OUTER_PAD_X,
        OUTER_PAD_Y,
        OUTER_PAD_Y,
    );
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(HEADER_HEIGHT),
            Constraint::Min(5),
            Constraint::Length(FOOTER_HEIGHT),
        ])
        .split(area);

    app.header_area = root[0];
    app.body_area = root[1];

    draw_header(frame, app, &theme, root[0]);
    draw_body(frame, app, &theme, root[1]);
    draw_footer(frame, app, &theme, root[2]);
}

fn draw_header(frame: &mut ratatui::Frame, app: &App, theme: &Theme, area: Rect) {
    frame.render_widget(Block::default().style(theme.panel), area);
    let inner = inset(area, PANEL_PAD_X, PANEL_PAD_X, 0, 0);

    let query_style = if matches!(app.focus, Focus::Query) {
        theme.focus
    } else {
        theme.text
    };
    let project_style = if matches!(app.focus, Focus::Project) {
        theme.focus
    } else {
        theme.text
    };
    let find_style = if matches!(app.focus, Focus::Find) {
        theme.focus
    } else {
        theme.text
    };

    let line1 = Line::from(vec![
        Span::styled("memex", theme.text_bold),
        Span::raw("  "),
        Span::styled("source ", theme.muted),
        Span::styled(app.source.label(), theme.accent),
        Span::raw("  "),
        Span::styled("mode ", theme.muted),
        Span::styled(
            match app.preview_mode {
                PreviewMode::Matches => "matches",
                PreviewMode::History => "history",
            },
            theme.text,
        ),
    ]);

    let query_value = if app.query.is_empty() {
        Span::styled("<empty>", theme.muted)
    } else {
        Span::styled(app.query.as_str(), query_style)
    };
    let project_value = if app.project.is_empty() {
        Span::styled("<any>", theme.muted)
    } else {
        Span::styled(app.project.as_str(), project_style)
    };
    let find_value = if app.find_query.is_empty() {
        Span::styled("<none>", theme.muted)
    } else {
        Span::styled(app.find_query.as_str(), find_style)
    };

    let line2 = Line::from(vec![
        Span::styled("query ", theme.muted),
        query_value,
        Span::raw("   "),
        Span::styled("project ", theme.muted),
        project_value,
        Span::raw("   "),
        Span::styled("find ", theme.muted),
        find_value,
    ]);

    let paragraph = Paragraph::new(vec![line1, line2]).alignment(Alignment::Left);
    frame.render_widget(paragraph, inner);
}

fn draw_body(frame: &mut ratatui::Frame, app: &mut App, theme: &Theme, area: Rect) {
    let min_left = 20u16;
    let min_right = 24u16;
    let total = area.width.max(min_left + min_right + SPLIT_GAP);
    let mut left_width = app.left_width.unwrap_or(total.saturating_mul(45) / 100);
    left_width = left_width.clamp(min_left, total.saturating_sub(min_right + SPLIT_GAP));
    app.left_width = Some(left_width);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(left_width),
            Constraint::Length(SPLIT_GAP),
            Constraint::Min(min_right),
        ])
        .split(area);

    if SPLIT_GAP > 0 {
        let divider_style = Style::default().bg(COLOR_DIVIDER);
        frame.render_widget(Block::default().style(divider_style), chunks[1]);
    }

    let mut project_area = None;
    let mut sessions_area = chunks[0];
    if matches!(app.focus, Focus::Project) {
        let left_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(PROJECT_PANEL_HEIGHT), Constraint::Min(5)])
            .split(chunks[0]);
        project_area = Some(left_chunks[0]);
        sessions_area = left_chunks[1];
    }

    if let Some(project_area) = project_area {
        let content_area = draw_project_panel(frame, app, theme, project_area);
        app.project_area = Some(content_area);
    } else {
        app.project_area = None;
    }

    let list_content = draw_sessions_panel(frame, app, theme, sessions_area);
    app.list_area = list_content;
    app.preview_area = draw_preview_panel(frame, app, theme, chunks[2]);
}

fn draw_sessions_panel(
    frame: &mut ratatui::Frame,
    app: &mut App,
    theme: &Theme,
    area: Rect,
) -> Rect {
    frame.render_widget(Block::default().style(theme.panel), area);
    let inner = inset(area, PANEL_PAD_X, PANEL_PAD_X, 0, 0);
    let header = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: PANEL_TITLE_HEIGHT.min(inner.height),
    };
    let content = Rect {
        x: inner.x,
        y: inner.y.saturating_add(PANEL_TITLE_HEIGHT),
        width: inner.width,
        height: inner.height.saturating_sub(PANEL_TITLE_HEIGHT),
    };
    let title = Paragraph::new(Line::from(Span::styled("Sessions", theme.text_bold)));
    frame.render_widget(title, header);

    let list_items: Vec<ListItem> = if app.results.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "no sessions",
            theme.muted,
        )))]
    } else {
        app.results
            .iter()
            .map(|session| {
                let ts = format_ts(session.last_ts);
                let line = Line::from(vec![
                    Span::styled(format!("{:>4}", session.hit_count), theme.accent),
                    Span::raw(" "),
                    Span::styled(session.project.as_str(), theme.text),
                    Span::raw(" "),
                    Span::styled(session.source.label(), theme.muted),
                    Span::raw(" "),
                    Span::styled(ts, theme.muted),
                    Span::raw(" "),
                    Span::styled(session.session_id.as_str(), theme.text),
                ]);
                ListItem::new(line)
            })
            .collect()
    };

    let list = List::new(list_items)
        .style(theme.text)
        .highlight_style(theme.selection)
        .highlight_symbol("");

    frame.render_stateful_widget(list, content, &mut app.selected);
    content
}

fn draw_project_panel(
    frame: &mut ratatui::Frame,
    app: &mut App,
    theme: &Theme,
    area: Rect,
) -> Rect {
    frame.render_widget(Block::default().style(theme.panel_alt), area);
    let inner = panel_inner(area);
    let header = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: PANEL_TITLE_HEIGHT.min(inner.height),
    };
    let content = Rect {
        x: inner.x,
        y: inner.y.saturating_add(PANEL_TITLE_HEIGHT),
        width: inner.width,
        height: inner.height.saturating_sub(PANEL_TITLE_HEIGHT),
    };
    let title = Paragraph::new(Line::from(Span::styled("Projects", theme.text_bold)));
    frame.render_widget(title, header);

    let project_items: Vec<ListItem> = if app.project_options.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "no projects",
            theme.muted,
        )))]
    } else {
        app.project_options
            .iter()
            .map(|project| ListItem::new(Line::from(Span::styled(project.as_str(), theme.text))))
            .collect()
    };
    let project_list = List::new(project_items)
        .style(theme.text)
        .highlight_style(theme.selection)
        .highlight_symbol("");
    let mut project_state = ListState::default();
    if !app.project_options.is_empty() {
        project_state.select(Some(
            app.project_selected
                .min(app.project_options.len().saturating_sub(1)),
        ));
    }
    frame.render_stateful_widget(project_list, content, &mut project_state);
    content
}

fn draw_preview_panel(
    frame: &mut ratatui::Frame,
    app: &mut App,
    theme: &Theme,
    area: Rect,
) -> Rect {
    frame.render_widget(Block::default().style(theme.panel_alt), area);
    let inner = panel_inner(area);
    let header = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: PANEL_TITLE_HEIGHT.min(inner.height),
    };
    let content = Rect {
        x: inner.x,
        y: inner.y.saturating_add(PANEL_TITLE_HEIGHT),
        width: inner.width,
        height: inner.height.saturating_sub(PANEL_TITLE_HEIGHT),
    };
    let detail_title = match app.preview_mode {
        PreviewMode::Matches => "Preview · Matches",
        PreviewMode::History => "Preview · History",
    };
    let title = Paragraph::new(Line::from(Span::styled(detail_title, theme.text_bold)));
    frame.render_widget(title, header);
    let view_height = content.height as usize;
    let start = app.detail_scroll.min(app.detail_lines.len());
    let end = if view_height == 0 {
        start
    } else {
        (start + view_height).min(app.detail_lines.len())
    };
    let visible_lines: Vec<Line> = app.detail_lines[start..end]
        .iter()
        .map(|line| render_preview_line(line, theme))
        .collect();
    let detail = Paragraph::new(visible_lines)
        .style(theme.text)
        .wrap(Wrap { trim: true });
    frame.render_widget(detail, content);
    content
}

fn draw_footer(frame: &mut ratatui::Frame, app: &App, theme: &Theme, area: Rect) {
    let status = if app.status.is_empty() {
        "ready"
    } else {
        &app.status
    };
    frame.render_widget(Block::default().style(theme.panel), area);
    let inner = inset(area, PANEL_PAD_X, PANEL_PAD_X, 0, 0);
    let status_line = Line::from(vec![
        Span::styled("status ", theme.muted),
        Span::styled(status, theme.text),
        Span::raw("  "),
        Span::styled("tools ", theme.muted),
        Span::styled(if app.show_tools { "on" } else { "off" }, theme.text),
        Span::raw("  "),
        Span::styled("focus ", theme.muted),
        Span::styled(format!("{:?}", app.focus).to_lowercase(), theme.text),
    ]);
    let shortcuts_line = Line::from(vec![
        Span::styled("tab", theme.accent),
        Span::styled(" focus  ", theme.muted),
        Span::styled("/", theme.accent),
        Span::styled(" query  ", theme.muted),
        Span::styled("f", theme.accent),
        Span::styled(" find  ", theme.muted),
        Span::styled("p", theme.accent),
        Span::styled(" project  ", theme.muted),
        Span::styled("m", theme.accent),
        Span::styled(" mode  ", theme.muted),
        Span::styled("t", theme.accent),
        Span::styled(" tools  ", theme.muted),
        Span::styled("r", theme.accent),
        Span::styled(" resume  ", theme.muted),
        Span::styled("S", theme.accent),
        Span::styled(" share  ", theme.muted),
        Span::styled("esc", theme.accent),
        Span::styled(" quit", theme.muted),
    ]);
    let paragraph = Paragraph::new(vec![status_line, shortcuts_line]);
    frame.render_widget(paragraph, inner);
}

fn sessions_from_query(
    index: &SearchIndex,
    query: &str,
    source: Option<SourceFilter>,
    project: Option<&str>,
    limit: usize,
) -> Result<Vec<SessionSummary>> {
    let options = QueryOptions {
        query: query.to_string(),
        project: project.map(|s| s.to_string()),
        role: None,
        tool: None,
        session_id: None,
        source,
        since: None,
        until: None,
        limit: limit.max(20),
    };
    let results = index.search(&options)?;
    let mut sessions: HashMap<String, SessionSummary> = HashMap::new();
    for (score, record) in results {
        add_record_to_session(&mut sessions, score, record);
    }
    let mut out: Vec<SessionSummary> = sessions.into_values().collect();
    out.sort_by(|a, b| {
        b.top_score
            .partial_cmp(&a.top_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.last_ts.cmp(&a.last_ts))
    });
    if out.len() > limit {
        out.truncate(limit);
    }
    Ok(out)
}

fn sessions_from_recent(
    index: &SearchIndex,
    source: Option<SourceFilter>,
    project: Option<&str>,
) -> Result<Vec<SessionSummary>> {
    let record_limit = (RECENT_SESSIONS_LIMIT * RECENT_RECORDS_MULTIPLIER).max(200);
    let records = index.recent_records(record_limit)?;
    let mut sessions: HashMap<String, SessionSummary> = HashMap::new();
    for record in records {
        if let Some(source_filter) = source
            && !source_filter.matches(record.source)
        {
            continue;
        }
        if let Some(project_filter) = project
            && record.project != project_filter
        {
            continue;
        }
        add_record_to_session(&mut sessions, 0.0, record);
        if sessions.len() >= RECENT_SESSIONS_LIMIT {
            break;
        }
    }
    let mut out: Vec<SessionSummary> = sessions.into_values().collect();
    out.sort_by(|a, b| b.last_ts.cmp(&a.last_ts));
    Ok(out)
}

fn add_record_to_session(
    sessions: &mut HashMap<String, SessionSummary>,
    score: f32,
    record: Record,
) {
    let entry = sessions
        .entry(record.session_id.clone())
        .or_insert(SessionSummary {
            session_id: record.session_id.clone(),
            project: record.project.clone(),
            source: record.source,
            last_ts: record.ts,
            hit_count: 0,
            top_score: score,
            snippet: summarize(&record.text, 160),
            source_path: record.source_path.clone(),
            source_dir: parent_dir(&record.source_path),
        });
    entry.hit_count += 1;
    if record.ts > entry.last_ts {
        entry.last_ts = record.ts;
    }
    if score >= entry.top_score {
        entry.top_score = score;
        let snippet = summarize(&record.text, 160);
        if !snippet.is_empty() {
            entry.snippet = snippet;
        }
        entry.source_path = record.source_path;
        entry.source_dir = parent_dir(&entry.source_path);
    }
}

fn build_detail_lines(
    index: &SearchIndex,
    session: &SessionSummary,
    mode: PreviewMode,
    query: &str,
    show_tools: bool,
) -> Result<Vec<PreviewLine>> {
    let mut records = index.records_by_session_id(&session.session_id)?;
    records.sort_by(|a, b| {
        a.turn_id
            .cmp(&b.turn_id)
            .then_with(|| a.ts.cmp(&b.ts))
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    let mut lines = vec![PreviewLine::SessionHeader {
        project: session.project.clone(),
        source: session.source.label().to_string(),
        session_id: session.session_id.clone(),
    }];
    if records.is_empty() {
        lines.push(PreviewLine::Text("no records in session".to_string()));
        return Ok(lines);
    }
    if !session.snippet.is_empty() {
        let snippet = strip_ansi_and_controls(&session.snippet);
        lines.push(PreviewLine::Text(format!("top hit: {snippet}")));
    }
    lines.push(PreviewLine::Empty);

    match mode {
        PreviewMode::Matches => {
            let query = query.trim();
            if query.is_empty() {
                let tail = records
                    .into_iter()
                    .rev()
                    .take(DETAIL_TAIL_LINES)
                    .collect::<Vec<_>>();
                append_records(&mut lines, tail.iter().rev());
            } else {
                let matchers = build_matchers(query)?;
                if matchers.is_empty() {
                    lines.push(PreviewLine::Text("no valid query terms".to_string()));
                } else {
                    let mut matches_all = false;
                    let mut matches_non_tools = false;
                    for record in records.iter() {
                        if matches_any(&record.text, &matchers) {
                            matches_all = true;
                            if !is_tool_role(&record.role) {
                                matches_non_tools = true;
                            }
                        }
                    }
                    let mut indices = Vec::new();
                    for (idx, record) in records.iter().enumerate() {
                        if !show_tools && is_tool_role(&record.role) {
                            continue;
                        }
                        if matches_any(&record.text, &matchers) {
                            indices.push(idx);
                        }
                    }
                    if indices.is_empty() {
                        if !matches_all {
                            lines.push(PreviewLine::Text(
                                "no literal matches (search matched via tokenizer)".to_string(),
                            ));
                        } else if !show_tools && !matches_non_tools {
                            lines.push(PreviewLine::Text(
                                "matches only in tool messages (press t to show)".to_string(),
                            ));
                        } else {
                            lines.push(PreviewLine::Text("no matches in session".to_string()));
                        }
                    } else {
                        let mut last_added: Option<usize> = None;
                        for idx in indices {
                            let start = idx.saturating_sub(CONTEXT_AROUND_MATCH);
                            let end = (idx + CONTEXT_AROUND_MATCH).min(records.len() - 1);
                            for (i, record) in records.iter().enumerate().take(end + 1).skip(start)
                            {
                                if !show_tools && is_tool_role(&record.role) {
                                    continue;
                                }
                                if let Some(last) = last_added
                                    && i <= last
                                {
                                    continue;
                                }
                                last_added = Some(i);
                                append_record(&mut lines, record, true);
                            }
                        }
                    }
                }
            }
        }
        PreviewMode::History => {
            for record in records.iter() {
                if !show_tools && is_tool_role(&record.role) {
                    continue;
                }
                append_record(&mut lines, record, false);
            }
        }
    }
    Ok(lines)
}

fn expand_resume_template(template: &str, session: &SessionSummary, cwd: &str) -> String {
    template
        .replace("{session_id}", &session.session_id)
        .replace("{project}", &session.project)
        .replace("{source}", session.source.label())
        .replace("{source_path}", &session.source_path)
        .replace("{source_dir}", &session.source_dir)
        .replace("{cwd}", cwd)
}

fn default_resume_template(cmd: &str) -> Option<String> {
    match cmd {
        "claude" => {
            find_in_path("claude").map(|_| "cd {cwd} && claude --resume {session_id}".to_string())
        }
        "codex" => find_in_path("codex").map(|_| "codex resume {session_id}".to_string()),
        "opencode" => find_in_path("opencode").map(|_| "opencode resume {session_id}".to_string()),
        _ => None,
    }
}

fn find_in_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

fn run_external_command(app: &mut App, terminal: &mut TuiTerminal, command: &str) -> Result<()> {
    app.restore_stdio()?;
    exit_terminal(terminal)?;
    let status = std::process::Command::new("sh")
        .arg("-lc")
        .arg(command)
        .status();
    match status {
        Ok(status) => {
            println!("command exited with {status}");
        }
        Err(err) => {
            println!("command failed: {err}");
        }
    }
    println!("press Enter to return to memex");
    let _ = std::io::stdin().read_line(&mut String::new());
    *terminal = enter_terminal()?;
    app.suppress_stdio()?;
    Ok(())
}

#[cfg(unix)]
fn open_tty() -> Result<TuiWriter> {
    Ok(OpenOptions::new().read(true).write(true).open("/dev/tty")?)
}

#[cfg(not(unix))]
fn open_tty() -> Result<TuiWriter> {
    Ok(std::io::stdout())
}

fn enter_terminal() -> Result<TuiTerminal> {
    let mut writer = open_tty()?;
    terminal::enable_raw_mode()?;
    execute!(writer, terminal::EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(writer);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    Ok(terminal)
}

fn exit_terminal(terminal: &mut TuiTerminal) -> Result<()> {
    terminal::disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        terminal::LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.backend_mut().flush()?;
    Ok(())
}

fn summarize(text: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    let mut out = String::new();
    let mut count = 0usize;
    let mut last_space = false;
    let mut truncated = false;
    for ch in text.chars() {
        if count >= max {
            truncated = true;
            break;
        }
        if ch.is_whitespace() {
            if out.is_empty() || last_space {
                continue;
            }
            out.push(' ');
            last_space = true;
            count += 1;
            continue;
        }
        out.push(ch);
        last_space = false;
        count += 1;
    }
    if truncated && max >= 3 {
        let keep = max.saturating_sub(3);
        let mut short = String::new();
        for (i, ch) in out.chars().enumerate() {
            if i >= keep {
                break;
            }
            short.push(ch);
        }
        short.push_str("...");
        return short.trim().to_string();
    }
    out.trim().to_string()
}

fn format_ts(ts: u64) -> String {
    if ts == 0 {
        return "-".to_string();
    }
    let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts as i64) else {
        return "-".to_string();
    };
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn build_matchers(query: &str) -> Result<Vec<regex::Regex>> {
    let mut terms = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for part in query.split_whitespace() {
        let cleaned = part.trim_matches(|c: char| !c.is_alphanumeric());
        if cleaned.len() < 2 {
            continue;
        }
        let key = cleaned.to_lowercase();
        if seen.insert(key.clone()) {
            terms.push(key);
        }
    }
    let mut out = Vec::new();
    for term in terms {
        let re = regex::RegexBuilder::new(&regex::escape(&term))
            .case_insensitive(true)
            .build()?;
        out.push(re);
    }
    Ok(out)
}

fn matches_any(text: &str, matchers: &[regex::Regex]) -> bool {
    matchers.iter().any(|re| re.is_match(text))
}

fn append_records<'a, I>(lines: &mut Vec<PreviewLine>, records: I)
where
    I: IntoIterator<Item = &'a Record>,
{
    for record in records {
        append_record(lines, record, false);
    }
}

fn append_record(lines: &mut Vec<PreviewLine>, record: &Record, highlight: bool) {
    let role = if record.role.is_empty() {
        "unknown"
    } else {
        record.role.as_str()
    };
    let ts = format_ts(record.ts);
    lines.push(PreviewLine::Meta {
        role: role.to_string(),
        ts,
        highlight,
    });
    let text = if record.text.len() > MAX_MESSAGE_CHARS {
        let trimmed = summarize(&record.text, MAX_MESSAGE_CHARS);
        format!("{trimmed} …")
    } else {
        record.text.clone()
    };
    let sanitized = sanitize_preview_lines(&text);
    if sanitized.is_empty() {
        lines.push(PreviewLine::Text("<empty>".to_string()));
    } else {
        for line in sanitized {
            lines.push(PreviewLine::Text(line));
        }
    }
    lines.push(PreviewLine::Empty);
}

fn sanitize_preview_lines(text: &str) -> Vec<String> {
    text.split('\n').map(strip_ansi_and_controls).collect()
}

fn role_color(role: &str) -> Color {
    match role {
        "user" => Color::Rgb(198, 150, 115),
        "assistant" => Color::Rgb(160, 180, 200),
        "system" => Color::Rgb(170, 150, 200),
        "tool_use" | "tool_result" | "tool" => Color::Rgb(150, 180, 150),
        _ => COLOR_MUTED,
    }
}

fn render_preview_line<'a>(line: &'a PreviewLine, theme: &Theme) -> Line<'a> {
    match line {
        PreviewLine::SessionHeader {
            project,
            source,
            session_id,
        } => Line::from(vec![
            Span::styled("project ", theme.muted),
            Span::styled(project.as_str(), theme.accent),
            Span::raw("  "),
            Span::styled("source ", theme.muted),
            Span::styled(source.as_str(), theme.muted),
            Span::raw("  "),
            Span::styled("session ", theme.muted),
            Span::styled(session_id.as_str(), theme.text),
        ]),
        PreviewLine::Meta {
            role,
            ts,
            highlight,
        } => {
            let meta_style = if *highlight {
                Style::default().fg(COLOR_ACCENT)
            } else {
                Style::default().fg(COLOR_MUTED)
            };
            let mut role_style = Style::default().fg(role_color(role));
            if *highlight {
                role_style = role_style.add_modifier(Modifier::BOLD);
            }
            Line::from(vec![
                Span::styled(role.as_str(), role_style),
                Span::raw(" "),
                Span::styled(ts.as_str(), meta_style),
            ])
        }
        PreviewLine::Text(text) => Line::from(Span::raw(text.as_str())),
        PreviewLine::Empty => Line::from(""),
    }
}

fn strip_ansi_and_controls(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    let mut count = 0usize;
    loop {
        let Some(ch) = chars.next() else {
            break;
        };
        if ch == '\u{1b}' {
            if matches!(chars.peek(), Some('[')) {
                chars.next();
                loop {
                    match chars.next() {
                        Some(c) if !c.is_ascii_alphabetic() => continue,
                        Some(_) | None => break,
                    }
                }
            }
            continue;
        }
        if ch == '\r' {
            continue;
        }
        if ch == '\t' {
            out.push(' ');
            count += 1;
            continue;
        }
        if ch.is_control() {
            continue;
        }
        out.push(ch);
        count += 1;
        if count >= PREVIEW_LINE_MAX_CHARS {
            out.push_str("...");
            break;
        }
    }
    out
}

fn is_tool_role(role: &str) -> bool {
    role == "tool_use" || role == "tool_result"
}

fn parent_dir(path: &str) -> String {
    std::path::Path::new(path)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default()
}

fn resolve_session_cwd(session: &SessionSummary) -> Option<String> {
    let file = std::fs::File::open(&session.source_path).ok()?;
    let reader = std::io::BufReader::new(file);
    let mut fallback: Option<String> = None;
    for line in reader.lines().map_while(Result::ok) {
        let value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let cwd = value
            .get("cwd")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        if fallback.is_none() {
            fallback = cwd.clone();
        }

        let session_id_match = value
            .get("sessionId")
            .and_then(|v| v.as_str())
            .or_else(|| value.get("session_id").and_then(|v| v.as_str()))
            .map(|s| s == session.session_id)
            .unwrap_or(false);

        if session_id_match && cwd.is_some() {
            return cwd;
        }

        if session.source == SourceKind::CodexSession
            && value.get("type").and_then(|v| v.as_str()) == Some("session_meta")
        {
            let payload_cwd = value
                .get("payload")
                .and_then(|v| v.get("cwd"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if payload_cwd.is_some() {
                return payload_cwd;
            }
        }
    }
    fallback
}
fn collect_projects(index: &SearchIndex, source: Option<SourceFilter>) -> Result<Vec<String>> {
    let mut set = HashSet::new();
    index.for_each_record(|record| {
        if let Some(source_filter) = source
            && !source_filter.matches(record.source)
        {
            return Ok(());
        }
        if !record.project.is_empty() {
            set.insert(record.project);
        }
        Ok(())
    })?;
    let mut projects: Vec<String> = set.into_iter().collect();
    projects.sort();
    Ok(projects)
}

fn handle_mouse(mouse: MouseEvent, app: &mut App) {
    match mouse.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            if near_divider(mouse.column, app.body_area, app.left_width.unwrap_or(0)) {
                app.dragging = true;
                return;
            }
            let pos = ratatui::layout::Position::new(mouse.column, mouse.row);
            if app.list_area.contains(pos) {
                app.focus = Focus::List;
                if let Some(idx) = list_index_from_mouse(pos, app.list_area, app.results.len()) {
                    app.selected.select(Some(idx));
                    app.last_detail_session = None;
                    app.update_detail();
                }
            } else if app.preview_area.contains(pos) {
                app.focus = Focus::Preview;
            } else if let Some(project_area) = app.project_area
                && project_area.contains(pos)
            {
                app.focus = Focus::Project;
                if let Some(idx) =
                    list_index_from_mouse(pos, project_area, app.project_options.len())
                {
                    app.project_selected = idx;
                }
            } else if app.header_area.contains(pos) {
                app.focus = Focus::Query;
            }
        }
        MouseEventKind::Drag(MouseButton::Left) => {
            if app.dragging {
                resize_split(mouse.column, app);
            }
        }
        MouseEventKind::Up(MouseButton::Left) => {
            app.dragging = false;
        }
        MouseEventKind::ScrollDown => {
            let pos = ratatui::layout::Position::new(mouse.column, mouse.row);
            if app.preview_area.contains(pos) {
                app.focus = Focus::Preview;
                app.scroll_detail(1);
            } else if app.list_area.contains(pos) {
                app.focus = Focus::List;
                app.move_selection(1);
            }
        }
        MouseEventKind::ScrollUp => {
            let pos = ratatui::layout::Position::new(mouse.column, mouse.row);
            if app.preview_area.contains(pos) {
                app.focus = Focus::Preview;
                app.scroll_detail(-1);
            } else if app.list_area.contains(pos) {
                app.focus = Focus::List;
                app.move_selection(-1);
            }
        }
        _ => {}
    }
}

fn near_divider(x: u16, body: Rect, left_width: u16) -> bool {
    if body.width == 0 {
        return false;
    }
    let divider_x = body
        .x
        .saturating_add(left_width)
        .saturating_add(SPLIT_GAP / 2);
    let min_x = divider_x.saturating_sub(1);
    let max_x = divider_x.saturating_add(1);
    x >= min_x && x <= max_x
}

fn resize_split(x: u16, app: &mut App) {
    let min_left = 20u16;
    let min_right = 24u16;
    let total = app.body_area.width.max(min_left + min_right + SPLIT_GAP);
    let mut left = x.saturating_sub(app.body_area.x);
    if left < min_left {
        left = min_left;
    }
    if left > total.saturating_sub(min_right + SPLIT_GAP) {
        left = total.saturating_sub(min_right + SPLIT_GAP);
    }
    app.left_width = Some(left);
}

fn inset(area: Rect, left: u16, right: u16, top: u16, bottom: u16) -> Rect {
    let x = area.x.saturating_add(left);
    let y = area.y.saturating_add(top);
    let width = area.width.saturating_sub(left + right);
    let height = area.height.saturating_sub(top + bottom);

    Rect {
        x,
        y,
        width,
        height,
    }
}

fn panel_inner(area: Rect) -> Rect {
    inset(area, PANEL_PAD_X, PANEL_PAD_X, PANEL_PAD_Y, PANEL_PAD_Y)
}

fn list_index_from_mouse(pos: ratatui::layout::Position, area: Rect, len: usize) -> Option<usize> {
    if len == 0 {
        return None;
    }
    if area.height == 0 || area.width == 0 {
        return None;
    }
    if !area.contains(pos) {
        return None;
    }
    let row = (pos.y - area.y) as usize;
    if row < len { Some(row) } else { None }
}
