use crate::analytics::{AnalyticsStore, ProjectGrouping, SessionRow, analytics_path};
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
use ratatui::widgets::{Block, Clear, List, ListItem, ListState, Paragraph, Wrap};
use serde::Deserialize;
use std::borrow::Cow;
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
    Timeline {
        rows: Vec<ProjectTimelineRow>,
        source: SourceChoice,
        range: TimelineRange,
        grouping: ProjectDisplayMode,
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
const PANEL_SPLIT_PAD_X: u16 = 1;
const PANEL_PAD_Y: u16 = 1;
const PANEL_TITLE_HEIGHT: u16 = 1;
const QUERY_BAR_HEIGHT: u16 = 1;
const FOOTER_HEIGHT: u16 = 1;
const PROJECT_PANEL_HEIGHT: u16 = 6;
const SPLIT_GAP: u16 = 1;

const COLOR_BASE: Color = Color::Reset;
const COLOR_PANEL: Color = Color::Reset;
const COLOR_PANEL_ALT: Color = Color::Reset;
const COLOR_TEXT: Color = Color::Reset;
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

impl Focus {
    fn next(self) -> Self {
        match self {
            Focus::Query => Focus::Project,
            Focus::Project => Focus::List,
            Focus::List => Focus::Preview,
            Focus::Preview => Focus::Find,
            Focus::Find => Focus::Query,
        }
    }

    fn prev(self) -> Self {
        match self {
            Focus::Query => Focus::Find,
            Focus::Project => Focus::Query,
            Focus::List => Focus::Project,
            Focus::Preview => Focus::List,
            Focus::Find => Focus::Preview,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PreviewMode {
    Matches,
    History,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum LayoutMode {
    Split,
    List,
    Timeline,
    Detail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimelineRange {
    Day,
    Week,
    Month,
    All,
}

impl TimelineRange {
    fn next(self) -> Self {
        match self {
            TimelineRange::Day => TimelineRange::Week,
            TimelineRange::Week => TimelineRange::Month,
            TimelineRange::Month => TimelineRange::All,
            TimelineRange::All => TimelineRange::Day,
        }
    }

    fn prev(self) -> Self {
        match self {
            TimelineRange::Day => TimelineRange::All,
            TimelineRange::Week => TimelineRange::Day,
            TimelineRange::Month => TimelineRange::Week,
            TimelineRange::All => TimelineRange::Month,
        }
    }

    fn label(self) -> &'static str {
        match self {
            TimelineRange::Day => "last 24h",
            TimelineRange::Week => "last 7d",
            TimelineRange::Month => "last 30d",
            TimelineRange::All => "all history",
        }
    }

    fn since_ms(self, now_ms: u64) -> Option<u64> {
        let day = 24 * 60 * 60 * 1000;
        match self {
            TimelineRange::Day => Some(now_ms.saturating_sub(day)),
            TimelineRange::Week => Some(now_ms.saturating_sub(7 * day)),
            TimelineRange::Month => Some(now_ms.saturating_sub(30 * day)),
            TimelineRange::All => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimelineDensityMode {
    Compact,
    Tall,
}

impl TimelineDensityMode {
    fn toggle(self) -> Self {
        match self {
            TimelineDensityMode::Compact => TimelineDensityMode::Tall,
            TimelineDensityMode::Tall => TimelineDensityMode::Compact,
        }
    }

    fn label(self) -> &'static str {
        match self {
            TimelineDensityMode::Compact => "1-row",
            TimelineDensityMode::Tall => "2-row",
        }
    }

    fn row_height(self) -> u16 {
        match self {
            TimelineDensityMode::Compact => 1,
            TimelineDensityMode::Tall => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProjectDisplayMode {
    Flat,
    NestedWorktrees,
}

impl ProjectDisplayMode {
    fn toggle(self) -> Self {
        match self {
            ProjectDisplayMode::Flat => ProjectDisplayMode::NestedWorktrees,
            ProjectDisplayMode::NestedWorktrees => ProjectDisplayMode::Flat,
        }
    }

    fn label(self) -> &'static str {
        match self {
            ProjectDisplayMode::Flat => "flat",
            ProjectDisplayMode::NestedWorktrees => "repo",
        }
    }

    fn grouping(self) -> ProjectGrouping {
        match self {
            ProjectDisplayMode::Flat => ProjectGrouping::Flat,
            ProjectDisplayMode::NestedWorktrees => ProjectGrouping::Repository,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum SourceChoice {
    All,
    Claude,
    Codex,
    Opencode,
    Cursor,
    Pi,
    Copilot,
}

impl SourceChoice {
    fn cycle(self) -> Self {
        match self {
            SourceChoice::All => SourceChoice::Claude,
            SourceChoice::Claude => SourceChoice::Codex,
            SourceChoice::Codex => SourceChoice::Opencode,
            SourceChoice::Opencode => SourceChoice::Cursor,
            SourceChoice::Cursor => SourceChoice::Pi,
            SourceChoice::Pi => SourceChoice::Copilot,
            SourceChoice::Copilot => SourceChoice::All,
        }
    }

    fn as_filter(self) -> Option<SourceFilter> {
        match self {
            SourceChoice::All => None,
            SourceChoice::Claude => Some(SourceFilter::Claude),
            SourceChoice::Codex => Some(SourceFilter::Codex),
            SourceChoice::Opencode => Some(SourceFilter::Opencode),
            SourceChoice::Cursor => Some(SourceFilter::Cursor),
            SourceChoice::Pi => Some(SourceFilter::Pi),
            SourceChoice::Copilot => Some(SourceFilter::Copilot),
        }
    }

    fn label(self) -> &'static str {
        match self {
            SourceChoice::All => "all",
            SourceChoice::Claude => "claude",
            SourceChoice::Codex => "codex",
            SourceChoice::Opencode => "opencode",
            SourceChoice::Cursor => "cursor",
            SourceChoice::Pi => "pi",
            SourceChoice::Copilot => "copilot",
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

#[derive(Clone, Debug)]
struct ProjectTimelineRow {
    project: String,
    session_count: usize,
    last_ts: u64,
    session_ts: Vec<u64>,
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
    layout_mode: LayoutMode,
    project_display: ProjectDisplayMode,
    session_project_cache: HashMap<String, String>,
    timeline_range: TimelineRange,
    timeline_density: TimelineDensityMode,
    timeline_rows: Vec<ProjectTimelineRow>,
    timeline_scroll: usize,
    timeline_loaded: Option<(SourceChoice, TimelineRange, ProjectDisplayMode)>,
    quick_popup: bool,
    quick_scroll: usize,
    quick_lines: Vec<PreviewLine>,
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
    querybar_area: Rect,
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
    let index = if config.auto_index_on_search_default() {
        paths.ensure_dirs()?;
        SearchIndex::open_or_create_for_ingest(&paths.index)?
    } else {
        SearchIndex::open_or_create(&paths.index)?
    };
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
            layout_mode: LayoutMode::Split,
            project_display: ProjectDisplayMode::NestedWorktrees,
            session_project_cache: HashMap::new(),
            timeline_range: TimelineRange::All,
            timeline_density: TimelineDensityMode::Compact,
            timeline_rows: Vec::new(),
            timeline_scroll: 0,
            timeline_loaded: None,
            quick_popup: false,
            quick_scroll: 0,
            quick_lines: Vec::new(),
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
            querybar_area: Rect::default(),
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
                let index = SearchIndex::open_or_create_for_ingest(&paths.index)?;
                let embeddings_default = config.embeddings_default();
                let model_choice = config.resolve_model(None)?;
                let opts = IngestOptions {
                    claude_source: default_claude_source(),
                    include_agents: false,
                    include_codex: true,
                    include_opencode: true,
                    include_cursor: true,
                    include_pi: true,
                    include_copilot: true,
                    embeddings: embeddings_default,
                    backfill_embeddings: false,
                    model: model_choice,
                    embed_runtime: config.resolve_embed_runtime()?,
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
        let grouping = self.project_display.grouping();
        self.set_status("searching...");
        std::thread::spawn(move || {
            let _ = tx.send(SearchUpdate::Started);
            let result = (|| -> Result<(Vec<SessionSummary>, Option<Vec<String>>)> {
                let sessions = if query.is_empty() {
                    sessions_from_analytics(
                        &paths,
                        source.as_filter(),
                        project_opt.as_deref(),
                        grouping,
                    )
                    .or_else(|_| {
                        let index = SearchIndex::open_or_create(&paths.index)?;
                        sessions_from_recent(&index, source.as_filter(), project_opt.as_deref())
                    })?
                } else {
                    let index = SearchIndex::open_or_create(&paths.index)?;
                    let tantivy_project = if grouping == ProjectGrouping::Flat {
                        project_opt.as_deref()
                    } else {
                        None
                    };
                    let mut sessions = sessions_from_query(
                        &index,
                        &query,
                        source.as_filter(),
                        tantivy_project,
                        RESULT_LIMIT,
                    )?;
                    enrich_session_projects(&paths, &mut sessions, grouping);
                    if let Some(project) = project_opt.as_deref() {
                        sessions.retain(|session| session.project == project);
                    }
                    sessions
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
        let grouping = self.project_display.grouping();
        std::thread::spawn(move || {
            let result = collect_projects_from_analytics(&paths, source.as_filter(), grouping)
                .or_else(|_| {
                    let index = SearchIndex::open_or_create(&paths.index)?;
                    collect_projects(&index, source.as_filter())
                });
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

    fn kickoff_timeline_load(&mut self) {
        let source = self.source;
        let range = self.timeline_range;
        let grouping = self.project_display;
        let paths = self.paths.clone();
        let tx = self.search_tx.clone();
        self.timeline_loaded = Some((source, range, grouping));
        self.set_status("loading timeline...");
        std::thread::spawn(move || {
            let result = build_project_timeline(&paths, source.as_filter(), range, grouping);
            match result {
                Ok(rows) => {
                    let _ = tx.send(SearchUpdate::Timeline {
                        rows,
                        source,
                        range,
                        grouping,
                    });
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
        self.quick_scroll = 0;
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

    fn focus_next(&mut self) {
        self.focus = match self.layout_mode {
            LayoutMode::Split => self.focus.next(),
            LayoutMode::List => match self.focus {
                Focus::Query => Focus::Project,
                Focus::Project => Focus::List,
                Focus::List | Focus::Preview => Focus::Find,
                Focus::Find => Focus::Query,
            },
            LayoutMode::Timeline => Focus::List,
            LayoutMode::Detail => match self.focus {
                Focus::Preview => Focus::Find,
                Focus::Find | Focus::Query | Focus::Project | Focus::List => Focus::Preview,
            },
        };
    }

    fn focus_prev(&mut self) {
        self.focus = match self.layout_mode {
            LayoutMode::Split => self.focus.prev(),
            LayoutMode::List => match self.focus {
                Focus::Query => Focus::Find,
                Focus::Project => Focus::Query,
                Focus::List | Focus::Preview => Focus::Project,
                Focus::Find => Focus::List,
            },
            LayoutMode::Timeline => Focus::List,
            LayoutMode::Detail => match self.focus {
                Focus::Preview | Focus::Query | Focus::Project | Focus::List => Focus::Find,
                Focus::Find => Focus::Preview,
            },
        };
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

    fn scroll_quick_popup(&mut self, delta: isize) {
        if self.quick_lines.is_empty() {
            return;
        }
        let view_height = quick_popup_content_height(self.body_area) as usize;
        let max_scroll = if view_height == 0 {
            self.quick_lines.len().saturating_sub(1)
        } else {
            self.quick_lines.len().saturating_sub(view_height)
        };
        let next = (self.quick_scroll as isize + delta).clamp(0, max_scroll as isize) as usize;
        self.quick_scroll = next;
    }

    fn toggle_layout_mode(&mut self) {
        self.layout_mode = match self.layout_mode {
            LayoutMode::Split => {
                self.focus = Focus::List;
                self.quick_popup = false;
                self.quick_lines.clear();
                LayoutMode::List
            }
            LayoutMode::List => {
                self.focus = Focus::List;
                self.quick_popup = false;
                self.quick_lines.clear();
                self.kickoff_timeline_load();
                LayoutMode::Timeline
            }
            LayoutMode::Timeline | LayoutMode::Detail => LayoutMode::Split,
        };
    }

    fn toggle_project_display(&mut self) {
        self.project_display = self.project_display.toggle();
        self.session_project_cache.clear();
        self.set_status(format!("projects: {}", self.project_display.label()));
        if matches!(self.layout_mode, LayoutMode::Timeline) {
            self.kickoff_timeline_load();
        } else {
            self.refresh_results();
            if self.project_source == self.source {
                self.kickoff_project_load();
            }
        }
    }

    fn cycle_timeline_range(&mut self, delta: isize) {
        self.timeline_range = if delta < 0 {
            self.timeline_range.prev()
        } else {
            self.timeline_range.next()
        };
        if matches!(self.layout_mode, LayoutMode::Timeline) {
            self.timeline_scroll = 0;
            self.kickoff_timeline_load();
        }
    }

    fn toggle_timeline_density(&mut self) {
        self.timeline_density = self.timeline_density.toggle();
        self.set_status(format!("density: {}", self.timeline_density.label()));
        if matches!(self.layout_mode, LayoutMode::Timeline) {
            self.scroll_timeline(0);
        }
    }

    fn scroll_timeline(&mut self, delta: isize) {
        if self.timeline_rows.is_empty() {
            self.timeline_scroll = 0;
            return;
        }
        let view_height = self.list_area.height as usize;
        let row_height = self.timeline_density.row_height().max(1) as usize;
        let view_rows = if view_height == 0 {
            0
        } else {
            (view_height / row_height).max(1)
        };
        let max_scroll = if view_rows == 0 {
            self.timeline_rows.len().saturating_sub(1)
        } else {
            self.timeline_rows.len().saturating_sub(view_rows)
        };
        self.timeline_scroll =
            (self.timeline_scroll as isize + delta).clamp(0, max_scroll as isize) as usize;
    }

    fn toggle_quick_popup(&mut self) {
        if self.quick_popup {
            self.quick_popup = false;
            self.quick_scroll = 0;
            self.quick_lines.clear();
            return;
        }
        self.update_quick_lines();
        self.quick_popup = !self.quick_popup;
        self.quick_scroll = 0;
    }

    fn update_quick_lines(&mut self) {
        let Some(idx) = self.selected.selected() else {
            self.quick_lines = vec![PreviewLine::Text("no session selected".to_string())];
            return;
        };
        let Some(session) = self.results.get(idx) else {
            self.quick_lines = vec![PreviewLine::Text("no session selected".to_string())];
            return;
        };
        let active_query = if self.find_query.trim().is_empty() {
            self.query.trim()
        } else {
            self.find_query.trim()
        };
        self.quick_lines = match build_detail_lines(
            &self.index,
            session,
            PreviewMode::Matches,
            active_query,
            self.show_tools,
        ) {
            Ok(lines) => lines,
            Err(err) => vec![PreviewLine::Text(format!("detail error: {err}"))],
        };
    }

    fn enter_preview(&mut self) {
        self.layout_mode = LayoutMode::Split;
        self.quick_popup = false;
        self.quick_lines.clear();
        self.focus = Focus::Preview;
    }

    fn enter_full_history(&mut self) {
        self.layout_mode = LayoutMode::Detail;
        self.quick_popup = false;
        self.quick_lines.clear();
        self.preview_mode = PreviewMode::History;
        self.focus = Focus::Preview;
        self.last_detail_session = None;
        self.update_detail();
    }

    fn return_to_list(&mut self) {
        self.layout_mode = LayoutMode::List;
        self.quick_popup = false;
        self.quick_lines.clear();
        self.focus = Focus::List;
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
            SourceKind::Cursor => self
                .config
                .cursor_resume_cmd
                .clone()
                .or_else(|| default_resume_template("cursor")),
            SourceKind::Pi => self
                .config
                .pi_resume_cmd
                .clone()
                .or_else(|| default_resume_template("pi")),
            SourceKind::Copilot => self
                .config
                .copilot_resume_cmd
                .clone()
                .or_else(|| default_resume_template("copilot")),
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
            SourceKind::Cursor => "cursor",
            SourceKind::Pi => "pi",
            SourceKind::Copilot => "copilot",
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
                    app.quick_popup = false;
                    app.quick_scroll = 0;
                    app.quick_lines.clear();
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
                SearchUpdate::Timeline {
                    rows,
                    source,
                    range,
                    grouping,
                } => {
                    if app.timeline_loaded == Some((source, range, grouping)) {
                        app.timeline_rows = rows;
                        app.timeline_scroll = 0;
                        app.set_status(format!("{} projects", app.timeline_rows.len()));
                    }
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
    if key.modifiers.contains(KeyModifiers::CONTROL)
        && matches!(
            key.code,
            KeyCode::Char('q') | KeyCode::Char('c') | KeyCode::Char('d')
        )
    {
        return Ok(true);
    }

    if app.quick_popup {
        match key.code {
            KeyCode::Esc | KeyCode::Char(' ') => {
                app.quick_popup = false;
            }
            KeyCode::Enter | KeyCode::Char('l') => {
                app.enter_full_history();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                app.scroll_quick_popup(-1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                app.scroll_quick_popup(1);
            }
            KeyCode::PageUp => {
                app.scroll_quick_popup(-8);
            }
            KeyCode::PageDown => {
                app.scroll_quick_popup(8);
            }
            _ => {}
        }
        return Ok(false);
    }

    if matches!(key.code, KeyCode::Esc) {
        if app.layout_mode == LayoutMode::Detail && !matches!(app.focus, Focus::Find) {
            app.return_to_list();
        } else if matches!(app.focus, Focus::Find) {
            app.focus = if app.layout_mode == LayoutMode::List {
                Focus::List
            } else {
                Focus::Preview
            };
        } else {
            app.focus = Focus::List;
        }
        return Ok(false);
    }

    if matches!(app.focus, Focus::Query | Focus::Project) {
        match key.code {
            KeyCode::Tab => {
                app.focus_next();
            }
            KeyCode::BackTab => {
                app.focus_prev();
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
                app.focus = if app.layout_mode == LayoutMode::Detail {
                    Focus::Preview
                } else {
                    Focus::List
                };
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
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
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
            _ => {}
        }
        return Ok(false);
    }

    if matches!(app.focus, Focus::Find) {
        match key.code {
            KeyCode::Tab => {
                app.focus_next();
            }
            KeyCode::BackTab => {
                app.focus_prev();
            }
            KeyCode::Enter => {
                app.update_find();
                app.focus = if app.layout_mode == LayoutMode::List {
                    Focus::List
                } else {
                    Focus::Preview
                };
            }
            KeyCode::Backspace => {
                app.find_query.pop();
                app.update_find();
            }
            KeyCode::Esc => {
                app.focus = if app.layout_mode == LayoutMode::List {
                    Focus::List
                } else {
                    Focus::Preview
                };
            }
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.find_query.push(ch);
                app.update_find();
            }
            _ => {}
        }
        return Ok(false);
    }

    match key.code {
        KeyCode::Tab => {
            app.focus_next();
        }
        KeyCode::BackTab => {
            app.focus_prev();
        }
        KeyCode::Up => {
            if matches!(app.layout_mode, LayoutMode::Timeline) {
                app.scroll_timeline(-1);
            } else if matches!(app.focus, Focus::List) {
                app.move_selection(-1);
            }
        }
        KeyCode::Down => {
            if matches!(app.layout_mode, LayoutMode::Timeline) {
                app.scroll_timeline(1);
            } else if matches!(app.focus, Focus::List) {
                app.move_selection(1);
            }
        }
        KeyCode::Char('j') => {
            if matches!(app.layout_mode, LayoutMode::Timeline) {
                app.scroll_timeline(1);
            } else if matches!(app.focus, Focus::Preview) {
                app.scroll_detail(1);
            } else {
                app.move_selection(1);
            }
        }
        KeyCode::Char('k') => {
            if matches!(app.layout_mode, LayoutMode::Timeline) {
                app.scroll_timeline(-1);
            } else if matches!(app.focus, Focus::Preview) {
                app.scroll_detail(-1);
            } else {
                app.move_selection(-1);
            }
        }
        KeyCode::Char('h') => {
            if matches!(app.focus, Focus::Preview) {
                if app.layout_mode == LayoutMode::Detail {
                    app.return_to_list();
                } else {
                    app.focus = Focus::List;
                }
            }
        }
        KeyCode::Char('l') => {
            if matches!(app.focus, Focus::List) {
                if app.layout_mode == LayoutMode::List {
                    app.enter_full_history();
                } else {
                    app.enter_preview();
                }
            }
        }
        KeyCode::Enter => {
            if matches!(app.focus, Focus::List) {
                if app.layout_mode == LayoutMode::List {
                    app.enter_full_history();
                } else {
                    app.enter_preview();
                }
            }
        }
        KeyCode::PageDown => {
            if matches!(app.layout_mode, LayoutMode::Timeline) {
                app.scroll_timeline(8);
            } else if matches!(app.focus, Focus::Preview) {
                app.scroll_detail(8);
            }
        }
        KeyCode::PageUp => {
            if matches!(app.layout_mode, LayoutMode::Timeline) {
                app.scroll_timeline(-8);
            } else if matches!(app.focus, Focus::Preview) {
                app.scroll_detail(-8);
            }
        }
        KeyCode::Char('s') => {
            app.source = app.source.cycle();
            app.set_status("searching...");
            terminal.draw(|f| draw_ui(f, app))?;
            if matches!(app.layout_mode, LayoutMode::Timeline) {
                app.kickoff_timeline_load();
            } else {
                app.refresh_results();
            }
        }
        KeyCode::Char('[') => {
            app.cycle_timeline_range(-1);
        }
        KeyCode::Char(']') => {
            app.cycle_timeline_range(1);
        }
        KeyCode::Char('d') if matches!(app.layout_mode, LayoutMode::Timeline) => {
            app.toggle_timeline_density();
        }
        KeyCode::Char('g') => {
            app.toggle_project_display();
        }
        KeyCode::Char('m') => {
            app.toggle_preview_mode();
        }
        KeyCode::Char('v') => {
            app.toggle_layout_mode();
        }
        KeyCode::Char(' ') => {
            if app.layout_mode == LayoutMode::List && matches!(app.focus, Focus::List) {
                app.toggle_quick_popup();
            }
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
    // The query bar only pops up while a text field is focused, so browsing
    // stays at a single row of chrome and typing is unmistakably in a box.
    let editing = matches!(app.focus, Focus::Query | Focus::Project | Focus::Find);
    let querybar_height = if editing { QUERY_BAR_HEIGHT } else { 0 };

    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),
            Constraint::Length(querybar_height),
            Constraint::Length(FOOTER_HEIGHT),
        ])
        .split(area);

    app.body_area = root[0];
    app.querybar_area = if editing { root[1] } else { Rect::default() };

    draw_body(frame, app, &theme, root[0]);
    if editing {
        draw_query_bar(frame, app, &theme, root[1]);
    }
    draw_footer(frame, app, &theme, root[2]);
    if app.quick_popup {
        draw_quick_popup(frame, app, &theme, app.body_area);
    }
}

fn draw_query_bar(frame: &mut ratatui::Frame, app: &App, theme: &Theme, area: Rect) {
    frame.render_widget(Block::default().style(theme.panel), area);
    let inner = inset(area, PANEL_PAD_X, PANEL_PAD_X, 0, 0);

    // Active field: bold label, bright value, and a single block-cursor cell so
    // it reads like a standard terminal input; inactive fields stay muted context.
    let mut left: Vec<Span> = Vec::new();
    let mut push_field =
        |label: &str, value: &str, placeholder: &str, active: bool, first: bool| {
            if !first {
                left.push(Span::raw("   "));
            }
            left.push(Span::styled(
                format!("{label} "),
                if active { theme.focus } else { theme.muted },
            ));
            if active {
                if !value.is_empty() {
                    left.push(Span::styled(value.to_string(), theme.text_bold));
                }
                // A reverse-video space is the conventional block cursor.
                left.push(Span::styled(" ", theme.selection));
            } else if value.is_empty() {
                left.push(Span::styled(placeholder.to_string(), theme.muted));
            } else {
                left.push(Span::styled(value.to_string(), theme.text));
            }
        };

    push_field(
        "query",
        &app.query,
        "<empty>",
        matches!(app.focus, Focus::Query),
        true,
    );
    push_field(
        "project",
        &app.project,
        "<any>",
        matches!(app.focus, Focus::Project),
        false,
    );
    push_field(
        "find",
        &app.find_query,
        "<none>",
        matches!(app.focus, Focus::Find),
        false,
    );

    let right = Line::from(vec![
        Span::styled("source ", theme.muted),
        Span::styled(app.source.label(), theme.accent),
    ]);
    let right_width = right.width() as u16;

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(10), Constraint::Length(right_width)])
        .split(inner);

    frame.render_widget(Paragraph::new(Line::from(left)), cols[0]);
    frame.render_widget(Paragraph::new(right).alignment(Alignment::Right), cols[1]);
}

fn draw_body(frame: &mut ratatui::Frame, app: &mut App, theme: &Theme, area: Rect) {
    if app.layout_mode == LayoutMode::Detail {
        app.list_area = Rect::default();
        app.project_area = None;
        app.dragging = false;
        app.preview_area = draw_preview_panel(frame, app, theme, area);
        return;
    }

    if app.layout_mode == LayoutMode::List {
        app.preview_area = Rect::default();
        app.dragging = false;
        let mut project_area = None;
        let mut sessions_area = area;
        if matches!(app.focus, Focus::Project) {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(PROJECT_PANEL_HEIGHT), Constraint::Min(5)])
                .split(area);
            project_area = Some(chunks[0]);
            sessions_area = chunks[1];
        }
        if let Some(project_area) = project_area {
            let content_area = draw_project_panel(frame, app, theme, project_area);
            app.project_area = Some(content_area);
        } else {
            app.project_area = None;
        }
        app.list_area = draw_sessions_panel(frame, app, theme, sessions_area);
        return;
    }

    if app.layout_mode == LayoutMode::Timeline {
        app.preview_area = Rect::default();
        app.project_area = None;
        app.dragging = false;
        app.list_area = draw_project_timeline(frame, app, theme, area);
        return;
    }

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
        draw_split_divider(frame, chunks[1]);
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
    let right_pad = if app.layout_mode == LayoutMode::Split {
        PANEL_SPLIT_PAD_X
    } else {
        PANEL_PAD_X
    };
    let inner = inset(area, PANEL_PAD_X, right_pad, 0, 0);
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
    let title_style = if matches!(app.focus, Focus::List) {
        theme.focus
    } else {
        theme.text_bold
    };
    let title = Paragraph::new(Line::from(Span::styled("Sessions", title_style)));
    frame.render_widget(title, header);

    let list_items: Vec<ListItem> = if app.results.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "no sessions",
            theme.muted,
        )))]
    } else {
        let sessions = app.results.clone();
        let mut items = Vec::with_capacity(sessions.len());
        for session in &sessions {
            let ts = format_relative_ts(session.last_ts);
            let project = display_project_for_session(app, session);
            let line = Line::from(vec![
                Span::styled(format!("{:>4}", session.hit_count), theme.accent),
                Span::raw(" "),
                Span::styled(project, theme.text),
                Span::raw(" "),
                Span::styled(session.source.label(), theme.muted),
                Span::raw(" "),
                Span::styled(format!("{ts:>4}"), theme.accent),
                Span::raw(" "),
                Span::styled(session.session_id.clone(), theme.text),
            ]);
            items.push(ListItem::new(line));
        }
        items
    };

    let list = List::new(list_items)
        .style(theme.text)
        .highlight_style(theme.selection)
        .highlight_symbol("");

    frame.render_stateful_widget(list, content, &mut app.selected);
    content
}

fn display_project_for_session(app: &mut App, session: &SessionSummary) -> String {
    if app.project_display == ProjectDisplayMode::Flat {
        return session.project.clone();
    }
    let key = format!(
        "{}\0{}\0{}",
        session.source.storage_label(),
        session.session_id,
        session.source_path
    );
    if let Some(project) = app.session_project_cache.get(&key) {
        return project.clone();
    }
    let project = AnalyticsStore::open(analytics_path(&app.paths.state))
        .ok()
        .and_then(|store| {
            store
                .project_for_session(
                    session.source,
                    &session.session_id,
                    &session.source_path,
                    app.project_display.grouping(),
                )
                .ok()
                .flatten()
        })
        .unwrap_or_else(|| session.project.clone());
    app.session_project_cache.insert(key, project.clone());
    project
}

fn draw_project_timeline(
    frame: &mut ratatui::Frame,
    app: &mut App,
    theme: &Theme,
    area: Rect,
) -> Rect {
    frame.render_widget(Block::default().style(theme.panel), area);
    let inner = inset(area, PANEL_PAD_X, PANEL_PAD_X, PANEL_PAD_Y, PANEL_PAD_Y);
    let content = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: inner.height,
    };

    if app.timeline_rows.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                "no sessions in this window",
                theme.muted,
            ))),
            content,
        );
        return content;
    }

    let rows_area = Rect {
        x: content.x,
        y: content.y,
        width: content.width,
        height: content.height,
    };
    let row_height = app
        .timeline_density
        .row_height()
        .min(rows_area.height.max(1));
    let rows_visible = if rows_area.height == 0 {
        0
    } else {
        (rows_area.height / row_height).max(1) as usize
    };
    let start = app.timeline_scroll.min(app.timeline_rows.len());
    let end = if rows_visible == 0 {
        start
    } else {
        (start + rows_visible).min(app.timeline_rows.len())
    };
    let project_width = timeline_project_width(&app.timeline_rows[start..end], content.width);
    let count_width = 5u16;
    let last_width = 4u16;
    let chart_width = timeline_chart_width(content.width, project_width, count_width, last_width);
    let row_widths = [
        Constraint::Length(project_width),
        Constraint::Length(chart_width as u16),
        Constraint::Length(1),
        Constraint::Length(count_width),
        Constraint::Length(1),
        Constraint::Length(last_width),
    ];
    let range = timeline_bounds(&app.timeline_rows, app.timeline_range);
    let density_max = timeline_density_max(&app.timeline_rows[start..end], range, chart_width);

    for (line_idx, row) in app.timeline_rows[start..end].iter().enumerate() {
        let row_area = Rect {
            x: rows_area.x,
            y: rows_area
                .y
                .saturating_add((line_idx as u16).saturating_mul(row_height)),
            width: rows_area.width,
            height: row_height,
        };
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(row_widths)
            .split(row_area);
        let label_area = Rect {
            height: 1,
            ..cols[0]
        };
        let count_area = Rect {
            height: 1,
            ..cols[3]
        };
        let last_area = Rect {
            height: 1,
            ..cols[5]
        };
        frame.render_widget(
            Paragraph::new(truncate_middle(&row.project, label_area.width as usize))
                .style(theme.text),
            label_area,
        );
        let chart_lines = timeline_chart_lines(
            &row.session_ts,
            range,
            cols[1].width as usize,
            density_max,
            row_height as usize,
        );
        for (chart_idx, chart) in chart_lines.into_iter().enumerate() {
            let chart_area = Rect {
                y: cols[1].y.saturating_add(chart_idx as u16),
                height: 1,
                ..cols[1]
            };
            frame.render_widget(Paragraph::new(chart).style(theme.muted), chart_area);
        }
        frame.render_widget(
            Paragraph::new(row.session_count.to_string())
                .style(theme.accent)
                .alignment(Alignment::Right),
            count_area,
        );
        frame.render_widget(
            Paragraph::new(format_relative_ts(row.last_ts)).style(theme.accent),
            last_area,
        );
    }
    content
}

fn draw_project_panel(
    frame: &mut ratatui::Frame,
    app: &mut App,
    theme: &Theme,
    area: Rect,
) -> Rect {
    frame.render_widget(Block::default().style(theme.panel_alt), area);
    let inner = panel_inner_before_split(area, app.layout_mode == LayoutMode::Split);
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
    let title_style = if matches!(app.focus, Focus::Project) {
        theme.focus
    } else {
        theme.text_bold
    };
    let title = Paragraph::new(Line::from(Span::styled("Projects", title_style)));
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
    let inner = panel_inner_after_split(area);
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
    let title_style = if matches!(app.focus, Focus::Preview | Focus::Find) {
        theme.focus
    } else {
        theme.text_bold
    };
    let title = Paragraph::new(Line::from(Span::styled(detail_title, title_style)));
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

fn draw_quick_popup(frame: &mut ratatui::Frame, app: &mut App, theme: &Theme, area: Rect) -> Rect {
    let popup = quick_popup_area(area);
    frame.render_widget(Clear, popup);
    frame.render_widget(Block::default().style(theme.panel_alt), popup);

    let inner = panel_inner(popup);
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

    let title = Line::from(vec![
        Span::styled("Quick matches", theme.text_bold),
        Span::styled("  enter history  esc close", theme.muted),
    ]);
    frame.render_widget(Paragraph::new(title), header);

    let view_height = content.height as usize;
    let start = app.quick_scroll.min(app.quick_lines.len());
    let end = if view_height == 0 {
        start
    } else {
        (start + view_height).min(app.quick_lines.len())
    };
    let visible_lines: Vec<Line> = app.quick_lines[start..end]
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
    frame.render_widget(Block::default().style(theme.panel), area);
    let inner = inset(area, PANEL_PAD_X, PANEL_PAD_X, 0, 0);

    let mode = match app.preview_mode {
        PreviewMode::Matches => "matches",
        PreviewMode::History => "history",
    };
    let view = match app.layout_mode {
        LayoutMode::Split => "split",
        LayoutMode::List => "list",
        LayoutMode::Timeline => "timeline",
        LayoutMode::Detail => "detail",
    };
    let mut right_spans = Vec::new();
    if !app.status.is_empty() {
        right_spans.push(Span::styled("\u{25cf} ", theme.accent));
        right_spans.push(Span::styled(app.status.as_str(), theme.text));
        right_spans.push(Span::raw("   "));
    }
    // Keep an active source filter visible while browsing, when the query bar
    // (the other source readout) is hidden. Omit it when unfiltered.
    if app.source != SourceChoice::All && app.layout_mode != LayoutMode::Timeline {
        right_spans.push(Span::styled("source ", theme.muted));
        right_spans.push(Span::styled(app.source.label(), theme.accent));
        right_spans.push(Span::raw("   "));
    }
    if app.layout_mode == LayoutMode::Timeline {
        right_spans.push(Span::styled("source", theme.muted));
        right_spans.push(Span::styled("(s) ", theme.accent));
        right_spans.push(Span::styled(app.source.label(), theme.accent));
        right_spans.push(Span::raw("   "));
        right_spans.push(Span::styled("range", theme.muted));
        right_spans.push(Span::styled("([]) ", theme.accent));
        right_spans.push(Span::styled(app.timeline_range.label(), theme.text));
        right_spans.push(Span::raw("   "));
        right_spans.push(Span::styled("dates ", theme.muted));
        right_spans.push(Span::styled(
            timeline_date_range(&app.timeline_rows, app.timeline_range),
            theme.text,
        ));
        right_spans.push(Span::raw("   "));
        right_spans.push(Span::styled("group", theme.muted));
        right_spans.push(Span::styled("(g) ", theme.accent));
        right_spans.push(Span::styled(app.project_display.label(), theme.text));
        right_spans.push(Span::raw("   "));
        right_spans.push(Span::styled("density", theme.muted));
        right_spans.push(Span::styled("(d) ", theme.accent));
        right_spans.push(Span::styled(app.timeline_density.label(), theme.text));
        right_spans.push(Span::raw("   "));
    }
    right_spans.push(Span::styled("view", theme.muted));
    if app.layout_mode == LayoutMode::Timeline {
        right_spans.push(Span::styled("(v) ", theme.accent));
    } else {
        right_spans.push(Span::raw(" "));
    }
    right_spans.push(Span::styled(view, theme.text));
    if app.layout_mode != LayoutMode::Timeline {
        right_spans.push(Span::raw("   "));
        right_spans.push(Span::styled("mode ", theme.muted));
        right_spans.push(Span::styled(mode, theme.text));
    }
    let right = Line::from(right_spans);
    let right_width = right.width() as u16;
    let shortcut_width = inner.width.saturating_sub(right_width);
    let shortcuts = footer_shortcuts(app, theme, shortcut_width);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(10), Constraint::Length(right_width)])
        .split(inner);

    frame.render_widget(Paragraph::new(shortcuts), cols[0]);
    frame.render_widget(Paragraph::new(right).alignment(Alignment::Right), cols[1]);
}

fn footer_shortcuts<'a>(app: &App, theme: &Theme, width: u16) -> Line<'a> {
    if app.layout_mode == LayoutMode::Detail {
        return Line::from(vec![
            Span::styled("h", theme.accent),
            Span::styled(" list  ", theme.muted),
            Span::styled("j/k", theme.accent),
            Span::styled(" scroll  ", theme.muted),
            Span::styled("f", theme.accent),
            Span::styled(" find  ", theme.muted),
            Span::styled("t", theme.accent),
            Span::styled(
                if app.show_tools {
                    " tools:on"
                } else {
                    " tools:off"
                },
                theme.muted,
            ),
        ]);
    }

    let tools_hint = if app.show_tools {
        " tools:on  "
    } else {
        " tools:off  "
    };

    if app.layout_mode == LayoutMode::Split {
        if width >= 110 {
            return Line::from(vec![
                Span::styled("tab", theme.accent),
                Span::styled(" focus  ", theme.muted),
                Span::styled("/", theme.accent),
                Span::styled(" query  ", theme.muted),
                Span::styled("f", theme.accent),
                Span::styled(" find  ", theme.muted),
                Span::styled("p", theme.accent),
                Span::styled(" project  ", theme.muted),
                Span::styled("s", theme.accent),
                Span::styled(" source  ", theme.muted),
                Span::styled("m", theme.accent),
                Span::styled(" mode  ", theme.muted),
                Span::styled("v", theme.accent),
                Span::styled(" list  ", theme.muted),
                Span::styled("t", theme.accent),
                Span::styled(tools_hint, theme.muted),
                Span::styled("r", theme.accent),
                Span::styled(" resume  ", theme.muted),
                Span::styled("S", theme.accent),
                Span::styled(" share", theme.muted),
            ]);
        }

        return Line::from(vec![
            Span::styled("tab", theme.accent),
            Span::styled(" focus  ", theme.muted),
            Span::styled("/", theme.accent),
            Span::styled(" query  ", theme.muted),
            Span::styled("v", theme.accent),
            Span::styled(" list  ", theme.muted),
            Span::styled("r", theme.accent),
            Span::styled(" resume", theme.muted),
        ]);
    }

    if app.layout_mode == LayoutMode::Timeline {
        return Line::from(vec![
            Span::styled("j/k", theme.accent),
            Span::styled(" scroll", theme.muted),
        ]);
    }

    if width >= 130 {
        return Line::from(vec![
            Span::styled("tab", theme.accent),
            Span::styled(" focus  ", theme.muted),
            Span::styled("/", theme.accent),
            Span::styled(" query  ", theme.muted),
            Span::styled("f", theme.accent),
            Span::styled(" find  ", theme.muted),
            Span::styled("p", theme.accent),
            Span::styled(" project  ", theme.muted),
            Span::styled("s", theme.accent),
            Span::styled(" source  ", theme.muted),
            Span::styled("m", theme.accent),
            Span::styled(" mode  ", theme.muted),
            Span::styled("v", theme.accent),
            Span::styled(" view  ", theme.muted),
            Span::styled("space", theme.accent),
            Span::styled(" peek  ", theme.muted),
            Span::styled("enter", theme.accent),
            Span::styled(" history  ", theme.muted),
            Span::styled("t", theme.accent),
            Span::styled(tools_hint, theme.muted),
            Span::styled("r", theme.accent),
            Span::styled(" resume  ", theme.muted),
            Span::styled("S", theme.accent),
            Span::styled(" share", theme.muted),
        ]);
    }

    if width >= 90 {
        return Line::from(vec![
            Span::styled("tab", theme.accent),
            Span::styled(" focus  ", theme.muted),
            Span::styled("/", theme.accent),
            Span::styled(" query  ", theme.muted),
            Span::styled("v", theme.accent),
            Span::styled(" view  ", theme.muted),
            Span::styled("space", theme.accent),
            Span::styled(" peek  ", theme.muted),
            Span::styled("enter", theme.accent),
            Span::styled(" history  ", theme.muted),
            Span::styled("r", theme.accent),
            Span::styled(" resume", theme.muted),
        ]);
    }

    Line::from(vec![
        Span::styled("tab", theme.accent),
        Span::styled(" focus  ", theme.muted),
        Span::styled("/", theme.accent),
        Span::styled(" query  ", theme.muted),
        Span::styled("v", theme.accent),
        Span::styled(" view  ", theme.muted),
        Span::styled("sp", theme.accent),
        Span::styled(" peek  ", theme.muted),
        Span::styled("enter", theme.accent),
        Span::styled(" history", theme.muted),
    ])
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
    out.sort_by_key(|summary| std::cmp::Reverse(summary.last_ts));
    Ok(out)
}

fn sessions_from_analytics(
    paths: &Paths,
    source: Option<SourceFilter>,
    project: Option<&str>,
    grouping: ProjectGrouping,
) -> Result<Vec<SessionSummary>> {
    let store = AnalyticsStore::open(analytics_path(&paths.state))?;
    let rows =
        store.query_sessions(source, None, project, grouping, Some(RECENT_SESSIONS_LIMIT))?;
    if rows.is_empty() {
        anyhow::bail!("no analytics sessions");
    }
    Ok(rows.into_iter().map(session_summary_from_row).collect())
}

fn session_summary_from_row(row: SessionRow) -> SessionSummary {
    SessionSummary {
        session_id: row.session_id,
        project: row.display_project,
        source: row.source,
        last_ts: row.last_at,
        hit_count: row.message_count.max(1) as usize,
        top_score: 0.0,
        snippet: String::new(),
        source_dir: row.cwd.unwrap_or_else(|| parent_dir(&row.source_path)),
        source_path: row.source_path,
    }
}

fn enrich_session_projects(
    paths: &Paths,
    sessions: &mut [SessionSummary],
    grouping: ProjectGrouping,
) {
    if grouping == ProjectGrouping::Flat {
        return;
    }
    let Ok(store) = AnalyticsStore::open(analytics_path(&paths.state)) else {
        return;
    };
    for session in sessions {
        if let Ok(Some(project)) = store.project_for_session(
            session.source,
            &session.session_id,
            &session.source_path,
            grouping,
        ) {
            session.project = project;
        }
    }
}

fn collect_projects_from_analytics(
    paths: &Paths,
    source: Option<SourceFilter>,
    grouping: ProjectGrouping,
) -> Result<Vec<String>> {
    let store = AnalyticsStore::open(analytics_path(&paths.state))?;
    let rows = store.query_sessions(source, None, None, grouping, None)?;
    if rows.is_empty() {
        anyhow::bail!("no analytics projects");
    }
    let mut set = HashSet::new();
    for row in rows {
        if !row.display_project.is_empty() {
            set.insert(row.display_project);
        }
    }
    let mut projects: Vec<String> = set.into_iter().collect();
    projects.sort();
    Ok(projects)
}

fn build_project_timeline(
    paths: &Paths,
    source: Option<SourceFilter>,
    range: TimelineRange,
    display: ProjectDisplayMode,
) -> Result<Vec<ProjectTimelineRow>> {
    let store = AnalyticsStore::open(analytics_path(&paths.state))?;
    let now = now_ms();
    let rows = store.query_sessions(source, range.since_ms(now), None, display.grouping(), None)?;
    let mut projects: HashMap<String, ProjectTimelineRow> = HashMap::new();
    for row in rows {
        if row.last_at == 0 {
            continue;
        }
        let project_name = if row.display_project.is_empty() {
            row.project
        } else {
            row.display_project
        };
        let entry = projects
            .entry(project_name.clone())
            .or_insert_with(|| ProjectTimelineRow {
                project: project_name,
                session_count: 0,
                last_ts: 0,
                session_ts: Vec::new(),
            });
        entry.session_count += 1;
        entry.last_ts = entry.last_ts.max(row.last_at);
        entry.session_ts.push(row.last_at);
    }
    let mut out: Vec<ProjectTimelineRow> = projects.into_values().collect();
    for row in &mut out {
        row.session_ts.sort_unstable();
    }
    out.sort_by(|a, b| {
        b.session_count
            .cmp(&a.session_count)
            .then_with(|| b.last_ts.cmp(&a.last_ts))
            .then_with(|| a.project.cmp(&b.project))
    });
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
        .replace("{source_path_shell}", &shell_quote(&session.source_path))
        .replace("{source_path}", &session.source_path)
        .replace("{source_dir_shell}", &shell_quote(&session.source_dir))
        .replace("{source_dir}", &session.source_dir)
        .replace("{cwd_shell}", &shell_quote(cwd))
        .replace("{cwd}", cwd)
}

fn default_resume_template(cmd: &str) -> Option<String> {
    match cmd {
        "claude" => {
            find_in_path("claude").map(|_| "cd {cwd} && claude --resume {session_id}".to_string())
        }
        "codex" => find_in_path("codex").map(|_| "codex resume {session_id}".to_string()),
        "opencode" => find_in_path("opencode").map(|_| "opencode resume {session_id}".to_string()),
        "cursor" => {
            find_in_path("cursor-agent").map(|_| "cursor-agent --resume {session_id}".to_string())
        }
        "pi" => find_in_path("pi").map(|_| "pi --session {source_path_shell}".to_string()),
        _ => None,
    }
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
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

fn format_relative_ts(ts: u64) -> String {
    let now = chrono::Utc::now().timestamp_millis();
    let now = u64::try_from(now).unwrap_or(0);
    format_relative_ts_at(ts, now)
}

fn format_relative_ts_at(ts: u64, now: u64) -> String {
    if ts == 0 {
        return "-".to_string();
    }
    if ts >= now {
        return "now".to_string();
    }

    let age_secs = (now - ts) / 1000;
    const MINUTE: u64 = 60;
    const HOUR: u64 = MINUTE * 60;
    const DAY: u64 = HOUR * 24;
    const MONTH: u64 = DAY * 30;
    const YEAR: u64 = DAY * 365;

    if age_secs < MINUTE {
        "now".to_string()
    } else if age_secs < HOUR {
        format!("{}m", age_secs / MINUTE)
    } else if age_secs < DAY {
        format!("{}h", age_secs / HOUR)
    } else if age_secs < MONTH {
        format!("{}d", age_secs / DAY)
    } else if age_secs < YEAR {
        format!("{}mo", age_secs / MONTH)
    } else {
        format!("{}y", age_secs / YEAR)
    }
}

fn now_ms() -> u64 {
    let now = chrono::Utc::now().timestamp_millis();
    u64::try_from(now).unwrap_or(0)
}

fn timeline_bounds(rows: &[ProjectTimelineRow], range: TimelineRange) -> (u64, u64) {
    let now = now_ms();
    let min_seen = rows
        .iter()
        .flat_map(|row| row.session_ts.iter())
        .copied()
        .filter(|ts| *ts > 0)
        .min()
        .unwrap_or(now);
    let max_seen = rows
        .iter()
        .flat_map(|row| row.session_ts.iter())
        .copied()
        .filter(|ts| *ts > 0)
        .max()
        .unwrap_or(now);
    match range.since_ms(now) {
        Some(since) => (since, now.max(since.saturating_add(1))),
        None => (min_seen, max_seen.max(min_seen.saturating_add(1))),
    }
}

fn timeline_date_range(rows: &[ProjectTimelineRow], range: TimelineRange) -> String {
    let (start, end) = timeline_bounds(rows, range);
    format!("{}..{}", format_day(start), format_day(end))
}

fn format_day(ts: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts as i64)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn timeline_project_width(rows: &[ProjectTimelineRow], total_width: u16) -> u16 {
    let max_sessions = rows.iter().map(|row| row.session_count).max().unwrap_or(0);
    let significant_sessions = (max_sessions / 20).max(3);
    let mut widths: Vec<usize> = rows
        .iter()
        .filter(|row| row.session_count >= significant_sessions)
        .map(|row| row.project.chars().count().saturating_add(1))
        .collect();
    if widths.is_empty() {
        widths = rows
            .iter()
            .map(|row| row.project.chars().count().saturating_add(1))
            .collect();
    }
    let width = widths.iter().max().copied().unwrap_or(12);
    let max_project = total_width.saturating_sub(24).clamp(12, 32);
    (width as u16).clamp(12, max_project)
}

fn timeline_chart_width(
    total_width: u16,
    project_width: u16,
    count_width: u16,
    last_width: u16,
) -> usize {
    let gutter_width = 2u16;
    total_width
        .saturating_sub(project_width)
        .saturating_sub(gutter_width)
        .saturating_sub(count_width)
        .saturating_sub(last_width) as usize
}

fn timeline_density_max(rows: &[ProjectTimelineRow], bounds: (u64, u64), width: usize) -> usize {
    rows.iter()
        .flat_map(|row| timeline_bucket_counts(&row.session_ts, bounds, width))
        .max()
        .unwrap_or(0)
}

fn timeline_bucket_counts(session_ts: &[u64], bounds: (u64, u64), width: usize) -> Vec<usize> {
    if width == 0 {
        return Vec::new();
    }
    let mut buckets = vec![0usize; width];
    let span = bounds.1.saturating_sub(bounds.0).max(1);
    for &ts in session_ts {
        let clamped = ts.clamp(bounds.0, bounds.1);
        let offset = clamped.saturating_sub(bounds.0);
        let mut idx = ((offset as u128 * width as u128) / span as u128) as usize;
        if idx >= width {
            idx = width - 1;
        }
        buckets[idx] += 1;
    }
    buckets
}

fn timeline_chart_lines(
    session_ts: &[u64],
    bounds: (u64, u64),
    width: usize,
    density_max: usize,
    height: usize,
) -> Vec<String> {
    let buckets = timeline_bucket_counts(session_ts, bounds, width);
    if height <= 1 {
        return vec![
            buckets
                .into_iter()
                .map(|count| timeline_glyph(count, density_max))
                .collect(),
        ];
    }

    let mut lines = vec![String::with_capacity(width), String::with_capacity(width)];
    for count in buckets {
        let level = timeline_density_level(count, density_max, 8);
        lines[0].push(timeline_level_glyph(level.saturating_sub(4)));
        lines[1].push(timeline_level_glyph(level.min(4)));
    }
    lines
}

fn timeline_density_level(count: usize, max: usize, levels: usize) -> usize {
    if count == 0 || max == 0 || levels == 0 {
        return 0;
    }
    if max == 1 {
        return 1;
    }
    ((count * levels).saturating_add(max - 1)) / max
}

fn timeline_glyph(count: usize, max: usize) -> char {
    timeline_level_glyph(timeline_density_level(count, max, 4))
}

fn timeline_level_glyph(level: usize) -> char {
    match level {
        0 => ' ',
        1 => '⠁',
        2 => '⠃',
        3 => '⠇',
        _ => '⣿',
    }
}

fn truncate_middle(value: &str, width: usize) -> String {
    let len = value.chars().count();
    if len <= width {
        return value.to_string();
    }
    if width <= 1 {
        return "…".to_string();
    }
    let keep = width.saturating_sub(1);
    let head = keep / 2;
    let tail = keep.saturating_sub(head);
    let mut out = String::new();
    out.extend(value.chars().take(head));
    out.push('…');
    let tail_chars: Vec<char> = value.chars().rev().take(tail).collect();
    out.extend(tail_chars.into_iter().rev());
    out
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
    let preview_text = record_preview_text(record);
    let text = if preview_text.len() > MAX_MESSAGE_CHARS {
        let trimmed = summarize(&preview_text, MAX_MESSAGE_CHARS);
        Cow::Owned(format!("{trimmed} …"))
    } else {
        preview_text
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

fn record_preview_text(record: &Record) -> Cow<'_, str> {
    if is_tool_role(&record.role)
        && let Some(pretty) = pretty_json_text(&record.text)
    {
        return Cow::Owned(pretty);
    }
    Cow::Borrowed(&record.text)
}

fn pretty_json_text(text: &str) -> Option<String> {
    if text.len() > MAX_MESSAGE_CHARS {
        return None;
    }
    let trimmed = text.trim();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return None;
    }
    if !is_valid_json(trimmed) {
        return None;
    }
    Some(format_json_preserving_order(trimmed))
}

fn is_valid_json(text: &str) -> bool {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    serde::de::IgnoredAny::deserialize(&mut deserializer).is_ok() && deserializer.end().is_ok()
}

fn format_json_preserving_order(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut indent = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in chars.iter().copied().enumerate() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => {
                in_string = true;
                out.push(ch);
            }
            '{' | '[' => {
                out.push(ch);
                indent += 1;
                if !next_significant_char(&chars, idx + 1)
                    .is_some_and(|next| is_matching_close(ch, next))
                {
                    push_json_indent(&mut out, indent);
                }
            }
            '}' | ']' => {
                indent = indent.saturating_sub(1);
                if !last_significant_char(&out).is_some_and(|last| is_matching_open(last, ch)) {
                    push_json_indent(&mut out, indent);
                }
                out.push(ch);
            }
            ',' => {
                out.push(ch);
                push_json_indent(&mut out, indent);
            }
            ':' => out.push_str(": "),
            ch if ch.is_whitespace() => {}
            _ => out.push(ch),
        }
    }

    out
}

fn next_significant_char(chars: &[char], start: usize) -> Option<char> {
    chars
        .iter()
        .skip(start)
        .copied()
        .find(|ch| !ch.is_whitespace())
}

fn last_significant_char(text: &str) -> Option<char> {
    text.chars().rev().find(|ch| !ch.is_whitespace())
}

fn is_matching_close(open: char, close: char) -> bool {
    matches!((open, close), ('{', '}') | ('[', ']'))
}

fn is_matching_open(open: char, close: char) -> bool {
    is_matching_close(open, close)
}

fn push_json_indent(out: &mut String, indent: usize) {
    out.push('\n');
    for _ in 0..indent {
        out.push_str("  ");
    }
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
    while let Some(ch) = chars.next() {
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
    if session.source == SourceKind::Copilot
        && let Some(cwd) = resolve_copilot_workspace_cwd(session)
    {
        return Some(cwd);
    }
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

        if session.source == SourceKind::Pi
            && value.get("type").and_then(|v| v.as_str()) == Some("session")
        {
            let cwd = value
                .get("cwd")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if cwd.is_some() {
                return cwd;
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
            if app.layout_mode == LayoutMode::Split
                && near_divider(mouse.column, app.body_area, app.left_width.unwrap_or(0))
            {
                app.dragging = true;
                return;
            }
            let pos = ratatui::layout::Position::new(mouse.column, mouse.row);
            if app.list_area.contains(pos) {
                app.focus = Focus::List;
                if app.layout_mode == LayoutMode::Timeline {
                    return;
                }
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
            } else if app.querybar_area.contains(pos) {
                app.focus = query_bar_focus_from_mouse(app, mouse.column);
            }
        }
        MouseEventKind::Drag(MouseButton::Left) => {
            if app.dragging && app.layout_mode == LayoutMode::Split {
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
                if app.layout_mode == LayoutMode::Timeline {
                    app.scroll_timeline(1);
                } else {
                    app.move_selection(1);
                }
            } else if let Some(project_area) = app.project_area
                && project_area.contains(pos)
            {
                app.focus = Focus::Project;
                app.move_project_selection(1);
            }
        }
        MouseEventKind::ScrollUp => {
            let pos = ratatui::layout::Position::new(mouse.column, mouse.row);
            if app.preview_area.contains(pos) {
                app.focus = Focus::Preview;
                app.scroll_detail(-1);
            } else if app.list_area.contains(pos) {
                app.focus = Focus::List;
                if app.layout_mode == LayoutMode::Timeline {
                    app.scroll_timeline(-1);
                } else {
                    app.move_selection(-1);
                }
            } else if let Some(project_area) = app.project_area
                && project_area.contains(pos)
            {
                app.focus = Focus::Project;
                app.move_project_selection(-1);
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
    x == divider_x
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

fn query_bar_focus_from_mouse(app: &App, x: u16) -> Focus {
    let mut field_x = app.querybar_area.x.saturating_add(PANEL_PAD_X);
    for (focus, width) in [
        (
            Focus::Query,
            query_bar_field_width(
                "query",
                &app.query,
                "<empty>",
                matches!(app.focus, Focus::Query),
            ),
        ),
        (
            Focus::Project,
            query_bar_field_width(
                "project",
                &app.project,
                "<any>",
                matches!(app.focus, Focus::Project),
            ),
        ),
        (
            Focus::Find,
            query_bar_field_width(
                "find",
                &app.find_query,
                "<none>",
                matches!(app.focus, Focus::Find),
            ),
        ),
    ] {
        let field_end = field_x.saturating_add(width);
        if x >= field_x && x < field_end {
            return focus;
        }
        field_x = field_end.saturating_add(3);
    }
    Focus::Query
}

fn query_bar_field_width(label: &str, value: &str, placeholder: &str, active: bool) -> u16 {
    let value_width = if active {
        value.chars().count().saturating_add(1)
    } else if value.is_empty() {
        placeholder.chars().count()
    } else {
        value.chars().count()
    };
    label
        .chars()
        .count()
        .saturating_add(1)
        .saturating_add(value_width)
        .try_into()
        .unwrap_or(u16::MAX)
}

fn panel_inner(area: Rect) -> Rect {
    inset(area, PANEL_PAD_X, PANEL_PAD_X, PANEL_PAD_Y, PANEL_PAD_Y)
}

fn quick_popup_area(area: Rect) -> Rect {
    let width = area
        .width
        .saturating_mul(4)
        .saturating_div(5)
        .clamp(40, 100);
    let height = area
        .height
        .saturating_mul(3)
        .saturating_div(5)
        .clamp(10, 30);
    Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width: width.min(area.width),
        height: height.min(area.height),
    }
}

fn quick_popup_content_height(area: Rect) -> u16 {
    let popup = quick_popup_area(area);
    let inner = panel_inner(popup);
    inner.height.saturating_sub(PANEL_TITLE_HEIGHT)
}

fn panel_inner_before_split(area: Rect, compact: bool) -> Rect {
    let right_pad = if compact {
        PANEL_SPLIT_PAD_X
    } else {
        PANEL_PAD_X
    };
    inset(area, PANEL_PAD_X, right_pad, PANEL_PAD_Y, PANEL_PAD_Y)
}

fn panel_inner_after_split(area: Rect) -> Rect {
    inset(
        area,
        PANEL_SPLIT_PAD_X,
        PANEL_PAD_X,
        PANEL_PAD_Y,
        PANEL_PAD_Y,
    )
}

fn draw_split_divider(frame: &mut ratatui::Frame, area: Rect) {
    let style = Style::default().fg(COLOR_DIVIDER);
    for y in area.y..area.y.saturating_add(area.height) {
        let row = Rect {
            x: area.x,
            y,
            width: area.width,
            height: 1,
        };
        frame.render_widget(
            Paragraph::new(ratatui::symbols::line::VERTICAL).style(style),
            row,
        );
    }
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

#[derive(Default)]
struct CopilotWorkspaceCwd {
    cwd: Option<String>,
    git_root: Option<String>,
}

fn resolve_copilot_workspace_cwd(session: &SessionSummary) -> Option<String> {
    let workspace_path = std::path::Path::new(&session.source_path)
        .parent()?
        .join("workspace.yaml");
    let contents = std::fs::read_to_string(workspace_path).ok()?;
    let workspace = parse_copilot_workspace_cwd(&contents);
    workspace.cwd.or(workspace.git_root)
}

fn parse_copilot_workspace_cwd(contents: &str) -> CopilotWorkspaceCwd {
    let mut workspace = CopilotWorkspaceCwd::default();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty()
            || trimmed.starts_with('#')
            || line.chars().next().is_some_and(|c| c.is_whitespace())
        {
            continue;
        }
        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        let value = value
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        if value.is_empty() {
            continue;
        }
        match key.trim() {
            "cwd" => workspace.cwd = Some(value),
            "gitRoot" | "git_root" => workspace.git_root = Some(value),
            _ => {}
        }
    }
    workspace
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RecordLinks, SourceKind};

    fn record(role: &str, text: &str) -> Record {
        Record {
            source: SourceKind::CodexSession,
            doc_id: 1,
            ts: 0,
            project: "project".to_string(),
            session_id: "session".to_string(),
            turn_id: 1,
            role: role.to_string(),
            text: text.to_string(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks::default(),
            source_path: "source.jsonl".to_string(),
        }
    }

    #[test]
    fn record_preview_text_pretty_prints_tool_json() {
        let record = record(
            "tool_use",
            r#"{"cmd":"pwd && rg --files","workdir":"/tmp/app","yield_time_ms":1000}"#,
        );

        assert_eq!(
            record_preview_text(&record),
            "{\n  \"cmd\": \"pwd && rg --files\",\n  \"workdir\": \"/tmp/app\",\n  \"yield_time_ms\": 1000\n}"
        );
    }

    #[test]
    fn record_preview_text_preserves_tool_json_key_order() {
        let record = record("tool_use", r#"{"z":1,"a":2,"nested":{"b":3,"a":4}}"#);

        assert_eq!(
            record_preview_text(&record),
            "{\n  \"z\": 1,\n  \"a\": 2,\n  \"nested\": {\n    \"b\": 3,\n    \"a\": 4\n  }\n}"
        );
    }

    #[test]
    fn record_preview_text_ignores_json_punctuation_inside_strings() {
        let record = record(
            "tool_use",
            r#"{"cmd":"printf '{x: [1,2]}'","args":["a,b","c:d"]}"#,
        );

        assert_eq!(
            record_preview_text(&record),
            "{\n  \"cmd\": \"printf '{x: [1,2]}'\",\n  \"args\": [\n    \"a,b\",\n    \"c:d\"\n  ]\n}"
        );
    }

    #[test]
    fn timeline_chart_uses_shared_density_scale() {
        let dense = timeline_chart_lines(&[10, 10, 10, 50], (0, 100), 5, 3, 1)
            .into_iter()
            .next()
            .unwrap();
        let sparse = timeline_chart_lines(&[50], (0, 100), 5, 3, 1)
            .into_iter()
            .next()
            .unwrap();

        assert!(dense.contains('⣿'));
        assert!(sparse.contains('⠃'));
        assert!(!sparse.contains('⣿'));
    }

    #[test]
    fn timeline_chart_lines_tall_uses_two_density_rows() {
        let lines = timeline_chart_lines(&[10, 10, 10, 10, 10, 50], (0, 100), 2, 5, 2);

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].chars().next(), Some('⣿'));
        assert_eq!(lines[1].chars().next(), Some('⣿'));
        assert_eq!(lines[0].chars().nth(1), Some(' '));
        assert_eq!(lines[1].chars().nth(1), Some('⠃'));
    }

    #[test]
    fn timeline_chart_lines_compact_uses_one_density_row() {
        let lines = timeline_chart_lines(&[10, 50], (0, 100), 2, 1, 1);

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].chars().count(), 2);
    }

    #[test]
    fn timeline_default_range_is_all_history() {
        assert_eq!(TimelineRange::All.label(), "all history");
        assert_eq!(TimelineRange::All.since_ms(123), None);
    }

    #[test]
    fn timeline_chart_width_reserves_numeric_gutters() {
        assert_eq!(timeline_chart_width(100, 20, 5, 4), 69);
        assert_eq!(timeline_chart_width(40, 20, 5, 4), 9);
    }

    #[test]
    fn timeline_bounds_ignore_zero_timestamps() {
        let rows = vec![
            timeline_row_with_ts("bad", 1, vec![0]),
            timeline_row_with_ts("good", 1, vec![1_700_000_000_000]),
        ];

        let (start, end) = timeline_bounds(&rows, TimelineRange::All);

        assert_eq!(start, 1_700_000_000_000);
        assert_eq!(end, 1_700_000_000_001);
    }

    #[test]
    fn timeline_project_width_ignores_low_count_long_names() {
        let rows = vec![
            timeline_row("mdnb", 925),
            timeline_row("sidequery-backend", 413),
            timeline_row("nico-duckdb-iceberg", 51),
            timeline_row(
                "generated-harness-directory-name-that-should-not-set-width",
                1,
            ),
        ];

        assert_eq!(timeline_project_width(&rows, 120), 20);
    }

    #[test]
    fn timeline_project_width_keeps_significant_long_names() {
        let rows = vec![
            timeline_row("mdnb", 925),
            timeline_row("sidequery-backend", 413),
            timeline_row("important-long-project-name", 300),
        ];

        assert_eq!(timeline_project_width(&rows, 120), 28);
    }

    fn timeline_row(project: &str, session_count: usize) -> ProjectTimelineRow {
        timeline_row_with_ts(project, session_count, Vec::new())
    }

    fn timeline_row_with_ts(
        project: &str,
        session_count: usize,
        session_ts: Vec<u64>,
    ) -> ProjectTimelineRow {
        ProjectTimelineRow {
            project: project.to_string(),
            session_count,
            last_ts: 0,
            session_ts,
        }
    }

    #[test]
    fn record_preview_text_leaves_non_tool_json_unchanged() {
        let text = r#"{"content":"not a tool call"}"#;
        let record = record("assistant", text);
        let preview = record_preview_text(&record);

        assert!(matches!(preview, Cow::Borrowed(_)));
        assert_eq!(preview, text);
    }

    #[test]
    fn record_preview_text_leaves_invalid_tool_json_unchanged() {
        let text = r#"{"cmd":"unterminated"#;
        let record = record("tool_use", text);

        assert_eq!(record_preview_text(&record), text);
    }

    #[test]
    fn record_preview_text_leaves_large_tool_json_unchanged() {
        let text = format!(r#"{{"payload":"{}"}}"#, "x".repeat(MAX_MESSAGE_CHARS));
        let record = record("tool_result", &text);
        let preview = record_preview_text(&record);

        assert!(matches!(preview, Cow::Borrowed(_)));
        assert_eq!(preview, text);
    }
}
