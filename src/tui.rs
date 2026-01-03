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
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use std::collections::{HashMap, HashSet};
use std::io::{Stdout, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

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
const CONTEXT_AROUND_MATCH: usize = 1;
const RECENT_SESSIONS_LIMIT: usize = 200;
const RECENT_RECORDS_MULTIPLIER: usize = 50;

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
}

impl SourceChoice {
    fn cycle(self) -> Self {
        match self {
            SourceChoice::All => SourceChoice::Claude,
            SourceChoice::Claude => SourceChoice::Codex,
            SourceChoice::Codex => SourceChoice::All,
        }
    }

    fn as_filter(self) -> Option<SourceFilter> {
        match self {
            SourceChoice::All => None,
            SourceChoice::Claude => Some(SourceFilter::Claude),
            SourceChoice::Codex => Some(SourceFilter::Codex),
        }
    }

    fn label(self) -> &'static str {
        match self {
            SourceChoice::All => "all",
            SourceChoice::Claude => "claude",
            SourceChoice::Codex => "codex",
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
    detail_lines: Vec<Line<'static>>,
    detail_scroll: usize,
    last_detail_session: Option<String>,
    last_detail_query: Option<String>,
    last_detail_mode: PreviewMode,
    last_detail_find: Option<String>,
    status: String,
    last_status_at: Option<Instant>,
    index_rx: std::sync::mpsc::Receiver<IndexUpdate>,
    index_tx: std::sync::mpsc::Sender<IndexUpdate>,
    search_rx: std::sync::mpsc::Receiver<SearchUpdate>,
    search_tx: std::sync::mpsc::Sender<SearchUpdate>,
    header_area: Rect,
    body_area: Rect,
    list_area: Rect,
    preview_area: Rect,
    project_area: Option<Rect>,
    left_width: Option<u16>,
    dragging: bool,
}

pub fn run(root: Option<PathBuf>) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let (index_tx, index_rx) = std::sync::mpsc::channel();
    let (search_tx, search_rx) = std::sync::mpsc::channel();

    let mut app = App::new(
        paths, config, index, index_tx, index_rx, search_tx, search_rx,
    );
    app.kickoff_index_refresh();
    app.kickoff_search();

    let mut terminal = enter_terminal()?;
    let res = run_loop(&mut terminal, &mut app);
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
            index_tx,
            index_rx,
            search_tx,
            search_rx,
            header_area: Rect::default(),
            body_area: Rect::default(),
            list_area: Rect::default(),
            preview_area: Rect::default(),
            project_area: None,
            left_width: None,
            dragging: false,
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
                    embeddings: embeddings_default,
                    backfill_embeddings,
                    model: model_choice,
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
            self.detail_lines = vec![Line::from("no session selected")];
            self.detail_scroll = 0;
            return;
        };
        if idx >= self.results.len() {
            self.detail_lines = vec![Line::from("no session selected")];
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
                self.detail_lines = vec![Line::from(format!("detail error: {err}"))];
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

    fn clear_status_if_old(&mut self) {
        if let Some(at) = self.last_status_at {
            if at.elapsed() > Duration::from_secs(4) {
                self.status.clear();
                self.last_status_at = None;
            }
        }
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
        let max_scroll = self.detail_lines.len().saturating_sub(1);
        let next = (self.detail_scroll as isize + delta).clamp(0, max_scroll as isize) as usize;
        self.detail_scroll = next;
    }

    fn update_find(&mut self) {
        self.last_detail_session = None;
        self.update_detail();
    }

    fn resume_selected(&mut self, terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
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
        };
        let Some(template) = template else {
            self.set_status("resume command not configured in config.toml");
            return Ok(());
        };
        let command = expand_resume_template(&template, session);
        run_external_command(terminal, &command)?;
        self.set_status(format!("ran: {command}"));
        Ok(())
    }
}

fn run_loop(terminal: &mut Terminal<CrosstermBackend<Stdout>>, app: &mut App) -> Result<()> {
    loop {
        app.clear_status_if_old();
        terminal.draw(|f| draw_ui(f, app))?;
        if let Ok(update) = app.index_rx.try_recv() {
            match update {
                IndexUpdate::Started => app.set_status("indexing..."),
                IndexUpdate::Skipped => app.set_status("index up to date"),
                IndexUpdate::Done { added, embedded } => {
                    app.set_status(format!("indexed {added} records, embedded {embedded}"))
                }
                IndexUpdate::Error(msg) => app.set_status(format!("index error: {msg}")),
            }
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
        }
        if !crossterm::event::poll(Duration::from_millis(100))? {
            continue;
        }
        match crossterm::event::read()? {
            Event::Key(key) => {
                if handle_key(key, terminal, app)? {
                    break;
                }
            }
            Event::Mouse(mouse) => {
                handle_mouse(mouse, app);
            }
            _ => {}
        }
    }
    Ok(())
}

fn handle_key(
    key: KeyEvent,
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: &mut App,
) -> Result<bool> {
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
                if matches!(app.focus, Focus::Project) {
                    if let Some(project) = app.project_options.get(app.project_selected) {
                        app.project = project.clone();
                    }
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
        _ => {}
    }
    Ok(false)
}

fn draw_ui(frame: &mut ratatui::Frame, app: &mut App) {
    frame.render_widget(Clear, frame.area());
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(5),
            Constraint::Length(3),
        ])
        .split(frame.area());

    app.header_area = root[0];
    app.body_area = root[1];

    draw_header(frame, app, root[0]);
    draw_body(frame, app, root[1]);
    draw_footer(frame, app, root[2]);
}

fn draw_header(frame: &mut ratatui::Frame, app: &App, area: Rect) {
    let border = Block::default().borders(Borders::ALL).title("sessions");

    let highlight = Style::default()
        .fg(Color::Black)
        .bg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let idle = Style::default().fg(Color::Gray);

    let query_style = if matches!(app.focus, Focus::Query) {
        highlight
    } else {
        idle
    };
    let project_style = if matches!(app.focus, Focus::Project) {
        highlight
    } else {
        idle
    };

    let line = Line::from(vec![
        Span::styled(" query: ", Style::default().fg(Color::Yellow)),
        Span::styled(
            if app.query.is_empty() {
                "<empty>"
            } else {
                app.query.as_str()
            },
            query_style,
        ),
        Span::raw("   "),
        Span::styled("project: ", Style::default().fg(Color::Yellow)),
        Span::styled(
            if app.project.is_empty() {
                "<any>"
            } else {
                app.project.as_str()
            },
            project_style,
        ),
        Span::raw("   "),
        Span::styled("source: ", Style::default().fg(Color::Yellow)),
        Span::styled(app.source.label(), Style::default().fg(Color::Green)),
        Span::raw("   "),
        Span::styled("find: ", Style::default().fg(Color::Yellow)),
        Span::styled(
            if app.find_query.is_empty() {
                "<none>"
            } else {
                app.find_query.as_str()
            },
            if matches!(app.focus, Focus::Find) {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            },
        ),
    ]);
    let shortcuts = Line::from(vec![
        Span::styled("keys: ", Style::default().fg(Color::Yellow)),
        Span::raw("tab/shift+tab focus "),
        Span::raw("| / query (clear) "),
        Span::raw("| f find "),
        Span::raw("| p project "),
        Span::raw("| j/k move "),
        Span::raw("| h/l pane "),
        Span::raw("| m mode "),
        Span::raw("| t tools "),
        Span::raw("| r resume "),
        Span::raw("| i index "),
        Span::raw("| esc/ctrl+q quit"),
    ]);

    let paragraph = Paragraph::new(vec![line, shortcuts])
        .block(border)
        .alignment(Alignment::Left);
    frame.render_widget(paragraph, area);
}

fn draw_body(frame: &mut ratatui::Frame, app: &mut App, area: Rect) {
    let min_left = 20u16;
    let min_right = 24u16;
    let total = area.width.max(min_left + min_right);
    let mut left_width = app.left_width.unwrap_or(total.saturating_mul(45) / 100);
    left_width = left_width.clamp(min_left, total.saturating_sub(min_right));
    app.left_width = Some(left_width);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(left_width), Constraint::Min(min_right)])
        .split(area);

    let mut project_area = None;
    let mut sessions_area = chunks[0];
    if matches!(app.focus, Focus::Project) {
        let left_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(8), Constraint::Min(5)])
            .split(chunks[0]);
        project_area = Some(left_chunks[0]);
        sessions_area = left_chunks[1];
    }

    if let Some(project_area) = project_area {
        let project_items: Vec<ListItem> = if app.project_options.is_empty() {
            vec![ListItem::new(Line::from("no projects"))]
        } else {
            app.project_options
                .iter()
                .map(|project| ListItem::new(Line::from(project.as_str())))
                .collect()
        };
        let project_list = List::new(project_items)
            .block(Block::default().borders(Borders::ALL).title("projects"))
            .highlight_style(Style::default().bg(Color::Blue).fg(Color::White))
            .highlight_symbol("> ");
        let mut project_state = ListState::default();
        if !app.project_options.is_empty() {
            project_state.select(Some(
                app.project_selected
                    .min(app.project_options.len().saturating_sub(1)),
            ));
        }
        frame.render_stateful_widget(project_list, project_area, &mut project_state);
    }

    app.project_area = project_area;
    app.list_area = sessions_area;
    app.preview_area = chunks[1];

    let list_items: Vec<ListItem> = if app.results.is_empty() {
        vec![ListItem::new(Line::from("no sessions"))]
    } else {
        app.results
            .iter()
            .map(|session| {
                let ts = format_ts(session.last_ts);
                let line = Line::from(vec![
                    Span::styled(
                        format!("{:>4}", session.hit_count),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::raw(" "),
                    Span::styled(session.project.as_str(), Style::default().fg(Color::Cyan)),
                    Span::raw(" "),
                    Span::styled(session.source.label(), Style::default().fg(Color::Magenta)),
                    Span::raw(" "),
                    Span::styled(ts, Style::default().fg(Color::Gray)),
                    Span::raw(" "),
                    Span::styled(
                        session.session_id.as_str(),
                        Style::default().fg(Color::White),
                    ),
                ]);
                ListItem::new(line)
            })
            .collect()
    };

    let list = List::new(list_items)
        .block(Block::default().borders(Borders::ALL).title("sessions"))
        .highlight_style(Style::default().bg(Color::Blue).fg(Color::White))
        .highlight_symbol("> ");

    frame.render_stateful_widget(list, sessions_area, &mut app.selected);

    let detail_title = match app.preview_mode {
        PreviewMode::Matches => "preview: matches",
        PreviewMode::History => "preview: history",
    };
    let detail_block = Block::default().borders(Borders::ALL).title(detail_title);
    let detail = Paragraph::new(app.detail_lines.clone())
        .block(detail_block)
        .scroll((app.detail_scroll.min(u16::MAX as usize) as u16, 0))
        .wrap(Wrap { trim: true });
    frame.render_widget(detail, chunks[1]);
}

fn draw_footer(frame: &mut ratatui::Frame, app: &App, area: Rect) {
    let help = "enter search | s source | pgup/pgdn scroll (preview)";
    let status = if app.status.is_empty() {
        "ready"
    } else {
        &app.status
    };
    let status_line = Line::from(vec![
        Span::styled("status: ", Style::default().fg(Color::Yellow)),
        Span::raw(status),
        Span::raw("  "),
        Span::styled("mode: ", Style::default().fg(Color::Yellow)),
        Span::raw(match app.preview_mode {
            PreviewMode::Matches => "matches",
            PreviewMode::History => "history",
        }),
        Span::raw("  "),
        Span::styled("tools: ", Style::default().fg(Color::Yellow)),
        Span::raw(if app.show_tools { "on" } else { "off" }),
    ]);
    let block = Block::default().borders(Borders::ALL);
    let paragraph = Paragraph::new(vec![status_line, Line::from(help)])
        .block(block)
        .style(Style::default().fg(Color::Gray));
    frame.render_widget(paragraph, area);
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
    }
}

fn build_detail_lines(
    index: &SearchIndex,
    session: &SessionSummary,
    mode: PreviewMode,
    query: &str,
    show_tools: bool,
) -> Result<Vec<Line<'static>>> {
    let mut records = index.records_by_session_id(&session.session_id)?;
    records.sort_by(|a, b| {
        a.turn_id
            .cmp(&b.turn_id)
            .then_with(|| a.ts.cmp(&b.ts))
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    let header = Line::from(vec![
        Span::styled("session ", Style::default().fg(Color::Yellow)),
        Span::styled(
            session.session_id.clone(),
            Style::default().fg(Color::White),
        ),
        Span::raw("  "),
        Span::styled(session.project.clone(), Style::default().fg(Color::Cyan)),
        Span::raw("  "),
        Span::styled(session.source.label(), Style::default().fg(Color::Magenta)),
    ]);
    let mut lines = vec![header];
    if records.is_empty() {
        lines.push(Line::from("no records in session"));
        return Ok(lines);
    }
    if !session.snippet.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("top hit: ", Style::default().fg(Color::Green)),
            Span::raw(session.snippet.clone()),
        ]));
    }
    lines.push(Line::from(""));

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
                    lines.push(Line::from("no valid query terms"));
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
                            lines.push(Line::from(
                                "no literal matches (search matched via tokenizer)",
                            ));
                        } else if !show_tools && !matches_non_tools {
                            lines.push(Line::from(
                                "matches only in tool messages (press t to show)",
                            ));
                        } else {
                            lines.push(Line::from("no matches in session"));
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
                            lines.push(Line::from(""));
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

fn expand_resume_template(template: &str, session: &SessionSummary) -> String {
    template
        .replace("{session_id}", &session.session_id)
        .replace("{project}", &session.project)
        .replace("{source}", session.source.label())
        .replace("{source_path}", &session.source_path)
}

fn default_resume_template(cmd: &str) -> Option<String> {
    match cmd {
        "claude" => find_in_path("claude").map(|_| "claude --resume {session_id}".to_string()),
        "codex" => find_in_path("codex").map(|_| "codex resume {session_id}".to_string()),
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

fn run_external_command(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    command: &str,
) -> Result<()> {
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
    Ok(())
}

fn enter_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, terminal::EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    Ok(terminal)
}

fn exit_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
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

fn append_records<'a, I>(lines: &mut Vec<Line<'static>>, records: I)
where
    I: IntoIterator<Item = &'a Record>,
{
    for record in records {
        append_record(lines, record, false);
    }
}

fn append_record(lines: &mut Vec<Line<'static>>, record: &Record, highlight: bool) {
    let role = if record.role.is_empty() {
        "unknown"
    } else {
        record.role.as_str()
    };
    let ts = format_ts(record.ts);
    let style = if highlight {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::Gray)
    };
    lines.push(Line::from(vec![
        Span::styled(ts, style),
        Span::raw(" "),
        Span::styled(role.to_string(), Style::default().fg(Color::Yellow)),
    ]));
    let text = if record.text.len() > MAX_MESSAGE_CHARS {
        let trimmed = summarize(&record.text, MAX_MESSAGE_CHARS);
        format!("{trimmed} …")
    } else {
        record.text.clone()
    };
    if !text.is_empty() {
        lines.push(Line::from(text));
    } else {
        lines.push(Line::from("<empty>"));
    }
    lines.push(Line::from(""));
}

fn is_tool_role(role: &str) -> bool {
    role == "tool_use" || role == "tool_result"
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
            if near_divider(mouse.column, app.body_area, app.list_area) {
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

fn near_divider(x: u16, body: Rect, list: Rect) -> bool {
    if body.width == 0 {
        return false;
    }
    let divider_x = list.x.saturating_add(list.width);
    let min_x = divider_x.saturating_sub(1);
    let max_x = divider_x.saturating_add(1);
    x >= min_x && x <= max_x
}

fn resize_split(x: u16, app: &mut App) {
    let min_left = 20u16;
    let min_right = 24u16;
    let total = app.body_area.width.max(min_left + min_right);
    let mut left = x.saturating_sub(app.body_area.x);
    if left < min_left {
        left = min_left;
    }
    if left > total.saturating_sub(min_right) {
        left = total.saturating_sub(min_right);
    }
    app.left_width = Some(left);
}

fn list_index_from_mouse(pos: ratatui::layout::Position, area: Rect, len: usize) -> Option<usize> {
    if len == 0 {
        return None;
    }
    let content_y = area.y.saturating_add(1);
    let content_h = area.height.saturating_sub(2);
    if pos.y < content_y || pos.y >= content_y.saturating_add(content_h) {
        return None;
    }
    let row = (pos.y - content_y) as usize;
    if row < len { Some(row) } else { None }
}
