//! UI-independent browser/reader state. All I/O runs on workers; replies carry
//! epochs so an older query, chart, or reader can never replace current state.
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

/// Matches the web client: a cached transcript is shown as-is for this long,
/// then shown while it is revalidated against the latest page.
const READER_CACHE_LIFETIME_MS: i64 = 30_000;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Kind {
    Restore,
    Machines,
    Sessions(String),
    Count(String),
    Projects(String),
    Activity(String),
    Metadata,
    OpeningTitle,
    Page {
        at: usize,
        mode: String,
        target: String,
        occurrence: usize,
    },
    Locate {
        at: usize,
        target: String,
    },
    Find {
        at: usize,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Request {
    kind: Kind,
    generation: u64,
    reader: u64,
    find: u64,
    activity: u64,
    page: u64,
}
struct Reply {
    request: Request,
    result: Result<Value, String>,
}
#[derive(Clone)]
struct ActivityCache {
    charts: HashMap<String, Value>,
    failures: HashMap<String, String>,
    updated_at: i64,
    revision: u64,
    complete: bool,
}

pub struct Store {
    state: Value,
    #[cfg_attr(test, allow(dead_code))]
    sender: Sender<Reply>,
    receiver: Receiver<Reply>,
    generation: u64,
    reader: u64,
    find: u64,
    activity: u64,
    page: u64,
    batches: HashMap<String, Vec<Value>>,
    projects: HashMap<String, Vec<Value>>,
    counts: HashMap<String, Option<u64>>,
    charts: HashMap<String, Value>,
    catalog: HashMap<String, Value>,
    cache: BTreeMap<String, Value>,
    cache_keys: Vec<String>,
    persistence: Option<PathBuf>,
    workers: Vec<(Request, Arc<AtomicBool>)>,
    worker_threads: Vec<std::thread::JoinHandle<()>>,
    shutting_down: bool,
    dirty: bool,
    configured: bool,
    session_criteria: String,
    activity_revision: u64,
    activity_pending: Vec<String>,
    activity_failures: HashMap<String, String>,
    activity_caches: HashMap<String, ActivityCache>,
    activity_cache_keys: Vec<String>,
    #[cfg(test)]
    requests: Vec<(Request, Value)>,
}
impl Default for Store {
    fn default() -> Self {
        Self::new(storage_path())
    }
}
impl Store {
    fn new(persistence: Option<PathBuf>) -> Self {
        let (sender, receiver) = mpsc::channel();
        let mut result = Self {
            state: initial_state(),
            sender,
            receiver,
            generation: 0,
            reader: 0,
            find: 0,
            activity: 0,
            page: 0,
            batches: HashMap::new(),
            projects: HashMap::new(),
            counts: HashMap::new(),
            charts: HashMap::new(),
            catalog: HashMap::new(),
            cache: BTreeMap::new(),
            cache_keys: Vec::new(),
            persistence,
            workers: Vec::new(),
            worker_threads: Vec::new(),
            shutting_down: false,
            dirty: true,
            configured: false,
            session_criteria: String::new(),
            activity_revision: 0,
            activity_pending: Vec::new(),
            activity_failures: HashMap::new(),
            activity_caches: HashMap::new(),
            activity_cache_keys: Vec::new(),
            #[cfg(test)]
            requests: Vec::new(),
        };
        if let Some(path) = result.persistence.clone() {
            result.request(Kind::Restore, json!({"path":path}));
        }
        result.merge_projects();
        result
    }
    pub fn action(&mut self, action: Value) {
        if self.shutting_down {
            return;
        }
        // Anchor updates are local reader bookkeeping and do not need to reset
        // a QML model or produce a snapshot at scrolling frequency.
        if action["op"] != "saveAnchor" {
            self.dirty = true;
        }
        match string(&action, "op") {
            "initialize" => self.request(Kind::Machines, json!({"op":"machines"})),
            "shutdown" => self.shutdown(),
            "refresh" => {
                self.activity_revision += 1;
                self.refresh();
                self.revalidate_reader();
            }
            "configure" => {
                self.configured = true;
                let mut refresh = false;
                let mut activity = false;
                for key in [
                    "query",
                    "project",
                    "machine",
                    "provider",
                    "timeframe",
                    "origin",
                    "projectSort",
                    "metric",
                ] {
                    if action["changes"][key].is_string()
                        && action["changes"][key] != self.state[key]
                    {
                        self.state[key] = action["changes"][key].clone();
                        if key == "metric" {
                            activity = true;
                        } else if key != "projectSort" {
                            refresh = true;
                        }
                    }
                }
                self.persist();
                self.merge_projects();
                if refresh {
                    self.refresh();
                } else if activity {
                    self.refresh_activity();
                }
            }
            "resetFilters" => {
                self.configured = true;
                self.state["provider"] = json!("");
                self.state["timeframe"] = json!("all");
                self.state["origin"] = json!("interactive");
                self.persist();
                self.refresh();
            }
            "moreSessions"
                if number(&self.state, "pending") == 0 && self.state["canLoadMore"] == true =>
            {
                self.state["limit"] = json!(number(&self.state, "limit") + 200);
                for id in self.ids() {
                    self.load_machine(&id);
                }
            }
            "refreshActivity" => {
                self.activity_revision += 1;
                self.refresh_activity();
            }
            "openSession" if action["session"].is_object() => self.open(action["session"].clone()),
            "earlier" if self.state["readerBusy"] != true && number(&self.state, "offset") > 0 => {
                self.load_page(
                    number(&self.state, "offset").saturating_sub(60),
                    "before",
                    "",
                    0,
                )
            }
            "later" if self.state["readerBusy"] != true => {
                let next = number(&self.state, "offset") + array(&self.state["records"]).len();
                if next < number(&self.state, "recordTotal") {
                    self.load_page(next, "after", "", 0);
                }
            }
            "find" => self.start_find(string(&action, "text")),
            "navigateFind" => self.navigate_find(action["direction"].as_i64().unwrap_or(1)),
            "saveAnchor" => {
                self.state["anchor"] = action["id"].clone();
                self.state["anchorOffset"] = action["offset"].clone();
                if action["expansion"].is_object() {
                    self.state["expansion"] = action["expansion"].clone();
                }
            }
            _ => {}
        }
    }
    pub fn poll(&mut self) -> Value {
        self.reap_finished_workers();
        while let Ok(reply) = self.receiver.try_recv() {
            self.accept(reply.request, reply.result);
        }
        let mut snapshot = self.state.clone();
        snapshot["criteria"] = self.filters();
        snapshot
    }
    pub fn poll_changed(&mut self) -> Option<Value> {
        self.reap_finished_workers();
        while let Ok(reply) = self.receiver.try_recv() {
            self.accept(reply.request, reply.result);
        }
        if !std::mem::take(&mut self.dirty) {
            return None;
        }
        let snapshot = self.poll();
        self.dirty = false;
        Some(snapshot)
    }
    fn request(&mut self, kind: Kind, payload: Value) {
        if self.shutting_down {
            return;
        }
        let request = Request {
            kind,
            generation: self.generation,
            reader: self.reader,
            find: self.find,
            activity: self.activity,
            page: self.page,
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        self.workers.push((request.clone(), cancelled.clone()));
        #[cfg(test)]
        {
            self.requests.push((request, payload));
        }
        #[cfg(not(test))]
        {
            let sender = self.sender.clone();
            let worker = std::thread::spawn(move || {
                let result = if request.kind == Kind::Restore {
                    std::fs::read(string(&payload, "path"))
                        .map_err(|error| error.to_string())
                        .and_then(|bytes| {
                            serde_json::from_slice(&bytes).map_err(|error| error.to_string())
                        })
                } else {
                    crate::client::request_cancellable(payload, cancelled)
                };
                let _ = sender.send(Reply { request, result });
            });
            self.worker_threads.push(worker);
        }
    }
    /// Cancel and reap only this controller's transport workers before the Qt
    /// owner exits the process. Client cancellation kills and waits for its own
    /// CLI child; terminal applications launched separately are not involved.
    pub fn shutdown(&mut self) {
        self.shutting_down = true;
        for (_, cancelled) in &self.workers {
            cancelled.store(true, Ordering::Relaxed);
        }
        self.workers.clear();
        // Stale workers already have their cancellation flags set, but their
        // handles remain owned until they finish cleanup or shutdown joins them.
        for worker in self.worker_threads.drain(..) {
            let _ = worker.join();
        }
    }
    fn reap_finished_workers(&mut self) {
        let mut index = 0;
        while index < self.worker_threads.len() {
            if self.worker_threads[index].is_finished() {
                let _ = self.worker_threads.swap_remove(index).join();
            } else {
                index += 1;
            }
        }
    }
    fn cancel_stale(&mut self) {
        self.workers.retain(|(request, cancelled)| {
            let stale = match request.kind {
                Kind::Machines | Kind::Restore => false,
                Kind::Metadata | Kind::OpeningTitle => request.reader != self.reader,
                Kind::Find { .. } => request.reader != self.reader || request.find != self.find,
                Kind::Page { .. } | Kind::Locate { .. } => {
                    request.reader != self.reader || request.page != self.page
                }
                Kind::Activity(_) => {
                    request.generation != self.generation || request.activity != self.activity
                }
                _ => request.generation != self.generation,
            };
            if stale {
                cancelled.store(true, Ordering::Relaxed);
            }
            !stale
        });
    }
    fn ids(&self) -> Vec<String> {
        if self.state["machine"] == "all" {
            array(&self.state["machines"])
                .iter()
                .map(|m| string(m, "id").to_owned())
                .collect()
        } else {
            vec![string(&self.state, "machine").to_owned()]
        }
    }
    fn filters(&self) -> Value {
        let days = match string(&self.state, "timeframe") {
            "day" => 1,
            "week" => 7,
            "month" => 30,
            _ => 0,
        };
        json!({"project":nonempty(&self.state["project"]),"source":nonempty(&self.state["provider"]),"since":if days==0 {Value::Null} else {json!(iso_date(now_ms()/1000-days*86400))},"origin":self.state["origin"],"limit":self.state["limit"]})
    }
    fn refresh(&mut self) {
        self.generation += 1;
        self.cancel_stale();
        let criteria = self.criteria_key();
        if criteria != self.session_criteria {
            self.batches.clear();
        }
        self.session_criteria = criteria;
        self.counts.clear();
        self.state["pending"] = json!(0);
        self.state["limit"] = json!(200);
        self.merge_sessions();
        self.state["total"] = json!(-1);
        self.state["canLoadMore"] = json!(false);
        self.state["errors"] = json!({});
        for id in self.ids() {
            self.load_machine(&id);
            self.pending(1);
            self.request(
                Kind::Projects(id.clone()),
                json!({"op":"projects","machine":id}),
            );
        }
        self.merge_projects();
        self.refresh_activity();
    }
    fn criteria_key(&self) -> String {
        json!([
            self.ids(),
            self.state["query"],
            self.state["project"],
            self.state["provider"],
            self.state["timeframe"],
            self.state["origin"]
        ])
        .to_string()
    }
    fn activity_key(&self) -> String {
        format!("{}|{}", self.criteria_key(), string(&self.state, "metric"))
    }
    fn load_machine(&mut self, id: &str) {
        let filters = self.filters();
        let query = string(&self.state, "query").trim();
        let payload = if query.is_empty() {
            json!({"op":"sessions","machine":id,"filters":filters})
        } else {
            let mut p = filters.clone();
            p["op"] = json!("search");
            p["machine"] = json!(id);
            p["query"] = json!(query);
            p
        };
        let count = json!({"op":"count","machine":id,"query":if query.is_empty(){Value::Null}else{json!(query)},"filters":filters});
        self.pending(1);
        self.request(Kind::Sessions(id.into()), payload);
        self.request(Kind::Count(id.into()), count);
    }
    fn pending(&mut self, delta: i64) {
        self.state["pending"] = json!((self.state["pending"].as_i64().unwrap_or(0) + delta).max(0));
    }
    fn error(&mut self, key: String, error: String) {
        self.state["errors"][key] = json!(error);
    }
    fn accept(&mut self, request: Request, result: Result<Value, String>) {
        self.workers.retain(|(active, _)| active != &request);
        match &request.kind {
            Kind::Page { .. }
            | Kind::Locate { .. }
            | Kind::Find { .. }
            | Kind::Metadata
            | Kind::OpeningTitle
                if request.reader != self.reader =>
            {
                return;
            }
            Kind::Find { .. } if request.find != self.find => return,
            Kind::Page { .. } | Kind::Locate { .. } if request.page != self.page => return,
            Kind::Activity(_)
                if request.generation != self.generation || request.activity != self.activity =>
            {
                return;
            }
            Kind::Sessions(_) | Kind::Count(_) | Kind::Projects(_)
                if request.generation != self.generation =>
            {
                return;
            }
            _ => {}
        }
        self.dirty = true;
        match request.kind {
            Kind::Metadata => {
                self.state["metadataBusy"] = json!(false);
                match result {
                    Ok(rows) => {
                        let selected = self.state["selected"].clone();
                        let machine = selected["machine"].as_str().unwrap_or("local");
                        if let Some(detail) = array(&rows).iter().find(|row| {
                            ["source", "session_id", "source_path"]
                                .iter()
                                .all(|key| row[key] == selected[key])
                                && row["machine"].as_str().unwrap_or(machine) == machine
                        }) {
                            self.apply_metadata(detail.clone());
                            if string(&self.state["selected"], "label").trim().is_empty() {
                                self.state["metadataBusy"] = json!(true);
                                self.request(Kind::OpeningTitle, self.page_payload(0, 16));
                            }
                        } else {
                            self.state["metadataError"] =
                                json!("Session details did not match the selected conversation");
                        }
                    }
                    Err(error) => self.state["metadataError"] = json!(error),
                }
            }
            Kind::OpeningTitle => {
                self.state["metadataBusy"] = json!(false);
                match result {
                    Ok(page) => {
                        if string(&self.state["selected"], "label").trim().is_empty()
                            && let Some(title) = opening_title(&decode_page(page).0)
                        {
                            self.apply_metadata(json!({"label":title}));
                        }
                    }
                    Err(error) => self.state["metadataError"] = json!(error),
                }
            }
            Kind::Restore => {
                if let Ok(saved) = result {
                    if !self.configured {
                        for key in ["machine", "provider", "timeframe", "origin", "projectSort"] {
                            if saved[key].is_string() {
                                self.state[key] = saved[key].clone();
                            }
                        }
                    }
                    let projects: HashMap<String, Vec<Value>> =
                        serde_json::from_value(saved["projects"].clone()).unwrap_or_default();
                    for (machine, rows) in projects {
                        self.projects.entry(machine).or_insert(rows);
                    }
                    self.merge_projects();
                    if !self.configured && self.generation > 0 {
                        self.refresh();
                    }
                }
            }
            Kind::Machines => {
                let error = result.as_ref().err().cloned();
                match result {
                    Ok(value) => {
                        self.state["machines"] = value;
                        if self.ids().is_empty() {
                            self.state["machines"] =
                                json!([{"id":"local","label":"This computer"}]);
                        }
                    }
                    Err(error) => self.error("machines".into(), error),
                }
                self.refresh();
                if let Some(error) = error {
                    self.error("machines".into(), error);
                }
            }
            Kind::Sessions(id) => {
                self.pending(-1);
                match result {
                    Err(error) => self.error(format!("sessions:{id}"), error),
                    Ok(value) => {
                        let mut rows = Vec::new();
                        for mut row in array(&value).to_vec() {
                            row["machine"] = json!(id);
                            let key = identity(&row);
                            let mut merged = self.catalog.get(&key).cloned().unwrap_or(json!({}));
                            if let Some(fields) = row.as_object() {
                                for (key, value) in fields {
                                    if !value.is_null() {
                                        merged[key] = value.clone();
                                    }
                                }
                            }
                            if row["record_id"].is_string() {
                                merged["search_record_id"] = row["record_id"].clone();
                            } else {
                                merged.as_object_mut().unwrap().remove("search_record_id");
                            }
                            if merged["last_at"].is_null() {
                                merged["last_at"] = row["ts"].clone();
                            }
                            self.catalog.insert(key, merged.clone());
                            rows.push(merged);
                        }
                        self.batches.insert(id, rows);
                        self.merge_sessions();
                    }
                }
            }
            Kind::Count(id) => {
                let count = result.ok().and_then(|v| v["total"].as_u64());
                self.counts.insert(id, count);
                let values: Option<Vec<u64>> = self
                    .ids()
                    .iter()
                    .map(|id| self.counts.get(id).copied().flatten())
                    .collect();
                self.state["total"] = values
                    .map(|v| json!(v.iter().sum::<u64>()))
                    .unwrap_or(json!(-1));
            }
            Kind::Projects(id) => {
                self.pending(-1);
                match result {
                    Ok(value) => {
                        self.projects.insert(id, array(&value).to_vec());
                        self.merge_projects();
                        self.persist();
                    }
                    Err(error) => self.error(format!("projects:{id}"), error),
                }
            }
            Kind::Activity(id) => {
                self.activity_pending.retain(|pending| pending != &id);
                match result {
                    Ok(value) => {
                        self.activity_failures.remove(&id);
                        self.charts.insert(id, value);
                    }
                    Err(error) => {
                        self.activity_failures.insert(id.clone(), error.clone());
                        self.error(format!("activity:{id}"), error);
                    }
                }
                self.activity_status();
                self.merge_activity();
                self.cache_activity();
            }
            Kind::Page {
                at,
                mode,
                target,
                occurrence,
            } => {
                self.state["readerBusy"] = json!(false);
                match result {
                    Err(error) => self.state["readerError"] = json!(error),
                    Ok(value) => {
                        let (rows, total) = decode_page(value);
                        if mode == "revalidate" {
                            if self.reader_matches(at, &rows, total) {
                                self.state["readerLoadedAt"] = json!(now_ms());
                                self.save_reader();
                            } else {
                                self.load_reader(total);
                            }
                            return;
                        }
                        self.state["recordTotal"] = json!(total);
                        self.state["readerLoadedAt"] = json!(now_ms());
                        if mode == "initial" && at != total.saturating_sub(60) {
                            self.load_page(total.saturating_sub(60), "initial", "", 0);
                            return;
                        }
                        let mut combined = rows.clone();
                        if mode == "before" {
                            combined.extend(array(&self.state["records"]).to_vec());
                            self.state["offset"] = json!(at);
                        } else if mode == "after" {
                            combined = array(&self.state["records"]).to_vec();
                            combined.extend(rows.clone());
                        } else {
                            self.state["offset"] = json!(at);
                        }
                        // Overlapping pages can occur after append-only source growth.
                        let mut seen = std::collections::HashSet::new();
                        combined.retain(|row| seen.insert(string(row, "record_id").to_owned()));
                        self.state["records"] = json!(combined);
                        if !target.is_empty() {
                            self.navigate(&target, occurrence);
                        } else if mode == "before" || mode == "after" {
                            let anchor = string(&self.state, "anchor").to_owned();
                            if !anchor.is_empty() {
                                self.navigate(&anchor, 0);
                                self.state["navigationKind"] = json!("restore");
                            }
                        } else if mode == "initial"
                            && let Some(last) = rows.last()
                        {
                            self.navigate(string(last, "record_id"), 0);
                        }
                        self.save_reader();
                    }
                }
            }
            Kind::Locate { at, target } => match result {
                Err(error) => {
                    self.state["readerBusy"] = json!(false);
                    self.state["readerError"] = json!(error);
                }
                Ok(value) => {
                    let (rows, total) = decode_page(value);
                    if let Some(index) = rows.iter().position(|r| string(r, "record_id") == target)
                    {
                        self.load_page((at + index).saturating_sub(30), "replace", &target, 0);
                    } else if !rows.is_empty() && at + rows.len() < total {
                        self.locate_page(at + rows.len(), &target);
                    } else {
                        self.load_page(total.saturating_sub(60), "initial", "", 0);
                    }
                }
            },
            Kind::Find { at } => match result {
                Err(error) => {
                    self.state["finding"] = json!(false);
                    self.state["findError"] = json!(error);
                }
                Ok(value) => {
                    let (rows, total) = decode_page(value);
                    let needle = string(&self.state, "findText").to_lowercase();
                    let mut matches = array(&self.state["matches"]).to_vec();
                    for (index, row) in rows.iter().enumerate() {
                        let body = find_body(row).to_lowercase();
                        for (occurrence, (position, _)) in body.match_indices(&needle).enumerate() {
                            matches.push(json!({"id":row["record_id"],"offset":at+index,"position":position,"occurrence":occurrence}));
                        }
                    }
                    self.state["matches"] = json!(matches);
                    self.state["scannedRecords"] = json!(at + rows.len());
                    if self.state["matchIndex"] == -1 && !array(&self.state["matches"]).is_empty() {
                        self.navigate_find(1);
                    }
                    if !rows.is_empty() && at + rows.len() < total {
                        self.find_page(at + rows.len());
                    } else {
                        self.state["finding"] = json!(false);
                    }
                }
            },
        }
    }
    fn merge_sessions(&mut self) {
        let ids = self.ids();
        // Search hits arrive in relevance order; interleave machines by rank and
        // fall back to recency only between equally ranked hits.
        let ranked = !string(&self.state, "query").trim().is_empty();
        let mut rows: Vec<(usize, Value)> = ids
            .iter()
            .flat_map(|id| {
                self.batches
                    .get(id)
                    .into_iter()
                    .flat_map(|rows| rows.iter().cloned().enumerate())
            })
            .collect();
        rows.sort_by(|(a_rank, a), (b_rank, b)| {
            (if ranked {
                a_rank.cmp(b_rank)
            } else {
                std::cmp::Ordering::Equal
            })
            .then_with(|| string(b, "last_at").cmp(string(a, "last_at")))
            .then_with(|| identity(a).cmp(&identity(b)))
        });
        let rows: Vec<Value> = rows.into_iter().map(|(_, row)| row).collect();
        self.state["sessions"] = json!(rows);
        self.state["canLoadMore"] = json!(ids.iter().any(|id| {
            self.batches
                .get(id)
                .is_some_and(|rows| rows.len() >= number(&self.state, "limit"))
        }));
    }
    fn merge_projects(&mut self) {
        let mut merged: BTreeMap<String, Value> = BTreeMap::new();
        for id in self.ids() {
            for row in self.projects.get(&id).into_iter().flatten() {
                let name = string(row, "project");
                let item = merged
                    .entry(name.into())
                    .or_insert(json!({"project":name,"session_count":0,"last_at":""}));
                item["session_count"] =
                    json!(number(item, "session_count") + number(row, "session_count"));
                if string(row, "last_at") > string(item, "last_at") {
                    item["last_at"] = row["last_at"].clone();
                }
            }
        }
        let mut rows: Vec<Value> = merged.into_values().collect();
        let sort = string(&self.state, "projectSort");
        rows.sort_by(|a, b| {
            match sort {
                "count" => number(b, "session_count").cmp(&number(a, "session_count")),
                "name" => string(a, "project").cmp(string(b, "project")),
                _ => string(b, "last_at").cmp(string(a, "last_at")),
            }
            .then_with(|| string(a, "project").cmp(string(b, "project")))
        });
        self.state["projects"] = json!(rows);
    }
    fn refresh_activity(&mut self) {
        self.activity += 1;
        self.cancel_stale();
        self.charts.clear();
        self.activity_failures.clear();
        self.activity_pending.clear();
        self.state["activityNow"] = json!(now_ms());
        if let Some(cache) = self.activity_caches.get(&self.activity_key()).cloned() {
            self.charts = cache.charts;
            if cache.complete
                && cache.revision == self.activity_revision
                && now_ms() - cache.updated_at < 60_000
            {
                self.activity_failures = cache.failures;
                self.activity_status();
                self.merge_activity();
                return;
            }
        }
        self.activity_pending = self.ids();
        self.activity_status();
        self.merge_activity();
        let range = match string(&self.state, "timeframe") {
            "day" => "24h",
            "week" => "7d",
            "month" => "30d",
            _ => "all",
        };
        for id in self.ids() {
            self.request(Kind::Activity(id.clone()),json!({"op":"activity","machine":id,"metric":self.state["metric"],"range":range,"now_ms":self.state["activityNow"],"query":nonempty(&self.state["query"]),"project":nonempty(&self.state["project"]),"source":nonempty(&self.state["provider"]),"origin":self.state["origin"]}));
        }
    }
    fn activity_status(&mut self) {
        if let Some(errors) = self.state["errors"].as_object_mut() {
            errors.retain(|key, _| !key.starts_with("activity:"));
            for (machine, error) in &self.activity_failures {
                errors.insert(format!("activity:{machine}"), json!(error));
            }
        }
        self.state["activityPending"] = json!(self.activity_pending.len());
        self.state["activityPendingMachines"] = json!(self.activity_pending);
        self.state["activityBusy"] = json!(!self.activity_pending.is_empty());
        self.state["activityComplete"] = json!(self.activity_pending.is_empty());
        self.state["tokenUsageEnabled"] = json!(
            self.charts
                .values()
                .any(|chart| chart["token_usage_enabled"] != false)
        );
        self.state["activityPartial"] = json!(
            !self.activity_pending.is_empty()
                || !self.activity_failures.is_empty()
                || self.charts.values().any(|chart| chart["partial"] == true
                    || (self.state["metric"] == "tokens" && chart["token_usage_enabled"] == false))
        );
        self.state["activityFailures"] = json!(self.activity_failures);
    }
    fn cache_activity(&mut self) {
        let key = self.activity_key();
        self.activity_caches.insert(
            key.clone(),
            ActivityCache {
                charts: self.charts.clone(),
                failures: self.activity_failures.clone(),
                updated_at: now_ms(),
                revision: self.activity_revision,
                complete: self.activity_pending.is_empty(),
            },
        );
        self.activity_cache_keys.retain(|cached| cached != &key);
        self.activity_cache_keys.push(key);
        while self.activity_cache_keys.len() > 8 {
            self.activity_caches
                .remove(&self.activity_cache_keys.remove(0));
        }
    }
    fn merge_activity(&mut self) {
        let unit = if self.state["timeframe"] == "day" {
            3_600_000_i64
        } else {
            86_400_000
        };
        let now = self.state["activityNow"].as_i64().unwrap_or_default();
        let end = now / unit * unit;
        let points: Vec<&Value> = self
            .charts
            .values()
            .flat_map(|v| array(&v["points"]))
            .collect();
        let days = match string(&self.state, "timeframe") {
            "day" => 1,
            "week" => 7,
            "month" => 30,
            _ => 0,
        };
        let start = if days > 0 {
            (now - days * 86_400_000) / unit * unit
        } else {
            points
                .iter()
                .filter_map(|p| p["timestamp_ms"].as_i64())
                .min()
                .unwrap_or(end)
                .min(end)
                / unit
                * unit
        };
        let step = (((end - start) / unit + 1 + 59) / 60).max(1) * unit;
        let mut bins: Vec<Value> = (0..=((end - start) / step))
            .map(|i| json!({"timestamp":start+i*step,"value":0.0,"sources":{}}))
            .collect();
        for point in points {
            let timestamp = point["timestamp_ms"].as_i64().unwrap_or_default();
            if timestamp < start || timestamp > end {
                continue;
            }
            let index = ((timestamp - start) / step) as usize;
            if let Some(bin) = bins.get_mut(index) {
                let value = point["value"].as_f64().unwrap_or_default();
                bin["value"] = json!(bin["value"].as_f64().unwrap_or_default() + value);
                let source = string(point, "source");
                bin["sources"][source] =
                    json!(bin["sources"][source].as_f64().unwrap_or_default() + value);
            }
        }
        self.state["activity"] = json!(bins);
    }
    fn open(&mut self, session: Value) {
        self.save_reader();
        self.reader += 1;
        self.find += 1;
        self.page += 1;
        self.cancel_stale();
        for (key, value) in [
            ("selected", session.clone()),
            ("readerBusy", json!(false)),
            ("readerError", json!("")),
            ("metadataError", json!("")),
            ("metadataBusy", json!(false)),
            ("records", json!([])),
            ("offset", json!(0)),
            ("recordTotal", json!(0)),
            ("findText", json!("")),
            ("matches", json!([])),
            ("matchIndex", json!(-1)),
            ("finding", json!(false)),
            ("anchor", json!("")),
            ("anchorOffset", json!(0)),
            ("expansion", json!({})),
            ("readerLoadedAt", json!(0)),
        ] {
            self.state[key] = value;
        }
        self.state["readerOpenedSerial"] = json!(number(&self.state, "readerOpenedSerial") + 1);
        if string(&session, "search_record_id").is_empty()
            && let Some(cached) = self.cache.get(&identity(&session)).cloned()
        {
            for (key, source) in [
                ("records", "records"),
                ("offset", "offset"),
                ("recordTotal", "total"),
                ("anchor", "anchor"),
                ("anchorOffset", "anchorOffset"),
                ("expansion", "expansion"),
                ("readerLoadedAt", "loadedAt"),
            ] {
                self.state[key] = cached[source].clone();
            }
            let target = if string(&cached, "anchor").is_empty() {
                array(&cached["records"])
                    .last()
                    .map(|row| string(row, "record_id"))
                    .unwrap_or_default()
            } else {
                string(&cached, "anchor")
            };
            self.navigate(target, 0);
            self.state["navigationKind"] = json!("restore");
            if now_ms() - self.state["readerLoadedAt"].as_i64().unwrap_or_default()
                >= READER_CACHE_LIFETIME_MS
            {
                self.revalidate_reader();
            }
            self.load_metadata();
            return;
        }
        self.load_reader(number(&session, "message_count"));
        self.load_metadata();
    }
    /// Load the selected session from scratch: its search hit when it has one,
    /// otherwise the latest page of `total_hint` records.
    fn load_reader(&mut self, total_hint: usize) {
        let target = string(&self.state["selected"], "search_record_id").to_owned();
        if target.is_empty() {
            self.load_page(total_hint.saturating_sub(60), "initial", "", 0);
        } else {
            self.locate_page(0, &target);
        }
    }
    /// Keep showing the current window while the latest page is refetched;
    /// the window is replaced only if the session changed underneath it.
    fn revalidate_reader(&mut self) {
        if self.state["selected"].is_null() || self.state["readerBusy"] == true {
            return;
        }
        if array(&self.state["records"]).is_empty() {
            self.load_reader(number(&self.state["selected"], "message_count"));
        } else {
            let total = number(&self.state, "recordTotal");
            self.load_page(total.saturating_sub(60), "revalidate", "", 0);
        }
    }
    /// The native transports carry no snapshot version, so an unchanged total
    /// and identical overlapping records stand in for the web client's check.
    fn reader_matches(&self, at: usize, rows: &[Value], total: usize) -> bool {
        let records = array(&self.state["records"]);
        let offset = number(&self.state, "offset");
        total == number(&self.state, "recordTotal")
            && rows.iter().enumerate().all(|(index, row)| {
                (at + index)
                    .checked_sub(offset)
                    .and_then(|position| records.get(position))
                    .is_none_or(|cached| cached == row)
            })
    }
    fn load_metadata(&mut self) {
        let selected = &self.state["selected"];
        if !string(selected, "label").trim().is_empty()
            && !string(selected, "cwd").trim().is_empty()
        {
            return;
        }
        let payload = json!({"op":"sessions","machine":selected["machine"].as_str().unwrap_or("local"),"filters":{"source":selected["source"],"session_id":selected["session_id"],"source_path":selected["source_path"],"origin":"all","limit":1}});
        self.state["metadataBusy"] = json!(true);
        self.request(Kind::Metadata, payload);
    }
    fn apply_metadata(&mut self, metadata: Value) {
        let mut selected = self.state["selected"].clone();
        let key = identity(&selected);
        if let Some(fields) = metadata.as_object() {
            for (field, value) in fields {
                if !value.is_null()
                    && !(field == "label"
                        && value.as_str().is_none_or(|label| label.trim().is_empty()))
                    && !["snippet", "search_record_id", "record_id", "machine"]
                        .contains(&field.as_str())
                {
                    selected[field] = value.clone();
                }
            }
        }
        self.catalog.insert(key.clone(), selected.clone());
        for rows in self.batches.values_mut() {
            for row in rows.iter_mut().filter(|row| identity(row) == key) {
                *row = selected.clone();
            }
        }
        for row in self.state["sessions"]
            .as_array_mut()
            .into_iter()
            .flatten()
            .filter(|row| identity(row) == key)
        {
            *row = selected.clone();
        }
        self.state["selected"] = selected;
    }
    fn page_payload(&self, at: usize, limit: usize) -> Value {
        let s = &self.state["selected"];
        json!({"op":"session_page","machine":s["machine"].as_str().unwrap_or("local"),"session_id":s["session_id"],"source_path":s["source_path"],"offset":at,"limit":limit})
    }
    fn load_page(&mut self, at: usize, mode: &str, target: &str, occurrence: usize) {
        if self.state["selected"].is_null() {
            return;
        }
        self.page += 1;
        self.cancel_stale();
        self.state["readerBusy"] = json!(true);
        self.state["readerError"] = json!("");
        self.request(
            Kind::Page {
                at,
                mode: mode.into(),
                target: target.into(),
                occurrence,
            },
            self.page_payload(
                at,
                if mode == "before" {
                    number(&self.state, "offset").saturating_sub(at).min(60)
                } else {
                    60
                },
            ),
        );
    }
    fn locate_page(&mut self, at: usize, target: &str) {
        self.state["readerBusy"] = json!(true);
        self.request(
            Kind::Locate {
                at,
                target: target.into(),
            },
            self.page_payload(at, 500),
        );
    }
    fn start_find(&mut self, text: &str) {
        self.find += 1;
        // A prior find may still be fetching a replacement page. Its navigation
        // must not land after the query was cleared or changed.
        if self.workers.iter().any(
            |(request, _)| matches!(&request.kind, Kind::Page{target,..} if !target.is_empty()),
        ) {
            self.page += 1;
            self.state["readerBusy"] = json!(false);
        }
        self.cancel_stale();
        self.state["findText"] = json!(text);
        self.state["matches"] = json!([]);
        self.state["matchIndex"] = json!(-1);
        self.state["findError"] = json!("");
        self.state["scannedRecords"] = json!(0);
        let active = !text.is_empty() && !self.state["selected"].is_null();
        self.state["finding"] = json!(active);
        if active {
            self.find_page(0);
        }
    }
    fn find_page(&mut self, at: usize) {
        self.request(Kind::Find { at }, self.page_payload(at, 200));
    }
    fn navigate_find(&mut self, direction: i64) {
        let matches = array(&self.state["matches"]);
        if matches.is_empty() {
            return;
        }
        let prior = self.state["matchIndex"].as_i64().unwrap_or(-1);
        let initial = if prior < 0 && direction < 0 { 0 } else { prior };
        let index = (initial + direction).rem_euclid(matches.len() as i64) as usize;
        let target = matches[index].clone();
        self.state["matchIndex"] = json!(index);
        let id = string(&target, "id");
        let occurrence = number(&target, "occurrence");
        if array(&self.state["records"])
            .iter()
            .any(|r| string(r, "record_id") == id)
        {
            self.page += 1;
            self.cancel_stale();
            self.state["readerBusy"] = json!(false);
            self.navigate(id, occurrence);
        } else {
            self.load_page(
                number(&target, "offset").saturating_sub(30),
                "replace",
                id,
                occurrence,
            );
        }
    }
    fn navigate(&mut self, id: &str, occurrence: usize) {
        self.state["navigationKind"] = json!("record");
        self.state["navigationSerial"] = json!(number(&self.state, "navigationSerial") + 1);
        self.state["targetRecord"] = json!(id);
        self.state["targetOccurrence"] = json!(occurrence);
    }
    fn save_reader(&mut self) {
        if self.state["selected"].is_null() || array(&self.state["records"]).is_empty() {
            return;
        }
        let key = identity(&self.state["selected"]);
        self.cache.insert(key.clone(),json!({"records":self.state["records"],"offset":self.state["offset"],"total":self.state["recordTotal"],"anchor":self.state["anchor"],"anchorOffset":self.state["anchorOffset"],"expansion":self.state["expansion"],"loadedAt":self.state["readerLoadedAt"]}));
        self.cache_keys.retain(|k| k != &key);
        self.cache_keys.push(key);
        while self.cache_keys.len() > 20 {
            self.cache.remove(&self.cache_keys.remove(0));
        }
    }
    fn persist(&self) {
        let Some(path) = self.persistence.clone() else {
            return;
        };
        let mut saved = json!({"projects":self.projects});
        for key in ["machine", "provider", "timeframe", "origin", "projectSort"] {
            saved[key] = self.state[key].clone();
        }
        // A single ordered queue avoids an older disk write winning over newer settings.
        static WRITER: std::sync::OnceLock<Sender<(PathBuf, Value)>> = std::sync::OnceLock::new();
        let writer = WRITER.get_or_init(|| {
            let (tx, rx) = mpsc::channel::<(PathBuf, Value)>();
            std::thread::spawn(move || {
                for (path, saved) in rx {
                    let Ok(bytes) = serde_json::to_vec(&saved) else {
                        continue;
                    };
                    if let Some(parent) = path.parent() {
                        if std::fs::create_dir_all(parent).is_err() {
                            continue;
                        }
                        if let Ok(mut file) = tempfile::NamedTempFile::new_in(parent) {
                            use std::io::Write;
                            if file.write_all(&bytes).is_ok() {
                                let _ = file.persist(path);
                            }
                        }
                    }
                }
            });
            tx
        });
        let _ = writer.send((path, saved));
    }
}
fn opening_title(records: &[Value]) -> Option<String> {
    for row in records {
        let record = &row["record"];
        if record["role"] != "user" {
            continue;
        }
        let mut body = string(record, "text").trim();
        // Opening user envelopes may precede the actual request in one record.
        // Strip only recognized leading context, never arbitrary XML in a task.
        loop {
            let context = [
                "environment_context",
                "context_window",
                "INSTRUCTIONS",
                "instructions",
                "skills_instructions",
                "permissions instructions",
                "app-context",
            ]
            .into_iter()
            .find(|tag| body.starts_with(&format!("<{tag}>")));
            let Some(tag) = context else {
                break;
            };
            let closing = format!("</{tag}>");
            let Some(end) = body.find(&closing) else {
                body = "";
                break;
            };
            body = body[end + closing.len()..].trim();
        }
        if body.starts_with("# AGENTS.md instructions") {
            continue;
        }
        if !body.is_empty() {
            let normalized = body.split_whitespace().collect::<Vec<_>>().join(" ");
            let title: String = normalized.chars().take(160).collect();
            return Some(if normalized.chars().count() > 160 {
                format!("{title}…")
            } else {
                title
            });
        }
    }
    for row in records {
        let record = &row["record"];
        if record["role"] == "developer" && string(record, "text").starts_with("<context_window>") {
            for line in string(record, "text").lines() {
                if let Some(name) = line.strip_prefix("Agent name: /root/") {
                    let title: String = name.replace('_', " ").chars().take(160).collect();
                    if let Some(first) = title.chars().next() {
                        return Some(first.to_uppercase().to_string() + &title[first.len_utf8()..]);
                    }
                }
            }
        }
    }
    None
}
fn initial_state() -> Value {
    let mut state = json!({
        "query":"", "project":"", "machine":"all", "provider":"",
        "timeframe":"all", "origin":"interactive", "projectSort":"activity", "metric":"sessions",
        "machines":[{"id":"local","label":"This computer"}],
        "projects":[], "sessions":[], "errors":{}, "total":-1, "pending":0, "limit":200, "canLoadMore":false,
        "activity":[], "tokenUsageEnabled":true, "activityPartial":false,
        "activityBusy":false, "activityComplete":false, "activityPending":0,
        "activityPendingMachines":[], "activityFailures":{}
    });
    let reader = json!({
        "selected":null, "records":[], "offset":0, "recordTotal":0,
        "readerBusy":false, "readerError":"", "metadataBusy":false,"metadataError":"", "findText":"", "matches":[],
        "matchIndex":-1, "finding":false, "findError":"", "scannedRecords":0,
        "navigationSerial":0, "readerOpenedSerial":0, "targetRecord":"", "targetOccurrence":0,
        "navigationKind":"record", "anchor":"", "anchorOffset":0, "expansion":{}, "readerLoadedAt":0
    });
    state
        .as_object_mut()
        .unwrap()
        .extend(reader.as_object().unwrap().clone());
    state
}
fn string<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap_or_default()
}
fn number(value: &Value, key: &str) -> usize {
    value[key].as_u64().unwrap_or_default() as usize
}
fn array(value: &Value) -> &[Value] {
    value.as_array().map(Vec::as_slice).unwrap_or(&[])
}
fn nonempty(value: &Value) -> Value {
    if value.as_str().is_some_and(|v| !v.trim().is_empty()) {
        value.clone()
    } else {
        Value::Null
    }
}
fn identity(value: &Value) -> String {
    [
        value["machine"].as_str().unwrap_or("local"),
        string(value, "source"),
        string(value, "session_id"),
        string(value, "source_path"),
    ]
    .join("\u{1f}")
}
fn decode_page(value: Value) -> (Vec<Value>, usize) {
    let rows = array(&value);
    let total = rows
        .iter()
        .find(|r| r["type"] == "page")
        .map(|r| number(r, "total"))
        .unwrap_or(rows.len());
    (
        rows.iter()
            .filter(|r| r["type"] != "page")
            .cloned()
            .collect(),
        total,
    )
}
fn find_body(row: &Value) -> String {
    let r = &row["record"];
    if [
        "tool_use",
        "tool_result",
        "tool",
        "reasoning",
        "system",
        "developer",
    ]
    .contains(&string(r, "role"))
    {
        let mut parts = Vec::new();
        for key in ["tool_input", "tool_output", "text"] {
            if let Some(value) = r[key].as_str()
                && !value.trim().is_empty()
                && !parts.contains(&value)
            {
                parts.push(value);
            }
        }
        parts.join("\n\n")
    } else {
        string(r, "text").to_owned()
    }
}
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
fn iso_date(seconds: i64) -> String {
    // Gregorian civil date conversion; avoids a platform-dependent time API.
    let days = seconds.div_euclid(86400) + 719468;
    let era = days.div_euclid(146097);
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    let time = seconds.rem_euclid(86400);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        time / 3600,
        time / 60 % 60,
        time % 60
    )
}
fn storage_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let root = std::env::var_os("MEMEX_ROOT")
        .map(PathBuf::from)
        .or_else(|| home.as_ref().map(|p| p.join(".memex")))?;
    let mut hash = 0xcbf29ce484222325u64;
    for byte in root.to_string_lossy().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let base = if cfg!(target_os = "macos") {
        home?.join("Library/Caches")
    } else if cfg!(windows) {
        PathBuf::from(std::env::var_os("LOCALAPPDATA")?)
    } else {
        std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .or_else(|| home.map(|p| p.join(".cache")))?
    };
    Some(base.join("memex-qt").join(format!("{hash:016x}.json")))
}

impl Drop for Store {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> Store {
        Store::new(None)
    }
    fn session(id: &str) -> Value {
        json!({"machine":"local","source":"codex","session_id":id,"source_path":format!("/{id}"),"message_count":2})
    }
    fn page(records: &[(&str, &str)], total: usize) -> Value {
        let mut rows: Vec<Value> = records
            .iter()
            .map(|(id, text)| json!({"record_id":id,"record":{"role":"user","text":text}}))
            .collect();
        rows.push(json!({"type":"page","total":total}));
        json!(rows)
    }
    fn respond(
        store: &mut Store,
        predicate: impl Fn(&Kind) -> bool,
        result: Result<Value, String>,
    ) -> Value {
        let index = store
            .requests
            .iter()
            .position(|(request, _)| predicate(&request.kind))
            .expect("expected request");
        let (request, payload) = store.requests.remove(index);
        store.accept(request, result);
        payload
    }
    #[test]
    fn defaults_and_complete_origin_filter_set() {
        let mut s = store();
        assert_eq!(s.poll()["origin"], "interactive");
        for origin in ["interactive", "regular", "subagent", "all"] {
            s.action(json!({"op":"configure","changes":{"origin":origin,"provider":"codex","timeframe":"week"}}));
            assert_eq!(s.filters()["origin"], origin);
            assert_eq!(s.filters()["source"], "codex");
            assert!(s.filters()["since"].as_str().unwrap().ends_with('Z'));
        }
        s.action(json!({"op":"resetFilters"}));
        assert_eq!(s.state["origin"], "interactive");
        assert!(s.filters()["source"].is_null());
    }
    #[test]
    fn stale_search_and_count_cannot_replace_current_query() {
        let mut s = store();
        s.refresh();
        let old = std::mem::take(&mut s.requests);
        s.action(json!({"op":"configure","changes":{"query":"new"}}));
        let pending = s.state["pending"].clone();
        for (request, _) in old {
            s.accept(request, Ok(json!([])));
        }
        assert_eq!(s.state["pending"], pending);
        assert_eq!(s.state["sessions"], json!([]));
        let payload = respond(&mut s, |k| matches!(k, Kind::Sessions(_)), Ok(json!([])));
        assert_eq!(payload["op"], "search");
        assert_eq!(payload["query"], "new");
    }
    #[test]
    fn fast_machine_results_survive_remote_failure_and_counts_stay_unknown() {
        let mut s = store();
        s.state["machines"] = json!([{"id":"local"},{"id":"peer"}]);
        s.refresh();
        respond(
            &mut s,
            |k| matches!(k,Kind::Sessions(id) if id=="local"),
            Ok(json!([session("fast")])),
        );
        assert_eq!(s.state["sessions"][0]["session_id"], "fast");
        respond(
            &mut s,
            |k| matches!(k,Kind::Sessions(id) if id=="peer"),
            Err("offline".into()),
        );
        respond(
            &mut s,
            |k| matches!(k,Kind::Count(id) if id=="local"),
            Ok(json!({"total":10})),
        );
        respond(
            &mut s,
            |k| matches!(k,Kind::Count(id) if id=="peer"),
            Err("offline".into()),
        );
        assert_eq!(s.state["total"], -1);
        assert_eq!(array(&s.state["sessions"]).len(), 1);
        assert_eq!(s.state["errors"]["sessions:peer"], "offline");
    }
    #[test]
    fn project_cache_survives_errors_and_aggregates_selected_machines() {
        let mut s = store();
        s.state["machines"] = json!([{"id":"local"},{"id":"peer"}]);
        s.projects.insert(
            "local".into(),
            vec![json!({"project":"p","session_count":3,"last_at":"2026-01-01"})],
        );
        s.projects.insert(
            "peer".into(),
            vec![json!({"project":"p","session_count":4,"last_at":"2026-02-01"})],
        );
        s.refresh();
        respond(
            &mut s,
            |k| matches!(k, Kind::Projects(_)),
            Err("offline".into()),
        );
        assert_eq!(s.state["projects"][0]["session_count"], 7);
        s.action(json!({"op":"configure","changes":{"machine":"peer"}}));
        assert_eq!(s.state["projects"][0]["session_count"], 4);
    }
    #[test]
    fn search_record_anchor_does_not_leak_into_recent_results() {
        let mut s = store();
        s.refresh();
        let mut hit = session("a");
        hit["record_id"] = json!("r");
        respond(&mut s, |k| matches!(k, Kind::Sessions(_)), Ok(json!([hit])));
        assert_eq!(s.state["sessions"][0]["search_record_id"], "r");
        s.refresh();
        respond(
            &mut s,
            |k| matches!(k, Kind::Sessions(_)),
            Ok(json!([session("a")])),
        );
        assert!(s.state["sessions"][0]["search_record_id"].is_null());
    }
    #[test]
    fn initial_reader_corrects_stale_message_count_and_prepends_exact_gap() {
        let mut s = store();
        s.open(session("a"));
        respond(
            &mut s,
            |k| matches!(k, Kind::Page { .. }),
            Ok(page(&[("r0", "zero")], 125)),
        );
        assert!(
            s.requests
                .iter()
                .any(|(_, p)| p["offset"] == 65 && p["limit"] == 60)
        );
        respond(
            &mut s,
            |k| matches!(k, Kind::Page { at: 65, .. }),
            Ok(page(&[("r65", "end")], 125)),
        );
        assert_eq!(s.state["targetRecord"], "r65");
        s.action(json!({"op":"earlier"}));
        let payload = respond(
            &mut s,
            |k| matches!(k,Kind::Page{mode,..} if mode=="before"),
            Ok(page(&[("r5", "before")], 125)),
        );
        assert_eq!(payload["offset"], 5);
        assert_eq!(s.state["records"][0]["record_id"], "r5");
        assert_eq!(s.state["offset"], 5);
    }
    #[test]
    fn reader_switch_rejects_old_pages_and_clears_busy_on_cached_open() {
        let mut s = store();
        s.open(session("a"));
        let old = s.requests.remove(0).0;
        s.open(session("b"));
        s.accept(old, Ok(page(&[("wrong", "wrong")], 1)));
        assert!(array(&s.state["records"]).is_empty());
        respond(
            &mut s,
            |k| matches!(k, Kind::Page { .. }),
            Ok(page(&[("b", "body")], 1)),
        );
        s.action(json!({"op":"saveAnchor","id":"b","offset":12,"expansion":{"b":true}}));
        s.open(session("a"));
        s.open(session("b"));
        assert_eq!(s.state["readerBusy"], false);
        assert_eq!(s.state["anchorOffset"], 12);
        assert_eq!(s.state["expansion"]["b"], true);
        assert_eq!(s.state["targetRecord"], "b");
    }
    #[test]
    fn find_scans_all_pages_and_navigates_exact_literal_occurrence() {
        let mut s = store();
        s.open(session("a"));
        respond(
            &mut s,
            |k| matches!(k, Kind::Page { .. }),
            Ok(page(&[("r0", "Needle needle")], 2)),
        );
        s.start_find("needle");
        respond(
            &mut s,
            |k| matches!(k, Kind::Find { at: 0 }),
            Ok(page(&[("r0", "Needle needle")], 2)),
        );
        assert_eq!(s.state["targetOccurrence"], 0);
        assert_eq!(s.state["finding"], true);
        respond(
            &mut s,
            |k| matches!(k, Kind::Find { at: 1 }),
            Ok(page(&[("r1", "needle")], 2)),
        );
        assert_eq!(array(&s.state["matches"]).len(), 3);
        assert_eq!(s.state["finding"], false);
        s.navigate_find(1);
        assert_eq!(s.state["targetOccurrence"], 1);
        s.navigate_find(1);
        respond(
            &mut s,
            |k| matches!(k, Kind::Page { .. }),
            Ok(page(&[("r0", "Needle needle"), ("r1", "needle")], 2)),
        );
        assert_eq!(s.state["targetRecord"], "r1");
        assert_eq!(s.state["targetOccurrence"], 0);
    }
    #[test]
    fn stale_find_and_activity_requests_are_cancelled() {
        let mut s = store();
        s.open(session("a"));
        s.start_find("old");
        let cancellation = s.workers.last().unwrap().1.clone();
        let old = s.requests.last().unwrap().0.clone();
        s.start_find("new");
        assert!(cancellation.load(Ordering::Relaxed));
        s.accept(old, Ok(page(&[("a", "old")], 1)));
        assert!(array(&s.state["matches"]).is_empty());
        s.refresh_activity();
        let old = s.requests.last().unwrap().0.clone();
        s.refresh_activity();
        s.accept(
            old,
            Ok(json!({"points":[{"timestamp_ms":0,"value":999,"source":"codex"}]})),
        );
        assert!(s.charts.is_empty());
    }
    #[test]
    fn search_results_keep_rank_order_across_machines() {
        let mut s = store();
        s.state["machines"] = json!([{"id":"local"},{"id":"peer"}]);
        s.action(json!({"op":"configure","changes":{"query":"needle"}}));
        let hit = |id: &str, last_at: &str| {
            let mut row = session(id);
            row["last_at"] = json!(last_at);
            row
        };
        respond(
            &mut s,
            |k| matches!(k, Kind::Sessions(id) if id == "local"),
            Ok(json!([
                hit("best", "2026-01-01"),
                hit("third", "2026-03-01")
            ])),
        );
        respond(
            &mut s,
            |k| matches!(k, Kind::Sessions(id) if id == "peer"),
            Ok(json!([hit("second", "2026-02-01")])),
        );
        let order: Vec<&str> = array(&s.state["sessions"])
            .iter()
            .map(|row| string(row, "session_id"))
            .collect();
        assert_eq!(order, ["second", "best", "third"]);
        s.action(json!({"op":"configure","changes":{"query":""}}));
        respond(
            &mut s,
            |k| matches!(k, Kind::Sessions(id) if id == "local"),
            Ok(json!([hit("old", "2026-01-01"), hit("new", "2026-03-01")])),
        );
        assert_eq!(s.state["sessions"][0]["session_id"], "new");
    }
    #[test]
    fn stale_reader_cache_is_revalidated_on_reopen_and_refresh() {
        let mut s = store();
        s.open(session("a"));
        respond(
            &mut s,
            |k| matches!(k, Kind::Page { .. }),
            Ok(page(&[("r0", "zero"), ("r1", "one")], 2)),
        );
        s.open(session("b"));
        s.requests.clear();
        s.open(session("a"));
        assert!(
            !s.requests
                .iter()
                .any(|(r, _)| matches!(r.kind, Kind::Page { .. })),
            "a fresh cache is shown without refetching"
        );

        s.open(session("b"));
        s.cache.get_mut(&identity(&session("a"))).unwrap()["loadedAt"] = json!(0);
        s.requests.clear();
        s.open(session("a"));
        assert_eq!(s.state["records"][1]["record_id"], "r1");
        respond(
            &mut s,
            |k| matches!(k, Kind::Page { mode, .. } if mode == "revalidate"),
            Ok(page(&[("r0", "zero"), ("r1", "one")], 2)),
        );
        assert_eq!(s.state["readerBusy"], false);
        assert!(
            s.requests
                .iter()
                .all(|(r, _)| !matches!(r.kind, Kind::Page { .. }))
        );

        s.action(json!({"op":"refresh"}));
        respond(
            &mut s,
            |k| matches!(k, Kind::Page { mode, .. } if mode == "revalidate"),
            Ok(page(&[("r1", "one"), ("r2", "two")], 3)),
        );
        let payload = respond(
            &mut s,
            |k| matches!(k, Kind::Page { mode, .. } if mode == "initial"),
            Ok(page(&[("r0", "zero"), ("r1", "one"), ("r2", "two")], 3)),
        );
        assert_eq!(payload["offset"], 0);
        assert_eq!(array(&s.state["records"]).len(), 3);
        assert_eq!(s.state["recordTotal"], 3);
        assert_eq!(s.state["targetRecord"], "r2");
    }
    #[test]
    fn reader_cache_is_bounded_to_twenty_sessions() {
        let mut s = store();
        for index in 0..25 {
            s.state["selected"] = session(&index.to_string());
            s.state["records"] = json!([{"record_id":"r"}]);
            s.save_reader();
        }
        assert_eq!(s.cache.len(), 20);
        assert!(!s.cache.contains_key(&identity(&session("0"))));
        assert!(s.cache.contains_key(&identity(&session("24"))));
    }
    #[test]
    fn idle_pump_and_scrolling_do_not_publish_replacement_models() {
        let mut s = store();
        assert!(s.poll_changed().is_some());
        assert!(s.poll_changed().is_none());
        s.action(json!({"op":"saveAnchor","id":"r","offset":14}));
        assert!(s.poll_changed().is_none());
        s.action(json!({"op":"configure","changes":{"query":"query"}}));
        assert!(s.poll_changed().is_some());
        assert!(s.poll_changed().is_none());
        respond(
            &mut s,
            |kind| matches!(kind, Kind::Sessions(_)),
            Ok(json!([])),
        );
        assert!(s.poll_changed().is_some());
    }
    #[test]
    fn prepend_near_start_requests_only_the_missing_records() {
        let mut s = store();
        s.state["selected"] = session("a");
        s.state["offset"] = json!(5);
        s.state["records"] = json!([{"record_id":"existing"}]);
        s.action(json!({"op":"earlier"}));
        let payload = &s.requests.last().unwrap().1;
        assert_eq!(payload["offset"], 0);
        assert_eq!(payload["limit"], 5);
    }
    #[test]
    fn changed_find_cancels_old_navigation_page() {
        let mut s = store();
        s.state["selected"] = session("a");
        s.load_page(500, "replace", "old-hit", 1);
        let (request, _) = s.requests.remove(0);
        let token = s.workers.last().unwrap().1.clone();
        s.start_find("new query");
        assert!(token.load(Ordering::Relaxed));
        s.accept(request, Ok(page(&[("old-hit", "old text")], 600)));
        assert_eq!(s.state["targetRecord"], "");
        assert!(array(&s.state["records"]).is_empty());
    }
    #[test]
    fn activity_retains_partial_success_and_token_opt_out() {
        let mut s = store();
        s.state["machines"] = json!([{"id":"local"},{"id":"peer"}]);
        s.refresh_activity();
        let timestamp = s.state["activityNow"].as_i64().unwrap() / 86_400_000 * 86_400_000;
        respond(
            &mut s,
            |k| matches!(k,Kind::Activity(id) if id=="local"),
            Ok(
                json!({"points":[{"timestamp_ms":timestamp,"value":5,"source":"codex"}],"token_usage_enabled":false}),
            ),
        );
        respond(
            &mut s,
            |k| matches!(k,Kind::Activity(id) if id=="peer"),
            Err("offline".into()),
        );
        assert_eq!(s.state["activityPartial"], true);
        assert_eq!(s.state["tokenUsageEnabled"], false);
        assert_eq!(s.state["activity"][0]["value"], 5.0);
    }
    #[test]
    fn activity_and_instruction_find_deduplicates_provider_echoes() {
        let record = json!({"record":{"role":"tool_use","text":"needle","tool_input":"needle","tool_output":"result","source_content":"unrendered needle"}});
        assert_eq!(find_body(&record), "needle\n\nresult");
        let normal =
            json!({"record":{"role":"assistant","text":"answer","tool_input":"unrendered"}});
        assert_eq!(find_body(&normal), "answer");
    }
    #[test]
    fn delayed_cache_restore_does_not_override_user_or_fresh_projects() {
        let mut s = store();
        s.request(Kind::Restore, json!({}));
        s.action(json!({"op":"configure","changes":{"machine":"peer"}}));
        s.projects.insert(
            "peer".into(),
            vec![json!({"project":"fresh","session_count":1})],
        );
        respond(
            &mut s,
            |k| matches!(k, Kind::Restore),
            Ok(
                json!({"machine":"local","projects":{"peer":[{"project":"stale"}],"local":[{"project":"cached"}]}}),
            ),
        );
        assert_eq!(s.state["machine"], "peer");
        assert_eq!(s.state["projects"][0]["project"], "fresh");
        assert!(s.projects.contains_key("local"));
    }
    #[test]
    fn refresh_preserves_same_criteria_sessions_when_machine_is_offline() {
        let mut s = store();
        s.refresh();
        respond(
            &mut s,
            |k| matches!(k, Kind::Sessions(_)),
            Ok(json!([session("cached")])),
        );
        s.action(json!({"op":"refresh"}));
        assert_eq!(s.state["sessions"][0]["session_id"], "cached");
        respond(
            &mut s,
            |k| matches!(k, Kind::Sessions(_)),
            Err("offline".into()),
        );
        assert_eq!(s.state["sessions"][0]["session_id"], "cached");
        s.action(json!({"op":"configure","changes":{"query":"different"}}));
        assert!(array(&s.state["sessions"]).is_empty());
    }
    #[test]
    fn activity_reuses_fresh_criteria_but_explicit_refresh_retains_and_reloads() {
        let mut s = store();
        s.refresh_activity();
        let timestamp = s.state["activityNow"].as_i64().unwrap() / 86_400_000 * 86_400_000;
        assert_eq!(s.state["activityBusy"], true);
        assert_eq!(s.state["activityPending"], 1);
        respond(
            &mut s,
            |k| matches!(k, Kind::Activity(_)),
            Ok(
                json!({"points":[{"timestamp_ms":timestamp,"value":7,"source":"codex"}],"token_usage_enabled":true}),
            ),
        );
        assert_eq!(s.state["activityBusy"], false);
        assert_eq!(s.state["activityPartial"], false);
        s.refresh_activity();
        assert!(
            !s.requests
                .iter()
                .any(|(r, _)| matches!(r.kind, Kind::Activity(_)))
        );
        s.action(json!({"op":"refreshActivity"}));
        assert_eq!(s.state["activityBusy"], true);
        assert_eq!(s.state["activity"][0]["value"], 7.0);
        respond(
            &mut s,
            |k| matches!(k, Kind::Activity(_)),
            Err("offline".into()),
        );
        assert_eq!(s.state["activity"][0]["value"], 7.0);
        assert_eq!(s.state["activityPartial"], true);
    }
    #[test]
    fn activity_cache_expires_and_is_bounded_to_eight_criteria() {
        let mut s = store();
        for index in 0..10 {
            s.state["project"] = json!(index.to_string());
            s.cache_activity();
        }
        assert_eq!(s.activity_caches.len(), 8);
        let key = s.activity_key();
        s.activity_caches.get_mut(&key).unwrap().updated_at = now_ms() - 61_000;
        s.refresh_activity();
        assert_eq!(s.state["activityBusy"], true);
    }
    #[test]
    fn shutdown_waits_for_owned_worker_cleanup_and_is_idempotent() {
        let mut s = store();
        s.request(Kind::Machines, json!({"op":"machines"}));
        let cancelled = s.workers.last().unwrap().1.clone();
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let finished = cleaned_up.clone();
        s.worker_threads.push(std::thread::spawn(move || {
            while !cancelled.load(Ordering::Relaxed) {
                std::thread::yield_now();
            }
            // Model the client's asynchronous child kill/wait cleanup. Merely
            // setting the flag must not allow shutdown to return before this.
            std::thread::sleep(std::time::Duration::from_millis(5));
            finished.store(true, Ordering::Release);
        }));
        s.shutdown();
        assert!(cleaned_up.load(Ordering::Acquire));
        assert!(s.worker_threads.is_empty());
        s.shutdown();
        let before = s.requests.len();
        s.action(json!({"op":"refresh"}));
        assert_eq!(s.requests.len(), before);
    }
    #[test]
    fn drop_joins_already_cancelled_stale_workers() {
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let finished = cleaned_up.clone();
        {
            let mut s = store();
            s.request(Kind::Sessions("local".into()), json!({}));
            let cancelled = s.workers.last().unwrap().1.clone();
            s.worker_threads.push(std::thread::spawn(move || {
                while !cancelled.load(Ordering::Relaxed) {
                    std::thread::yield_now();
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
                finished.store(true, Ordering::Release);
            }));
            s.generation += 1;
            s.cancel_stale();
            assert!(s.workers.is_empty());
        }
        assert!(cleaned_up.load(Ordering::Acquire));
    }
    #[test]
    fn search_metadata_uses_exact_identity_and_preserves_hit_anchor() {
        let mut s = store();
        let mut hit = session("search");
        hit["search_record_id"] = json!("matched");
        hit["snippet"] = json!("matched snippet");
        s.state["sessions"] = json!([hit.clone()]);
        s.open(hit);
        let payload = respond(
            &mut s,
            |k| matches!(k, Kind::Metadata),
            Ok(
                json!([{"source":"codex","session_id":"search","source_path":"/search","label":"Actual title","cwd":"/project","message_count":42,"resume_cmd":"codex resume search","snippet":"wrong"}]),
            ),
        );
        assert_eq!(
            payload["filters"],
            json!({"source":"codex","session_id":"search","source_path":"/search","origin":"all","limit":1})
        );
        assert_eq!(s.state["selected"]["label"], "Actual title");
        assert_eq!(s.state["selected"]["cwd"], "/project");
        assert_eq!(s.state["selected"]["search_record_id"], "matched");
        assert_eq!(s.state["selected"]["snippet"], "matched snippet");
        assert_eq!(s.state["sessions"][0]["resume_cmd"], "codex resume search");
    }
    #[test]
    fn metadata_rejects_wrong_identity_and_stale_reader_details() {
        let mut s = store();
        s.open(session("a"));
        respond(
            &mut s,
            |k| matches!(k, Kind::Metadata),
            Ok(json!([{"source":"codex","session_id":"other","source_path":"/a","label":"Wrong"}])),
        );
        assert!(s.state["selected"]["label"].is_null());
        assert!(
            s.state["metadataError"]
                .as_str()
                .unwrap()
                .contains("did not match")
        );
        s.load_metadata();
        let index = s
            .requests
            .iter()
            .position(|(r, _)| matches!(r.kind, Kind::Metadata))
            .unwrap();
        let stale = s.requests.remove(index).0;
        s.open(session("b"));
        s.accept(
            stale,
            Ok(json!([{"source":"codex","session_id":"a","source_path":"/a","label":"Stale"}])),
        );
        assert_eq!(s.state["selected"]["session_id"], "b");
        assert!(s.state["selected"]["label"].is_null());
    }
    #[test]
    fn metadata_falls_back_to_opening_request_or_subagent_name() {
        let mut s = store();
        s.open(session("a"));
        respond(
            &mut s,
            |k| matches!(k, Kind::Metadata),
            Ok(json!([session("a")])),
        );
        let payload = respond(
            &mut s,
            |k| matches!(k, Kind::OpeningTitle),
            Ok(page(
                &[
                    (
                        "context",
                        "<environment_context>context</environment_context>",
                    ),
                    ("request", "Fix the\n startup issue"),
                ],
                2,
            )),
        );
        assert_eq!(payload["offset"], 0);
        assert_eq!(payload["limit"], 16);
        assert_eq!(s.state["selected"]["label"], "Fix the startup issue");
        assert_eq!(
            opening_title(&[
                json!({"record":{"role":"developer","text":"<context_window>\nAgent name: /root/rust_state\n</context_window>"}})
            ]),
            Some("Rust state".into())
        );
    }
    #[test]
    fn calendar_conversion_is_utc_and_handles_leap_days() {
        assert_eq!(iso_date(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso_date(1709164800), "2024-02-29T00:00:00Z");
    }
}
