use anyhow::{Context, Result, bail};
use serde_json::json;
use std::cell::OnceCell;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

const MAX_THREADS: usize = 64;
const EVENTS_PER_THREAD: usize = 8192;
static ACTIVE: OnceLock<Arc<Capture>> = OnceLock::new();
type SharedBuffer = Arc<Mutex<Buffer>>;
thread_local! {
    static BUFFER: OnceCell<Option<SharedBuffer>> = const { OnceCell::new() };
}

struct Capture {
    start: Instant,
    buffers: Mutex<Vec<SharedBuffer>>,
    refused_threads: AtomicU64,
}

#[derive(Default)]
struct Buffer {
    events: Vec<Event>,
    counters: BTreeMap<&'static str, u64>,
    dropped_events: u64,
}

struct Event {
    name: &'static str,
    start_us: u64,
    duration_us: Option<u64>,
}

pub struct Session {
    output: Option<File>,
}

impl Session {
    pub fn from_env() -> Result<Self> {
        let Some(path) = std::env::var_os("MEMEX_PROFILE") else {
            return Ok(Self { output: None });
        };
        if ACTIVE.get().is_some() {
            bail!("a profiling session is already active");
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let output = options
            .open(&path)
            .context("create MEMEX_PROFILE output (must not already exist)")?;
        ACTIVE
            .set(Arc::new(Capture::new()))
            .map_err(|_| anyhow::anyhow!("a profiling session is already active"))?;
        Ok(Self {
            output: Some(output),
        })
    }

    pub fn finish(self) -> Result<()> {
        let Some(output) = self.output else {
            return Ok(());
        };
        let capture = ACTIVE.get().expect("profiling session initialized");
        let end_us = capture.elapsed_us();
        let document = capture.snapshot(end_us);
        let mut output = BufWriter::new(output);
        serde_json::to_writer(&mut output, &document).context("serialize profiling output")?;
        output.write_all(b"\n")?;
        output.flush().context("flush profiling output")
    }
}

impl Capture {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            buffers: Mutex::new(Vec::new()),
            refused_threads: AtomicU64::new(0),
        }
    }

    fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
    }

    fn register_thread(&self) -> Option<SharedBuffer> {
        let mut buffers = self.buffers.lock().unwrap();
        if buffers.len() == MAX_THREADS {
            self.refused_threads.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let buffer = Arc::new(Mutex::new(Buffer::default()));
        buffers.push(Arc::clone(&buffer));
        Some(buffer)
    }

    fn snapshot(&self, end_us: u64) -> serde_json::Value {
        let mut events = Vec::new();
        let mut threads = Vec::new();
        let mut incomplete_spans = 0;
        for (tid, shared) in self.buffers.lock().unwrap().iter().enumerate() {
            let buffer = shared.lock().unwrap();
            for event in &buffer.events {
                if event.start_us > end_us {
                    continue;
                }
                let incomplete = event.duration_us.is_none();
                incomplete_spans += usize::from(incomplete);
                let available = end_us.saturating_sub(event.start_us);
                events.push(json!({
                    "name": event.name, "cat": "memex", "ph": "X",
                    "pid": 1, "tid": tid, "ts": event.start_us,
                    "dur": event.duration_us.unwrap_or(available).min(available),
                    "args": { "incomplete": incomplete }
                }));
            }
            threads.push(json!({
                "tid": tid, "counters": buffer.counters,
                "dropped_spans": buffer.dropped_events
            }));
        }
        json!({
            "schema_version": 1,
            "displayTimeUnit": "ms",
            "traceEvents": events,
            "capture_duration_us": end_us,
            "limits": { "threads": MAX_THREADS, "spans_per_thread": EVENTS_PER_THREAD },
            "refused_threads": self.refused_threads.load(Ordering::Relaxed),
            "incomplete_spans": incomplete_spans,
            "threads": threads
        })
    }
}

pub(crate) struct Scope {
    record: Option<(SharedBuffer, usize)>,
    _thread_bound: PhantomData<Rc<()>>,
}

impl Scope {
    pub(crate) fn enter(name: &'static str) -> Self {
        let record = ACTIVE.get().and_then(|capture| {
            BUFFER.with(|local| {
                let shared = local.get_or_init(|| capture.register_thread()).as_ref()?;
                let entry = shared.lock().unwrap().enter(name, capture.elapsed_us())?;
                Some((Arc::clone(shared), entry))
            })
        });
        Self {
            record,
            _thread_bound: PhantomData,
        }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        if let Some((shared, entry)) = &self.record {
            let end = ACTIVE.get().expect("active span has capture").elapsed_us();
            shared.lock().unwrap().close(*entry, end);
        }
    }
}

impl Buffer {
    fn enter(&mut self, name: &'static str, start_us: u64) -> Option<usize> {
        if self.events.len() == EVENTS_PER_THREAD {
            self.dropped_events += 1;
            return None;
        }
        let entry = self.events.len();
        self.events.push(Event {
            name,
            start_us,
            duration_us: None,
        });
        Some(entry)
    }

    fn close(&mut self, entry: usize, end_us: u64) {
        let event = &mut self.events[entry];
        event.duration_us = Some(end_us.saturating_sub(event.start_us));
    }
}

pub(crate) fn record_count(name: &'static str, amount: u64) {
    if let Some(capture) = ACTIVE.get() {
        BUFFER.with(|local| {
            if let Some(shared) = local.get_or_init(|| capture.register_thread()) {
                let mut buffer = shared.lock().unwrap();
                let value = buffer.counters.entry(name).or_default();
                *value = value.saturating_add(amount);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_spans_preserve_outer_closes_and_count_drops() {
        let mut buffer = Buffer::default();
        let outer = buffer.enter("outer", 1).unwrap();
        for _ in 1..EVENTS_PER_THREAD {
            let entry = buffer.enter("inner", 2).unwrap();
            buffer.close(entry, 3);
        }
        assert!(buffer.enter("overflow", 4).is_none());
        buffer.close(outer, 5);
        assert_eq!(buffer.events.len(), EVENTS_PER_THREAD);
        assert_eq!(buffer.dropped_events, 1);
        assert_eq!(buffer.events[outer].duration_us, Some(4));
    }

    #[test]
    fn thread_limit_and_incomplete_spans_are_reported() {
        let capture = Capture::new();
        for _ in 0..MAX_THREADS {
            let thread = capture.register_thread().unwrap();
            thread.lock().unwrap().enter("unfinished", 1);
        }
        assert!(capture.register_thread().is_none());
        let snapshot = capture.snapshot(10);
        assert_eq!(snapshot["refused_threads"], 1);
        assert_eq!(snapshot["incomplete_spans"], MAX_THREADS);
        assert_eq!(snapshot["traceEvents"][0]["dur"], 9);
        assert_eq!(snapshot["traceEvents"][0]["args"]["incomplete"], true);
    }
}
