//! Run against a frozen CODEX_HOME and an isolated usage-cache path.
//! The first invocation warms the disk cache; compare subsequent fresh processes.
//! Arguments: CACHE_PATH [--verify] [--refreshes=N] [--no-memo] [--stream] [--all].
//! `--verify` hashes detailed reports and filter/cost variants without printing records.
//! Live/peak bytes count requested Rust allocations; native allocations and allocator
//! retention are excluded. macOS footprint/RSS measurements include the whole process.
//! Allocation counters instrument both variants equally; times are instrumented timings.
//! `--stream` benchmarks daily bucket aggregation. Build the candidate example with
//! `mbx rustc --release --example usage_cache_bench -- --cfg memex_native_usage_visitor`;
//! the baseline build collects points before invoking the same aggregation callback.
#![allow(unexpected_cfgs)] // This example's opt-in cfg works on pre-visitor baseline commits too.

use memex::types::SourceFilter;
use memex::usage::{CostMode, UsageQuery, scan_usage, scan_usage_activity};
use sha2::{Digest, Sha256};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

struct MeasuredAllocator;
// Keep the hot counters together and isolated from unrelated globals. Otherwise
// binary layout can change cache-line contention during parallel blob decoding.
#[repr(align(128))]
struct AllocationCounters {
    live: AtomicUsize,
    peak: AtomicUsize,
}
static ALLOCATIONS: AllocationCounters = AllocationCounters {
    live: AtomicUsize::new(0),
    peak: AtomicUsize::new(0),
};

fn allocated(bytes: usize) {
    let live = ALLOCATIONS.live.fetch_add(bytes, Ordering::Relaxed) + bytes;
    ALLOCATIONS.peak.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for MeasuredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        ALLOCATIONS.live.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(pointer, layout, size) };
        if !replacement.is_null() {
            if size >= layout.size() {
                allocated(size - layout.size());
            } else {
                ALLOCATIONS
                    .live
                    .fetch_sub(layout.size() - size, Ordering::Relaxed);
            }
        }
        replacement
    }
}

#[global_allocator]
static ALLOCATOR: MeasuredAllocator = MeasuredAllocator;

#[cfg(target_os = "macos")]
fn process_memory() -> (u64, u64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage_info_v0>::uninit();
    // The flavor selects exactly the rusage_info_v0 layout allocated above.
    let status = unsafe {
        libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V0,
            usage.as_mut_ptr().cast(),
        )
    };
    assert_eq!(status, 0, "proc_pid_rusage failed");
    let usage = unsafe { usage.assume_init() };
    (usage.ri_resident_size, usage.ri_phys_footprint)
}

fn measure<T>(name: &str, action: impl FnOnce() -> T) -> T {
    ALLOCATIONS
        .peak
        .store(ALLOCATIONS.live.load(Ordering::Relaxed), Ordering::Relaxed);
    let start = Instant::now();
    let result = action();
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let live_bytes = ALLOCATIONS.live.load(Ordering::Relaxed);
    let peak_bytes = ALLOCATIONS.peak.load(Ordering::Relaxed);
    println!(
        "phase={name} elapsed_ms={elapsed_ms:.3} live_bytes={live_bytes} peak_bytes={peak_bytes}"
    );
    #[cfg(target_os = "macos")]
    {
        let (resident_bytes, footprint_bytes) = process_memory();
        println!(
            "memory_phase={name} resident_bytes={resident_bytes} footprint_bytes={footprint_bytes}"
        );
    }
    result
}

fn chart_digest(query: &UsageQuery) -> anyhow::Result<String> {
    if std::env::args().any(|argument| argument == "--stream") {
        return stream_chart(query);
    }
    let (points, partial) = scan_usage_activity(query)?;
    let mut digest = Sha256::new();
    digest.update([u8::from(partial)]);
    for point in &points {
        digest.update(point.source.as_bytes());
        digest.update(point.timestamp_ms.to_le_bytes());
        digest.update(point.total_tokens.to_le_bytes());
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn stream_chart(query: &UsageQuery) -> anyhow::Result<String> {
    let mut buckets = BTreeMap::<u64, u64>::new();
    let mut consume = |point: memex::usage::UsageActivityPoint| {
        let total = buckets.entry(point.timestamp_ms / 86_400_000).or_default();
        *total = total.saturating_add(point.total_tokens);
    };
    #[cfg(memex_native_usage_visitor)]
    let partial = !memex::usage::visit_usage_activity(query, &mut consume)?.is_empty();
    #[cfg(not(memex_native_usage_visitor))]
    let partial = {
        let (points, partial) = scan_usage_activity(query)?;
        for point in points {
            consume(point);
        }
        partial
    };
    let mut digest = Sha256::new();
    digest.update([u8::from(partial)]);
    for (day, total) in buckets {
        digest.update(day.to_le_bytes());
        digest.update(total.to_le_bytes());
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn main() -> anyhow::Result<()> {
    let cache = std::env::args().nth(1).expect("usage-cache path required");
    let mut query = UsageQuery {
        source: Some(SourceFilter::Codex),
        cache_path: Some(cache.into()),
        memo_ttl_ms: 60_000,
        ..UsageQuery::default()
    };
    // `--all` exercises the merged multi-partition path instead of one source.
    if std::env::args().any(|argument| argument == "--all") {
        query.source = None;
    }
    if std::env::args().any(|argument| argument == "--no-memo") {
        query.memo_ttl_ms = 0;
        let digest = measure("no_memo_chart", || chart_digest(&query))?;
        println!("chart_sha256={digest}");
        return Ok(());
    }
    let first_digest = measure("first_chart", || chart_digest(&query))?;
    println!("chart_sha256={first_digest}");
    measure("warm_chart_20", || -> anyhow::Result<()> {
        for _ in 0..20 {
            if std::env::args().any(|argument| argument == "--stream") {
                std::hint::black_box(stream_chart(&query)?);
            } else {
                std::hint::black_box(scan_usage_activity(&query)?);
            }
        }
        Ok(())
    })?;
    measure("report", || -> anyhow::Result<()> {
        let report = scan_usage(&query)?;
        println!(
            "events={} warnings={} report_sha256={:x}",
            report.events,
            report.warnings.len(),
            Sha256::digest(serde_json::to_vec(&report)?)
        );
        Ok(())
    })?;
    query.memo_ttl_ms = 1;
    std::thread::sleep(Duration::from_millis(2));
    let refresh_digest = measure("refresh", || chart_digest(&query))?;
    assert_eq!(refresh_digest, first_digest);
    let refreshes = std::env::args()
        .find_map(|argument| {
            argument
                .strip_prefix("--refreshes=")
                .map(str::parse::<usize>)
        })
        .transpose()?
        .unwrap_or(1);
    if refreshes > 1 {
        for refresh in 2..=refreshes {
            std::thread::sleep(Duration::from_millis(2));
            let digest = measure(&format!("refresh_{refresh}"), || chart_digest(&query))?;
            assert_eq!(digest, first_digest);
        }
    }
    println!(
        "retained_bytes={}",
        ALLOCATIONS.live.load(Ordering::Relaxed)
    );
    if std::env::args().any(|argument| argument == "--verify") {
        query.memo_ttl_ms = 60_000;
        query.include_events = true;
        let report = scan_usage(&query)?;
        println!(
            "details_sha256={:x}",
            Sha256::digest(serde_json::to_vec(&report)?)
        );
        let mut cases = vec![("all", query.clone())];
        for (name, mode) in [
            ("source_cost", CostMode::Source),
            ("reprice", CostMode::Reprice),
        ] {
            let mut priced = query.clone();
            priced.cost_mode = mode;
            cases.push((name, priced));
        }
        let mut reviews = query.clone();
        reviews.include_reviews = true;
        cases.push(("reviews", reviews));
        if let Some(event) = report.details.get(report.details.len() / 2) {
            let mut time = query.clone();
            time.since_ms = Some(event.timestamp_ms);
            cases.push(("time", time));
            if let Some(project) = &event.project {
                let mut project_query = query.clone();
                project_query.project = Some(project.clone());
                cases.push(("project", project_query));
            }
            if let Some(session) = &event.session_id {
                let mut session_query = query.clone();
                session_query.session_keys = Some(
                    [(event.source.to_owned(), session.clone())]
                        .into_iter()
                        .collect(),
                );
                cases.push(("session", session_query));
            }
        }
        drop(report);
        for (case, filtered) in cases {
            let report = scan_usage(&filtered)?;
            println!(
                "verify={case} events={} chart_sha256={} report_sha256={:x}",
                report.events,
                chart_digest(&filtered)?,
                Sha256::digest(serde_json::to_vec(&report)?)
            );
        }
    }
    Ok(())
}
