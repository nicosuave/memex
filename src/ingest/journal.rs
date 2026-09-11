//! macOS keeps a persistent per-volume journal of file-system events. Replaying it from the
//! event id recorded by the last committed refresh names every path that changed under the
//! source roots since then, so a refresh can skip the stat pass over every known file. Any
//! sign that the journal is incomplete (dropped events, a wrapped or purged id range, a
//! different volume) falls back to the full stamped walk.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalCursor {
    pub device_uuid: String,
    pub event_id: u64,
}

/// Cursor to persist with the refresh that captured it, valid for one root/filter fingerprint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalCursorUpdate {
    pub fingerprint: String,
    pub cursor: JournalCursor,
}

#[derive(Debug)]
pub enum Replay {
    /// Every path that may have changed since the previous cursor.
    Changed(HashSet<PathBuf>),
    Unusable(&'static str),
}

#[derive(Debug)]
pub struct JournalReplay {
    /// Captured before any events were read, so the next replay overlaps this one.
    pub next: Option<JournalCursor>,
    pub outcome: Replay,
}

impl JournalReplay {
    fn unusable(next: Option<JournalCursor>, reason: &'static str) -> Self {
        Self {
            next,
            outcome: Replay::Unusable(reason),
        }
    }
}

pub const REPLAY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// A replay normally answers in 6–25 ms. When a write anywhere on the volume is still in
/// flight, fseventsd holds the answer for about 160 ms; past this budget a walk is cheaper.
pub const REPLAY_BUDGET: std::time::Duration = std::time::Duration::from_millis(50);

/// A replay running on its own thread so the refresh can open its checkpoint and refresh
/// memories meanwhile. The next cursor arrives as soon as it is captured, before any event is
/// read, so it can be persisted even when the outcome is abandoned.
pub struct ReplayHandle {
    fingerprint: String,
    streaming: std::sync::mpsc::Receiver<()>,
    next: std::sync::mpsc::Receiver<Option<JournalCursor>>,
    outcome: std::sync::mpsc::Receiver<Replay>,
    started: std::time::Instant,
    cancelled: Arc<AtomicBool>,
}

impl ReplayHandle {
    /// `previous` loads the cursor stored under `fingerprint`; it runs on the replay thread.
    pub fn spawn(
        roots: Vec<PathBuf>,
        fingerprint: String,
        previous: impl FnOnce(&str) -> Option<JournalCursor> + Send + 'static,
    ) -> Self {
        let (streaming_tx, streaming) = std::sync::mpsc::channel();
        let (next_tx, next) = std::sync::mpsc::channel();
        let (outcome_tx, outcome) = std::sync::mpsc::channel();
        let key = fingerprint.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let thread_cancelled = cancelled.clone();
        std::thread::Builder::new()
            .name("memex-journal".into())
            .spawn(move || {
                let previous = previous(&key);
                if thread_cancelled.load(Ordering::Relaxed) {
                    return;
                }
                let replay = replay_with(
                    &roots,
                    previous.as_ref(),
                    REPLAY_TIMEOUT,
                    &thread_cancelled,
                    |next| {
                        let _ = next_tx.send(next.cloned());
                    },
                    || {
                        let _ = streaming_tx.send(());
                    },
                );
                let _ = outcome_tx.send(replay.outcome);
            })
            .expect("spawn journal thread");
        Self {
            fingerprint,
            streaming,
            next,
            outcome,
            started: std::time::Instant::now(),
            cancelled,
        }
    }

    /// Blocks until the stream is registered with fseventsd, or `limit` passes. A write issued
    /// in the few milliseconds before registration makes fseventsd hold the replay for about
    /// 160 ms; writes issued afterwards do not, so callers register first and write after.
    pub fn wait_until_streaming(&self, limit: std::time::Duration) {
        let _ = self.streaming.recv_timeout(limit);
    }

    /// Waits until `budget` after spawning for the outcome. The cursor arrives before any
    /// event is read, so it is available even when the outcome is abandoned.
    pub fn wait(mut self, budget: std::time::Duration) -> (String, JournalReplay) {
        let deadline = self.started + budget;
        // The cursor normally lands well before the budget, but the replay thread reaches it
        // only after opening the checkpoint, so this waits against the same deadline as the
        // outcome rather than blocking on that work.
        let next = self
            .next
            .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
            .unwrap_or(None);
        let outcome = self
            .outcome
            .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
            .unwrap_or(Replay::Unusable("replay budget exceeded"));
        (
            std::mem::take(&mut self.fingerprint),
            JournalReplay { next, outcome },
        )
    }
}

impl Drop for ReplayHandle {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

pub fn replay(
    roots: &[PathBuf],
    previous: Option<&JournalCursor>,
    timeout: std::time::Duration,
) -> JournalReplay {
    replay_with(
        roots,
        previous,
        timeout,
        &AtomicBool::new(false),
        |_| {},
        || {},
    )
}

/// Replay cost grows with every event the volume logged since the cursor, not only those under
/// the roots: about 150 ms per million on an M1 Pro. Beyond this distance a walk is cheaper.
pub const MAX_REPLAY_DISTANCE: u64 = 500_000;

#[cfg(not(target_os = "macos"))]
fn replay_with(
    _roots: &[PathBuf],
    _previous: Option<&JournalCursor>,
    _timeout: std::time::Duration,
    _cancelled: &AtomicBool,
    _captured: impl FnOnce(Option<&JournalCursor>),
    _streaming: impl FnOnce(),
) -> JournalReplay {
    JournalReplay::unusable(None, "unsupported platform")
}

#[cfg(target_os = "macos")]
use fsevents::replay_with;

#[cfg(target_os = "macos")]
mod fsevents {
    use super::{JournalCursor, JournalReplay, Replay};
    use fsevent_sys as fse;
    use fsevent_sys::core_foundation as cf;
    use std::collections::HashSet;
    use std::ffi::{CStr, CString};
    use std::os::raw::{c_char, c_void};
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    type CFUUIDRef = cf::CFRef;

    #[link(name = "CoreServices", kind = "framework")]
    unsafe extern "C" {
        fn FSEventsCopyUUIDForDevice(dev: libc::dev_t) -> CFUUIDRef;
    }

    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFUUIDCreateString(allocator: cf::CFAllocatorRef, uuid: CFUUIDRef) -> cf::CFStringRef;
        fn CFRunLoopRunInMode(
            mode: cf::CFStringRef,
            seconds: cf::CFTimeInterval,
            return_after_source_handled: cf::Boolean,
        ) -> i32;
    }

    const INVALIDATING: fse::FSEventStreamEventFlags = fse::kFSEventStreamEventFlagMustScanSubDirs
        | fse::kFSEventStreamEventFlagUserDropped
        | fse::kFSEventStreamEventFlagKernelDropped
        | fse::kFSEventStreamEventFlagEventIdsWrapped
        | fse::kFSEventStreamEventFlagRootChanged
        | fse::kFSEventStreamEventFlagMount
        | fse::kFSEventStreamEventFlagUnmount;

    struct Collector {
        paths: HashSet<PathBuf>,
        events: usize,
        done: bool,
        unusable: Option<&'static str>,
    }

    extern "C" fn collect(
        _stream: fse::FSEventStreamRef,
        info: *mut c_void,
        count: usize,
        paths: *mut c_void,
        flags: *const fse::FSEventStreamEventFlags,
        _ids: *const fse::FSEventStreamEventId,
    ) {
        // SAFETY: `info` is the `Collector` owned by `replay`, which outlives the stream, and the
        // arrays hold `count` entries for the duration of the callback.
        let collector = unsafe { &mut *(info as *mut Collector) };
        let paths = paths as *const *const c_char;
        for index in 0..count {
            let flags = unsafe { *flags.add(index) };
            collector.events += 1;
            if flags & fse::kFSEventStreamEventFlagHistoryDone != 0 {
                collector.done = true;
                continue;
            }
            if flags & INVALIDATING != 0 {
                collector.unusable = Some("journal incomplete");
                continue;
            }
            let directory_kept = flags & fse::kFSEventStreamEventFlagItemIsDir != 0
                && flags
                    & (fse::kFSEventStreamEventFlagItemRenamed
                        | fse::kFSEventStreamEventFlagItemRemoved)
                    == 0;
            if directory_kept {
                continue;
            }
            let path = unsafe { CStr::from_ptr(*paths.add(index)) };
            let path = String::from_utf8_lossy(path.to_bytes());
            let path = path.trim_end_matches('/');
            if !path.is_empty() {
                collector.paths.insert(PathBuf::from(path));
            }
        }
    }

    fn cf_string(value: &str) -> Option<cf::CFStringRef> {
        let value = CString::new(value).ok()?;
        let string = unsafe {
            cf::CFStringCreateWithCString(
                cf::kCFAllocatorDefault,
                value.as_ptr(),
                cf::kCFStringEncodingUTF8,
            )
        };
        (!string.is_null()).then_some(string)
    }

    fn rust_string(string: cf::CFStringRef) -> Option<String> {
        let mut buffer = vec![0u8; 128];
        let ok = unsafe {
            cf::CFStringGetCString(
                string,
                buffer.as_mut_ptr() as *mut c_char,
                buffer.len() as cf::CFIndex,
                cf::kCFStringEncodingUTF8,
            )
        };
        if !ok {
            return None;
        }
        let end = buffer.iter().position(|byte| *byte == 0)?;
        String::from_utf8(buffer[..end].to_vec()).ok()
    }

    fn device_uuid(device: libc::dev_t) -> Option<String> {
        let uuid = unsafe { FSEventsCopyUUIDForDevice(device) };
        if uuid.is_null() {
            return None;
        }
        let string = unsafe { CFUUIDCreateString(cf::kCFAllocatorDefault, uuid) };
        unsafe { cf::CFRelease(uuid) };
        if string.is_null() {
            return None;
        }
        let value = rust_string(string);
        unsafe { cf::CFRelease(string) };
        value
    }

    pub fn replay_with(
        roots: &[PathBuf],
        previous: Option<&JournalCursor>,
        timeout: Duration,
        cancelled: &AtomicBool,
        captured: impl FnOnce(Option<&JournalCursor>),
        streaming: impl FnOnce(),
    ) -> JournalReplay {
        crate::profiling::span!("journal.replay");
        replay_inner(roots, previous, timeout, cancelled, captured, streaming)
    }

    fn replay_inner(
        roots: &[PathBuf],
        previous: Option<&JournalCursor>,
        timeout: Duration,
        cancelled: &AtomicBool,
        captured: impl FnOnce(Option<&JournalCursor>),
        streaming: impl FnOnce(),
    ) -> JournalReplay {
        let _streaming = Streaming(Some(streaming));
        let mut device = None;
        let mut watched = Vec::new();
        for root in roots {
            let Ok(metadata) = std::fs::metadata(root) else {
                captured(None);
                return JournalReplay::unusable(None, "root vanished");
            };
            match device {
                None => device = Some(metadata.dev()),
                Some(seen) if seen != metadata.dev() => {
                    captured(None);
                    return JournalReplay::unusable(None, "roots span devices");
                }
                Some(_) => {}
            }
            watched.push(root.clone());
        }
        let Some(device) = device else {
            captured(None);
            return JournalReplay::unusable(None, "no roots");
        };
        let Some(device_uuid) = device_uuid(device as libc::dev_t) else {
            captured(None);
            return JournalReplay::unusable(None, "device without journal");
        };
        let event_id = unsafe { fse::FSEventsGetCurrentEventId() };
        let next = Some(JournalCursor {
            device_uuid: device_uuid.clone(),
            event_id,
        });
        captured(next.as_ref());
        let Some(previous) = previous else {
            return JournalReplay::unusable(next, "no cursor");
        };
        if previous.device_uuid != device_uuid {
            return JournalReplay::unusable(next, "device changed");
        }
        if previous.event_id > event_id {
            return JournalReplay::unusable(next, "cursor ahead of journal");
        }
        if event_id - previous.event_id > super::MAX_REPLAY_DISTANCE {
            return JournalReplay::unusable(next, "cursor too far behind");
        }
        if previous.event_id == event_id {
            return JournalReplay {
                next,
                outcome: Replay::Changed(HashSet::new()),
            };
        }

        let mut collector = Collector {
            paths: HashSet::new(),
            events: 0,
            done: false,
            unusable: None,
        };
        let outcome = unsafe {
            run_stream(
                &watched,
                previous.event_id,
                timeout,
                cancelled,
                &mut collector,
                _streaming,
            )
        };
        crate::profiling::count!("journal.events", collector.events);
        JournalReplay {
            next,
            outcome: match outcome {
                Err(reason) => Replay::Unusable(reason),
                Ok(()) => match collector.unusable {
                    Some(reason) => Replay::Unusable(reason),
                    None if collector.done => Replay::Changed(collector.paths),
                    None => Replay::Unusable("replay timed out"),
                },
            },
        }
    }

    /// Fires once, at the latest when dropped, so an early exit never leaves a waiter hanging.
    struct Streaming<F: FnOnce()>(Option<F>);

    impl<F: FnOnce()> Streaming<F> {
        fn fire(&mut self) {
            if let Some(callback) = self.0.take() {
                callback();
            }
        }
    }

    impl<F: FnOnce()> Drop for Streaming<F> {
        fn drop(&mut self) {
            self.fire();
        }
    }

    unsafe fn run_stream(
        roots: &[PathBuf],
        since: fse::FSEventStreamEventId,
        timeout: Duration,
        cancelled: &AtomicBool,
        collector: &mut Collector,
        mut streaming: Streaming<impl FnOnce()>,
    ) -> Result<(), &'static str> {
        let array = unsafe {
            cf::CFArrayCreateMutable(cf::kCFAllocatorDefault, 0, &cf::kCFTypeArrayCallBacks)
        };
        if array.is_null() {
            return Err("array allocation failed");
        }
        for root in roots {
            let Some(string) = root.to_str().and_then(cf_string) else {
                unsafe { cf::CFRelease(array) };
                return Err("root path is not UTF-8");
            };
            unsafe {
                cf::CFArrayAppendValue(array, string);
                cf::CFRelease(string);
            }
        }
        let context = fse::FSEventStreamContext {
            version: 0,
            info: collector as *mut Collector as *mut c_void,
            retain: None,
            release: None,
            copy_description: None,
        };
        let stream = unsafe {
            fse::FSEventStreamCreate(
                cf::kCFAllocatorDefault,
                collect,
                &context,
                array,
                since,
                0.0,
                fse::kFSEventStreamCreateFlagFileEvents | fse::kFSEventStreamCreateFlagNoDefer,
            )
        };
        unsafe { cf::CFRelease(array) };
        if stream.is_null() {
            return Err("stream creation failed");
        }
        let run_loop = unsafe { cf::CFRunLoopGetCurrent() };
        unsafe {
            fse::FSEventStreamScheduleWithRunLoop(stream, run_loop, cf::kCFRunLoopDefaultMode)
        };
        let started = unsafe { fse::FSEventStreamStart(stream) } != 0;
        streaming.fire();
        if started {
            let deadline = Instant::now() + timeout;
            while !collector.done
                && collector.unusable.is_none()
                && !cancelled.load(Ordering::Relaxed)
            {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                unsafe {
                    CFRunLoopRunInMode(
                        cf::kCFRunLoopDefaultMode,
                        remaining.min(Duration::from_millis(5)).as_secs_f64(),
                        1,
                    )
                };
            }
            unsafe { fse::FSEventStreamStop(stream) };
        }
        unsafe {
            fse::FSEventStreamInvalidate(stream);
            fse::FSEventStreamRelease(stream);
        }
        if started {
            Ok(())
        } else {
            Err("stream did not start")
        }
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use std::time::Duration;

    #[test]
    fn abandoned_replay_cancels_before_loading_finishes() {
        let (release, resume) = std::sync::mpsc::channel();
        let handle = ReplayHandle::spawn(Vec::new(), "test".into(), move |_| {
            resume.recv().unwrap();
            None
        });
        let cancelled = Arc::downgrade(&handle.cancelled);
        let streaming = &handle.streaming;
        assert!(matches!(
            streaming.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        drop(handle);
        assert!(cancelled.upgrade().unwrap().load(Ordering::Relaxed));
        release.send(()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while cancelled.upgrade().is_some() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            cancelled.upgrade().is_none(),
            "cancelled replay thread must exit"
        );
    }

    #[test]
    fn blocked_checkpoint_load_does_not_extend_replay_budget() {
        let (release, resume) = std::sync::mpsc::channel();
        let handle = ReplayHandle::spawn(Vec::new(), "test".into(), move |_| {
            resume.recv().unwrap();
            None
        });
        let started = std::time::Instant::now();
        let (_, replay) = handle.wait(Duration::from_millis(10));
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(matches!(
            replay.outcome,
            Replay::Unusable("replay budget exceeded")
        ));
        release.send(()).unwrap();
    }

    fn touch(path: &Path, text: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = fs::File::create(path).unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    /// The journal trails the kernel by a few tens of milliseconds.
    fn settle() {
        std::thread::sleep(std::time::Duration::from_millis(150));
    }

    fn changed(replay: JournalReplay) -> HashSet<PathBuf> {
        match replay.outcome {
            Replay::Changed(paths) => paths,
            Replay::Unusable(reason) => panic!("journal unusable: {reason}"),
        }
    }

    #[test]
    fn a_replay_names_the_files_written_since_the_cursor_and_nothing_older() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap().join("sessions");
        touch(&root.join("old.jsonl"), "1");
        settle();
        let roots = vec![root.clone()];
        let first = replay(&roots, None, REPLAY_TIMEOUT);
        assert!(matches!(first.outcome, Replay::Unusable("no cursor")));
        let cursor = first.next.expect("cursor on a journaled volume");
        touch(&root.join("new.jsonl"), "2");
        touch(&root.join("deep/agent.jsonl"), "3");
        settle();
        let second = replay(&roots, Some(&cursor), REPLAY_TIMEOUT);
        let paths = changed(second);
        assert!(paths.contains(&root.join("new.jsonl")), "{paths:?}");
        assert!(paths.contains(&root.join("deep/agent.jsonl")), "{paths:?}");
        assert!(!paths.contains(&root.join("old.jsonl")), "{paths:?}");
    }

    #[test]
    fn a_foreign_or_future_cursor_is_unusable() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let roots = vec![root];
        let current = replay(&roots, None, REPLAY_TIMEOUT).next.unwrap();
        let foreign = JournalCursor {
            device_uuid: "not-this-volume".into(),
            event_id: current.event_id,
        };
        assert!(matches!(
            replay(&roots, Some(&foreign), REPLAY_TIMEOUT).outcome,
            Replay::Unusable("device changed")
        ));
        let future = JournalCursor {
            event_id: u64::MAX / 2,
            ..current.clone()
        };
        assert!(matches!(
            replay(&roots, Some(&future), REPLAY_TIMEOUT).outcome,
            Replay::Unusable("cursor ahead of journal")
        ));
        let stale = JournalCursor {
            event_id: current.event_id.saturating_sub(MAX_REPLAY_DISTANCE + 1),
            ..current
        };
        assert!(matches!(
            replay(&roots, Some(&stale), REPLAY_TIMEOUT).outcome,
            Replay::Unusable("cursor too far behind")
        ));
        assert!(matches!(
            replay(&[], None, REPLAY_TIMEOUT).outcome,
            Replay::Unusable("no roots")
        ));
    }
}
