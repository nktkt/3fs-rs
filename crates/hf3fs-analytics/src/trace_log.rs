//! Structured trace log for analytics events.
//!
//! Mirrors the C++ `StructuredTraceLog<T>` which writes structured events to
//! rotating files. In the C++ version, events are written to Parquet files via
//! `SerdeObjectWriter`. In this Rust version, we write JSON-lines files which
//! are simpler and equally suitable for analytics pipelines.
//!
//! The trace log:
//! - Wraps each event with a `TraceMeta` (timestamp + hostname).
//! - Buffers events and writes them to files in a configurable directory.
//! - Rotates files based on a configurable dump interval.
//! - Can be enabled/disabled at runtime.

use chrono::Utc;
use parking_lot::Mutex;
use serde::Serialize;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Configuration for a `StructuredTraceLog`.
///
/// Corresponds to the C++ `StructuredTraceLog::Config`.
#[derive(Debug, Clone)]
pub struct TraceLogConfig {
    /// Directory where trace files are written.
    pub trace_file_dir: PathBuf,
    /// Whether the trace log is enabled.
    pub enabled: bool,
    /// How often to rotate / flush trace files.
    pub dump_interval: Duration,
    /// Maximum number of events per file before rotation.
    pub max_events_per_file: usize,
}

impl Default for TraceLogConfig {
    fn default() -> Self {
        Self {
            trace_file_dir: PathBuf::from("."),
            enabled: cfg!(not(debug_assertions)),
            dump_interval: if cfg!(debug_assertions) {
                Duration::from_secs(3600)
            } else {
                Duration::from_secs(30)
            },
            max_events_per_file: 100_000,
        }
    }
}

/// Metadata prepended to each structured trace entry.
#[derive(Debug, Clone, Serialize)]
struct TraceMeta {
    timestamp: i64,
    hostname: String,
}

/// A structured trace entry: metadata + typed event payload.
#[derive(Debug, Clone, Serialize)]
struct StructuredTrace<T: Serialize> {
    trace_meta: TraceMeta,
    #[serde(flatten)]
    data: T,
}

/// Inner writer state, protected by a mutex.
struct TraceWriter {
    /// Current file writer, if open.
    writer: Option<BufWriter<fs::File>>,
    /// Path of the current file.
    current_path: Option<PathBuf>,
    /// Number of events written to the current file.
    events_in_file: usize,
}

/// A structured trace log that writes typed events to JSON-lines files.
///
/// Corresponds to the C++ `StructuredTraceLog<SerdeType>`.
///
/// # Type Parameters
///
/// - `T`: The event type. Must implement `Serialize + Send + 'static`.
pub struct StructuredTraceLog<T: Serialize + Send + 'static> {
    config: TraceLogConfig,
    enabled: AtomicBool,
    hostname: String,
    type_name: &'static str,
    writer: Mutex<TraceWriter>,
    total_events: AtomicU64,
    file_index: AtomicU64,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Serialize + Send + 'static> StructuredTraceLog<T> {
    /// Create a new structured trace log.
    pub fn new(config: TraceLogConfig, type_name: &'static str) -> Self {
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "unknown_host".to_string());

        let enabled = config.enabled;

        Self {
            config,
            enabled: AtomicBool::new(enabled),
            hostname,
            type_name,
            writer: Mutex::new(TraceWriter {
                writer: None,
                current_path: None,
                events_in_file: 0,
            }),
            total_events: AtomicU64::new(0),
            file_index: AtomicU64::new(1),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Open the trace log for writing. Creates the initial file.
    pub fn open(&self) -> bool {
        if !self.enabled.load(Ordering::Relaxed) {
            return false;
        }
        let mut guard = self.writer.lock();
        self.create_new_writer(&mut guard)
    }

    /// Append an event to the trace log.
    ///
    /// If the trace log is disabled, this is a no-op.
    pub fn append(&self, event: &T)
    where
        T: Serialize,
    {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        let trace = StructuredTrace {
            trace_meta: TraceMeta {
                timestamp: Utc::now().timestamp(),
                hostname: self.hostname.clone(),
            },
            data: event,
        };

        let mut guard = self.writer.lock();

        // Ensure we have a writer.
        if guard.writer.is_none() && !self.create_new_writer(&mut guard) {
            tracing::error!(
                type_name = self.type_name,
                dir = %self.config.trace_file_dir.display(),
                "Cannot get a writer for trace log"
            );
            self.enabled.store(false, Ordering::Relaxed);
            return;
        }

        if let Some(ref mut writer) = guard.writer {
            match serde_json::to_string(&trace) {
                Ok(json) => {
                    if let Err(e) = writeln!(writer, "{}", json) {
                        tracing::error!(
                            error = %e,
                            type_name = self.type_name,
                            "Failed to write trace log entry"
                        );
                        self.enabled.store(false, Ordering::Relaxed);
                        return;
                    }
                    guard.events_in_file += 1;
                    self.total_events.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        type_name = self.type_name,
                        "Failed to serialize trace log entry"
                    );
                }
            }

            // Rotate if we hit the per-file event limit.
            if guard.events_in_file >= self.config.max_events_per_file {
                drop(guard.writer.take());
                self.create_new_writer(&mut guard);
            }
        }
    }

    /// Flush the current file writer and optionally rotate.
    pub fn flush(&self) {
        let mut guard = self.writer.lock();
        if let Some(ref mut writer) = guard.writer {
            let _ = writer.flush();
        }

        tracing::info!(
            type_name = self.type_name,
            dir = %self.config.trace_file_dir.display(),
            events_in_file = guard.events_in_file,
            "Flushed trace log"
        );

        // Rotate to a new file.
        drop(guard.writer.take());
        self.create_new_writer(&mut guard);
    }

    /// Close the trace log. Flushes and closes the current file.
    pub fn close(&self) {
        self.enabled.store(false, Ordering::Relaxed);
        let mut guard = self.writer.lock();
        if let Some(ref mut writer) = guard.writer {
            let _ = writer.flush();
        }
        guard.writer = None;
        guard.current_path = None;
        tracing::info!(
            type_name = self.type_name,
            dir = %self.config.trace_file_dir.display(),
            "Closed trace log"
        );
    }

    /// Enable or disable the trace log.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
        tracing::info!(
            enabled,
            type_name = self.type_name,
            dir = %self.config.trace_file_dir.display(),
            "Trace log enabled state changed"
        );
    }

    /// Return whether the trace log is currently enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Return the total number of events written.
    pub fn total_events(&self) -> u64 {
        self.total_events.load(Ordering::Relaxed)
    }

    /// Create a new writer, rotating away from the current file.
    fn create_new_writer(&self, guard: &mut TraceWriter) -> bool {
        let now = Utc::now();
        let date_str = now.format("%Y-%m-%d").to_string();
        let time_str = now.format("%Y-%m-%d-%H-%M-%S").to_string();
        let index = self.file_index.fetch_add(1, Ordering::Relaxed);

        let dir = self
            .config
            .trace_file_dir
            .join(&date_str)
            .join(&self.hostname);

        if let Err(e) = fs::create_dir_all(&dir) {
            tracing::error!(
                error = %e,
                dir = %dir.display(),
                "Failed to create trace log directory"
            );
            return false;
        }

        let filename = format!(
            "{}.{}.{}.{}.jsonl",
            self.type_name, self.hostname, time_str, index
        );
        let path = dir.join(&filename);

        match fs::File::create(&path) {
            Ok(file) => {
                tracing::info!(
                    type_name = self.type_name,
                    path = %path.display(),
                    "Opening trace log file"
                );
                guard.writer = Some(BufWriter::new(file));
                guard.current_path = Some(path);
                guard.events_in_file = 0;
                true
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    path = %path.display(),
                    "Failed to create trace log file"
                );
                false
            }
        }
    }
}

impl<T: Serialize + Send + 'static> Drop for StructuredTraceLog<T> {
    fn drop(&mut self) {
        // Flush on drop.
        let guard = self.writer.get_mut();
        if let Some(ref mut writer) = guard.writer {
            let _ = writer.flush();
        }
    }
}

/// Helper function to get the system hostname.
mod hostname {
    use std::ffi::OsString;

    pub fn get() -> Result<OsString, std::io::Error> {
        #[cfg(unix)]
        {
            let mut buf = vec![0u8; 256];
            let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
            if ret != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            buf.truncate(end);
            Ok(OsString::from(String::from_utf8_lossy(&buf).into_owned()))
        }
        #[cfg(not(unix))]
        {
            Ok(OsString::from("unknown"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::IoEvent;
    use std::path::Path;

    #[test]
    fn test_trace_log_config_default() {
        let config = TraceLogConfig::default();
        assert_eq!(config.max_events_per_file, 100_000);
        assert_eq!(config.trace_file_dir, Path::new("."));
    }

    #[test]
    fn test_trace_log_disabled() {
        let config = TraceLogConfig {
            enabled: false,
            trace_file_dir: PathBuf::from("/tmp/hf3fs-test-trace-disabled"),
            ..Default::default()
        };
        let log = StructuredTraceLog::<IoEvent>::new(config, "IoEvent");
        assert!(!log.is_enabled());

        // Append should be a no-op.
        let event = IoEvent::read("/test", 0, 100, 10);
        log.append(&event);
        assert_eq!(log.total_events(), 0);
    }

    #[test]
    fn test_trace_log_write_and_flush() {
        let dir = std::env::temp_dir().join("hf3fs-test-trace-log");
        let _ = fs::remove_dir_all(&dir);

        let config = TraceLogConfig {
            enabled: true,
            trace_file_dir: dir.clone(),
            max_events_per_file: 1000,
            dump_interval: Duration::from_secs(300),
        };

        let log = StructuredTraceLog::<IoEvent>::new(config, "IoEvent");
        assert!(log.open());

        for i in 0..5 {
            let event = IoEvent::read(format!("/file-{}", i), i as u64 * 4096, 4096, 100);
            log.append(&event);
        }

        assert_eq!(log.total_events(), 5);

        log.close();

        // Verify files were created.
        let mut found_jsonl = false;
        for entry in walkdir(&dir) {
            if entry.extension().map(|e| e == "jsonl").unwrap_or(false) {
                found_jsonl = true;
                let content = fs::read_to_string(&entry).unwrap();
                let lines: Vec<&str> = content.lines().collect();
                assert_eq!(lines.len(), 5);
                // Each line should be valid JSON.
                for line in &lines {
                    let _: serde_json::Value = serde_json::from_str(line).unwrap();
                }
            }
        }
        assert!(found_jsonl, "Expected to find .jsonl file in {}", dir.display());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_trace_log_rotation() {
        let dir = std::env::temp_dir().join("hf3fs-test-trace-rotation");
        let _ = fs::remove_dir_all(&dir);

        let config = TraceLogConfig {
            enabled: true,
            trace_file_dir: dir.clone(),
            max_events_per_file: 3,
            dump_interval: Duration::from_secs(300),
        };

        let log = StructuredTraceLog::<IoEvent>::new(config, "IoEvent");
        assert!(log.open());

        // Write 7 events. With max 3 per file, we should get at least 2 files.
        for i in 0..7 {
            let event = IoEvent::read(format!("/file-{}", i), 0, 100, 10);
            log.append(&event);
        }

        assert_eq!(log.total_events(), 7);
        log.close();

        let jsonl_files: Vec<_> = walkdir(&dir)
            .into_iter()
            .filter(|p| p.extension().map(|e| e == "jsonl").unwrap_or(false))
            .collect();

        assert!(jsonl_files.len() >= 2, "Expected at least 2 rotated files, got {}", jsonl_files.len());

        let _ = fs::remove_dir_all(&dir);
    }

    /// Simple recursive directory walker for tests.
    fn walkdir(dir: &Path) -> Vec<PathBuf> {
        let mut results = Vec::new();
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    results.extend(walkdir(&path));
                } else {
                    results.push(path);
                }
            }
        }
        results
    }
}
