//! Progress bar and status reporting utilities for long-running CLI operations.
//!
//! Provides a simple progress tracker that can be used by commands that
//! iterate over many items (e.g., scanning a directory tree, checking chunks).

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A thread-safe progress tracker for long-running operations.
///
/// Reports progress to stderr so it does not interfere with structured
/// stdout output.
#[derive(Clone)]
pub struct ProgressBar {
    inner: Arc<ProgressInner>,
}

struct ProgressInner {
    total: u64,
    current: AtomicU64,
    start: Instant,
    message: String,
    finished: AtomicBool,
}

impl ProgressBar {
    /// Create a new progress bar with a known total.
    pub fn new(total: u64, message: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(ProgressInner {
                total,
                current: AtomicU64::new(0),
                start: Instant::now(),
                message: message.into(),
                finished: AtomicBool::new(false),
            }),
        }
    }

    /// Create a progress bar with unknown total (spinner mode).
    pub fn spinner(message: impl Into<String>) -> Self {
        Self::new(0, message)
    }

    /// Increment the current count by one.
    pub fn inc(&self) {
        self.inc_by(1);
    }

    /// Increment the current count by `n`.
    pub fn inc_by(&self, n: u64) {
        self.inner.current.fetch_add(n, Ordering::Relaxed);
    }

    /// Set the current count to an exact value.
    pub fn set_position(&self, pos: u64) {
        self.inner.current.store(pos, Ordering::Relaxed);
    }

    /// Get the current count.
    pub fn position(&self) -> u64 {
        self.inner.current.load(Ordering::Relaxed)
    }

    /// Get the total count (0 means unknown).
    pub fn total(&self) -> u64 {
        self.inner.total
    }

    /// Get elapsed time since creation.
    pub fn elapsed(&self) -> Duration {
        self.inner.start.elapsed()
    }

    /// Mark the progress as finished and print a final summary.
    pub fn finish(&self) {
        if self.inner.finished.swap(true, Ordering::Relaxed) {
            return; // Already finished.
        }

        let current = self.position();
        let elapsed = self.elapsed();
        let rate = if elapsed.as_secs_f64() > 0.0 {
            current as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        let _ = writeln!(
            std::io::stderr(),
            "{}: {}/{} done in {:.1}s ({:.0}/s)",
            self.inner.message,
            current,
            if self.inner.total > 0 {
                self.inner.total.to_string()
            } else {
                "?".to_string()
            },
            elapsed.as_secs_f64(),
            rate,
        );
    }

    /// Print current status to stderr (for periodic updates).
    pub fn print_status(&self) {
        let current = self.position();
        let elapsed = self.elapsed();

        if self.inner.total > 0 {
            let pct = (current as f64 / self.inner.total as f64) * 100.0;
            let _ = eprint!(
                "\r{}: {}/{} ({:.1}%) [{:.1}s]",
                self.inner.message,
                current,
                self.inner.total,
                pct,
                elapsed.as_secs_f64(),
            );
        } else {
            let _ = eprint!(
                "\r{}: {} [{:.1}s]",
                self.inner.message,
                current,
                elapsed.as_secs_f64(),
            );
        }
        let _ = std::io::stderr().flush();
    }

    /// Finish and print a newline to stderr to clear the progress line.
    pub fn finish_and_clear(&self) {
        self.finish();
        let _ = eprint!("\r\x1b[K"); // Clear the line.
        let _ = std::io::stderr().flush();
    }
}

impl Drop for ProgressInner {
    fn drop(&mut self) {
        // No automatic finishing on drop; user must call finish() explicitly.
    }
}

/// A simple elapsed-time reporter for profiling CLI commands.
///
/// Mirrors the C++ `profile` flag behavior in the `Dispatcher::run` method.
pub struct Timer {
    label: String,
    start: Instant,
}

impl Timer {
    /// Start a new timer with the given label.
    pub fn start(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            start: Instant::now(),
        }
    }

    /// Get elapsed duration.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Stop the timer and print the elapsed time to stderr.
    pub fn stop(self) {
        let elapsed = self.start.elapsed();
        eprintln!("> Time {}: {:.3}s", self.label, elapsed.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_bar_basic() {
        let pb = ProgressBar::new(100, "test");
        assert_eq!(pb.position(), 0);
        assert_eq!(pb.total(), 100);

        pb.inc();
        assert_eq!(pb.position(), 1);

        pb.inc_by(9);
        assert_eq!(pb.position(), 10);

        pb.set_position(50);
        assert_eq!(pb.position(), 50);
    }

    #[test]
    fn test_progress_bar_spinner() {
        let pb = ProgressBar::spinner("scanning");
        assert_eq!(pb.total(), 0);
        pb.inc();
        assert_eq!(pb.position(), 1);
    }

    #[test]
    fn test_progress_bar_finish_idempotent() {
        let pb = ProgressBar::new(10, "test");
        pb.set_position(10);
        pb.finish();
        pb.finish(); // Should not panic or double-print.
    }

    #[test]
    fn test_progress_bar_clone() {
        let pb1 = ProgressBar::new(100, "shared");
        let pb2 = pb1.clone();
        pb1.inc();
        assert_eq!(pb2.position(), 1);
    }

    #[test]
    fn test_timer() {
        let timer = Timer::start("test_op");
        let elapsed = timer.elapsed();
        assert!(elapsed.as_nanos() > 0 || elapsed.as_nanos() == 0);
        // Just verify it doesn't panic.
        timer.stop();
    }
}
