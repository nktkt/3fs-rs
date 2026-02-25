//! Analytics collector trait and implementations.
//!
//! The `AnalyticsCollector` trait abstracts over how analytics events are
//! accumulated and flushed. Implementations include:
//!
//! - `NoopCollector`: Discards all events (useful for benchmarks or when analytics is disabled).
//! - `InMemoryCollector`: Accumulates events in memory for testing and inspection.

use parking_lot::Mutex;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

/// Trait for collecting analytics events of type `T`.
///
/// Implementations decide how to buffer, aggregate, and persist events.
pub trait AnalyticsCollector<T: Serialize + Send>: Send + Sync {
    /// Record a single event.
    fn record(&self, event: T);

    /// Flush any buffered events to their destination.
    ///
    /// Returns the number of events flushed.
    fn flush(&self) -> usize;

    /// Return the total number of events recorded since creation.
    fn total_recorded(&self) -> u64;

    /// Whether this collector is enabled and accepting events.
    fn is_enabled(&self) -> bool;
}

/// A no-op collector that discards all events.
///
/// Useful when analytics is disabled but callers still need to satisfy
/// the `AnalyticsCollector` trait bound.
pub struct NoopCollector {
    count: AtomicU64,
}

impl NoopCollector {
    pub fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
        }
    }
}

impl Default for NoopCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Serialize + Send> AnalyticsCollector<T> for NoopCollector {
    fn record(&self, _event: T) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn flush(&self) -> usize {
        0
    }

    fn total_recorded(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    fn is_enabled(&self) -> bool {
        false
    }
}

/// An in-memory collector that stores events for later inspection.
///
/// Primarily intended for testing. Events are stored in a `Vec` protected
/// by a mutex.
pub struct InMemoryCollector<T> {
    events: Mutex<Vec<T>>,
    total: AtomicU64,
    enabled: bool,
}

impl<T> InMemoryCollector<T> {
    pub fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            total: AtomicU64::new(0),
            enabled: true,
        }
    }

    /// Create a disabled collector (records nothing).
    pub fn disabled() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            total: AtomicU64::new(0),
            enabled: false,
        }
    }

    /// Take all accumulated events, leaving the internal buffer empty.
    pub fn take_events(&self) -> Vec<T> {
        std::mem::take(&mut *self.events.lock())
    }

    /// Return the number of currently buffered events.
    pub fn buffered_count(&self) -> usize {
        self.events.lock().len()
    }
}

impl<T> Default for InMemoryCollector<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Serialize + Send> AnalyticsCollector<T> for InMemoryCollector<T> {
    fn record(&self, event: T) {
        if !self.enabled {
            return;
        }
        self.total.fetch_add(1, Ordering::Relaxed);
        self.events.lock().push(event);
    }

    fn flush(&self) -> usize {
        let events = self.take_events();
        let count = events.len();
        // In a real implementation, events would be written to storage here.
        // For the in-memory collector, flushing just drains the buffer.
        tracing::debug!(count, "InMemoryCollector flushed events");
        count
    }

    fn total_recorded(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::IoEvent;

    #[test]
    fn test_noop_collector() {
        let collector = NoopCollector::new();
        // Use the trait method with an explicit type parameter via a helper reference.
        let c: &dyn AnalyticsCollector<IoEvent> = &collector;
        c.record(IoEvent::read("/test", 0, 100, 10));
        c.record(IoEvent::write("/test", 0, 200, 20));

        assert_eq!(c.total_recorded(), 2);
        assert_eq!(c.flush(), 0);
        assert!(!c.is_enabled());
    }

    #[test]
    fn test_in_memory_collector() {
        let collector = InMemoryCollector::new();
        collector.record(IoEvent::read("/a", 0, 100, 10));
        collector.record(IoEvent::write("/b", 0, 200, 20));
        collector.record(IoEvent::read("/c", 512, 4096, 30));

        assert_eq!(collector.total_recorded(), 3);
        assert_eq!(collector.buffered_count(), 3);
        assert!(collector.is_enabled());

        let events = collector.take_events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].path, "/a");
        assert_eq!(events[1].path, "/b");
        assert_eq!(events[2].path, "/c");

        // After take, buffer is empty.
        assert_eq!(collector.buffered_count(), 0);
    }

    #[test]
    fn test_in_memory_collector_disabled() {
        let collector = InMemoryCollector::<IoEvent>::disabled();
        collector.record(IoEvent::read("/test", 0, 100, 10));

        assert_eq!(collector.total_recorded(), 0);
        assert_eq!(collector.buffered_count(), 0);
        assert!(!collector.is_enabled());
    }

    #[test]
    fn test_in_memory_collector_flush() {
        let collector = InMemoryCollector::new();
        collector.record(IoEvent::read("/a", 0, 100, 10));
        collector.record(IoEvent::read("/b", 0, 200, 20));

        let flushed = collector.flush();
        assert_eq!(flushed, 2);
        assert_eq!(collector.buffered_count(), 0);
    }
}
