//! Event types for analytics.
//!
//! Mirrors the C++ analytics event types that are recorded via `StructuredTraceLog`.
//! Events are serializable so they can be written to trace log files and aggregated.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The kind of analytics event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// A read I/O operation.
    Read,
    /// A write I/O operation.
    Write,
    /// A metadata operation (stat, mkdir, readdir, etc.).
    Metadata,
    /// A client-side cache event (hit, miss, eviction).
    Cache,
    /// A network RPC event.
    Rpc,
    /// A storage chunk operation.
    ChunkOp,
    /// A custom / user-defined event kind.
    Custom,
}

/// An I/O event capturing read or write operation details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoEvent {
    /// Whether this is a read or write.
    pub kind: EventKind,
    /// The file or object path involved.
    pub path: String,
    /// Offset within the file (bytes).
    pub offset: u64,
    /// Number of bytes transferred.
    pub length: u64,
    /// Latency in microseconds.
    pub latency_us: u64,
    /// Whether the operation succeeded.
    pub success: bool,
    /// Optional error message if the operation failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl IoEvent {
    /// Create a new successful read event.
    pub fn read(path: impl Into<String>, offset: u64, length: u64, latency_us: u64) -> Self {
        Self {
            kind: EventKind::Read,
            path: path.into(),
            offset,
            length,
            latency_us,
            success: true,
            error: None,
        }
    }

    /// Create a new successful write event.
    pub fn write(path: impl Into<String>, offset: u64, length: u64, latency_us: u64) -> Self {
        Self {
            kind: EventKind::Write,
            path: path.into(),
            offset,
            length,
            latency_us,
            success: true,
            error: None,
        }
    }

    /// Mark this event as failed with the given error message.
    pub fn with_error(mut self, err: impl Into<String>) -> Self {
        self.success = false;
        self.error = Some(err.into());
        self
    }
}

/// A metadata operation event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataEvent {
    /// The type of metadata operation.
    pub op: String,
    /// The path involved.
    pub path: String,
    /// Latency in microseconds.
    pub latency_us: u64,
    /// Whether the operation succeeded.
    pub success: bool,
    /// Optional error message if the operation failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl MetadataEvent {
    /// Create a new successful metadata event.
    pub fn new(op: impl Into<String>, path: impl Into<String>, latency_us: u64) -> Self {
        Self {
            op: op.into(),
            path: path.into(),
            latency_us,
            success: true,
            error: None,
        }
    }

    /// Mark this event as failed with the given error message.
    pub fn with_error(mut self, err: impl Into<String>) -> Self {
        self.success = false;
        self.error = Some(err.into());
        self
    }
}

/// A generic analytics event that wraps the trace metadata and a typed payload.
///
/// Corresponds to the C++ `StructuredTrace` struct which pairs a `TraceMeta`
/// (timestamp + hostname) with the actual event data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsEvent<T: Serialize> {
    /// Timestamp of when the event occurred.
    pub timestamp: DateTime<Utc>,
    /// Hostname of the machine that generated the event.
    pub hostname: String,
    /// The event payload.
    pub data: T,
}

impl<T: Serialize> AnalyticsEvent<T> {
    /// Create a new analytics event with the current timestamp.
    pub fn new(hostname: impl Into<String>, data: T) -> Self {
        Self {
            timestamp: Utc::now(),
            hostname: hostname.into(),
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_event_read() {
        let event = IoEvent::read("/data/file.bin", 0, 4096, 120);
        assert_eq!(event.kind, EventKind::Read);
        assert!(event.success);
        assert!(event.error.is_none());

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"kind\":\"read\""));
        assert!(json.contains("\"offset\":0"));
    }

    #[test]
    fn test_io_event_write_with_error() {
        let event = IoEvent::write("/data/file.bin", 1024, 512, 500)
            .with_error("disk full");
        assert_eq!(event.kind, EventKind::Write);
        assert!(!event.success);
        assert_eq!(event.error.as_deref(), Some("disk full"));
    }

    #[test]
    fn test_metadata_event() {
        let event = MetadataEvent::new("stat", "/data/dir", 50);
        assert!(event.success);

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"op\":\"stat\""));
    }

    #[test]
    fn test_analytics_event_wrapper() {
        let io = IoEvent::read("/test", 0, 100, 10);
        let event = AnalyticsEvent::new("node-01", io);
        assert_eq!(event.hostname, "node-01");

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"hostname\":\"node-01\""));
        assert!(json.contains("\"timestamp\""));
    }

    #[test]
    fn test_event_kind_serde_roundtrip() {
        let kinds = vec![
            EventKind::Read,
            EventKind::Write,
            EventKind::Metadata,
            EventKind::Cache,
            EventKind::Rpc,
            EventKind::ChunkOp,
            EventKind::Custom,
        ];

        for kind in kinds {
            let json = serde_json::to_string(&kind).unwrap();
            let back: EventKind = serde_json::from_str(&json).unwrap();
            assert_eq!(kind, back);
        }
    }
}
