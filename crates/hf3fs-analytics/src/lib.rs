//! Analytics module for hf3fs.
//!
//! Provides structured trace logging for analytics events. Modeled after the C++
//! `StructuredTraceLog` and `SerdeObjectWriter`/`SerdeObjectReader` pattern, but
//! adapted for Rust using serde serialization to JSON-lines files instead of Parquet.
//!
//! The core abstraction is the `AnalyticsCollector` trait, which accumulates events
//! of a parameterized type and periodically flushes them to persistent storage.

pub mod collector;
pub mod event;
pub mod trace_log;

pub use collector::{AnalyticsCollector, InMemoryCollector, NoopCollector};
pub use event::{AnalyticsEvent, EventKind, IoEvent, MetadataEvent};
pub use trace_log::{StructuredTraceLog, TraceLogConfig};
