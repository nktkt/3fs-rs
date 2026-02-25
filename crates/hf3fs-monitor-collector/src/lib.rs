//! Monitor collector service for hf3fs.
//!
//! Aggregates metrics samples from multiple nodes and forwards them to a
//! configured reporting backend (e.g., log, file, or a remote metrics store).
//!
//! Modeled after the C++ `MonitorCollectorOperator` / `MonitorCollectorService`
//! / `MonitorCollectorServer` classes. The C++ version uses a MPMC queue with
//! connection threads that batch-commit samples to ClickHouse or a log reporter.
//!
//! This Rust implementation uses tokio channels and async tasks for the same
//! pattern, with pluggable `Reporter` backends from `hf3fs-monitor`.

pub mod aggregator;
pub mod config;
pub mod exporter;

pub use aggregator::MetricsAggregator;
pub use config::MonitorCollectorConfig;
pub use exporter::{FileExporter, Exporter};
