//! Configuration for the monitor collector.
//!
//! Corresponds to the C++ `MonitorCollectorService::Config` and
//! `MonitorCollectorServer::Config`.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Configuration for the monitor collector service.
///
/// Controls queue capacity, batching, backend selection, and metric filtering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorCollectorConfig {
    /// Type of the reporting backend: "log", "file", or "remote".
    #[serde(default = "default_reporter_type")]
    pub reporter_type: String,

    /// Number of connection / worker threads for committing samples.
    #[serde(default = "default_conn_threads")]
    pub conn_threads: usize,

    /// Maximum number of sample batches that can be queued.
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,

    /// Number of sample batches to gather before committing.
    #[serde(default = "default_batch_commit_size")]
    pub batch_commit_size: usize,

    /// Metric names that should be dropped and not forwarded.
    #[serde(default)]
    pub blacklisted_metric_names: HashSet<String>,

    /// TCP port for the collector service to listen on.
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,

    /// Path for file-based export (only used when `reporter_type` is "file").
    #[serde(default = "default_export_path")]
    pub export_path: String,
}

fn default_reporter_type() -> String {
    "log".to_string()
}

fn default_conn_threads() -> usize {
    32
}

fn default_queue_capacity() -> usize {
    204_800
}

fn default_batch_commit_size() -> usize {
    4096
}

fn default_listen_port() -> u16 {
    10000
}

fn default_export_path() -> String {
    "./metrics_export".to_string()
}

impl Default for MonitorCollectorConfig {
    fn default() -> Self {
        Self {
            reporter_type: default_reporter_type(),
            conn_threads: default_conn_threads(),
            queue_capacity: default_queue_capacity(),
            batch_commit_size: default_batch_commit_size(),
            blacklisted_metric_names: HashSet::new(),
            listen_port: default_listen_port(),
            export_path: default_export_path(),
        }
    }
}

impl MonitorCollectorConfig {
    /// Returns whether a metric name is blacklisted.
    pub fn is_blacklisted(&self, name: &str) -> bool {
        self.blacklisted_metric_names.contains(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MonitorCollectorConfig::default();
        assert_eq!(config.reporter_type, "log");
        assert_eq!(config.conn_threads, 32);
        assert_eq!(config.queue_capacity, 204_800);
        assert_eq!(config.batch_commit_size, 4096);
        assert_eq!(config.listen_port, 10000);
        assert!(config.blacklisted_metric_names.is_empty());
    }

    #[test]
    fn test_blacklist() {
        let mut config = MonitorCollectorConfig::default();
        config
            .blacklisted_metric_names
            .insert("internal.debug_counter".to_string());

        assert!(config.is_blacklisted("internal.debug_counter"));
        assert!(!config.is_blacklisted("user.request_count"));
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = MonitorCollectorConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let back: MonitorCollectorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.conn_threads, config.conn_threads);
        assert_eq!(back.queue_capacity, config.queue_capacity);
    }
}
