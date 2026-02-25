//! Configuration for the trash cleaner.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the trash cleaner service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashCleanerConfig {
    /// Paths to scan for trash directories.
    #[serde(default)]
    pub paths: Vec<PathBuf>,

    /// Scan interval. If zero, scan once and exit.
    #[serde(default = "default_interval")]
    pub interval_secs: u64,

    /// Whether to abort on encountering an error during scanning.
    #[serde(default)]
    pub abort_on_error: bool,

    /// Whether to also clean unknown (unparseable) items.
    #[serde(default)]
    pub clean_unknown: bool,

    /// Log directory for event logs.
    #[serde(default = "default_log_dir")]
    pub log_dir: PathBuf,
}

fn default_interval() -> u64 {
    3600
}

fn default_log_dir() -> PathBuf {
    PathBuf::from("./")
}

impl Default for TrashCleanerConfig {
    fn default() -> Self {
        Self {
            paths: Vec::new(),
            interval_secs: default_interval(),
            abort_on_error: false,
            clean_unknown: false,
            log_dir: default_log_dir(),
        }
    }
}

impl TrashCleanerConfig {
    /// Return the scan interval as a `Duration`, or `None` if the cleaner
    /// should run once and exit.
    pub fn interval(&self) -> Option<Duration> {
        if self.interval_secs == 0 {
            None
        } else {
            Some(Duration::from_secs(self.interval_secs))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TrashCleanerConfig::default();
        assert!(config.paths.is_empty());
        assert_eq!(config.interval_secs, 3600);
        assert!(!config.abort_on_error);
        assert!(!config.clean_unknown);
    }

    #[test]
    fn test_interval_none_when_zero() {
        let mut config = TrashCleanerConfig::default();
        config.interval_secs = 0;
        assert!(config.interval().is_none());
    }

    #[test]
    fn test_interval_some_when_nonzero() {
        let config = TrashCleanerConfig::default();
        assert_eq!(config.interval(), Some(Duration::from_secs(3600)));
    }
}
