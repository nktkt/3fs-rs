//! FUSE filesystem configuration.
//!
//! Mirrors the C++ `FuseConfig` from `src/fuse/FuseConfig.h`.
//! Uses `#[derive(Config)]` from hf3fs-config for hot-reload support.

use hf3fs_config::Config;
use serde::{Deserialize, Serialize};

/// Configuration for periodic sync of dirty inodes.
#[derive(Clone, Debug, Config, Serialize, Deserialize)]
pub struct PeriodicSyncConfig {
    /// Whether periodic sync is enabled.
    #[config(default = true, hot_updated)]
    pub enable: bool,

    /// Interval between periodic sync scans, in seconds.
    #[config(default = 30, min = 1, max = 3600, hot_updated)]
    pub interval_secs: u64,

    /// Maximum number of dirty inodes to sync per scan.
    #[config(default = 1000, min = 1, max = 100000, hot_updated)]
    pub limit: u32,

    /// Whether to flush write buffers during periodic sync.
    #[config(default = true, hot_updated)]
    pub flush_write_buf: bool,

    /// Number of coroutines for the periodic sync worker pool.
    #[config(default = 4, min = 1, max = 64)]
    pub coroutines_num: u32,
}

/// Configuration for I/O buffer sizes.
#[derive(Clone, Debug, Config, Serialize, Deserialize)]
pub struct IoBufsConfig {
    /// Maximum buffer size for I/O operations.
    #[config(default = 1048576, min = 4096)]
    pub max_buf_size: u64,

    /// Maximum readahead buffer size.
    #[config(default = 262144, min = 4096)]
    pub max_readahead: u64,

    /// Write buffer size (0 to disable write buffering).
    #[config(default = 1048576)]
    pub write_buf_size: u64,
}

/// Configuration for I/O job queue sizes (high/low priority).
#[derive(Clone, Debug, Config, Serialize, Deserialize)]
pub struct IoJobQueueSizesConfig {
    /// High priority job queue size.
    #[config(default = 32, min = 1)]
    pub hi: u32,

    /// Low priority job queue size.
    #[config(default = 4096, min = 1)]
    pub lo: u32,
}

/// Configuration for I/O worker coroutine counts per priority.
#[derive(Clone, Debug, Config, Serialize, Deserialize)]
pub struct IoWorkerCorosConfig {
    /// Number of high-priority I/O worker coroutines.
    #[config(default = 8, min = 1, max = 256, hot_updated)]
    pub hi: u32,

    /// Number of low-priority I/O worker coroutines.
    #[config(default = 8, min = 1, max = 256, hot_updated)]
    pub lo: u32,
}

/// Main FUSE configuration.
///
/// Corresponds to the C++ `FuseConfig` struct. Fields marked `hot_updated`
/// can be reloaded at runtime without restarting the FUSE daemon.
#[derive(Clone, Debug, Config, Serialize, Deserialize)]
pub struct FuseConfig {
    // ---- Identification ----
    /// Cluster identifier for this mount.
    #[config(default = "")]
    pub cluster_id: String,

    /// Path to the authentication token file.
    #[config(default = "")]
    pub token_file: String,

    /// Filesystem mount point.
    #[config(default = "")]
    pub mountpoint: String,

    /// Whether to pass `-o allow_other` to FUSE.
    #[config(default = true)]
    pub allow_other: bool,

    // ---- Timeouts (seconds) ----
    /// Attribute cache timeout in seconds.
    #[config(default = 30, hot_updated)]
    pub attr_timeout: u64,

    /// Entry (lookup) cache timeout in seconds.
    #[config(default = 30, hot_updated)]
    pub entry_timeout: u64,

    /// Negative lookup cache timeout in seconds.
    #[config(default = 5, hot_updated)]
    pub negative_timeout: u64,

    /// Symlink cache timeout in seconds.
    #[config(default = 5, hot_updated)]
    pub symlink_timeout: u64,

    // ---- Feature toggles ----
    /// Whether to use prioritized I/O scheduling.
    #[config(default = false, hot_updated)]
    pub enable_priority: bool,

    /// Whether to check for FUSE request interrupts.
    #[config(default = false, hot_updated)]
    pub enable_interrupt: bool,

    /// Whether the filesystem is mounted read-only.
    #[config(default = false, hot_updated)]
    pub readonly: bool,

    /// Whether to memset buffers before read operations.
    #[config(default = false, hot_updated)]
    pub memset_before_read: bool,

    /// Whether the kernel read cache is enabled.
    #[config(default = true, hot_updated)]
    pub enable_read_cache: bool,

    /// Whether to pass length hint to fsync (for testing).
    #[config(default = false, hot_updated)]
    pub fsync_length_hint: bool,

    /// Whether fdatasync updates the file length.
    #[config(default = false, hot_updated)]
    pub fdatasync_update_length: bool,

    /// Whether to enable writeback cache mode.
    #[config(default = false)]
    pub enable_writeback_cache: bool,

    /// Whether to flush write buffer when stat is called.
    #[config(default = true, hot_updated)]
    pub flush_on_stat: bool,

    /// Whether to sync metadata when stat is called.
    #[config(default = true, hot_updated)]
    pub sync_on_stat: bool,

    /// Dry-run bench mode for performance testing.
    #[config(default = false, hot_updated)]
    pub dryrun_bench_mode: bool,

    /// Whether to check for recursive rm -rf operations.
    #[config(default = true, hot_updated)]
    pub check_rmrf: bool,

    // ---- Threading ----
    /// Maximum number of idle FUSE threads (-1 = auto).
    #[config(default = -1)]
    pub max_idle_threads: i32,

    /// Maximum number of FUSE threads.
    #[config(default = 256, min = 1)]
    pub max_threads: i32,

    // ---- I/O parameters ----
    /// Maximum readahead in bytes.
    #[config(default = 16777216)]
    pub max_readahead: u32,

    /// Maximum number of background FUSE requests.
    #[config(default = 32, min = 1)]
    pub max_background: u32,

    /// I/O vector size limit in bytes.
    #[config(default = 1048576)]
    pub iov_limit: u64,

    /// Size of the I/O job queue.
    #[config(default = 1024, min = 1)]
    pub io_jobq_size: u32,

    /// Number of batch I/O coroutines.
    #[config(default = 128, min = 1)]
    pub batch_io_coros: u32,

    /// Size of the RDMA buffer pool.
    #[config(default = 1024, min = 1)]
    pub rdma_buf_pool_size: u32,

    /// Time granularity in microseconds.
    #[config(default = 1000000)]
    pub time_granularity_us: u64,

    /// Number of threads for notify_inval operations.
    #[config(default = 32, min = 1)]
    pub notify_inval_threads: u32,

    /// Maximum UID (for user config table sizing).
    #[config(default = 1000000)]
    pub max_uid: u32,

    /// Chunk size limit (0 = unlimited).
    #[config(default = 0, hot_updated)]
    pub chunk_size_limit: u64,

    /// I/O job dequeue timeout in milliseconds.
    #[config(default = 1, hot_updated)]
    pub io_job_deq_timeout_ms: u64,

    /// Submit wait jitter in milliseconds.
    #[config(default = 1, hot_updated)]
    pub submit_wait_jitter_ms: u64,

    /// Maximum number of jobs per I/O ring.
    #[config(default = 32, hot_updated)]
    pub max_jobs_per_ioring: u32,

    /// Optional remount prefix path (e.g., for bind mounts).
    #[config(default = "")]
    pub remount_prefix: String,

    // ---- Sub-configs ----
    /// I/O buffer size configuration.
    #[config(section)]
    pub io_bufs: IoBufsConfig,

    /// I/O job queue sizes per priority.
    #[config(section)]
    pub io_jobq_sizes: IoJobQueueSizesConfig,

    /// I/O worker coroutine counts per priority.
    #[config(section)]
    pub io_worker_coros: IoWorkerCorosConfig,

    /// Periodic sync configuration.
    #[config(section)]
    pub periodic_sync: PeriodicSyncConfig,
}

impl FuseConfig {
    /// Returns the remount prefix if configured, or `None` if empty.
    pub fn remount_prefix_opt(&self) -> Option<&str> {
        if self.remount_prefix.is_empty() {
            None
        } else {
            Some(&self.remount_prefix)
        }
    }

    /// Returns the effective attribute timeout as a `std::time::Duration`.
    pub fn attr_timeout_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.attr_timeout)
    }

    /// Returns the effective entry timeout as a `std::time::Duration`.
    pub fn entry_timeout_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.entry_timeout)
    }

    /// Returns the effective negative lookup timeout as a `std::time::Duration`.
    pub fn negative_timeout_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.negative_timeout)
    }

    /// Returns the effective symlink timeout as a `std::time::Duration`.
    pub fn symlink_timeout_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.symlink_timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_config::Config;

    #[test]
    fn test_fuse_config_defaults() {
        let cfg = FuseConfig::default();
        assert_eq!(cfg.cluster_id, "");
        assert_eq!(cfg.mountpoint, "");
        assert!(cfg.allow_other);
        assert_eq!(cfg.attr_timeout, 30);
        assert_eq!(cfg.entry_timeout, 30);
        assert_eq!(cfg.negative_timeout, 5);
        assert_eq!(cfg.symlink_timeout, 5);
        assert!(!cfg.readonly);
        assert!(!cfg.enable_priority);
        assert!(cfg.enable_read_cache);
        assert!(!cfg.enable_writeback_cache);
        assert_eq!(cfg.max_threads, 256);
        assert_eq!(cfg.max_readahead, 16777216);
        assert_eq!(cfg.max_background, 32);
        assert_eq!(cfg.io_bufs.max_buf_size, 1048576);
        assert_eq!(cfg.io_bufs.write_buf_size, 1048576);
        assert!(cfg.periodic_sync.enable);
        assert_eq!(cfg.periodic_sync.interval_secs, 30);
    }

    #[test]
    fn test_fuse_config_from_toml() {
        let toml_str = r#"
            cluster_id = "test-cluster"
            mountpoint = "/mnt/3fs"
            allow_other = false
            attr_timeout = 60
            readonly = true
            max_threads = 128

            [io_bufs]
            max_buf_size = 2097152

            [periodic_sync]
            enable = false
            interval_secs = 60
        "#;
        let value: toml::Value = toml_str.parse().unwrap();
        let cfg = FuseConfig::from_toml(&value).unwrap();
        assert_eq!(cfg.cluster_id, "test-cluster");
        assert_eq!(cfg.mountpoint, "/mnt/3fs");
        assert!(!cfg.allow_other);
        assert_eq!(cfg.attr_timeout, 60);
        assert!(cfg.readonly);
        assert_eq!(cfg.max_threads, 128);
        assert_eq!(cfg.io_bufs.max_buf_size, 2097152);
        assert!(!cfg.periodic_sync.enable);
        assert_eq!(cfg.periodic_sync.interval_secs, 60);
    }

    #[test]
    fn test_fuse_config_hot_update() {
        let mut cfg = FuseConfig::default();
        assert_eq!(cfg.attr_timeout, 30);
        assert!(!cfg.readonly);

        let toml_str = r#"
            cluster_id = "new-cluster"
            mountpoint = "/mnt/new"
            attr_timeout = 120
            readonly = true
            max_threads = 64
        "#;
        let value: toml::Value = toml_str.parse().unwrap();
        let new_cfg = FuseConfig::from_toml(&value).unwrap();
        cfg.hot_update(&new_cfg);

        // Hot-updated fields change
        assert_eq!(cfg.attr_timeout, 120);
        assert!(cfg.readonly);

        // Non-hot-updated fields stay the same
        assert_eq!(cfg.cluster_id, "");
        assert_eq!(cfg.mountpoint, "");
        assert_eq!(cfg.max_threads, 256);
    }

    #[test]
    fn test_fuse_config_validate() {
        let cfg = FuseConfig::default();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_remount_prefix_opt() {
        let mut cfg = FuseConfig::default();
        assert!(cfg.remount_prefix_opt().is_none());

        cfg.remount_prefix = "/mnt/bind".to_string();
        assert_eq!(cfg.remount_prefix_opt(), Some("/mnt/bind"));
    }
}
