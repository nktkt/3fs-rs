//! Configuration for the metadata service.
//!
//! Based on 3FS/src/meta/base/Config.h

use std::time::Duration;

/// Configuration for the metadata service.
#[derive(Debug, Clone)]
pub struct MetaServiceConfig {
    /// Whether the file system is in read-only mode.
    pub readonly: bool,

    /// Whether to require authentication.
    pub authenticate: bool,

    /// Whether to use GRV (get-read-version) cache.
    pub grv_cache: bool,

    /// Maximum symlink resolution depth (nested symlinks).
    pub max_symlink_depth: usize,

    /// Maximum number of symlinks to follow during path resolution.
    pub max_symlink_count: usize,

    /// Maximum directory depth.
    pub max_directory_depth: usize,

    /// Duration to cache ACL lookups.
    pub acl_cache_time: Duration,

    /// Default limit for list (readdir) operations.
    pub list_default_limit: i32,

    /// Whether to sync on prune session.
    pub sync_on_prune_session: bool,

    /// Maximum chunks to remove per request.
    pub max_remove_chunks_per_request: u32,

    /// Whether to allow stat on deleted inodes (those with nlink=0).
    pub allow_stat_deleted_inodes: bool,

    /// Whether to ignore length hints from clients.
    pub ignore_length_hint: bool,

    /// Time granularity for inode timestamps (in milliseconds).
    pub time_granularity_ms: u64,

    /// Whether to enable dynamic striping.
    pub dynamic_stripe: bool,

    /// Initial dynamic stripe count.
    pub dynamic_stripe_initial: u32,

    /// Dynamic stripe growth factor.
    pub dynamic_stripe_growth: u32,

    /// Concurrency for batch stat operations.
    pub batch_stat_concurrent: u32,

    /// Concurrency for batch stat-by-path operations.
    pub batch_stat_by_path_concurrent: u32,

    /// Maximum batched operations on a single inode.
    pub max_batch_operations: u32,

    /// Whether to use new chunk engine for newly created files.
    pub enable_new_chunk_engine: bool,

    /// Whether to check inode ID uniqueness on allocation.
    pub inode_id_check_unique: bool,

    /// Whether to replace file with new inode on O_TRUNC.
    pub otrunc_replace_file: bool,

    /// Threshold (in bytes) above which O_TRUNC replaces the file inode.
    pub otrunc_replace_file_threshold: u64,

    /// Whether idempotent remove is enabled.
    pub idempotent_remove: bool,

    /// Whether idempotent rename is enabled.
    pub idempotent_rename: bool,

    /// Operation timeout.
    pub operation_timeout: Duration,

    /// Maximum transaction retries.
    pub max_retries: u32,

    /// Initial retry wait time.
    pub retry_init_wait: Duration,

    /// Maximum retry wait time.
    pub retry_max_wait: Duration,

    /// Whether to check owner on recursive remove.
    pub recursive_remove_check_owner: bool,

    /// Number of entries to check for permission in recursive remove.
    pub recursive_remove_perm_check: usize,
}

impl Default for MetaServiceConfig {
    fn default() -> Self {
        Self {
            readonly: false,
            authenticate: false,
            grv_cache: false,
            max_symlink_depth: 4,
            max_symlink_count: 10,
            max_directory_depth: 64,
            acl_cache_time: Duration::from_secs(15),
            list_default_limit: 128,
            sync_on_prune_session: false,
            max_remove_chunks_per_request: 32,
            allow_stat_deleted_inodes: true,
            ignore_length_hint: false,
            time_granularity_ms: 1000,
            dynamic_stripe: false,
            dynamic_stripe_initial: 16,
            dynamic_stripe_growth: 2,
            batch_stat_concurrent: 8,
            batch_stat_by_path_concurrent: 4,
            max_batch_operations: 4096,
            enable_new_chunk_engine: false,
            inode_id_check_unique: true,
            otrunc_replace_file: true,
            otrunc_replace_file_threshold: 1024 * 1024 * 1024, // 1 GB
            idempotent_remove: true,
            idempotent_rename: false,
            operation_timeout: Duration::from_secs(5),
            max_retries: 5,
            retry_init_wait: Duration::from_millis(100),
            retry_max_wait: Duration::from_secs(2),
            recursive_remove_check_owner: true,
            recursive_remove_perm_check: 1024,
        }
    }
}
