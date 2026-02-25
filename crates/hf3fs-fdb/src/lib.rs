//! FoundationDB KV backend for hf3fs.
//!
//! This crate provides an `FdbKvEngine` that implements the `hf3fs_kv::KvEngine`
//! trait backed by FoundationDB. The actual FDB integration requires the
//! `foundationdb-rs` crate and is gated behind the `fdb` feature flag.
//!
//! When the `fdb` feature is not enabled, only the placeholder types are available.

#[cfg(feature = "fdb")]
pub mod fdb_engine;

#[cfg(feature = "fdb")]
pub use fdb_engine::FdbKvEngine;

/// Placeholder configuration for the FoundationDB backend.
///
/// Contains the cluster file path and other connection parameters needed
/// to initialize the FDB client.
#[derive(Debug, Clone)]
pub struct FdbConfig {
    /// Path to the FoundationDB cluster file (e.g., `/etc/foundationdb/fdb.cluster`).
    pub cluster_file: String,
    /// Maximum number of retries for transient transaction failures.
    pub max_retries: u32,
    /// Transaction timeout in milliseconds.
    pub transaction_timeout_ms: u64,
}

impl Default for FdbConfig {
    fn default() -> Self {
        Self {
            cluster_file: "/etc/foundationdb/fdb.cluster".to_string(),
            max_retries: 5,
            transaction_timeout_ms: 5000,
        }
    }
}
