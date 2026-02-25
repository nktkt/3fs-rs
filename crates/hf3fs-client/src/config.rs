//! Client configuration types.
//!
//! Mirrors the C++ `MetaClient::Config`, `MgmtdClient::Config`, and
//! `StorageClient::Config` hierarchy.

use std::time::Duration;

use hf3fs_types::{Address, AddressType};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Retry configuration
// ---------------------------------------------------------------------------

/// Retry configuration shared across all sub-clients.
///
/// Corresponds to C++ `RetryConfig` found in both `MetaClient` and
/// `StorageClient`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Timeout for a single RPC call.
    #[serde(with = "humantime_compat")]
    pub rpc_timeout: Duration,

    /// Number of low-level send retries within a single attempt.
    pub retry_send: u32,

    /// Fast retry interval (used for transient errors like `kBusy`).
    #[serde(with = "humantime_compat")]
    pub retry_fast: Duration,

    /// Initial wait time before the first retry.
    #[serde(with = "humantime_compat")]
    pub retry_init_wait: Duration,

    /// Maximum wait time between retries (exponential back-off cap).
    #[serde(with = "humantime_compat")]
    pub retry_max_wait: Duration,

    /// Total time budget for retries before giving up.
    #[serde(with = "humantime_compat")]
    pub retry_total_time: Duration,

    /// Number of consecutive failures before switching to a different server.
    pub max_failures_before_failover: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            rpc_timeout: Duration::from_secs(5),
            retry_send: 1,
            retry_fast: Duration::from_secs(1),
            retry_init_wait: Duration::from_millis(500),
            retry_max_wait: Duration::from_secs(5),
            retry_total_time: Duration::from_secs(60),
            max_failures_before_failover: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// Server selection mode
// ---------------------------------------------------------------------------

/// Strategy for selecting which meta server to send requests to.
///
/// Corresponds to C++ `ServerSelectionMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerSelectionMode {
    /// Round-robin across known servers.
    RoundRobin,
    /// Uniform random selection.
    UniformRandom,
    /// Random selection with affinity (sticky to a server until failure).
    RandomFollow,
}

impl Default for ServerSelectionMode {
    fn default() -> Self {
        Self::RandomFollow
    }
}

// ---------------------------------------------------------------------------
// Meta client config
// ---------------------------------------------------------------------------

/// Configuration for the metadata client.
///
/// Corresponds to C++ `MetaClient::Config`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaClientConfig {
    /// Server selection strategy.
    pub selection_mode: ServerSelectionMode,

    /// Preferred network type when talking to meta servers.
    pub network_type: AddressType,

    /// How often to health-check error-listed servers.
    #[serde(with = "humantime_compat")]
    pub check_server_interval: Duration,

    /// Maximum number of concurrent in-flight RPC requests.
    pub max_concurrent_requests: u32,

    /// Batch size for background chunk removal.
    pub remove_chunks_batch_size: u32,

    /// Whether to use dynamic striping (FUSE client only).
    pub dynamic_stripe: bool,

    /// Default retry configuration.
    pub retry: RetryConfig,
}

impl Default for MetaClientConfig {
    fn default() -> Self {
        Self {
            selection_mode: ServerSelectionMode::default(),
            network_type: AddressType::TCP,
            check_server_interval: Duration::from_secs(5),
            max_concurrent_requests: 128,
            remove_chunks_batch_size: 32,
            dynamic_stripe: false,
            retry: RetryConfig::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Mgmtd client config
// ---------------------------------------------------------------------------

/// Configuration for the management daemon client.
///
/// Corresponds to C++ `MgmtdClient::Config`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MgmtdClientConfig {
    /// Seed addresses of management servers.
    pub mgmtd_server_addresses: Vec<Address>,

    /// Preferred network address type.
    pub network_type: Option<AddressType>,

    /// Whether to automatically refresh routing info in the background.
    pub enable_auto_refresh: bool,

    /// Interval between automatic routing info refreshes.
    #[serde(with = "humantime_compat")]
    pub auto_refresh_interval: Duration,

    /// Whether to automatically send heartbeats.
    pub enable_auto_heartbeat: bool,

    /// Interval between heartbeats.
    #[serde(with = "humantime_compat")]
    pub auto_heartbeat_interval: Duration,

    /// Whether to extend client sessions automatically.
    pub enable_auto_extend_client_session: bool,

    /// Interval for client session extension.
    #[serde(with = "humantime_compat")]
    pub auto_extend_client_session_interval: Duration,

    /// Default retry configuration.
    pub retry: RetryConfig,
}

impl Default for MgmtdClientConfig {
    fn default() -> Self {
        Self {
            mgmtd_server_addresses: Vec::new(),
            network_type: None,
            enable_auto_refresh: true,
            auto_refresh_interval: Duration::from_secs(10),
            enable_auto_heartbeat: true,
            auto_heartbeat_interval: Duration::from_secs(10),
            enable_auto_extend_client_session: true,
            auto_extend_client_session_interval: Duration::from_secs(10),
            retry: RetryConfig::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Storage client config
// ---------------------------------------------------------------------------

/// Concurrency limits for a single operation type.
///
/// Corresponds to C++ `OperationConcurrency`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationConcurrency {
    pub max_batch_size: u32,
    pub max_batch_bytes: u64,
    pub max_concurrent_requests: u32,
    pub max_concurrent_requests_per_server: u32,
    pub random_shuffle_requests: bool,
    pub process_batches_in_parallel: bool,
}

impl Default for OperationConcurrency {
    fn default() -> Self {
        Self {
            max_batch_size: 128,
            max_batch_bytes: 4 * 1024 * 1024,
            max_concurrent_requests: 32,
            max_concurrent_requests_per_server: 8,
            random_shuffle_requests: true,
            process_batches_in_parallel: true,
        }
    }
}

/// Traffic control settings for the storage client.
///
/// Corresponds to C++ `TrafficControlConfig`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficControlConfig {
    pub read: OperationConcurrency,
    pub write: OperationConcurrency,
    pub query: OperationConcurrency,
    pub remove: OperationConcurrency,
    pub truncate: OperationConcurrency,
}

impl Default for TrafficControlConfig {
    fn default() -> Self {
        Self {
            read: OperationConcurrency::default(),
            write: OperationConcurrency::default(),
            query: OperationConcurrency::default(),
            remove: OperationConcurrency::default(),
            truncate: OperationConcurrency::default(),
        }
    }
}

/// Configuration for the storage client.
///
/// Corresponds to C++ `StorageClient::Config`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageClientConfig {
    /// Retry configuration for storage operations.
    pub retry: RetryConfig,

    /// Traffic control configuration.
    pub traffic_control: TrafficControlConfig,

    /// Whether to verify checksums on read.
    pub enable_read_checksum: bool,

    /// Whether to compute checksums on write.
    pub enable_write_checksum: bool,

    /// Maximum number of bytes for inline read (0 = disabled).
    pub max_inline_read_bytes: u64,

    /// Maximum number of bytes for inline write (0 = disabled).
    pub max_inline_write_bytes: u64,
}

impl Default for StorageClientConfig {
    fn default() -> Self {
        Self {
            retry: RetryConfig {
                rpc_timeout: Duration::from_secs(10),
                retry_init_wait: Duration::from_secs(10),
                retry_max_wait: Duration::from_secs(30),
                retry_total_time: Duration::from_secs(60),
                ..RetryConfig::default()
            },
            traffic_control: TrafficControlConfig::default(),
            enable_read_checksum: false,
            enable_write_checksum: true,
            max_inline_read_bytes: 0,
            max_inline_write_bytes: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Top-level client config
// ---------------------------------------------------------------------------

/// Top-level configuration for the 3FS client.
///
/// Aggregates sub-client configurations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    /// Cluster identifier.
    pub cluster_id: String,

    /// Meta client configuration.
    pub meta: MetaClientConfig,

    /// Mgmtd client configuration.
    pub mgmtd: MgmtdClientConfig,

    /// Storage client configuration.
    pub storage: StorageClientConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            cluster_id: String::new(),
            meta: MetaClientConfig::default(),
            mgmtd: MgmtdClientConfig::default(),
            storage: StorageClientConfig::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Duration serialisation helper (seconds-based)
// ---------------------------------------------------------------------------

/// Serde helper that serialises `Duration` as floating-point seconds.
mod humantime_compat {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(duration.as_secs_f64())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = f64::deserialize(deserializer)?;
        Ok(Duration::from_secs_f64(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_client_config() {
        let cfg = ClientConfig::default();
        assert_eq!(cfg.meta.max_concurrent_requests, 128);
        assert_eq!(cfg.meta.retry.rpc_timeout, Duration::from_secs(5));
        assert_eq!(cfg.mgmtd.auto_refresh_interval, Duration::from_secs(10));
        assert_eq!(cfg.storage.retry.retry_total_time, Duration::from_secs(60));
    }

    #[test]
    fn test_retry_config_default() {
        let retry = RetryConfig::default();
        assert_eq!(retry.max_failures_before_failover, 1);
        assert_eq!(retry.retry_send, 1);
    }

    #[test]
    fn test_server_selection_mode_default() {
        assert_eq!(ServerSelectionMode::default(), ServerSelectionMode::RandomFollow);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = ClientConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let parsed: ClientConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.meta.max_concurrent_requests, cfg.meta.max_concurrent_requests);
        assert_eq!(parsed.storage.enable_write_checksum, cfg.storage.enable_write_checksum);
    }

    #[test]
    fn test_operation_concurrency_default() {
        let oc = OperationConcurrency::default();
        assert_eq!(oc.max_batch_size, 128);
        assert_eq!(oc.max_batch_bytes, 4 * 1024 * 1024);
        assert!(oc.random_shuffle_requests);
    }
}
