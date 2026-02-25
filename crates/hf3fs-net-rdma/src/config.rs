//! RDMA configuration types.
//!
//! Corresponds to the C++ `IBConfig`, `IBSocket::Config`, and `IBConnectConfig`.

use serde::{Deserialize, Serialize};

/// Configuration for an RDMA connection.
///
/// Mirrors the C++ `IBConnectConfig` and `IBSocket::Config` structures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdmaConfig {
    /// Service level (InfiniBand SL).
    #[serde(default)]
    pub sl: u8,

    /// Traffic class for RoCE.
    #[serde(default)]
    pub traffic_class: u8,

    /// P-Key index.
    #[serde(default)]
    pub pkey_index: u16,

    /// Starting packet sequence number.
    #[serde(default)]
    pub start_psn: u32,

    /// Minimum RNR (Receiver Not Ready) retry timer.
    #[serde(default = "default_min_rnr_timer")]
    pub min_rnr_timer: u8,

    /// QP timeout exponent. The actual timeout is `4.096us * 2^timeout`.
    #[serde(default = "default_timeout")]
    pub timeout: u8,

    /// Number of transport-level retries.
    #[serde(default = "default_retry_cnt")]
    pub retry_cnt: u8,

    /// Number of RNR retries.
    #[serde(default)]
    pub rnr_retry: u8,

    /// Maximum number of scatter/gather elements per WR.
    #[serde(default = "default_max_sge")]
    pub max_sge: u32,

    /// Maximum number of outstanding RDMA work requests.
    #[serde(default = "default_max_rdma_wr")]
    pub max_rdma_wr: u32,

    /// Maximum RDMA WRs per ibv_post_send call.
    #[serde(default = "default_max_rdma_wr_per_post")]
    pub max_rdma_wr_per_post: u32,

    /// Maximum number of outstanding atomic/RDMA read operations.
    #[serde(default = "default_max_rd_atomic")]
    pub max_rd_atomic: u32,

    /// Size of each send/receive buffer in bytes.
    #[serde(default = "default_buf_size")]
    pub buf_size: u32,

    /// Number of send buffers.
    #[serde(default = "default_send_buf_cnt")]
    pub send_buf_cnt: u32,

    /// Number of buffers to acknowledge in a single batch.
    #[serde(default = "default_buf_ack_batch")]
    pub buf_ack_batch: u32,

    /// Number of send operations between signaled completions.
    #[serde(default = "default_buf_signal_batch")]
    pub buf_signal_batch: u32,

    /// Number of CQ events to acknowledge in a single batch.
    #[serde(default = "default_event_ack_batch")]
    pub event_ack_batch: u32,

    /// Whether to record per-peer latency metrics.
    #[serde(default)]
    pub record_latency_per_peer: bool,

    /// Whether to skip inactive IB ports during device initialization.
    #[serde(default = "default_true")]
    pub skip_inactive_ports: bool,

    /// Whether to skip unusable IB devices.
    #[serde(default = "default_true")]
    pub skip_unusable_device: bool,

    /// Whether to allow starting without any usable RDMA devices.
    #[serde(default)]
    pub allow_no_usable_devices: bool,

    /// Whether to prefer InfiniBand devices over RoCE.
    #[serde(default = "default_true")]
    pub prefer_ib_device: bool,

    /// Device name filter (empty = accept all).
    #[serde(default)]
    pub device_filter: Vec<String>,

    /// Drain timeout in seconds when closing connections.
    #[serde(default = "default_drain_timeout_secs")]
    pub drain_timeout_secs: u64,
}

fn default_min_rnr_timer() -> u8 {
    1
}
fn default_timeout() -> u8 {
    14
}
fn default_retry_cnt() -> u8 {
    7
}
fn default_max_sge() -> u32 {
    16
}
fn default_max_rdma_wr() -> u32 {
    128
}
fn default_max_rdma_wr_per_post() -> u32 {
    32
}
fn default_max_rd_atomic() -> u32 {
    16
}
fn default_buf_size() -> u32 {
    16 * 1024
}
fn default_send_buf_cnt() -> u32 {
    32
}
fn default_buf_ack_batch() -> u32 {
    8
}
fn default_buf_signal_batch() -> u32 {
    8
}
fn default_event_ack_batch() -> u32 {
    128
}
fn default_true() -> bool {
    true
}
fn default_drain_timeout_secs() -> u64 {
    5
}

impl Default for RdmaConfig {
    fn default() -> Self {
        Self {
            sl: 0,
            traffic_class: 0,
            pkey_index: 0,
            start_psn: 0,
            min_rnr_timer: default_min_rnr_timer(),
            timeout: default_timeout(),
            retry_cnt: default_retry_cnt(),
            rnr_retry: 0,
            max_sge: default_max_sge(),
            max_rdma_wr: default_max_rdma_wr(),
            max_rdma_wr_per_post: default_max_rdma_wr_per_post(),
            max_rd_atomic: default_max_rd_atomic(),
            buf_size: default_buf_size(),
            send_buf_cnt: default_send_buf_cnt(),
            buf_ack_batch: default_buf_ack_batch(),
            buf_signal_batch: default_buf_signal_batch(),
            event_ack_batch: default_event_ack_batch(),
            record_latency_per_peer: false,
            skip_inactive_ports: true,
            skip_unusable_device: true,
            allow_no_usable_devices: false,
            prefer_ib_device: true,
            device_filter: Vec::new(),
            drain_timeout_secs: default_drain_timeout_secs(),
        }
    }
}

impl RdmaConfig {
    /// Calculate the number of QP ACK buffers needed.
    ///
    /// Corresponds to the C++ `IBConnectConfig::qpAckBufs()`.
    pub fn qp_ack_bufs(&self) -> u32 {
        (self.send_buf_cnt + self.buf_ack_batch - 1) / self.buf_ack_batch + 4
    }

    /// Calculate the maximum number of send WRs for the QP.
    ///
    /// Corresponds to the C++ `IBConnectConfig::qpMaxSendWR()`.
    pub fn qp_max_send_wr(&self) -> u32 {
        self.send_buf_cnt
            + self.qp_ack_bufs()
            + self.max_rdma_wr
            + 1 // close message
            + 1 // check message
    }

    /// Calculate the maximum number of receive WRs for the QP.
    ///
    /// Corresponds to the C++ `IBConnectConfig::qpMaxRecvWR()`.
    pub fn qp_max_recv_wr(&self) -> u32 {
        self.send_buf_cnt + self.qp_ack_bufs() + 1 // close message
    }

    /// Calculate the maximum number of CQ entries needed.
    ///
    /// Corresponds to the C++ `IBConnectConfig::qpMaxCQEntries()`.
    pub fn qp_max_cq_entries(&self) -> u32 {
        self.qp_max_send_wr() + self.qp_max_recv_wr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RdmaConfig::default();
        assert_eq!(config.timeout, 14);
        assert_eq!(config.retry_cnt, 7);
        assert_eq!(config.buf_size, 16384);
        assert_eq!(config.send_buf_cnt, 32);
        assert!(config.prefer_ib_device);
    }

    #[test]
    fn test_qp_calculations() {
        let config = RdmaConfig::default();

        let ack_bufs = config.qp_ack_bufs();
        assert_eq!(ack_bufs, (32 + 8 - 1) / 8 + 4); // (32+7)/8 + 4 = 4+4 = 8

        let max_send = config.qp_max_send_wr();
        // 32 + 8 + 128 + 1 + 1 = 170
        assert_eq!(max_send, 32 + ack_bufs + 128 + 2);

        let max_recv = config.qp_max_recv_wr();
        // 32 + 8 + 1 = 41
        assert_eq!(max_recv, 32 + ack_bufs + 1);

        let max_cq = config.qp_max_cq_entries();
        assert_eq!(max_cq, max_send + max_recv);
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = RdmaConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let back: RdmaConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.timeout, config.timeout);
        assert_eq!(back.buf_size, config.buf_size);
    }
}
