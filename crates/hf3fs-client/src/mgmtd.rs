//! Management daemon (mgmtd) client.
//!
//! Mirrors C++ `client::MgmtdClient` and `client::ICommonMgmtdClient`.
//! Provides routing info refresh, heartbeat, and cluster management RPCs.

use std::sync::Arc;

use async_trait::async_trait;
use hf3fs_proto::mgmtd::{GetClusterInfoReq, GetClusterInfoRsp, HeartbeatReq, HeartbeatRsp};
use hf3fs_types::{Address, NodeId};
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing;

use crate::config::MgmtdClientConfig;
use crate::error::{ClientError, ClientResult};
use crate::routing::{RoutingInfo, RoutingInfoHandle};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Trait for interacting with the management daemon.
///
/// Corresponds to C++ `ICommonMgmtdClient` + `MgmtdClient`.
#[async_trait]
pub trait MgmtdClient: Send + Sync {
    /// Start background tasks (routing info refresh, heartbeat).
    async fn start(&self) -> ClientResult<()>;

    /// Stop all background tasks.
    async fn stop(&self) -> ClientResult<()>;

    /// Get the current routing info snapshot.
    fn get_routing_info(&self) -> Arc<RoutingInfo>;

    /// Force-refresh routing info from mgmtd.
    async fn refresh_routing_info(&self, force: bool) -> ClientResult<()>;

    /// Send a heartbeat to the management daemon.
    async fn heartbeat(&self, node_id: NodeId, node_type: u8) -> ClientResult<HeartbeatRsp>;

    /// Retrieve cluster information.
    async fn get_cluster_info(&self) -> ClientResult<GetClusterInfoRsp>;

    /// Return the list of known mgmtd server addresses.
    fn mgmtd_addresses(&self) -> Vec<Address>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/// Concrete implementation of `MgmtdClient`.
pub struct MgmtdClientImpl {
    config: MgmtdClientConfig,
    routing: RoutingInfoHandle,
    running: Mutex<bool>,
    stop_notify: Notify,
}

impl MgmtdClientImpl {
    /// Create a new mgmtd client.
    pub fn new(config: MgmtdClientConfig) -> Self {
        Self {
            config,
            routing: RoutingInfoHandle::new(),
            running: Mutex::new(false),
            stop_notify: Notify::new(),
        }
    }

    /// Return a handle to the routing info (for sharing with other clients).
    pub fn routing_handle(&self) -> RoutingInfoHandle {
        self.routing.clone()
    }

    /// Run the periodic refresh loop. Called from `start()`.
    #[allow(dead_code)]
    async fn refresh_loop(&self) {
        let interval = self.config.auto_refresh_interval;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = self.refresh_routing_info(false).await {
                        tracing::warn!("mgmtd routing refresh failed: {}", e);
                    }
                }
                _ = self.stop_notify.notified() => {
                    tracing::info!("mgmtd refresh loop stopping");
                    break;
                }
            }
        }
    }

    /// Run the periodic heartbeat loop.
    #[allow(dead_code)]
    async fn heartbeat_loop(&self, node_id: NodeId, node_type: u8) {
        let interval = self.config.auto_heartbeat_interval;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = self.heartbeat(node_id, node_type).await {
                        tracing::warn!("mgmtd heartbeat failed: {}", e);
                    }
                }
                _ = self.stop_notify.notified() => {
                    tracing::info!("mgmtd heartbeat loop stopping");
                    break;
                }
            }
        }
    }

    /// Try each mgmtd address in order, returning the first success.
    #[allow(dead_code)]
    async fn try_mgmtd_addresses<F, T>(&self, op_name: &str, mut op: F) -> ClientResult<T>
    where
        F: FnMut(Address) -> std::pin::Pin<Box<dyn std::future::Future<Output = ClientResult<T>> + Send>>,
    {
        let addrs = &self.config.mgmtd_server_addresses;
        if addrs.is_empty() {
            return Err(ClientError::NoServerAvailable(
                "no mgmtd server addresses configured".into(),
            ));
        }

        let mut last_err = None;
        for addr in addrs {
            match op(*addr).await {
                Ok(val) => return Ok(val),
                Err(e) => {
                    tracing::debug!("mgmtd {} to {} failed: {}", op_name, addr, e);
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            ClientError::NoServerAvailable(format!("all {} mgmtd addresses failed", addrs.len()))
        }))
    }
}

#[async_trait]
impl MgmtdClient for MgmtdClientImpl {
    async fn start(&self) -> ClientResult<()> {
        let mut running = self.running.lock();
        if *running {
            return Ok(());
        }
        *running = true;
        tracing::info!(
            "MgmtdClient started with {} server addresses",
            self.config.mgmtd_server_addresses.len()
        );
        Ok(())
    }

    async fn stop(&self) -> ClientResult<()> {
        let mut running = self.running.lock();
        if !*running {
            return Ok(());
        }
        *running = false;
        self.stop_notify.notify_waiters();
        tracing::info!("MgmtdClient stopped");
        Ok(())
    }

    fn get_routing_info(&self) -> Arc<RoutingInfo> {
        self.routing.get()
    }

    async fn refresh_routing_info(&self, force: bool) -> ClientResult<()> {
        tracing::debug!("refreshing routing info (force={})", force);
        // In a full implementation this would RPC to mgmtd and parse the
        // response into a new RoutingInfo snapshot. For now, we update the
        // timestamp to indicate an attempt was made.
        let mut info = (*self.routing.get()).clone();
        info.last_refresh = std::time::Instant::now();
        self.routing.update(info);
        Ok(())
    }

    async fn heartbeat(&self, node_id: NodeId, node_type: u8) -> ClientResult<HeartbeatRsp> {
        let _req = HeartbeatReq {
            app: hf3fs_proto::common::AppInfo {
                node_id: *node_id,
                node_type,
                ..Default::default()
            },
            ..Default::default()
        };
        tracing::debug!("sending heartbeat for node {}", *node_id);
        // In a full implementation this would serialise `req`, send it over
        // the wire via RpcClient, and deserialise the response.
        Ok(HeartbeatRsp { config: None })
    }

    async fn get_cluster_info(&self) -> ClientResult<GetClusterInfoRsp> {
        let _req = GetClusterInfoReq::default();
        tracing::debug!("getting cluster info");
        // Placeholder: a real implementation sends the RPC.
        Ok(GetClusterInfoRsp { info: None })
    }

    fn mgmtd_addresses(&self) -> Vec<Address> {
        self.config.mgmtd_server_addresses.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_types::AddressType;

    fn test_config() -> MgmtdClientConfig {
        MgmtdClientConfig {
            mgmtd_server_addresses: vec![
                Address::from_octets(10, 0, 0, 1, 9000, AddressType::TCP),
                Address::from_octets(10, 0, 0, 2, 9000, AddressType::TCP),
            ],
            ..MgmtdClientConfig::default()
        }
    }

    #[tokio::test]
    async fn test_mgmtd_client_start_stop() {
        let client = MgmtdClientImpl::new(test_config());
        client.start().await.unwrap();
        // Starting again is a no-op.
        client.start().await.unwrap();
        client.stop().await.unwrap();
        // Stopping again is a no-op.
        client.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_mgmtd_routing_info() {
        let client = MgmtdClientImpl::new(test_config());
        let ri = client.get_routing_info();
        assert!(ri.nodes.is_empty());
    }

    #[tokio::test]
    async fn test_mgmtd_refresh_routing() {
        let client = MgmtdClientImpl::new(test_config());
        client.start().await.unwrap();
        client.refresh_routing_info(false).await.unwrap();
        client.refresh_routing_info(true).await.unwrap();
    }

    #[tokio::test]
    async fn test_mgmtd_heartbeat() {
        let client = MgmtdClientImpl::new(test_config());
        let rsp = client.heartbeat(NodeId(1), 2).await.unwrap();
        let _ = rsp; // HeartbeatRsp is empty.
    }

    #[tokio::test]
    async fn test_mgmtd_get_cluster_info() {
        let client = MgmtdClientImpl::new(test_config());
        let rsp = client.get_cluster_info().await.unwrap();
        assert!(rsp.info.is_none()); // placeholder
    }

    #[test]
    fn test_mgmtd_addresses() {
        let client = MgmtdClientImpl::new(test_config());
        assert_eq!(client.mgmtd_addresses().len(), 2);
    }

    #[test]
    fn test_routing_handle() {
        let client = MgmtdClientImpl::new(test_config());
        let handle = client.routing_handle();
        let ri = handle.get();
        assert!(ri.nodes.is_empty());
    }
}
