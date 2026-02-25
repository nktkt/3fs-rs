//! Implementation of the mgmtd RPC service.

use std::sync::Arc;

use async_trait::async_trait;
use hf3fs_proto::mgmtd::{
    GetClusterInfoReq, GetClusterInfoRsp, HeartbeatReq, HeartbeatRsp, NodeType,
};
use hf3fs_types::{Result, Status};
use hf3fs_types::status_code::MgmtdCode;
use tracing::{debug, warn};

use crate::state::MgmtdState;
use crate::IMgmtdService;

/// Concrete implementation of [`IMgmtdService`].
///
/// Holds a shared reference to the [`MgmtdState`] and services heartbeat
/// and cluster-info queries.
pub struct MgmtdServiceImpl {
    state: Arc<MgmtdState>,
}

impl MgmtdServiceImpl {
    /// Create a new service backed by the given shared state.
    pub fn new(state: Arc<MgmtdState>) -> Self {
        Self { state }
    }

    /// Access the underlying cluster state.
    pub fn state(&self) -> &Arc<MgmtdState> {
        &self.state
    }
}

#[async_trait]
impl IMgmtdService for MgmtdServiceImpl {
    /// Process a heartbeat from a cluster node.
    ///
    /// If the node is already registered the timestamp is refreshed.
    /// If the node is unknown it is auto-registered with an empty address
    /// (a production system would require an explicit registration step,
    /// but this keeps the initial implementation simple and testable).
    async fn heartbeat(&self, req: HeartbeatReq) -> Result<HeartbeatRsp> {
        let node_type = NodeType::try_from(req.node_type).map_err(|_| {
            warn!(node_id = req.node_id, raw_type = req.node_type, "invalid node type in heartbeat");
            Status::with_message(
                MgmtdCode::HEARTBEAT_FAIL,
                format!("invalid node_type {}", req.node_type),
            )
        })?;

        if self.state.touch_heartbeat(req.node_id) {
            debug!(node_id = req.node_id, "heartbeat refreshed");
        } else {
            // Auto-register unknown nodes so heartbeat is self-contained.
            debug!(node_id = req.node_id, ?node_type, "auto-registering node via heartbeat");
            self.state.upsert_node(req.node_id, node_type, String::new());
        }

        Ok(HeartbeatRsp {})
    }

    /// Return basic cluster metadata.
    async fn get_cluster_info(&self, _req: GetClusterInfoReq) -> Result<GetClusterInfoRsp> {
        debug!(cluster_id = %self.state.cluster_id, "serving cluster info");
        Ok(GetClusterInfoRsp {
            cluster_id: self.state.cluster_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_service(cluster_id: &str) -> MgmtdServiceImpl {
        let state = MgmtdState::new(cluster_id);
        MgmtdServiceImpl::new(Arc::new(state))
    }

    #[tokio::test]
    async fn test_heartbeat_registers_new_node() {
        let svc = make_service("test-cluster");
        assert_eq!(svc.state().node_count(), 0);

        let rsp = svc
            .heartbeat(HeartbeatReq {
                node_id: 1,
                node_type: NodeType::Storage as u8,
            })
            .await;
        assert!(rsp.is_ok());
        assert_eq!(svc.state().node_count(), 1);

        let info = svc.state().get_node(1).unwrap();
        assert_eq!(info.node_type, NodeType::Storage);
    }

    #[tokio::test]
    async fn test_heartbeat_refreshes_existing_node() {
        let svc = make_service("test-cluster");

        // First heartbeat auto-registers.
        svc.heartbeat(HeartbeatReq {
            node_id: 1,
            node_type: NodeType::Meta as u8,
        })
        .await
        .unwrap();

        let ts_before = svc.state().get_node(1).unwrap().last_heartbeat;

        // Second heartbeat should refresh.
        svc.heartbeat(HeartbeatReq {
            node_id: 1,
            node_type: NodeType::Meta as u8,
        })
        .await
        .unwrap();

        let ts_after = svc.state().get_node(1).unwrap().last_heartbeat;
        assert!(ts_after >= ts_before);
        // Should still be a single node.
        assert_eq!(svc.state().node_count(), 1);
    }

    #[tokio::test]
    async fn test_heartbeat_invalid_node_type() {
        let svc = make_service("test-cluster");

        let rsp = svc
            .heartbeat(HeartbeatReq {
                node_id: 1,
                node_type: 255, // invalid
            })
            .await;

        assert!(rsp.is_err());
        let status = rsp.unwrap_err();
        assert_eq!(status.code(), MgmtdCode::HEARTBEAT_FAIL);
        assert_eq!(svc.state().node_count(), 0);
    }

    #[tokio::test]
    async fn test_heartbeat_multiple_nodes() {
        let svc = make_service("c1");

        for id in 1..=5 {
            svc.heartbeat(HeartbeatReq {
                node_id: id,
                node_type: NodeType::Storage as u8,
            })
            .await
            .unwrap();
        }

        assert_eq!(svc.state().node_count(), 5);
    }

    #[tokio::test]
    async fn test_get_cluster_info() {
        let svc = make_service("my-cluster-42");

        let rsp = svc
            .get_cluster_info(GetClusterInfoReq {})
            .await
            .unwrap();

        assert_eq!(rsp.cluster_id, "my-cluster-42");
    }

    #[tokio::test]
    async fn test_get_cluster_info_empty_id() {
        let svc = make_service("");

        let rsp = svc
            .get_cluster_info(GetClusterInfoReq {})
            .await
            .unwrap();

        assert_eq!(rsp.cluster_id, "");
    }

    #[tokio::test]
    async fn test_service_state_accessor() {
        let state = Arc::new(MgmtdState::new("c"));
        let svc = MgmtdServiceImpl::new(Arc::clone(&state));
        assert!(Arc::ptr_eq(svc.state(), &state));
    }
}
