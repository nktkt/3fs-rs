//! Implementation of the mgmtd RPC service.

use std::sync::Arc;

use async_trait::async_trait;
use hf3fs_proto::common::NodeType;
use hf3fs_proto::mgmtd::{GetClusterInfoReq, GetClusterInfoRsp, HeartbeatReq, HeartbeatRsp};
use hf3fs_types::status_code::MgmtdCode;
use hf3fs_types::{Result, Status};
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
    /// The node is identified via `app.node_id` inside the HeartbeatReq.
    /// If the node is already registered the timestamp is refreshed.
    /// If the node is unknown it is auto-registered.
    async fn heartbeat(&self, req: HeartbeatReq) -> Result<HeartbeatRsp> {
        let node_id = req.app.node_id;
        let raw_node_type = req.app.node_type;

        let node_type = NodeType::try_from(raw_node_type).map_err(|_| {
            warn!(node_id, raw_type = raw_node_type, "invalid node type in heartbeat");
            Status::with_message(
                MgmtdCode::HEARTBEAT_FAIL,
                format!("invalid node_type {}", raw_node_type),
            )
        })?;

        if self.state.touch_heartbeat(node_id) {
            debug!(node_id, "heartbeat refreshed");
        } else {
            debug!(node_id, ?node_type, "auto-registering node via heartbeat");
            self.state
                .upsert_node(node_id, node_type, String::new());
        }

        Ok(HeartbeatRsp { config: None })
    }

    /// Return basic cluster metadata.
    async fn get_cluster_info(&self, _req: GetClusterInfoReq) -> Result<GetClusterInfoRsp> {
        debug!(cluster_id = %self.state.cluster_id, "serving cluster info");
        Ok(GetClusterInfoRsp { info: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_proto::common::AppInfo;

    fn make_service(cluster_id: &str) -> MgmtdServiceImpl {
        let state = MgmtdState::new(cluster_id);
        MgmtdServiceImpl::new(Arc::new(state))
    }

    fn heartbeat_req(node_id: u32, node_type: NodeType) -> HeartbeatReq {
        HeartbeatReq {
            app: AppInfo {
                node_id,
                node_type: node_type as u8,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_heartbeat_registers_new_node() {
        let svc = make_service("test-cluster");
        assert_eq!(svc.state().node_count(), 0);

        let rsp = svc.heartbeat(heartbeat_req(1, NodeType::Storage)).await;
        assert!(rsp.is_ok());
        assert_eq!(svc.state().node_count(), 1);

        let info = svc.state().get_node(1).unwrap();
        assert_eq!(info.node_type, NodeType::Storage);
    }

    #[tokio::test]
    async fn test_heartbeat_refreshes_existing_node() {
        let svc = make_service("test-cluster");

        svc.heartbeat(heartbeat_req(1, NodeType::Meta)).await.unwrap();
        let ts_before = svc.state().get_node(1).unwrap().last_heartbeat;

        svc.heartbeat(heartbeat_req(1, NodeType::Meta)).await.unwrap();
        let ts_after = svc.state().get_node(1).unwrap().last_heartbeat;

        assert!(ts_after >= ts_before);
        assert_eq!(svc.state().node_count(), 1);
    }

    #[tokio::test]
    async fn test_heartbeat_invalid_node_type() {
        let svc = make_service("test-cluster");

        let rsp = svc
            .heartbeat(HeartbeatReq {
                app: AppInfo {
                    node_id: 1,
                    node_type: 255, // invalid
                    ..Default::default()
                },
                ..Default::default()
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
            svc.heartbeat(heartbeat_req(id, NodeType::Storage)).await.unwrap();
        }

        assert_eq!(svc.state().node_count(), 5);
    }

    #[tokio::test]
    async fn test_get_cluster_info() {
        let svc = make_service("my-cluster-42");
        let rsp = svc
            .get_cluster_info(GetClusterInfoReq::default())
            .await
            .unwrap();
        // Returns routing info (None in this basic impl)
        assert!(rsp.info.is_none());
    }

    #[tokio::test]
    async fn test_service_state_accessor() {
        let state = Arc::new(MgmtdState::new("c"));
        let svc = MgmtdServiceImpl::new(Arc::clone(&state));
        assert!(Arc::ptr_eq(svc.state(), &state));
    }
}
