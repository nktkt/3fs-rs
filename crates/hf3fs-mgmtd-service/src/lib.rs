//! Management daemon (mgmtd) service crate.
//!
//! The mgmtd service is responsible for cluster membership, routing, and node
//! lifecycle management. Nodes send periodic heartbeats so that mgmtd can
//! track liveness, and clients query cluster metadata through the
//! `get_cluster_info` RPC.

pub mod service;
pub mod state;

use async_trait::async_trait;
use hf3fs_proto::mgmtd::{GetClusterInfoReq, GetClusterInfoRsp, HeartbeatReq, HeartbeatRsp};
use hf3fs_service_derive::{hf3fs_service, method};
use hf3fs_types::Result;

/// The mgmtd RPC service trait.
///
/// Service id 4 (`Mgmtd`) exposes two methods:
///   1. `heartbeat` -- nodes report liveness
///   2. `get_cluster_info` -- query cluster metadata
#[hf3fs_service(id = 4, name = "Mgmtd")]
#[async_trait]
pub trait IMgmtdService: Send + Sync {
    /// Process a heartbeat from a cluster node.
    #[method(id = 1)]
    async fn heartbeat(&self, req: HeartbeatReq) -> Result<HeartbeatRsp>;

    /// Return cluster metadata.
    #[method(id = 2)]
    async fn get_cluster_info(&self, req: GetClusterInfoReq) -> Result<GetClusterInfoRsp>;
}

// Re-export key types for ergonomic downstream use.
pub use service::MgmtdServiceImpl;
pub use state::{MgmtdState, NodeInfo};

#[cfg(test)]
mod tests {
    use super::i_mgmtd_service_service_meta::*;

    #[test]
    fn test_service_meta_constants() {
        assert_eq!(SERVICE_ID, 4);
        assert_eq!(SERVICE_NAME, "Mgmtd");
    }

    #[test]
    fn test_method_id_heartbeat() {
        let id = MethodId::from_u16(1);
        assert_eq!(id, Some(MethodId::Heartbeat));
        assert_eq!(MethodId::Heartbeat.as_u16(), 1);
    }

    #[test]
    fn test_method_id_get_cluster_info() {
        let id = MethodId::from_u16(2);
        assert_eq!(id, Some(MethodId::GetClusterInfo));
        assert_eq!(MethodId::GetClusterInfo.as_u16(), 2);
    }

    #[test]
    fn test_method_id_unknown() {
        assert_eq!(MethodId::from_u16(99), None);
    }

    #[test]
    fn test_method_name_lookup() {
        assert_eq!(method_name(1), Some("heartbeat"));
        assert_eq!(method_name(2), Some("get_cluster_info"));
        assert_eq!(method_name(42), None);
    }
}
