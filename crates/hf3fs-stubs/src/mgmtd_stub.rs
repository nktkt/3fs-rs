//! Mgmtd (management daemon) service stub trait and mock implementation.

use async_trait::async_trait;
use hf3fs_proto::mgmtd::{
    GetClusterInfoReq, GetClusterInfoRsp, HeartbeatReq, HeartbeatRsp,
};
use hf3fs_types::Result;
use parking_lot::Mutex;
use std::sync::Arc;

/// Client-side stub for calling the management daemon service.
///
/// The mgmtd service is responsible for cluster-wide coordination: node
/// heartbeats, cluster membership, and configuration distribution.
#[async_trait]
pub trait IMgmtdServiceStub: Send + Sync {
    async fn heartbeat(&self, req: HeartbeatReq) -> Result<HeartbeatRsp>;
    async fn get_cluster_info(&self, req: GetClusterInfoReq) -> Result<GetClusterInfoRsp>;
}

// ---------------------------------------------------------------------------
// Mock implementation
// ---------------------------------------------------------------------------

type Handler<Req, Rsp> = Box<dyn Fn(Req) -> Result<Rsp> + Send + Sync>;

/// A configurable mock for [`IMgmtdServiceStub`].
///
/// Each RPC method can be overridden with a closure. If no handler is
/// installed the mock returns a default (success) response.
pub struct MockMgmtdServiceStub {
    pub heartbeat_handler: Mutex<Option<Handler<HeartbeatReq, HeartbeatRsp>>>,
    pub get_cluster_info_handler: Mutex<Option<Handler<GetClusterInfoReq, GetClusterInfoRsp>>>,
}

impl MockMgmtdServiceStub {
    pub fn new() -> Self {
        Self {
            heartbeat_handler: Mutex::new(None),
            get_cluster_info_handler: Mutex::new(None),
        }
    }

    /// Wrap in an `Arc` for convenient sharing.
    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn on_heartbeat(
        &self,
        f: impl Fn(HeartbeatReq) -> Result<HeartbeatRsp> + Send + Sync + 'static,
    ) {
        *self.heartbeat_handler.lock() = Some(Box::new(f));
    }

    pub fn on_get_cluster_info(
        &self,
        f: impl Fn(GetClusterInfoReq) -> Result<GetClusterInfoRsp> + Send + Sync + 'static,
    ) {
        *self.get_cluster_info_handler.lock() = Some(Box::new(f));
    }
}

impl Default for MockMgmtdServiceStub {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IMgmtdServiceStub for MockMgmtdServiceStub {
    async fn heartbeat(&self, req: HeartbeatReq) -> Result<HeartbeatRsp> {
        let guard = self.heartbeat_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(HeartbeatRsp {}),
        }
    }

    async fn get_cluster_info(&self, req: GetClusterInfoReq) -> Result<GetClusterInfoRsp> {
        let guard = self.get_cluster_info_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(GetClusterInfoRsp {
                cluster_id: String::new(),
            }),
        }
    }
}

/// Blanket implementation: `Arc<T>` delegates to `T` for any `T: IMgmtdServiceStub`.
#[async_trait]
impl<T: IMgmtdServiceStub + ?Sized> IMgmtdServiceStub for Arc<T> {
    async fn heartbeat(&self, req: HeartbeatReq) -> Result<HeartbeatRsp> {
        (**self).heartbeat(req).await
    }
    async fn get_cluster_info(&self, req: GetClusterInfoReq) -> Result<GetClusterInfoRsp> {
        (**self).get_cluster_info(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_heartbeat_default() {
        let mock = MockMgmtdServiceStub::new();
        let rsp = mock
            .heartbeat(HeartbeatReq {
                node_id: 1,
                node_type: 0,
            })
            .await;
        assert!(rsp.is_ok());
    }

    #[tokio::test]
    async fn test_mock_get_cluster_info_default() {
        let mock = MockMgmtdServiceStub::new();
        let rsp = mock
            .get_cluster_info(GetClusterInfoReq {})
            .await
            .unwrap();
        assert_eq!(rsp.cluster_id, "");
    }

    #[tokio::test]
    async fn test_mock_get_cluster_info_custom() {
        let mock = MockMgmtdServiceStub::new();
        mock.on_get_cluster_info(|_req| {
            Ok(GetClusterInfoRsp {
                cluster_id: "test-cluster-42".into(),
            })
        });
        let rsp = mock
            .get_cluster_info(GetClusterInfoReq {})
            .await
            .unwrap();
        assert_eq!(rsp.cluster_id, "test-cluster-42");
    }

    #[tokio::test]
    async fn test_mock_via_arc() {
        let mock = MockMgmtdServiceStub::new().into_arc();
        let rsp = mock.heartbeat(HeartbeatReq::default()).await;
        assert!(rsp.is_ok());
    }
}
