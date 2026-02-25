//! Top-level client composing meta, storage, and mgmtd sub-clients.
//!
//! Mirrors the overall C++ client architecture where a single `Client`
//! object owns and coordinates all sub-clients.

use std::sync::Arc;

use tracing;

use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult};
use crate::meta::{MetaClient, MetaClientImpl};
use crate::mgmtd::{MgmtdClient, MgmtdClientImpl};
use crate::routing::RoutingInfoHandle;
use crate::storage::{StorageClient, StorageClientImpl};

/// The top-level 3FS client.
///
/// Owns shared routing state and provides access to the meta, storage,
/// and mgmtd sub-clients. Typical usage:
///
/// ```ignore
/// let client = Client::new(config);
/// client.start().await?;
///
/// let meta = client.meta();
/// let inode = meta.stat(&user, root, Some("/myfile"), true).await?;
///
/// client.stop().await?;
/// ```
pub struct Client {
    config: ClientConfig,
    routing: RoutingInfoHandle,
    mgmtd: Arc<MgmtdClientImpl>,
    meta: Arc<MetaClientImpl>,
    storage: Arc<StorageClientImpl>,
    started: parking_lot::Mutex<bool>,
}

impl Client {
    /// Create a new client from configuration.
    pub fn new(config: ClientConfig) -> Self {
        let mgmtd = Arc::new(MgmtdClientImpl::new(config.mgmtd.clone()));
        let routing = mgmtd.routing_handle();
        let meta = Arc::new(MetaClientImpl::new(config.meta.clone(), routing.clone()));
        let storage = Arc::new(StorageClientImpl::new(
            config.storage.clone(),
            routing.clone(),
        ));

        Self {
            config,
            routing,
            mgmtd,
            meta,
            storage,
            started: parking_lot::Mutex::new(false),
        }
    }

    /// Start all sub-clients.
    ///
    /// This begins background routing info refresh, heartbeat loops, and
    /// server health checks. Must be called before issuing any RPCs.
    pub async fn start(&self) -> ClientResult<()> {
        let mut started = self.started.lock();
        if *started {
            return Ok(());
        }

        tracing::info!(cluster = %self.config.cluster_id, "starting 3FS client");

        self.mgmtd.start().await?;
        self.meta.start().await?;
        self.storage.start().await?;

        *started = true;
        tracing::info!("3FS client started");
        Ok(())
    }

    /// Stop all sub-clients.
    pub async fn stop(&self) -> ClientResult<()> {
        let mut started = self.started.lock();
        if !*started {
            return Ok(());
        }

        tracing::info!("stopping 3FS client");

        self.storage.stop().await?;
        self.meta.stop().await?;
        self.mgmtd.stop().await?;

        *started = false;
        tracing::info!("3FS client stopped");
        Ok(())
    }

    /// Return whether the client has been started.
    pub fn is_started(&self) -> bool {
        *self.started.lock()
    }

    /// Get a reference to the meta client.
    pub fn meta(&self) -> &dyn MetaClient {
        self.meta.as_ref()
    }

    /// Get a reference to the storage client.
    pub fn storage(&self) -> &dyn StorageClient {
        self.storage.as_ref()
    }

    /// Get a reference to the mgmtd client.
    pub fn mgmtd(&self) -> &dyn MgmtdClient {
        self.mgmtd.as_ref()
    }

    /// Get the shared routing info handle.
    pub fn routing(&self) -> &RoutingInfoHandle {
        &self.routing
    }

    /// Get the client configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Get an `Arc` to the meta client (for sharing across tasks).
    pub fn meta_arc(&self) -> Arc<MetaClientImpl> {
        Arc::clone(&self.meta)
    }

    /// Get an `Arc` to the storage client (for sharing across tasks).
    pub fn storage_arc(&self) -> Arc<StorageClientImpl> {
        Arc::clone(&self.storage)
    }

    /// Get an `Arc` to the mgmtd client (for sharing across tasks).
    pub fn mgmtd_arc(&self) -> Arc<MgmtdClientImpl> {
        Arc::clone(&self.mgmtd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfig;

    #[tokio::test]
    async fn test_client_lifecycle() {
        let config = ClientConfig {
            cluster_id: "test-cluster".into(),
            ..ClientConfig::default()
        };
        let client = Client::new(config);
        assert!(!client.is_started());

        client.start().await.unwrap();
        assert!(client.is_started());

        // Starting again is idempotent.
        client.start().await.unwrap();
        assert!(client.is_started());

        client.stop().await.unwrap();
        assert!(!client.is_started());

        // Stopping again is idempotent.
        client.stop().await.unwrap();
        assert!(!client.is_started());
    }

    #[test]
    fn test_client_config_access() {
        let config = ClientConfig {
            cluster_id: "my-cluster".into(),
            ..ClientConfig::default()
        };
        let client = Client::new(config);
        assert_eq!(client.config().cluster_id, "my-cluster");
    }

    #[test]
    fn test_client_sub_client_access() {
        let client = Client::new(ClientConfig::default());
        // These should not panic -- just verify the accessors compile.
        let _meta = client.meta();
        let _storage = client.storage();
        let _mgmtd = client.mgmtd();
        let _routing = client.routing();
    }

    #[test]
    fn test_client_arc_access() {
        let client = Client::new(ClientConfig::default());
        let meta_arc = client.meta_arc();
        let storage_arc = client.storage_arc();
        let mgmtd_arc = client.mgmtd_arc();
        // Arc strong count should be 2 (one in Client, one here).
        assert_eq!(Arc::strong_count(&meta_arc), 2);
        assert_eq!(Arc::strong_count(&storage_arc), 2);
        assert_eq!(Arc::strong_count(&mgmtd_arc), 2);
    }

    #[tokio::test]
    async fn test_client_routing_handle() {
        let client = Client::new(ClientConfig::default());
        client.start().await.unwrap();
        let ri = client.routing().get();
        // Empty by default since no mgmtd is connected.
        assert!(ri.nodes.is_empty());
        client.stop().await.unwrap();
    }
}
