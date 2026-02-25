//! RDMA listener for accepting incoming connections.
//!
//! Corresponds to the RDMA server-side connection setup in the C++ `IBSocket`
//! and `IBConnectService` classes.

use async_trait::async_trait;
use hf3fs_net::socket::Listener;
use hf3fs_net::error::NetError;
use hf3fs_types::Address;

use crate::config::RdmaConfig;
use crate::connection::RdmaConnection;

/// An RDMA listener that accepts incoming RDMA connections.
///
/// Implements the `hf3fs_net::Listener` trait. Without the `rdma` feature,
/// `accept()` always returns an error.
pub struct RdmaListener {
    /// Configuration for accepted connections.
    config: RdmaConfig,
    /// The local address this listener is bound to.
    local_addr: Address,
}

impl RdmaListener {
    /// Create a new RDMA listener.
    ///
    /// In a full implementation, this would set up the RDMA connection
    /// management infrastructure (register with the IBConnectService, etc.).
    pub fn new(config: RdmaConfig, local_addr: Address) -> Self {
        tracing::info!(
            addr = %local_addr,
            "Created RDMA listener"
        );
        Self {
            config,
            local_addr,
        }
    }

    /// Return a reference to the listener configuration.
    pub fn config(&self) -> &RdmaConfig {
        &self.config
    }
}

#[async_trait]
impl Listener for RdmaListener {
    type Socket = RdmaConnection;

    async fn accept(&self) -> Result<RdmaConnection, NetError> {
        #[cfg(feature = "rdma")]
        {
            // Would wait for an incoming RDMA connection request via the
            // IBConnectService, create a QP, and transition it to RTS state.
            todo!("RDMA accept not yet implemented");
        }

        #[cfg(not(feature = "rdma"))]
        {
            // Without RDMA support, we cannot accept connections.
            // Return a "not supported" error that callers can handle.
            Err(NetError::ConnectionRefused)
        }
    }

    fn local_addr(&self) -> Address {
        self.local_addr
    }
}

/// Create an `AsyncConnector` for RDMA connections.
///
/// Corresponds to the C++ client-side connection setup where the client
/// queries the remote IBConnectService and creates a QP.
pub struct RdmaConnector {
    #[allow(dead_code)]
    config: RdmaConfig,
    #[allow(dead_code)]
    local_addr: Address,
}

impl RdmaConnector {
    /// Create a new RDMA connector.
    pub fn new(config: RdmaConfig, local_addr: Address) -> Self {
        Self {
            config,
            local_addr,
        }
    }
}

#[async_trait]
impl hf3fs_net::AsyncConnector<RdmaConnection> for RdmaConnector {
    async fn connect(&self, _addr: Address) -> Result<RdmaConnection, NetError> {
        #[cfg(feature = "rdma")]
        {
            // Would perform the RDMA connection handshake:
            // 1. Query remote IBConnectService for device info.
            // 2. Select matching local/remote device and port.
            // 3. Create QP and exchange QP info.
            // 4. Transition QP to RTS.
            let _ = addr;
            todo!("RDMA connect not yet implemented");
        }

        #[cfg(not(feature = "rdma"))]
        {
            // Without RDMA, return a connection in Init state.
            // This allows the type system to work without actual RDMA hardware.
            Err(NetError::ConnectionRefused)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_types::AddressType;

    fn test_addr(port: u16) -> Address {
        Address::from_octets(10, 0, 0, 1, port, AddressType::TCP)
    }

    #[test]
    fn test_listener_creation() {
        let config = RdmaConfig::default();
        let listener = RdmaListener::new(config, test_addr(8000));
        assert_eq!(listener.local_addr(), test_addr(8000));
    }

    #[tokio::test]
    async fn test_listener_accept_no_rdma() {
        let config = RdmaConfig::default();
        let listener = RdmaListener::new(config, test_addr(8000));

        // Without rdma feature, accept should fail.
        let result = listener.accept().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_connector_creation() {
        let config = RdmaConfig::default();
        let _connector = RdmaConnector::new(config, test_addr(8000));
    }

    #[tokio::test]
    async fn test_connector_no_rdma() {
        use hf3fs_net::AsyncConnector;

        let config = RdmaConfig::default();
        let connector = RdmaConnector::new(config, test_addr(8000));

        let result = connector.connect(test_addr(9000)).await;
        assert!(result.is_err());
    }
}
