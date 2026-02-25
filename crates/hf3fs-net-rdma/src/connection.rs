//! RDMA connection implementing the hf3fs-net `Socket` trait.
//!
//! Corresponds to the C++ `IBSocket` from `src/common/net/ib/IBSocket.h`.

use async_trait::async_trait;
use bytes::Bytes;
use hf3fs_net::error::NetError;
use hf3fs_net::Socket;
use hf3fs_types::Address;

use crate::config::RdmaConfig;

/// State of an RDMA connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Initial state, not connected.
    Init,
    /// Connection in progress.
    Connecting,
    /// Connection accepted (server side), waiting for QP transition.
    Accepted,
    /// Connection is ready for data transfer.
    Ready,
    /// Connection is being closed.
    Closing,
    /// Connection encountered an error.
    Error,
}

/// An RDMA-based network connection.
///
/// Implements the `hf3fs_net::Socket` trait for RDMA transport. Without the
/// `rdma` feature, all operations return errors indicating RDMA is not
/// available.
///
/// Corresponds to the C++ `IBSocket`.
pub struct RdmaConnection {
    /// Connection configuration.
    config: RdmaConfig,
    /// Local address.
    local_addr: Address,
    /// Remote peer address.
    peer_addr: Address,
    /// Current connection state.
    state: ConnectionState,
}

impl RdmaConnection {
    /// Create a new RDMA connection (not yet connected).
    pub fn new(config: RdmaConfig, local_addr: Address, peer_addr: Address) -> Self {
        Self {
            config,
            local_addr,
            peer_addr,
            state: ConnectionState::Init,
        }
    }

    /// Return the current connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Return a reference to the connection configuration.
    pub fn config(&self) -> &RdmaConfig {
        &self.config
    }

    /// Return a description of this connection.
    pub fn describe(&self) -> String {
        format!(
            "RdmaConnection({} -> {}, state={:?})",
            self.local_addr, self.peer_addr, self.state
        )
    }
}

#[async_trait]
impl Socket for RdmaConnection {
    async fn send(&self, _data: Bytes) -> Result<(), NetError> {
        match self.state {
            ConnectionState::Ready => {
                // Without the rdma feature, RDMA send is not available.
                #[cfg(feature = "rdma")]
                {
                    // Would post send WR to the QP.
                    todo!("RDMA send not yet implemented");
                }
                #[cfg(not(feature = "rdma"))]
                {
                    Err(NetError::SendFailed(
                        "RDMA transport not available (rdma feature not enabled)".to_string(),
                    ))
                }
            }
            ConnectionState::Closing | ConnectionState::Error => {
                Err(NetError::ConnectionClosed)
            }
            _ => Err(NetError::SendFailed(format!(
                "connection not ready (state={:?})",
                self.state
            ))),
        }
    }

    async fn recv(&self) -> Result<Bytes, NetError> {
        match self.state {
            ConnectionState::Ready => {
                #[cfg(feature = "rdma")]
                {
                    // Would poll the CQ for receive completions.
                    todo!("RDMA recv not yet implemented");
                }
                #[cfg(not(feature = "rdma"))]
                {
                    Err(NetError::ConnectionClosed)
                }
            }
            _ => Err(NetError::ConnectionClosed),
        }
    }

    fn peer_addr(&self) -> Address {
        self.peer_addr
    }

    fn local_addr(&self) -> Address {
        self.local_addr
    }

    async fn close(&self) {
        // Without rdma feature, nothing to clean up.
        tracing::debug!(
            peer = %self.peer_addr,
            "Closing RDMA connection"
        );
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
    fn test_connection_creation() {
        let config = RdmaConfig::default();
        let conn = RdmaConnection::new(config, test_addr(8000), test_addr(9000));

        assert_eq!(conn.state(), ConnectionState::Init);
        assert!(conn.describe().contains("Init"));
    }

    #[tokio::test]
    async fn test_send_not_ready() {
        let config = RdmaConfig::default();
        let conn = RdmaConnection::new(config, test_addr(8000), test_addr(9000));

        let result = conn.send(Bytes::from("hello")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_recv_not_ready() {
        let config = RdmaConfig::default();
        let conn = RdmaConnection::new(config, test_addr(8000), test_addr(9000));

        let result = conn.recv().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_peer_addr() {
        let config = RdmaConfig::default();
        let local = test_addr(8000);
        let peer = test_addr(9000);
        let conn = RdmaConnection::new(config, local, peer);

        assert_eq!(conn.peer_addr(), peer);
        assert_eq!(conn.local_addr(), local);
    }
}
